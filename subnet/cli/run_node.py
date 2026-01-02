"""CLI command to run a libp2p subnet server node."""

import argparse
import logging
import os
from pathlib import Path
import random
import secrets
import sys
import time

from dotenv import load_dotenv
from substrateinterface import (
    Keypair as SubstrateKeypair,
    KeypairType,
)
import trio

from libp2p.crypto.ed25519 import (
    Ed25519PrivateKey,
    create_new_key_pair,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.secp256k1 import Secp256k1PrivateKey
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2
from subnet.db.database import RocksDB
from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.server.server import Server

load_dotenv(os.path.join(Path.cwd(), ".env"))

PHRASE = os.getenv("PHRASE")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run a libp2p subnet node",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a standalone node
  python -m subnet.cli.run_node

  # Run a node connecting to bootstrap peers
  python -m subnet.cli.run_node --bootstrap /ip4/127.0.0.1/tcp/31330/p2p/QmBootstrapPeerID

  # Connect to multiple bootstrap peers
  python -m subnet.cli.run_node \\
    --bootstrap /ip4/192.168.1.100/tcp/31330/p2p/QmPeer1 \\
    --bootstrap /ip4/192.168.1.101/tcp/31330/p2p/QmPeer2

# Start bootnode (or start bootnode through `run_bootnode`)

python -m subnet.cli.run_node \
--private_key_path alith-ed25519.key \
--port 38960 \
--subnet_id 1 \
--subnet_node_id 1 \
--no_blockchain_rpc

# Connect to bootnode (if we didn't use alith as the bootnode)

python -m subnet.cli.run_node \
--private_key_path alith-ed25519.key \
--port 38961 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 1 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path baltathar-ed25519.key \
--port 38962 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 2 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path charleth-ed25519.key \
--port 38963 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 3 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path dorothy.key \
--port 38964 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 4 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path faith.key \
--port 38965 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 5 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path george.key \
--port 38966 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 6 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path harry.key \
--port 38967 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 7 \
--no_blockchain_rpc

python -m subnet.cli.run_node \
--private_key_path ian.key \
--port 38968 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf \
--subnet_id 1 \
--subnet_node_id 8 \
--no_blockchain_rpc

        """,
    )

    parser.add_argument(
        "--mode",
        default="server",
        help="Run as a server or client node",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port to listen on (0 for random)",
    )
    parser.add_argument(
        "--bootstrap",
        type=str,
        nargs="*",
        help=(
            "Multiaddrs of bootstrap nodes. "
            "Provide a space-separated list of addresses. "
            "This is required for client mode."
        ),
    )

    parser.add_argument(
        "--is_bootstrap",
        action="store_true",
        help="Start a bootnode that doesn't publish messages or run consensus. ",
    )

    parser.add_argument("--base_path", type=str, default=None, help="Specify custom base path")

    parser.add_argument(
        "--private_key_path",
        type=str,
        default=None,
        help="Path to the private key file. ",
    )

    parser.add_argument("--subnet_id", type=int, default=1, help="Subnet ID this node belongs to. ")

    parser.add_argument(
        "--subnet_node_id",
        type=int,
        default=1,
        help="Subnet node ID this node belongs to. ",
    )

    parser.add_argument("--no_blockchain_rpc", action="store_true", help="[Testing] Run with no RPC")

    parser.add_argument(
        "--local_rpc",
        action="store_true",
        help="[Testing] Run in local RPC mode, uses LOCAL_RPC",
    )

    parser.add_argument(
        "--tensor_private_key",
        type=str,
        required=False,
        help="[Testing] Hypertensor blockchain private key",
    )

    parser.add_argument(
        "--phrase",
        type=str,
        required=False,
        help="[Testing] Coldkey phrase that controls actions which include funds, such as registering, and staking",
    )

    # add option to use verbose logging
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point for the CLI."""
    args = parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # Log startup information
    logger.info("Starting libp2p subnet server node...")

    port = args.port
    if port <= 0:
        port = random.randint(10000, 60000)
    logger.debug(f"Using port: {port}")

    if args.bootstrap:
        logger.info(f"Bootstrap peers: {args.bootstrap}")
    else:
        logger.info("Running as standalone node (no bootstrap peers)")

    if args.private_key_path is None:
        key_pair = create_new_key_pair(secrets.token_bytes(32))
    else:
        with open(f"{args.private_key_path}", "rb") as f:
            data = f.read()
        private_key = crypto_pb2.PrivateKey.FromString(data)
        if private_key.Type == crypto_pb2.KeyType.Ed25519:
            private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
        elif private_key.Type == crypto_pb2.KeyType.Secp256k1:
            private_key = Secp256k1PrivateKey.from_bytes(private_key.Data)
        else:
            raise ValueError("Unsupported key type")

    public_key = private_key.get_public_key()
    key_pair = KeyPair(private_key, public_key)

    if not args.base_path:
        base_path = f"/tmp/{random.randint(100, 1000000)}"
    else:
        base_path = args.base_path

    db = RocksDB(base_path, args.subnet_id)

    hotkey = None
    start_epoch = None
    if not args.no_blockchain_rpc:
        if args.local_rpc:
            rpc = os.getenv("LOCAL_RPC")
        else:
            rpc = os.getenv("DEV_RPC")

        if args.phrase is not None:
            hypertensor = Hypertensor(rpc, args.phrase)
            substrate_keypair = SubstrateKeypair.create_from_mnemonic(args.phrase, crypto_type=KeypairType.ECDSA)
            hotkey = substrate_keypair.ss58_address
            logger.info(f"hotkey: {hotkey}")
        elif args.tensor_private_key is not None:
            hypertensor = Hypertensor(rpc, args.tensor_private_key, KeypairFrom.PRIVATE_KEY)
            substrate_keypair = SubstrateKeypair.create_from_private_key(
                args.tensor_private_key, crypto_type=KeypairType.ECDSA
            )
            hotkey = substrate_keypair.ss58_address
            logger.info(f"hotkey: {hotkey}")
        else:
            # Default to using PHRASE if no other options are provided
            hypertensor = Hypertensor(rpc, PHRASE)

        if hotkey is not None:
            result = hypertensor.interface.query("System", "Account", [hotkey])
            balance = result.value["data"]["free"]
            assert balance >= 500, (
                f"Hotkey must have at least 0.0000000000000005 TENSOR to be a live account, balance is {float(balance / 1e18)}"
            )

        # Check subnet node exists
        subnet_node_info = hypertensor.get_formatted_get_subnet_node_info(args.subnet_id, args.subnet_node_id)
        if subnet_node_info is None:
            raise Exception("Subnet node does not exist")

        start_epoch = subnet_node_info.classification["start_epoch"]
        if start_epoch is None:
            raise Exception("Subnet node start epoch is None")
    else:
        # Run mock hypertensor blockchain for testing
        # This is a shared database between all local nodes
        hypertensor = LocalMockHypertensor(
            subnet_id=args.subnet_id,
            peer_id=PeerID.from_pubkey(key_pair.public_key),
            subnet_node_id=args.subnet_node_id,
            coldkey="",
            hotkey="",
            bootnode_peer_id="",
            client_peer_id="",
            reset_db=True if not args.bootstrap else False,
        )
        # Give time for database to be written
        # time.sleep(5.0)

    try:
        server = Server(
            port=port,
            bootstrap_addrs=args.bootstrap,
            key_pair=key_pair,
            db=db,
            subnet_id=args.subnet_id,
            subnet_node_id=args.subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=args.is_bootstrap,
        )
        trio.run(server.run)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            # Delete subnet node from mock db if it was created
            hypertensor.db.delete_subnet_node(args.subnet_id, args.subnet_node_id)
            logger.info("Removed subnet node from mock db")
        except Exception as e:
            logger.error(f"Failed to delete subnet node from mock db: {e}")


if __name__ == "__main__":
    main()
