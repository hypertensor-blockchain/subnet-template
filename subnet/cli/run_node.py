"""CLI command to run a libp2p subnet server node."""

import argparse
import logging
import sys
import os
from pathlib import Path

import trio

from subnet.server.server_v2 import Server
import random
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom
from libp2p.crypto.keys import KeyPair
from libp2p.peer.pb import crypto_pb2
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from libp2p.peer.id import ID as PeerID
from substrateinterface import Keypair as SubstrateKeypair, KeypairType
from dotenv import load_dotenv

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

  # Run a node with an identity file
  python -m subnet.cli.run_node --identity_path alith-ed25519.key --port 38960 --bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF --subnet_id 1 --subnet_node_id 1 --no_blockchain_rpc
  python -m subnet.cli.run_node --identity_path baltathar-ed25519.key --port 38961 --bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF --subnet_id 1 --subnet_node_id 2 --no_blockchain_rpc
  python -m subnet.cli.run_node --identity_path charleth-ed25519.key --port 38962 --bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF --subnet_id 1 --subnet_node_id 3 --no_blockchain_rpc
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
        "--identity_path", type=str, default=None, help="Path to the identity file. "
    )

    parser.add_argument(
        "--subnet_id", type=int, default=1, help="Subnet ID this node belongs to. "
    )

    parser.add_argument(
        "--subnet_node_id",
        type=int,
        default=1,
        help="Subnet node ID this node belongs to. ",
    )

    parser.add_argument(
        "--no_blockchain_rpc", action="store_true", help="[Testing] Run with no RPC"
    )

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

    with open(f"{args.identity_path}", "rb") as f:
        data = f.read()
    private_key = crypto_pb2.PrivateKey.FromString(data)
    ed25519_private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
    public_key = ed25519_private_key.get_public_key()
    key_pair = KeyPair(ed25519_private_key, public_key)

    hotkey = None
    start_epoch = None
    if not args.no_blockchain_rpc:
        if args.local_rpc:
            rpc = os.getenv("LOCAL_RPC")
        else:
            rpc = os.getenv("DEV_RPC")

        if args.phrase is not None:
            hypertensor = Hypertensor(rpc, args.phrase)
            substrate_keypair = SubstrateKeypair.create_from_mnemonic(
                args.phrase, crypto_type=KeypairType.ECDSA
            )
            hotkey = substrate_keypair.ss58_address
            logger.info(f"hotkey: {hotkey}")
        elif args.tensor_private_key is not None:
            hypertensor = Hypertensor(
                rpc, args.tensor_private_key, KeypairFrom.PRIVATE_KEY
            )
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
        subnet_node_info = hypertensor.get_formatted_get_subnet_node_info(
            args.subnet_id, args.subnet_node_id
        )
        if subnet_node_info is None:
            raise Exception("Subnet node does not exist")

        start_epoch = subnet_node_info.classification["start_epoch"]
        if start_epoch is None:
            raise Exception("Subnet node start epoch is None")
    else:
        # Run mock hypertensor blockchain for testing
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

    try:
        server = Server(
            port=port,
            bootstrap_addrs=args.bootstrap,
            key_pair=key_pair,
            subnet_id=args.subnet_id,
            subnet_node_id=args.subnet_node_id,
            hypertensor=hypertensor,
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
