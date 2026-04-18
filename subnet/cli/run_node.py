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
from libp2p.crypto.ed25519 import (
    create_new_key_pair,
)
from libp2p.peer.id import ID as PeerID
from substrateinterface import (
    Keypair as SubstrateKeypair,
    KeypairType,
)
import trio

from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.server.server import Server
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.crypto.store_key import get_key_pair
from subnet.utils.db.database import RocksDB

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
# Run locally with no RPC connection

# Start bootnode (or start bootnode through `run_bootnode`)

# 12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF

python -m subnet.cli.run_node \
--private_key_path bootnode.key \
--port 38960 \
--subnet_id 1 \
--no_blockchain_rpc \
--is_bootstrap \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest


# Connect to bootnode

# 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cb

python -m subnet.cli.run_node \
--private_key_path alith.key \
--port 38961 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 1 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

# 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsV

python -m subnet.cli.run_node \
--private_key_path baltathar.key \
--ip 127.0.0.1 \
--port 38962 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 2 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

# 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEt

python -m subnet.cli.run_node \
--private_key_path charleth.key \
--port 38963 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 3 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

# 12D3KooWD1BgwEJGUXz3DsKVXGFq3VcmHRjeX56NKpyEa1QAP6uV

python -m subnet.cli.run_node \
--private_key_path dorothy.key \
--port 38964 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 4 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

# 12D3KooWMGKEpzz3EWGU2ayhwFriRh23QnQ479Ctfj8xSmDRirde

python -m subnet.cli.run_node \
--private_key_path ethan.key \
--port 38965 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 5 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

# 12D3KooWF963f4jiFX26xDKu7BrqtVYTx4Jk8rUQQUxwiJQjVFWH

python -m subnet.cli.run_node \
--private_key_path faith.key \
--port 38966 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 6 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

python -m subnet.cli.run_node \
--private_key_path george.key \
--port 38967 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 7 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

python -m subnet.cli.run_node \
--private_key_path harry.key \
--port 38968 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 8 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest

python -m subnet.cli.run_node \
--private_key_path ian.key \
--port 38969 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 9 \
--no_blockchain_rpc \
--heartbeat_validator_log_level 20 \
--gossip_receiver_log_level 20 \
--publish_heartbeat_log_level 20 \
--maintain_connections_log_level 20 \
--telemetry_url ws://127.0.0.1:8080/ingest


# Run locally with local RPC connection

# Start bootnode (or start bootnode through `run_bootnode`)

- Register subnet
- Register subnet nodes

# Start nodes

python -m subnet.cli.run_node \
--private_key_path bootnode.key \
--port 38960 \
--subnet_id 1 \
--is_bootstrap \
--local_rpc

# Connect to bootnode

python -m subnet.cli.run_node \
--private_key_path alith.key \
--port 38961 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 128001 \
--subnet_node_id 1 \
--tensor_private_key 0x883189525adc71f940606d02671bd8b7dfe3b2f75e2a6ed1f5179ac794566b40 \
--local_rpc

python -m subnet.cli.run_node \
--private_key_path baltathar.key \
--port 38962 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 2 \
--tensor_private_key 0x6cbf451fc5850e75cd78055363725dcf8c80b3f1dfb9c29d131fece6dfb72490 \
--local_rpc

python -m subnet.cli.run_node \
--private_key_path charleth.key \
--port 38963 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 3 \
--tensor_private_key 0x51b7c50c1cd27de89a361210431e8f03a7ddda1a0c8c5ff4e4658ca81ac02720 \
--local_rpc

        """,
    )

    parser.add_argument(
        "--mode",
        default="server",
        help="Run as a server or client node",
    )
    parser.add_argument(
        "--ip",
        type=str,
        default=None,
        help=(
            "IP to listen on. "
            "For local testing this is not required. "
            "For live testing, this should default to local private IP if behind NAT. "
            "For live testing, this should default to public IP if not behind NAT. "
        ),
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
        help="Start a bootnode/bootstrap node that doesn't publish messages or run consensus. ",
    )

    parser.add_argument("--base_path", type=str, default=None, help="Specify custom base path")

    parser.add_argument(
        "--peerstore_db_path",
        type=str,
        default=None,
        help="[Currently not in use] Specify persistent peerstore db path",
    )

    parser.add_argument(
        "--disable_pubsub_validator",
        action="store_true",
        help="Disable pubsub validator",
    )

    parser.add_argument(
        "--disable_consensus",
        action="store_true",
        help="Disable consensus",
    )

    parser.add_argument(
        "--disable_proof_of_stake",
        action="store_true",
        help="Disable proof of stake",
    )

    parser.add_argument(
        "--disable_strict_maintain_connections",
        action="store_true",
        help="Disable strictly maintain connections",
    )

    # Host specific arguments
    parser.add_argument(
        "--enable_mDNS",
        action="store_true",
        help="Enable mDNS discovery",
    )
    parser.add_argument(
        "--enable_upnp",
        action="store_true",
        help="Enable UPnP discovery",
    )
    parser.add_argument(
        "--enable_autotls",
        action="store_true",
        help="Enable AutoTLS",
    )
    parser.add_argument(
        "--psk",
        type=str,
        default=None,
        help="Pre-shared key for libp2p",
    )

    parser.add_argument(
        "--heartbeat_validator_log_level",
        type=int,
        default=logging.DEBUG,
        help="Log level for heartbeat validator. 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL",
    )
    parser.add_argument(
        "--gossip_receiver_log_level",
        type=int,
        default=logging.DEBUG,
        help="Log level for gossip receiver. 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL",
    )
    parser.add_argument(
        "--publish_heartbeat_log_level",
        type=int,
        default=logging.DEBUG,
        help="Log level for publish heartbeat. 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL",
    )
    parser.add_argument(
        "--maintain_connections_log_level",
        type=int,
        default=logging.DEBUG,
        help="Log level for maintain connections. 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL",
    )

    parser.add_argument(
        "--private_key_path",
        type=str,
        default=None,
        help="Path to the private key file for peer ID. ",
    )

    parser.add_argument("--subnet_id", type=int, default=0, help="Subnet ID this node belongs to. ")

    parser.add_argument(
        "--subnet_node_id",
        type=int,
        default=0,
        help="Subnet node ID this node belongs to. This ID was logged on registration.",
    )

    parser.add_argument("--no_blockchain_rpc", action="store_true", help="[Testing] Run with no RPC")

    # Seed data for local db when using `--no_blockchain_rpc`
    parser.add_argument(
        "--insert_mock_overwatch_node", action="store_true", help="[Testing] Insert mock overwatch node"
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
        help="Hypertensor blockchain private key, the pk of the hotkey used for consensus extinsics",
    )

    parser.add_argument(
        "--phrase",
        type=str,
        required=False,
        help="Coldkey phrase that controls actions which include funds, such as registering, and staking",
    )

    parser.add_argument(
        "--telemetry_url",
        type=str,
        required=False,
        help="Telemetry URL for Prometheus",
    )

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
        key_pair = get_key_pair(args.private_key_path)

    if not args.base_path:
        if args.is_bootstrap:
            base_path = "/tmp/bootstrap"
        else:
            base_path = f"/tmp/{random.randint(100, 1000000)}"
    else:
        base_path = args.base_path

    db = RocksDB(base_path)

    telemetry = None
    if args.telemetry_url:
        logger.info(f"Telemetry events starting at URL: {args.telemetry_url}")
        telemetry = Telemetry(args.telemetry_url, args.subnet_id, args.subnet_node_id, key_pair)

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

        if args.subnet_id < 128000:
            real_subnet_id = hypertensor.get_subnet_id_from_friendly_id(args.subnet_id)
            logger.info(
                "Subnet ID %s is less than 128000 and likely a friendly ID, using real subnet ID %s",
                args.subnet_id,
                real_subnet_id,
            )
            args.subnet_id = int(str(real_subnet_id))

        if not args.is_bootstrap:
            if hotkey is not None:
                result = hypertensor.interface.query("System", "Account", [hotkey])
                balance = result.value["data"]["free"]
                assert balance >= 500, (
                    f"Hotkey must have at least 0.0000000000000005 TENSOR to be a live account, balance is {float(balance / 1e18)}"  # noqa: E501
                )

            # Check subnet node exists
            subnet_node_info = hypertensor.get_formatted_get_subnet_node_info(args.subnet_id, args.subnet_node_id)
            if subnet_node_info is None:
                raise Exception("Subnet node does not exist")

            if subnet_node_info.hotkey.lower() != hotkey.lower():
                raise Exception(
                    f"Subnet node hotkey does not match. Expected: {subnet_node_info.hotkey}, Actual: {hotkey}"
                )

            local_peer_id = PeerID.from_pubkey(key_pair.public_key)

            if not local_peer_id.__eq__(subnet_node_info.peer_info.peer_id):
                logger.warning(
                    "Subnet node peer ID does not match. This can be ignored if running a bootnode or client peer. "
                    f"Expected: {subnet_node_info.peer_info.peer_id}, Actual: {local_peer_id.to_string()}"  # noqa: E501
                )

            if subnet_node_info.bootnode_peer_info:
                if not local_peer_id.__eq__(subnet_node_info.bootnode_peer_info.peer_id):
                    logger.warning(
                        "Subnet node bootnode peer ID does not match. This can be ignored if you're not running a bootnode. "  # noqa: E501
                        f"Expected: {subnet_node_info.bootnode_peer_info.peer_id}, Actual: {local_peer_id.to_string()}"  # noqa: E501
                    )

            if subnet_node_info.client_peer_info:
                if not local_peer_id.__eq__(subnet_node_info.client_peer_info.peer_id):
                    logger.warning(
                        "Subnet node client peer ID does not match. This can be ignored if you're not running a client peer. "  # noqa: E501
                        f"Expected: {subnet_node_info.client_peer_info.peer_id}, Actual: {local_peer_id.to_string()}"  # noqa: E501
                    )

            start_epoch = subnet_node_info.classification["start_epoch"]
            if start_epoch is None:
                raise Exception("Subnet node start epoch is None")
    else:
        # Run mock hypertensor blockchain for testing
        # This is a shared database between all local nodes
        hypertensor = LocalMockHypertensor(
            subnet_id=args.subnet_id,
            peer_id=PeerID.from_pubkey(key_pair.public_key),
            subnet_node_id=args.subnet_node_id if not args.is_bootstrap else 0,
            coldkey="",
            hotkey="",
            bootnode_peer_id="",
            client_peer_id="",
            reset_db=True if not args.bootstrap else False,
            insert_mock_overwatch_node=True if not args.bootstrap and args.insert_mock_overwatch_node else False,
        )

    slot = hypertensor.get_subnet_slot(args.subnet_id)
    slot = int(str(slot))

    # Wait to start the node until the node is fully registered on-chain
    # NOTE: Once a node registers on-chain, it will not be considered fully registered to other nodes
    # until the following epoch to ensure it starts on a fresh epoch.
    if start_epoch is not None:
        subnet_epoch_data = hypertensor.get_subnet_epoch_data(slot)
        current_epoch = subnet_epoch_data.epoch
        logger.info(f"Current epoch is {current_epoch}")
        if current_epoch < start_epoch:
            logger.info(
                "Keep this running and the node will automatically join the subnet once it's fully registered on-chain"
            )
            logger.info(f"Subnet node start epoch is {start_epoch}")
            while current_epoch < start_epoch:
                subnet_epoch_data = hypertensor.get_subnet_epoch_data(slot)
                current_epoch = subnet_epoch_data.epoch
                logger.info(f"Current epoch is {current_epoch}")
                if current_epoch >= start_epoch:
                    break

                seconds_remaining = subnet_epoch_data.seconds_remaining
                logger.info(
                    f"Checking next epoch to see if we can join the subnet, sleeping for {seconds_remaining} seconds"
                )
                time.sleep(seconds_remaining)
        logger.info("Subnet node is about to join the subnets DHT")

    try:
        server = Server(
            ip=args.ip,
            port=port,
            peerstore_db_path=None,  # TODO: Libp2p persistent peerstore needs work to be implemented
            bootstrap_addrs=args.bootstrap,
            key_pair=key_pair,
            db=db,
            subnet_id=args.subnet_id,
            subnet_node_id=args.subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=args.is_bootstrap,
            enable_pubsub_validator=not args.disable_pubsub_validator,
            enable_consensus=not args.disable_consensus,
            enable_proof_of_stake=not args.disable_proof_of_stake,
            strict_maintain_connections=not args.disable_strict_maintain_connections,
            enable_mDNS=args.enable_mDNS,
            enable_upnp=args.enable_upnp,
            enable_autotls=args.enable_autotls,
            resource_manager=None,  # TODO: Libp2p resource manager needs work to be implemented
            psk=args.psk,
            telemetry=telemetry,
            heartbeat_validator_log_level=args.heartbeat_validator_log_level,
            gossip_receiver_log_level=args.gossip_receiver_log_level,
            publish_heartbeat_log_level=args.publish_heartbeat_log_level,
            maintain_connections_log_level=args.maintain_connections_log_level,
        )
        trio.run(server.run)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if isinstance(hypertensor, LocalMockHypertensor):
            try:
                # Delete subnet node from mock db if it was created
                hypertensor.db.delete_subnet_node(args.subnet_id, args.subnet_node_id)
                logger.info("Removed subnet node from mock db")
            except Exception as e:
                logger.error(f"Failed to delete subnet node from mock db: {e}")


if __name__ == "__main__":
    main()
