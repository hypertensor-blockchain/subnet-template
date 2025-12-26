"""CLI command to run a libp2p subnet server node."""

import argparse
import logging
import sys
import os

import trio
import random
from libp2p import new_host
import secrets
from libp2p.peer.id import ID as PeerID
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.utils.address_validation import (
    get_available_interfaces,
    get_optimal_binding_address,
)
from multiaddr import (
    Multiaddr,
)
from libp2p.tools.async_service import (
    background_trio_service,
)
from libp2p.custom_types import (
    TProtocol,
)
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.abc import (
    IHost,
    ISecureTransport,
)
import libp2p.security.secio.transport as secio
from libp2p.tools.utils import (
    info_from_p2p_addr,
)
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from collections.abc import (
    Mapping,
)
from libp2p.security.noise.transport import (
    PROTOCOL_ID as NOISE_PROTOCOL_ID,
    Transport as NoiseTransport,
)
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.security.insecure.transport import (
    PLAINTEXT_PROTOCOL_ID,
    InsecureTransport,
)
from subnet.network.pos.pos_transport import (
    POSTransport,
    PROTOCOL_ID as POS_PROTOCOL_ID,
)
from libp2p.crypto.keys import KeyPair
from libp2p.peer.pb import crypto_pb2
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from libp2p.records.validator import NamespacedValidator
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom
from subnet.utils.bootstrap import connect_to_bootstrap_nodes
from subnet.network.pos.proof_of_stake import ProofOfStake
from substrateinterface import Keypair as SubstrateKeypair, KeypairType
from dotenv import load_dotenv
from pathlib import Path

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
        description="Run a libp2p subnet bootnode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a standalone bootnode
  python -m subnet.cli.run_bootnode_v2

  # Run a bootnode connecting to bootstrap peers
  python -m subnet.cli.run_bootnode_v2 --bootstrap /ip4/127.0.0.1/tcp/31330/p2p/QmBootstrapPeerID

  # Connect to multiple bootstrap peers
  python -m subnet.cli.run_bootnode_v2 \\
    --bootstrap /ip4/192.168.1.100/tcp/31330/p2p/QmPeer1 \\
    --bootstrap /ip4/192.168.1.101/tcp/31330/p2p/QmPeer2

  # Run a bootnode with an identity file
  python -m subnet.cli.run_bootnode_v2 --identity_path bootnode-ed25519.key --port 38959 --no_blockchain_rpc
        """,
    )

    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port this server listens to. "
        "This is a simplified way to set the --host_maddrs and --announce_maddrs options (see below) "
        "that sets the port across all interfaces (IPv4, IPv6) and protocols (TCP, etc.) "
        "to the same number. Default: a random free port is chosen for each interface and protocol",
    )

    parser.add_argument(
        "--bootstrap",
        "-b",
        action="append",
        dest="bootstrap_addrs",
        default=[],
        metavar="MULTIADDR",
        help="Bootstrap peer multiaddress (can be specified multiple times). "
        "Format: /ip4/<IP>/tcp/<PORT>/p2p/<PEER_ID>",
    )

    parser.add_argument(
        "--identity_path", type=str, default=None, help="Path to the identity file. "
    )

    parser.add_argument(
        "--subnet_id", type=int, default=1, help="Subnet ID this bootnode belongs to. "
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

    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )

    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version="%(prog)s 0.1.0",
    )

    return parser.parse_args()


async def run_bootnode(args: argparse.Namespace):
    # Set logging level
    logging.getLogger().setLevel(args.log_level)

    # Log startup information
    logger.info("Starting libp2p subnet server node...")

    bootstrap_nodes = []

    try:
        if args.port <= 0:
            port = random.randint(10000, 60000)
        else:
            port = args.port
        logger.debug(f"Using port: {port}")

        if args.identity_path is None:
            key_pair = create_new_key_pair(secrets.token_bytes(32))
        else:
            with open(f"{args.identity_path}", "rb") as f:
                data = f.read()
            private_key = crypto_pb2.PrivateKey.FromString(data)
            ed25519_private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
            public_key = ed25519_private_key.get_public_key()
            key_pair = KeyPair(ed25519_private_key, public_key)

        if args.bootstrap_addrs:
            for addr in args.bootstrap_addrs:
                bootstrap_nodes.append(addr)

        if len(bootstrap_nodes) == 0:
            logger.warning(
                "No bootstrap nodes provided. The node will not be able to connect to other nodes."
            )

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
        else:
            # Run mock hypertensor blockchain for testing
            hypertensor = LocalMockHypertensor(
                subnet_id=args.subnet_id,
                peer_id=PeerID.from_pubkey(key_pair.public_key),
                subnet_node_id=0,
                coldkey="",
                hotkey="",
                bootnode_peer_id="",
                client_peer_id="",
                reset_db=True if not args.bootstrap_addrs else False,
            )

        pos_transport = POSTransport(
            noise_transport=NoiseTransport(
                key_pair, noise_privkey=create_new_x25519_key_pair().private_key
            ),
            pos=ProofOfStake(
                subnet_id=args.subnet_id,
                hypertensor=hypertensor,
                min_class=0,
            ),
        )

        secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
            POS_PROTOCOL_ID: pos_transport,
        }

        host = new_host(key_pair=key_pair, sec_opt=secure_transports_by_protocol)
        # host = new_host(key_pair=key_pair)

        from libp2p.utils.address_validation import (
            get_available_interfaces,
            get_optimal_binding_address,
        )

        listen_addrs = get_available_interfaces(port)

        async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
            # Start the peer-store cleanup task
            nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)

            peer_id = host.get_id().pretty()

            # Get all available addresses with peer ID
            all_addrs = host.get_addrs()

            logger.info("Listener ready, listening on:")
            for addr in all_addrs:
                logger.info(f"{addr}")

            # Use optimal address for the bootstrap command
            optimal_addr = get_optimal_binding_address(port)
            optimal_addr_with_peer = f"{optimal_addr}/p2p/{host.get_id().to_string()}"
            bootstrap_cmd = f"--bootstrap {optimal_addr_with_peer}"
            logger.info("To connect to this node, use: %s", bootstrap_cmd)

            await connect_to_bootstrap_nodes(host, bootstrap_nodes)
            dht = KadDHT(
                host,
                DHTMode.SERVER,
                enable_random_walk=True,
                validator=NamespacedValidator({"pk": PublicKeyValidator()}),
            )

            # take all peer ids from the host and add them to the dht
            for peer_id in host.get_peerstore().peer_ids():
                await dht.routing_table.add_peer(peer_id)
            logger.info(f"Connected to bootstrap nodes: {host.get_connected_peers()}")

            # Start the DHT service
            async with background_trio_service(dht):
                logger.info(f"DHT service started in {DHTMode.SERVER.value} mode")

                # Keep the node running
                while True:
                    logger.info(
                        "Status - Connected peers: %d,"
                        "Peers in store: %d, Values in store: %d",
                        len(dht.host.get_connected_peers()),
                        len(dht.host.get_peerstore().peer_ids()),
                        len(dht.value_store.store),
                    )
                    await trio.sleep(10)

        # key_pair = create_new_key_pair(secrets.token_bytes(32))
        # host = new_host(key_pair=key_pair)

        # # noise_key_pair = create_new_x25519_key_pair()

        # # secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
        # #     NOISE_PROTOCOL_ID: NoiseTransport(
        # #         key_pair, noise_privkey=noise_key_pair.private_key
        # #     ),
        # #     TProtocol(secio.ID): secio.Transport(key_pair),
        # #     TProtocol(PLAINTEXT_PROTOCOL_ID): InsecureTransport(
        # #         key_pair, peerstore=None
        # #     ),
        # # }

        # # host = new_host(key_pair=key_pair, sec_opt=secure_transports_by_protocol)

        # from libp2p.utils.address_validation import (
        #     get_available_interfaces,
        #     get_optimal_binding_address,
        # )

        # listen_addrs = get_available_interfaces(args.port)

        # async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
        #     # Start the peer-store cleanup task
        #     nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)

        #     peer_id = host.get_id().pretty()

        #     # Get all available addresses with peer ID
        #     all_addrs = host.get_addrs()

        #     logger.info("Listener ready, listening on:")
        #     for addr in all_addrs:
        #         logger.info(f"{addr}")

        #     # Use optimal address for the bootstrap command
        #     optimal_addr = get_optimal_binding_address(args.port)
        #     optimal_addr_with_peer = f"{optimal_addr}/p2p/{host.get_id().to_string()}"
        #     bootstrap_cmd = f"--bootstrap {optimal_addr_with_peer}"
        #     logger.info("To connect to this node, use: %s", bootstrap_cmd)

        #     await connect_to_bootstrap_nodes(host, bootstrap_nodes)
        #     dht = KadDHT(host, DHTMode.SERVER)
        #     # take all peer ids from the host and add them to the dht
        #     for peer_id in host.get_peerstore().peer_ids():
        #         await dht.routing_table.add_peer(peer_id)
        #     logger.info(f"Connected to bootstrap nodes: {host.get_connected_peers()}")

        #     # Start the DHT service
        #     async with background_trio_service(dht):
        #         # Keep the node running
        #         while True:
        #             logger.info(
        #                 "Status - Connected peers: %d,"
        #                 "Peers in store: %d, Values in store: %d",
        #                 len(dht.host.get_connected_peers()),
        #                 len(dht.host.get_peerstore().peer_ids()),
        #                 len(dht.value_store.store),
        #             )
        #             await trio.sleep(10)

    except Exception as e:
        logger.error(f"Server node error: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    args = parse_args()

    try:
        trio.run(run_bootnode, args)
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    main()
