"""CLI command to run a libp2p subnet server node."""

import argparse
import logging
import sys

import trio
import random
from libp2p import new_host
import secrets
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
from subnet.utils.bootstrap import connect_to_bootstrap_nodes

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
  python -m subnet.cli.run_bootnode

  # Run a bootnode connecting to bootstrap peers
  python -m subnet.cli.run_bootnode --bootstrap /ip4/127.0.0.1/tcp/31330/p2p/QmBootstrapPeerID

  # Connect to multiple bootstrap peers
  python -m subnet.cli.run_bootnode \\
    --bootstrap /ip4/192.168.1.100/tcp/31330/p2p/QmPeer1 \\
    --bootstrap /ip4/192.168.1.101/tcp/31330/p2p/QmPeer2
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
        logger.debug(f"Using port: {args.port}")

        if args.bootstrap_addrs:
            for addr in args.bootstrap_addrs:
                bootstrap_nodes.append(addr)

        key_pair = create_new_key_pair(secrets.token_bytes(32))
        host = new_host(key_pair=key_pair)

        # noise_key_pair = create_new_x25519_key_pair()

        # secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
        #     NOISE_PROTOCOL_ID: NoiseTransport(
        #         key_pair, noise_privkey=noise_key_pair.private_key
        #     ),
        #     TProtocol(secio.ID): secio.Transport(key_pair),
        #     TProtocol(PLAINTEXT_PROTOCOL_ID): InsecureTransport(
        #         key_pair, peerstore=None
        #     ),
        # }

        # host = new_host(key_pair=key_pair, sec_opt=secure_transports_by_protocol)

        from libp2p.utils.address_validation import (
            get_available_interfaces,
            get_optimal_binding_address,
        )

        listen_addrs = get_available_interfaces(args.port)

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
            optimal_addr = get_optimal_binding_address(args.port)
            optimal_addr_with_peer = f"{optimal_addr}/p2p/{host.get_id().to_string()}"
            bootstrap_cmd = f"--bootstrap {optimal_addr_with_peer}"
            logger.info("To connect to this node, use: %s", bootstrap_cmd)

            await connect_to_bootstrap_nodes(host, bootstrap_nodes)
            dht = KadDHT(host, DHTMode.SERVER)
            # take all peer ids from the host and add them to the dht
            for peer_id in host.get_peerstore().peer_ids():
                await dht.routing_table.add_peer(peer_id)
            logger.info(f"Connected to bootstrap nodes: {host.get_connected_peers()}")

            # Start the DHT service
            async with background_trio_service(dht):
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
