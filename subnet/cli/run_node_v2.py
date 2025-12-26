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
from libp2p.crypto.keys import KeyPair
from libp2p.peer.pb import crypto_pb2
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from subnet.utils.bootstrap import connect_to_bootstrap_nodes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run a libp2p subnet node",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a standalone node
  python -m subnet.cli.run_node_v2

  # Run a node connecting to bootstrap peers
  python -m subnet.cli.run_node_v2 --bootstrap /ip4/127.0.0.1/tcp/31330/p2p/QmBootstrapPeerID

  # Connect to multiple bootstrap peers
  python -m subnet.cli.run_node_v2 \\
    --bootstrap /ip4/192.168.1.100/tcp/31330/p2p/QmPeer1 \\
    --bootstrap /ip4/192.168.1.101/tcp/31330/p2p/QmPeer2

  # Run a node with an identity file
  python -m subnet.cli.run_node_v2 --identity_path alith-ed25519.key --port 38960 --bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF
  python -m subnet.cli.run_node_v2 --identity_path baltathar-ed25519.key --port 38961 --bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF
  python -m subnet.cli.run_node_v2 --identity_path charleth-ed25519.key --port 38962 --bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF
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
        "--private_key",
        type=str,
        required=False,
        help="[Testing] Hypertensor blockchain private key",
    )

    # add option to use verbose logging
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()
    # Set logging level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    return args


async def run_node(
    port: int,
    mode: str,
    subnet_id: int = 1,
    bootstrap_addrs: list[str] | None = None,
    identity_path: str | None = None,
) -> None:
    """Run a node that serves content in the DHT with setup inlined."""
    bootstrap_nodes = []

    try:
        if port <= 0:
            port = random.randint(10000, 60000)
        logger.debug(f"Using port: {port}")

        dht_mode = DHTMode.SERVER

        if bootstrap_addrs:
            for addr in bootstrap_addrs:
                bootstrap_nodes.append(addr)

        if identity_path is None:
            key_pair = create_new_key_pair(secrets.token_bytes(32))
        else:
            with open(f"{identity_path}", "rb") as f:
                data = f.read()
            private_key = crypto_pb2.PrivateKey.FromString(data)
            ed25519_private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
            public_key = ed25519_private_key.get_public_key()
            key_pair = KeyPair(ed25519_private_key, public_key)
        host = new_host(key_pair=key_pair)

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
            dht = KadDHT(host, DHTMode.SERVER, enable_random_walk=True)

            # take all peer ids from the host and add them to the dht
            for peer_id in host.get_peerstore().peer_ids():
                await dht.routing_table.add_peer(peer_id)
            logger.info(f"Connected to bootstrap nodes: {host.get_connected_peers()}")

            # Start the DHT service
            async with background_trio_service(dht):
                logger.info(f"DHT service started in {dht_mode.value} mode")

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


def main():
    """Main entry point for the kademlia demo."""
    try:
        args = parse_args()
        logger.info(
            "Running in %s mode on port %d",
            args.mode,
            args.port,
        )
        trio.run(
            run_node,
            args.port,
            args.mode,
            args.subnet_id,
            args.bootstrap,
            args.identity_path,
        )
    except Exception as e:
        logger.critical(f"Script failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
