import logging

from libp2p.abc import IHost
from libp2p.tools.utils import info_from_p2p_addr
from multiaddr import Multiaddr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")


async def connect_to_bootstrap_nodes(host: IHost, bootstrap_addrs: list[str]) -> None:
    """
    Connect to the bootstrap nodes provided in the list.

    params: host: The host instance to connect to
            bootstrap_addrs: List of bootstrap node addresses

    Returns
    -------
        None

    """
    connections = 0
    for addr in bootstrap_addrs:
        try:
            peerInfo = info_from_p2p_addr(Multiaddr(addr))
            host.get_peerstore().add_addrs(peerInfo.peer_id, peerInfo.addrs, 300)
            await host.connect(peerInfo)
            logger.info(f"Connected to bootstrap node {addr}")
            connections += 1
        except Exception as e:
            logger.error(f"Failed to connect to bootstrap node {addr}: {e}")

    if connections == 0:
        raise Exception("Failed to connect to any bootstrap nodes")
