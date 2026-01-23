import logging
import random

from libp2p.abc import (
    IHost,
)
from libp2p.kad_dht.kad_dht import KadDHT
from libp2p.peer.id import ID as PeerID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
import trio

from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.utils.hypertensor.subnet_info_tracker import SubnetInfoTracker

logger = logging.getLogger("subnet.utils.connection")


def filter_compatible_peer_info(peer_info) -> bool:
    """Filter peer info to check if it has compatible addresses (TCP + IPv4)."""
    if not hasattr(peer_info, "addrs") or not peer_info.addrs:
        return False

    for addr in peer_info.addrs:
        addr_str = str(addr)
        if "/tcp/" in addr_str and "/ip4/" in addr_str and "/quic" not in addr_str:
            return True
    return False


async def maintain_connections(
    host: IHost,
    subnet_info_tracker: SubnetInfoTracker,
    gossipsub: GossipSub | None = None,
    pubsub: Pubsub | None = None,
    dht: KadDHT | None = None,
    connection_backoff_duration: float = 300.0,  # 5 minutes
    max_backoff_duration: float = 1080.0,  # 18 minutes
    retry_multiplier: float = 1.2,
) -> None:
    """Maintain connections to ensure the host remains connected to healthy peers."""
    my_peer_id = host.get_id()
    # Track recent connection attempts to prevent thrashing
    # recent_connection_attempts: dict[PeerID, float] = {}
    # next_connection_attempts: dict[PeerID, float] = {}

    # Track retries for each peer, and next retry time
    peer_retries: dict[PeerID, int] = {}
    peer_next_retry: dict[PeerID, float] = {}

    while True:
        try:
            onchain_peer_ids = await subnet_info_tracker.get_all_peer_ids(force=True)
            # logger.info(f"All peer IDs: {onchain_peer_ids}")

            connected_peers = host.get_connected_peers()
            list_peers = host.get_peerstore().peers_with_addrs()

            # logger.info(f"Connected peers: {connected_peers}")

            if dht:
                peerstore_peer_ids = dht.host.get_peerstore().peer_ids()
                logger.debug(f"Peerstore peer IDs: {peerstore_peer_ids}")
                for peer_id in dht.routing_table.get_peer_ids():
                    if peer_id not in list_peers:
                        list_peers.append(peer_id)

            remove_peers = []

            # Remove peers that are not in the onchain peer list and are connected
            # Use list copies to avoid issues with modification during iteration
            for peer_id in list(connected_peers):
                if peer_id not in onchain_peer_ids and not peer_id.__eq__(my_peer_id):
                    remove_peers.append(peer_id)
                    connected_peers.remove(peer_id)
                    # recent_connection_attempts.pop(peer_id, None)
                    # next_connection_attempts.pop(peer_id, None)
                    peer_retries.pop(peer_id, None)
                    peer_next_retry.pop(peer_id, None)

            # Remove peers that are not in the onchain peer list and are in the peerstore
            for peer_id in list(list_peers):
                if peer_id not in onchain_peer_ids and not peer_id.__eq__(my_peer_id):
                    remove_peers.append(peer_id)
                    list_peers.remove(peer_id)
                    # recent_connection_attempts.pop(peer_id, None)
                    # next_connection_attempts.pop(peer_id, None)
                    peer_retries.pop(peer_id, None)
                    peer_next_retry.pop(peer_id, None)

            # Deduplicate the removal list
            remove_peers = list(set(remove_peers))

            await disconnect_peers(
                remove_peers,
                host,
                gossipsub,
                dht,
            )

            logger.debug(f"List peers: {list_peers}")

            if len(connected_peers) < 32:
                logger.debug("Reconnecting to maintain peer connections...")

                # Find compatible peers
                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        # if (
                        #     filter_compatible_peer_info(peer_info)
                        #     and trio.current_time() - recent_connection_attempts.get(peer_id, 0)
                        #     > connection_backoff_duration
                        # ):
                        if filter_compatible_peer_info(peer_info) and trio.current_time() >= peer_next_retry.get(
                            peer_id, 0
                        ):
                            compatible_peers.append(peer_id)
                    except Exception as e:
                        logger.warning(f"Failed to get peer info for {peer_id}: {e}", exc_info=True)
                        continue

                # Connect to random subset of compatible peers
                if compatible_peers:
                    random_peers = random.sample(compatible_peers, min(64, len(compatible_peers)))
                    for peer_id in random_peers:
                        if peer_id not in connected_peers and peer_id in onchain_peer_ids:
                            try:
                                # recent_connection_attempts[peer_id] = trio.current_time()
                                peer_retries[peer_id] = peer_retries.get(peer_id, 0) + 1
                                next_retry_time = trio.current_time() + connection_backoff_duration * (
                                    peer_retries.get(peer_id, 0) ** retry_multiplier
                                )
                                if next_retry_time > max_backoff_duration:
                                    next_retry_time = max_backoff_duration
                                peer_next_retry[peer_id] = next_retry_time

                                if dht:
                                    with trio.move_on_after(5):
                                        peer_info = await dht.find_peer(peer_id)
                                        if not peer_info:
                                            continue

                                with trio.move_on_after(5):
                                    peer_info = host.get_peerstore().peer_info(peer_id)
                                    logger.debug(f"Adding addresses for peer: {peer_id}")
                                    host.get_peerstore().add_addrs(peer_info.peer_id, peer_info.addrs, 300)
                                    logger.debug(f"Connecting to peer: {peer_id}")
                                    await host.connect(peer_info)
                                    logger.debug(f"Connected to peer: {peer_id}")
                            except Exception as e:
                                logger.warning(f"Failed to connect to {peer_id}: {e}", exc_info=True)

            if gossipsub:
                # Prune disconnected peers from gossipsub mesh
                mesh_peers = set()
                for topic_peers in gossipsub.mesh.values():
                    mesh_peers.update(topic_peers)

                logger.debug(f"GossipSub mesh: {gossipsub.mesh}")

                gossipsub_topic = next((t for t in gossipsub.mesh if str(t) == "heartbeat"), None)

                topic_peers = set()
                if gossipsub_topic:
                    topic_peers = gossipsub.mesh[gossipsub_topic]
                    logger.debug(f"Heartbeat number of peers: {len(topic_peers)}")
                    for peer_id in topic_peers:
                        logger.debug(f"Heartbeat mesh peer: {peer_id}")

                logger.debug(f"GossipSub fanout: {gossipsub.fanout}")
                ###

                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if filter_compatible_peer_info(peer_info):
                            compatible_peers.append(peer_id)
                    except Exception as e:
                        logger.warning(f"Failed to get peer info for {peer_id}: {e}", exc_info=True)
                        continue
                random_peers = random.sample(compatible_peers, min(64, len(compatible_peers)))
                random_peers = [p for p in random_peers if p in onchain_peer_ids and p not in topic_peers]

                await maintain_gossipsub_connections(random_peers, host, gossipsub, pubsub)

            await trio.sleep(15)
        except Exception as e:
            logger.error(f"Error maintaining connections: {e}", exc_info=True)


async def maintain_gossipsub_connections(
    peer_ids: list[PeerID],
    host: IHost,
    gossipsub: GossipSub,
    pubsub: Pubsub,
):
    """
    Maintain connections to peers in gossipsub.
    """
    for peer_id in peer_ids:
        await maintain_single_gossipsub_connection(
            peer_id,
            host,
            gossipsub,
            pubsub,
        )


async def maintain_single_gossipsub_connection(
    peer_id: PeerID,
    host: IHost,
    gossipsub: GossipSub,
    pubsub: Pubsub,
):
    try:
        # 1. Verify connection status
        # Make sure peer is connected before attempting to add to gossipsub
        if peer_id not in host.get_connected_peers():
            logger.debug(
                f"maintain_single_gossipsub_connection, Peer {peer_id} is not connected yet to add to gossipsub"  # noqa: E501
            )
            return

        # 2. Verify protocol support
        # Make sure peer supports /meshsub/1.0.0 before attempting to add to gossipsub
        try:
            protocols = host.get_peerstore().get_protocols(peer_id)
            logger.debug(
                f"maintain_single_gossipsub_connection, {peer_id} protocols: {protocols}"  # noqa: E501
            )
            if GOSSIPSUB_PROTOCOL_ID not in protocols:
                logger.debug(
                    f"maintain_single_gossipsub_connection, Peer {peer_id} does not support {GOSSIPSUB_PROTOCOL_ID} yet"  # noqa: E501
                )
                host.get_peerstore().add_protocols(peer_id, ["/meshsub/1.0.0"])
                return
        except Exception as e:
            logger.warning(
                f"maintain_single_gossipsub_connection, Failed to get protocols for {peer_id} with: {e}",
                exc_info=True,
            )
            return

        # 3. Verify blacklisting
        # Make sure peer is not blacklisted before attempting to add to gossipsub
        # If peer is blacklisted, remove it from the blacklist
        # Peers sent to this function should be verified they shouldn't be blacklisted
        if pubsub:
            is_blacklisted = pubsub.is_peer_blacklisted(peer_id)
            logger.debug(
                f"maintain_single_gossipsub_connection, {peer_id} is blacklisted: {is_blacklisted}"  # noqa: E501
            )
            if is_blacklisted:
                pubsub.remove_from_blacklist(peer_id)

        # 4. Verify existing connection
        # Make sure peer is not already in /meshsub/1.0.0 before attempting to add to gossipsub # noqa: E501
        if gossipsub.peer_protocol.get(peer_id) == GOSSIPSUB_PROTOCOL_ID:
            logger.debug(
                f"maintain_single_gossipsub_connection, Peer {peer_id} is already in {GOSSIPSUB_PROTOCOL_ID}"  # noqa: E501
            )
            return

        gossipsub.add_peer(peer_id, GOSSIPSUB_PROTOCOL_ID)
        logger.debug(f"maintain_single_gossipsub_connection, Added peer to gossipsub: {peer_id}")
    except Exception as e:
        logger.warning(f"maintain_single_gossipsub_connection, Failed to add peer {peer_id}: {e}")


async def disconnect_peers(
    peer_ids: list[PeerID],
    host: IHost,
    gossipsub: GossipSub | None = None,
    dht: KadDHT | None = None,
):
    for peer_id in peer_ids:
        # Remove from host
        try:
            await host.disconnect(peer_id)
            # logger.info(
            #     f"Disconnected peer {peer_id} because they are no longer registered on-chain"  # noqa: E501
            # )

            # Clear from peerstore so we stop dialing them
            # clear_peerdata completely removes the peer record from all internal maps
            host.get_peerstore().clear_peerdata(peer_id)
            logger.debug(
                f"Fully cleared peer {peer_id} from peerstore because they are no longer registered on-chain"  # noqa: E501
            )
        except Exception as e:
            logger.warning(f"Failed to disconnect peer {peer_id}: {e}", exc_info=True)
        if dht:
            # Remove from DHT routing table
            try:
                dht.routing_table.remove_peer(peer_id)
                dht.host.get_peerstore().clear_peerdata(peer_id)
                logger.debug(f"Removed peer {peer_id} from routing table and cleared peerdata")
            except Exception as e:
                logger.warning(f"Failed to remove peer {peer_id}: {e}", exc_info=True)
        if gossipsub:
            # Remove from GossipSub
            try:
                gossipsub.remove_peer(peer_id)
                logger.debug(
                    f"Removed peer {peer_id} from gossipsub because they are no longer registered on-chain"  # noqa: E501
                )
            except Exception as e:
                logger.warning(f"Failed to remove peer {peer_id}: {e}", exc_info=True)


async def demonstrate_random_walk_discovery(dht: KadDHT, interval: int = 30) -> None:
    """Demonstrate Random Walk peer discovery with periodic statistics."""
    while True:
        # logger.info(f"Routing table size: {dht.get_routing_table_size()}")
        # logger.info(f"Connected peers: {len(dht.host.get_connected_peers())}")
        # logger.info(f"Peerstore size: {len(dht.host.get_peerstore().peer_ids())}")

        if dht.get_routing_table_size() > 0:
            logger.debug("Peers in routing table:")
            for peer_id in dht.routing_table.get_peer_ids():
                logger.debug(f"  {peer_id}")

        await trio.sleep(interval)
