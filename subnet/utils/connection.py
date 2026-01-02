import logging
import random

import trio

from libp2p.abc import (
    IHost,
)
from libp2p.kad_dht.kad_dht import KadDHT
from libp2p.peer.id import ID as PeerID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.utils.subnet_info_tracker import SubnetInfoTracker

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
) -> None:
    """Maintain connections to ensure the host remains connected to healthy peers."""
    # await trio.sleep(30)

    my_peer_id = host.get_id()
    # Track recent connection attempts to prevent thrashing
    # recent_connection_attempts: dict[PeerID, float] = {}
    # BACKOFF_DURATION = 300  # 5 minutes

    while True:
        try:
            onchain_peer_ids = subnet_info_tracker.get_all_peer_ids(force=True)
            logger.info(f"All peer IDs: {onchain_peer_ids}")

            connected_peers = host.get_connected_peers()
            list_peers = host.get_peerstore().peers_with_addrs()

            logger.info(f"Connected peers: {connected_peers}")

            if dht:
                peerstore_peer_ids = dht.host.get_peerstore().peer_ids()
                logger.info(f"Peerstore peer IDs: {peerstore_peer_ids}")
                for peer_id in dht.routing_table.get_peer_ids():
                    if peer_id not in list_peers:
                        list_peers.append(peer_id)

            remove_peers = []
            # Use list copies to avoid issues with modification during iteration
            for peer_id in list(connected_peers):
                if peer_id not in onchain_peer_ids and not peer_id.__eq__(my_peer_id):
                    remove_peers.append(peer_id)
                    connected_peers.remove(peer_id)
                    # recent_connection_attempts.pop(peer_id, None)

            for peer_id in list(list_peers):
                if peer_id not in onchain_peer_ids and not peer_id.__eq__(my_peer_id):
                    remove_peers.append(peer_id)
                    list_peers.remove(peer_id)
                    # recent_connection_attempts.pop(peer_id, None)

            # Deduplicate the removal list
            remove_peers = list(set(remove_peers))

            await disconnect_peers(
                remove_peers,
                host,
                gossipsub,
                dht,
            )

            logger.info(f"List peers: {list_peers}")

            if len(connected_peers) < 32:
                logger.info("Reconnecting to maintain peer connections...")

                # Find compatible peers
                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if filter_compatible_peer_info(peer_info):
                            compatible_peers.append(peer_id)
                    except Exception:
                        continue

                # Filter out peers that are in backoff period
                # current_time = trio.current_time()
                # compatible_peers = [
                #     p
                #     for p in compatible_peers
                #     if p not in recent_connection_attempts
                #     or (current_time - recent_connection_attempts[p] > BACKOFF_DURATION)
                # ]

                # # Clean up expired entries in recent_connection_attempts
                # recent_connection_attempts = {
                #     p: t
                #     for p, t in recent_connection_attempts.items()
                #     if current_time - t <= BACKOFF_DURATION
                # }

                # Connect to random subset of compatible peers
                if compatible_peers:
                    random_peers = random.sample(compatible_peers, min(64, len(compatible_peers)))
                    for peer_id in random_peers:
                        if peer_id not in connected_peers and peer_id in onchain_peer_ids:
                            try:
                                with trio.move_on_after(5):
                                    peer_info = host.get_peerstore().peer_info(peer_id)
                                    logger.info(f"Adding addresses for peer: {peer_id}")
                                    host.get_peerstore().add_addrs(peer_info.peer_id, peer_info.addrs, 300)
                                    logger.info(f"Connecting to peer: {peer_id}")
                                    # Record attempt time BEFORE connecting
                                    # recent_connection_attempts[peer_id] = (
                                    #     trio.current_time()
                                    # )
                                    await host.connect(peer_info)
                                    logger.info(f"Connected to peer: {peer_id}")
                            except Exception as e:
                                logger.warning(f"Failed to connect to {peer_id}: {e}")

            if gossipsub:
                # Prune disconnected peers from gossipsub mesh
                mesh_peers = set()
                for topic_peers in gossipsub.mesh.values():
                    mesh_peers.update(topic_peers)

                for peer_id in mesh_peers:
                    if peer_id not in connected_peers:
                        logger.info(f"Pruning disconnected peer {peer_id} from gossipsub mesh")
                        gossipsub.remove_peer(peer_id)

                ###
                # Read only data
                logger.info(f"GossipSub mesh: {gossipsub.mesh}")

                gossipsub_topic = next((t for t in gossipsub.mesh if str(t) == "heartbeat"), None)

                topic_peers = set()
                if gossipsub_topic:
                    topic_peers = gossipsub.mesh[gossipsub_topic]
                    logger.info(f"Heartbeat number of peers: {len(topic_peers)}")
                    for peer_id in topic_peers:
                        logger.info(f"Heartbeat mesh peer: {peer_id}")

                logger.info(f"GossipSub fanout: {gossipsub.fanout}")
                ###

                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if filter_compatible_peer_info(peer_info):
                            compatible_peers.append(peer_id)
                    except Exception as e:
                        logger.debug(f"Failed to get peer info for {peer_id}: {e}")
                        continue
                random_peers = random.sample(compatible_peers, min(64, len(compatible_peers)))
                random_peers = [p for p in random_peers if p in onchain_peer_ids and p not in topic_peers]

                await maintain_gossipsub_connections(random_peers, host, gossipsub, pubsub)

            await trio.sleep(9)
        except Exception as e:
            logger.error(f"Error maintaining connections: {e}")


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
            logger.info(
                f"maintain_single_gossipsub_connection, Peer {peer_id} is not connected yet to add to gossipsub"  # noqa: E501
            )
            return

        # 2. Verify protocol support
        # Make sure peer supports /meshsub/1.0.0 before attempting to add to gossipsub
        try:
            protocols = host.get_peerstore().get_protocols(peer_id)
            logger.info(
                f"maintain_single_gossipsub_connection, {peer_id} protocols: {protocols}"  # noqa: E501
            )
            if GOSSIPSUB_PROTOCOL_ID not in protocols:
                logger.info(
                    f"maintain_single_gossipsub_connection, Peer {peer_id} does not support {GOSSIPSUB_PROTOCOL_ID} yet"  # noqa: E501
                )
                host.get_peerstore().add_protocols(peer_id, ["/meshsub/1.0.0"])
                return
        except Exception as e:
            logger.info(
                f"maintain_single_gossipsub_connection, Failed to get protocols for {peer_id} with: {e}"  # noqa: E501
            )
            return

        # 3. Verify blacklisting
        # Make sure peer is not blacklisted before attempting to add to gossipsub
        # If peer is blacklisted, remove it from the blacklist
        # Peers sent to this function should be verified they shouldn't be blacklisted
        if pubsub:
            is_blacklisted = pubsub.is_peer_blacklisted(peer_id)
            logger.info(
                f"maintain_single_gossipsub_connection, {peer_id} is blacklisted: {is_blacklisted}"  # noqa: E501
            )
            if is_blacklisted:
                pubsub.remove_from_blacklist(peer_id)

        # 4. Verify existing connection
        # Make sure peer is not already in /meshsub/1.0.0 before attempting to add to gossipsub # noqa: E501
        if gossipsub.peer_protocol.get(peer_id) == GOSSIPSUB_PROTOCOL_ID:
            logger.info(
                f"maintain_single_gossipsub_connection, Peer {peer_id} is already in {GOSSIPSUB_PROTOCOL_ID}"  # noqa: E501
            )
            return

        logger.info(f"maintain_single_gossipsub_connection, Adding peer to gossipsub: {peer_id}")
        gossipsub.add_peer(peer_id, GOSSIPSUB_PROTOCOL_ID)
        logger.info(f"maintain_single_gossipsub_connection, Added peer to gossipsub: {peer_id}")
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
            logger.info(
                f"Disconnected peer {peer_id} because they are no longer registered on-chain"  # noqa: E501
            )

            # Clear from peerstore so we stop dialing them
            # clear_peerdata completely removes the peer record from all internal maps
            host.get_peerstore().clear_peerdata(peer_id)
            logger.info(
                f"Fully cleared peer {peer_id} from peerstore because they are no longer registered on-chain"  # noqa: E501
            )
        except Exception as e:
            logger.warning(f"Failed to disconnect peer {peer_id}: {e}")
        if dht:
            # Remove from DHT routing table
            try:
                dht.routing_table.remove_peer(peer_id)
                dht.host.get_peerstore().clear_peerdata(peer_id)
                logger.info(f"Removed peer {peer_id} from routing table and cleared peerdata")
            except Exception as e:
                logger.warning(f"Failed to remove peer {peer_id}: {e}")
        if gossipsub:
            # Remove from GossipSub
            try:
                gossipsub.remove_peer(peer_id)
                logger.info(
                    f"Removed peer {peer_id} from gossipsub because they are no longer registered on-chain"  # noqa: E501
                )
            except Exception as e:
                logger.warning(f"Failed to remove peer {peer_id}: {e}")


async def demonstrate_random_walk_discovery(dht: KadDHT, interval: int = 30) -> None:
    """Demonstrate Random Walk peer discovery with periodic statistics."""
    iteration = 0
    while True:
        iteration += 1
        logger.info(f"--- Iteration {iteration} ---")
        logger.info(f"Routing table size: {dht.get_routing_table_size()}")
        logger.info(f"Connected peers: {len(dht.host.get_connected_peers())}")
        logger.info(f"Peerstore size: {len(dht.host.get_peerstore().peer_ids())}")

        if dht.get_routing_table_size() > 0:
            logger.info("Peers in routing table:")
            for peer_id in dht.routing_table.get_peer_ids():
                logger.info(f"  {peer_id}")

        await trio.sleep(interval)
