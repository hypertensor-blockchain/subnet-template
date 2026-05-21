import logging
import random
import time

from libp2p.abc import (
    IHost,
)
from libp2p.kad_dht.kad_dht import KadDHT
from libp2p.peer.id import ID as PeerID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
import trio

from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker

logger = logging.getLogger("subnet.utils.connection")

DEFAULT_ABUSIVE_PEER_SCORE_THRESHOLD = -100.0
DEFAULT_ABUSIVE_PEER_BEHAVIOR_PENALTY_THRESHOLD = 50.0
DEFAULT_ABUSIVE_PEER_INVALID_MESSAGE_THRESHOLD = 20.0
DEFAULT_ABUSIVE_PEER_CONTROL_PENALTY_THRESHOLD = 25.0
DEFAULT_ABUSIVE_PEER_EQUIVOCATION_PENALTY_THRESHOLD = 1.0
DEFAULT_ABUSIVE_PEER_BACKOFF_DURATION = 600.0


def log_gossipsub_peer_scores(
    gossipsub: GossipSub,
    pubsub: Pubsub | None,
    log_level: int = logging.DEBUG,
) -> None:
    """Log py-libp2p GossipSub scores and scoring counters for every known peer."""
    scorer = gossipsub.scorer
    if scorer is None:
        logger.log(log_level, "GossipSub peer scores: scorer unavailable")
        return

    try:
        current_time = time.time()
        topics = sorted({str(topic) for topic in gossipsub.mesh.keys()})
        peers: set[PeerID] = set(gossipsub.peer_protocol.keys())

        for topic_peers in gossipsub.mesh.values():
            peers.update(topic_peers)

        if pubsub is not None:
            peers.update(pubsub.peers.keys())
            topics.extend(topic for topic in pubsub.peer_topics.keys() if topic not in topics)
            for topic_peers in pubsub.peer_topics.values():
                peers.update(topic_peers)

        scorer_peer_maps = (
            "time_in_mesh",
            "first_message_deliveries",
            "mesh_message_deliveries",
            "invalid_messages",
            "behavior_penalty",
            "app_specific_scores",
            "graft_flood_penalties",
            "iwant_spam_penalties",
            "ihave_spam_penalties",
            "equivocation_penalties",
        )
        for map_name in scorer_peer_maps:
            peer_map = getattr(scorer, map_name, None)
            if peer_map is not None:
                peers.update(peer_map.keys())

        score_params = scorer.params
        logger.log(
            log_level,
            "GossipSub score params: publish_threshold=%s gossip_threshold=%s "
            "graylist_threshold=%s accept_px_threshold=%s p5_weight=%s "
            "p5_threshold=%s p5_decay=%s max_messages_per_topic_per_second=%s "
            "max_iwant_requests_per_second=%s max_ihave_messages_per_second=%s",
            score_params.publish_threshold,
            score_params.gossip_threshold,
            score_params.graylist_threshold,
            score_params.accept_px_threshold,
            score_params.p5_behavior_penalty_weight,
            score_params.p5_behavior_penalty_threshold,
            score_params.p5_behavior_penalty_decay,
            gossipsub.max_messages_per_topic_per_second,
            gossipsub.max_iwant_requests_per_second,
            gossipsub.max_ihave_messages_per_second,
        )

        peer_scores = []
        for peer_id in sorted(peers, key=str):
            topic_scores = {topic: scorer.score(peer_id, [topic]) for topic in topics}
            score_stats_by_topic = {topic: scorer.get_score_stats(peer_id, topic) for topic in topics}
            message_rate_limits = {
                topic: {
                    "last_1s": sum(1 for timestamp in timestamps if current_time - timestamp <= 1.0),
                    "last_60s": sum(1 for timestamp in timestamps if current_time - timestamp <= 60.0),
                    "tracked": len(timestamps),
                }
                for topic, timestamps in gossipsub.message_rate_limits.get(peer_id, {}).items()
            }
            ihave_rate_limits = {
                topic: {
                    "last_1s": sum(1 for timestamp in timestamps if current_time - timestamp <= 1.0),
                    "tracked": len(timestamps),
                }
                for topic, timestamps in gossipsub.ihave_message_limits.get(peer_id, {}).items()
            }
            iwant_rate_limits = {
                name: {
                    "last_1s": sum(1 for timestamp in timestamps if current_time - timestamp <= 1.0),
                    "tracked": len(timestamps),
                }
                for name, timestamps in gossipsub.iwant_request_limits.get(peer_id, {}).items()
            }
            subscribed_topics = (
                sorted(topic for topic, topic_peers in pubsub.peer_topics.items() if peer_id in topic_peers)
                if pubsub is not None
                else []
            )
            mesh_topics = sorted(topic for topic, topic_peers in gossipsub.mesh.items() if peer_id in topic_peers)
            peer_scores.append(
                {
                    "peer_id": str(peer_id),
                    "protocol": str(gossipsub.peer_protocol.get(peer_id)),
                    "connected_pubsub": pubsub is not None and peer_id in pubsub.peers,
                    "subscribed_topics": subscribed_topics,
                    "mesh_topics": mesh_topics,
                    "health_score_empty_topics": scorer.score(peer_id, []),
                    "all_topics_score": scorer.score(peer_id, topics),
                    "topic_scores": topic_scores,
                    "score_stats_by_topic": score_stats_by_topic,
                    "gates_by_topic": {
                        topic: {
                            "allow_publish": scorer.allow_publish(peer_id, [topic]),
                            "allow_gossip": scorer.allow_gossip(peer_id, [topic]),
                            "graylisted": scorer.is_graylisted(peer_id, [topic]),
                            "allow_px": scorer.allow_px_from(peer_id, [topic]),
                        }
                        for topic in topics
                    },
                    "rate_limits": {
                        "publish_messages": message_rate_limits,
                        "ihave": ihave_rate_limits,
                        "iwant": iwant_rate_limits,
                    },
                    "graft_flood_tracking": dict(gossipsub.graft_flood_tracking.get(peer_id, {})),
                }
            )

        logger.log(log_level, "GossipSub peer scores: %s", peer_scores)
    except Exception:
        logger.debug("Failed to log GossipSub peer scores", exc_info=True)


def _known_gossipsub_topics(gossipsub: GossipSub, pubsub: Pubsub | None) -> list[str]:
    topics = {str(topic) for topic in gossipsub.mesh.keys()}
    if pubsub is not None:
        topics.update(pubsub.peer_topics.keys())
    return sorted(topics)


def _known_gossipsub_peers(gossipsub: GossipSub, pubsub: Pubsub | None) -> set[PeerID]:
    peers: set[PeerID] = set(gossipsub.peer_protocol.keys())
    for topic_peers in gossipsub.mesh.values():
        peers.update(topic_peers)

    if pubsub is not None:
        peers.update(pubsub.peers.keys())
        for topic_peers in pubsub.peer_topics.values():
            peers.update(topic_peers)

    scorer = gossipsub.scorer
    if scorer is not None:
        for map_name in (
            "behavior_penalty",
            "graft_flood_penalties",
            "iwant_spam_penalties",
            "ihave_spam_penalties",
            "equivocation_penalties",
            "invalid_messages",
        ):
            peer_map = getattr(scorer, map_name, None)
            if peer_map is not None:
                peers.update(peer_map.keys())

    return peers


def _peer_topic_counter_total(counter_by_topic: object, peer_id: PeerID) -> float:
    if not hasattr(counter_by_topic, "get"):
        return 0.0

    peer_counters = counter_by_topic.get(peer_id, {})
    if not hasattr(peer_counters, "values"):
        return 0.0

    return float(sum(peer_counters.values()))


def find_abusive_gossipsub_peers(
    gossipsub: GossipSub | None,
    pubsub: Pubsub | None,
    *,
    score_threshold: float = DEFAULT_ABUSIVE_PEER_SCORE_THRESHOLD,
    behavior_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_BEHAVIOR_PENALTY_THRESHOLD,
    invalid_message_threshold: float = DEFAULT_ABUSIVE_PEER_INVALID_MESSAGE_THRESHOLD,
    control_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_CONTROL_PENALTY_THRESHOLD,
    equivocation_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_EQUIVOCATION_PENALTY_THRESHOLD,
    excluded_peers: set[PeerID] | None = None,
) -> dict[PeerID, list[str]]:
    """Return peers whose GossipSub counters exceed the local abuse policy."""
    if gossipsub is None or gossipsub.scorer is None:
        return {}

    scorer = gossipsub.scorer
    topics = _known_gossipsub_topics(gossipsub, pubsub)
    excluded_peers = excluded_peers or set()
    abusive_peers: dict[PeerID, list[str]] = {}

    for peer_id in _known_gossipsub_peers(gossipsub, pubsub):
        if peer_id in excluded_peers:
            continue

        reasons = []
        score = scorer.score(peer_id, topics)
        if score <= score_threshold:
            reasons.append(f"score={score} <= {score_threshold}")

        behavior_penalty = float(scorer.behavior_penalty.get(peer_id, 0.0))
        if behavior_penalty >= behavior_penalty_threshold:
            reasons.append(f"behavior_penalty={behavior_penalty} >= {behavior_penalty_threshold}")

        invalid_messages = _peer_topic_counter_total(scorer.invalid_messages, peer_id)
        if invalid_messages >= invalid_message_threshold:
            reasons.append(f"invalid_messages={invalid_messages} >= {invalid_message_threshold}")

        graft_flood_penalty = float(scorer.graft_flood_penalties.get(peer_id, 0.0))
        iwant_spam_penalty = float(scorer.iwant_spam_penalties.get(peer_id, 0.0))
        ihave_spam_penalty = float(scorer.ihave_spam_penalties.get(peer_id, 0.0))
        control_penalty = graft_flood_penalty + iwant_spam_penalty + ihave_spam_penalty
        if control_penalty >= control_penalty_threshold:
            reasons.append(
                f"control_penalty={control_penalty} >= {control_penalty_threshold} "
                f"(graft={graft_flood_penalty}, iwant={iwant_spam_penalty}, ihave={ihave_spam_penalty})"
            )

        equivocation_penalty = float(scorer.equivocation_penalties.get(peer_id, 0.0))
        if equivocation_penalty >= equivocation_penalty_threshold:
            reasons.append(f"equivocation_penalty={equivocation_penalty} >= {equivocation_penalty_threshold}")

        if reasons:
            abusive_peers[peer_id] = reasons

    return abusive_peers


def _expire_abusive_peer_backoffs(
    pubsub: Pubsub | None,
    abusive_peer_backoff_until: dict[PeerID, float],
    log_level: int,
) -> None:
    current_time = trio.current_time()
    for peer_id, blocked_until in list(abusive_peer_backoff_until.items()):
        if blocked_until > current_time:
            continue

        abusive_peer_backoff_until.pop(peer_id, None)
        if pubsub is not None and pubsub.is_peer_blacklisted(peer_id):
            pubsub.remove_from_blacklist(peer_id)
        logger.log(log_level, f"Abusive peer backoff expired for {peer_id}")


async def disconnect_abusive_gossipsub_peers(
    host: IHost,
    gossipsub: GossipSub | None,
    pubsub: Pubsub | None,
    dht: KadDHT | None,
    abusive_peer_backoff_until: dict[PeerID, float],
    *,
    local_peer_id: PeerID | None = None,
    score_threshold: float = DEFAULT_ABUSIVE_PEER_SCORE_THRESHOLD,
    behavior_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_BEHAVIOR_PENALTY_THRESHOLD,
    invalid_message_threshold: float = DEFAULT_ABUSIVE_PEER_INVALID_MESSAGE_THRESHOLD,
    control_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_CONTROL_PENALTY_THRESHOLD,
    equivocation_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_EQUIVOCATION_PENALTY_THRESHOLD,
    backoff_duration: float = DEFAULT_ABUSIVE_PEER_BACKOFF_DURATION,
    telemetry: Telemetry | None = None,
    log_level: int = logging.DEBUG,
) -> list[PeerID]:
    """Disconnect and temporarily blacklist peers with excessive GossipSub abuse counters."""
    _expire_abusive_peer_backoffs(pubsub, abusive_peer_backoff_until, log_level)

    excluded_peers = {local_peer_id} if local_peer_id is not None else set()
    abusive_peer_reasons = find_abusive_gossipsub_peers(
        gossipsub,
        pubsub,
        score_threshold=score_threshold,
        behavior_penalty_threshold=behavior_penalty_threshold,
        invalid_message_threshold=invalid_message_threshold,
        control_penalty_threshold=control_penalty_threshold,
        equivocation_penalty_threshold=equivocation_penalty_threshold,
        excluded_peers=excluded_peers,
    )
    if not abusive_peer_reasons:
        return []

    peers_to_disconnect = list(abusive_peer_reasons.keys())
    blocked_until = trio.current_time() + backoff_duration

    for peer_id, reasons in abusive_peer_reasons.items():
        abusive_peer_backoff_until[peer_id] = blocked_until
        if pubsub is not None:
            pubsub.add_to_blacklist(peer_id)
        logger.warning(
            "Disconnecting abusive GossipSub peer %s for %.1fs: %s",
            peer_id,
            backoff_duration,
            "; ".join(reasons),
        )
        if telemetry:
            await telemetry.emit_async(
                "gossipsub_abusive_peer_disconnected",
                peer_id=str(peer_id),
                backoff_seconds=backoff_duration,
                reasons=list(reasons),
            )

    await disconnect_peers(
        peers_to_disconnect,
        host,
        gossipsub,
        dht,
        log_level,
        reason="because GossipSub abuse counters exceeded the local safety policy",
    )
    return peers_to_disconnect


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
    telemetry: Telemetry | None = None,
    log_level: int = logging.DEBUG,
    abusive_peer_score_threshold: float = DEFAULT_ABUSIVE_PEER_SCORE_THRESHOLD,
    abusive_peer_behavior_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_BEHAVIOR_PENALTY_THRESHOLD,
    abusive_peer_invalid_message_threshold: float = DEFAULT_ABUSIVE_PEER_INVALID_MESSAGE_THRESHOLD,
    abusive_peer_control_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_CONTROL_PENALTY_THRESHOLD,
    abusive_peer_equivocation_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_EQUIVOCATION_PENALTY_THRESHOLD,
    abusive_peer_backoff_duration: float = DEFAULT_ABUSIVE_PEER_BACKOFF_DURATION,
) -> None:
    """Maintain connections to ensure the host remains connected to healthy peers."""
    my_peer_id = host.get_id()

    # Track retries for each peer, and next retry time
    peer_retries: dict[PeerID, int] = {}
    peer_next_retry: dict[PeerID, float] = {}
    abusive_peer_backoff_until: dict[PeerID, float] = {}

    while True:
        try:
            # Get all peers that are expected to be in the network
            onchain_peer_ids = await subnet_info_tracker.get_all_peer_ids(force=True)
            logger.log(log_level, f"All peer IDs: {onchain_peer_ids}")

            # Get all peers that are currently connected to the host
            connected_peers = host.get_connected_peers()
            logger.log(log_level, f"Host connected peers: {connected_peers}")

            # READ INFO
            transport_addrs = host.get_transport_addrs()
            logger.log(log_level, f"Host transport addrs: {transport_addrs}")

            list_peers = host.get_peerstore().peers_with_addrs()

            # Get all peers that are in the DHT
            if dht:
                peerstore_peer_ids = dht.host.get_peerstore().peer_ids()
                logger.log(log_level, f"DHT peerstore peer IDs: {peerstore_peer_ids}")
                for peer_id in dht.routing_table.get_peer_ids():
                    if peer_id not in list_peers:
                        list_peers.append(peer_id)

            all_peers = list(set(connected_peers + list_peers))
            for peer_id in all_peers:
                try:
                    peer_info = host.get_peerstore().peer_info(peer_id)
                    logger.log(log_level, f"Peer info addresses {peer_id}: {peer_info.addrs} \n\n")
                except Exception as e:
                    logger.debug(f"Failed to get peer info for {peer_id}: {e}", exc_info=True)

            remove_peers = []

            # Remove peers that are not in the onchain peer list and are connected
            # Use list copies to avoid issues with modification during iteration
            for peer_id in list(connected_peers):
                if peer_id not in onchain_peer_ids and not peer_id.__eq__(my_peer_id):
                    remove_peers.append(peer_id)
                    connected_peers.remove(peer_id)
                    peer_retries.pop(peer_id, None)
                    peer_next_retry.pop(peer_id, None)

            # Remove peers that are not in the onchain peer list and are in the peerstore
            for peer_id in list(list_peers):
                if peer_id not in onchain_peer_ids and not peer_id.__eq__(my_peer_id):
                    remove_peers.append(peer_id)
                    list_peers.remove(peer_id)
                    peer_retries.pop(peer_id, None)
                    peer_next_retry.pop(peer_id, None)

            # Deduplicate the removal list
            remove_peers = list(set(remove_peers))

            await disconnect_peers(
                remove_peers,
                host,
                gossipsub,
                dht,
                log_level,
            )

            disconnected_abusive_peers = await disconnect_abusive_gossipsub_peers(
                host,
                gossipsub,
                pubsub,
                dht,
                abusive_peer_backoff_until,
                local_peer_id=my_peer_id,
                score_threshold=abusive_peer_score_threshold,
                behavior_penalty_threshold=abusive_peer_behavior_penalty_threshold,
                invalid_message_threshold=abusive_peer_invalid_message_threshold,
                control_penalty_threshold=abusive_peer_control_penalty_threshold,
                equivocation_penalty_threshold=abusive_peer_equivocation_penalty_threshold,
                backoff_duration=abusive_peer_backoff_duration,
                telemetry=telemetry,
                log_level=log_level,
            )
            for peer_id in disconnected_abusive_peers:
                if peer_id in connected_peers:
                    connected_peers.remove(peer_id)
                if peer_id in list_peers:
                    list_peers.remove(peer_id)
                peer_retries.pop(peer_id, None)
                peer_next_retry.pop(peer_id, None)

            logger.log(log_level, f"List peers: {list_peers}")

            if len(connected_peers) < 32:
                logger.log(log_level, "Reconnecting to maintain peer connections...")

                # Find compatible peers
                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if (
                            filter_compatible_peer_info(peer_info)
                            and trio.current_time() >= peer_next_retry.get(peer_id, 0)
                            and peer_id not in abusive_peer_backoff_until
                        ):
                            compatible_peers.append(peer_id)
                    except Exception as e:
                        logger.debug(f"Failed to get peer info for {peer_id}: {e}", exc_info=True)
                        continue

                # Connect to random subset of compatible peers
                if compatible_peers:
                    logger.log(log_level, f"Compatible peers: {compatible_peers}")
                    random_peers = random.sample(compatible_peers, min(64, len(compatible_peers)))
                    logger.log(log_level, f"Random peers: {random_peers}")
                    for peer_id in random_peers:
                        if peer_id not in connected_peers and peer_id in onchain_peer_ids:
                            try:
                                peer_retries[peer_id] = peer_retries.get(peer_id, 0) + 1
                                next_retry_time = trio.current_time() + connection_backoff_duration * (
                                    peer_retries.get(peer_id, 0) ** retry_multiplier
                                )
                                if next_retry_time > max_backoff_duration:
                                    next_retry_time = max_backoff_duration
                                peer_next_retry[peer_id] = next_retry_time

                                if dht and not dht.enable_random_walk:
                                    # Only call if random walk is disabled
                                    # Otherwise `find_peer` compete for the same resources
                                    with trio.move_on_after(5):
                                        peer_info = await dht.find_peer(peer_id)
                                        if not peer_info:
                                            continue

                                with trio.move_on_after(5):
                                    peer_info = host.get_peerstore().peer_info(peer_id)
                                    logger.log(log_level, f"Adding addresses for peer: {peer_id}")
                                    host.get_peerstore().add_addrs(peer_info.peer_id, peer_info.addrs, 300)
                                    logger.log(log_level, f"Connecting to peer: {peer_id}")
                                    await host.connect(peer_info)
                                    logger.log(log_level, f"Connected to peer: {peer_id}")
                            except Exception as e:
                                logger.debug(f"Failed to connect to {peer_id}: {e}", exc_info=True)

            if gossipsub:
                # Prune disconnected peers from gossipsub mesh
                mesh_peers = set()
                for topic_peers in gossipsub.mesh.values():
                    mesh_peers.update(topic_peers)

                logger.log(log_level, f"GossipSub mesh: {gossipsub.mesh}")

                gossipsub_topic = next((t for t in gossipsub.mesh if str(t) == "heartbeat"), None)

                topic_peers = set()
                if gossipsub_topic:
                    topic_peers = gossipsub.mesh[gossipsub_topic]
                    logger.log(log_level, f"Heartbeat number of peers: {len(topic_peers)}")
                    for peer_id in topic_peers:
                        logger.log(log_level, f"Heartbeat mesh peer: {peer_id}")

                    connected_peers = set(pubsub.peers.keys())
                    subscribed_peers = pubsub.peer_topics.get("heartbeat", set())
                    valid_mesh_peers = topic_peers & connected_peers & subscribed_peers
                    stale_peers = topic_peers - valid_mesh_peers

                    logger.log(log_level, f"Pubsub Connected peers: {connected_peers}")
                    logger.log(log_level, f"Pubsub Subscribed peers: {subscribed_peers}")
                    logger.log(log_level, f"Pubsub Valid mesh peers: {valid_mesh_peers}")
                    logger.log(log_level, f"Pubsub Stale mesh peers: {stale_peers}")

                    # if stale_peers:
                    #     logger.log(
                    #         log_level,
                    #         "Removing %d stale peers from mesh for topic %s: %s",
                    #         len(stale_peers),
                    #         gossipsub_topic,
                    #         stale_peers,
                    #     )
                    #     for peer in stale_peers:
                    #         gossipsub.mesh[gossipsub_topic].discard(peer)
                    #         if gossipsub.scorer is not None:
                    #             gossipsub.scorer.on_leave_mesh(peer, gossipsub_topic)

                logger.log(log_level, f"GossipSub network health score: {gossipsub.network_health_score}")
                log_gossipsub_peer_scores(gossipsub, pubsub, log_level)

                ###

                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if filter_compatible_peer_info(peer_info):
                            compatible_peers.append(peer_id)
                    except Exception as e:
                        logger.debug(f"Failed to get peer info for {peer_id}: {e}", exc_info=True)
                        continue
                random_peers = random.sample(compatible_peers, min(64, len(compatible_peers)))
                random_peers = [
                    p
                    for p in random_peers
                    if p in onchain_peer_ids and p not in topic_peers and p not in abusive_peer_backoff_until
                ]

                await maintain_gossipsub_connections(random_peers, host, gossipsub, pubsub, log_level)

            await trio.sleep(60)
        except Exception as e:
            logger.error(f"Error maintaining connections: {e}", exc_info=True)


async def maintain_gossipsub_connections(
    peer_ids: list[PeerID],
    host: IHost,
    gossipsub: GossipSub,
    pubsub: Pubsub,
    log_level: int = logging.DEBUG,
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
            log_level,
        )


async def maintain_single_gossipsub_connection(
    peer_id: PeerID,
    host: IHost,
    gossipsub: GossipSub,
    pubsub: Pubsub,
    log_level: int = logging.DEBUG,
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
            logger.debug(
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
                logger.log(log_level, f"Skipping blacklisted GossipSub peer: {peer_id}")
                return

        # 4. Verify existing connection
        # Make sure peer is not already in /meshsub/1.0.0 before attempting to add to gossipsub # noqa: E501
        if gossipsub.peer_protocol.get(peer_id) == GOSSIPSUB_PROTOCOL_ID:
            logger.debug(
                f"maintain_single_gossipsub_connection, Peer {peer_id} is already in {GOSSIPSUB_PROTOCOL_ID}"  # noqa: E501
            )
            return

        gossipsub.add_peer(peer_id, GOSSIPSUB_PROTOCOL_ID)
        logger.log(log_level, f"maintain_single_gossipsub_connection, Added peer to gossipsub: {peer_id}")
    except Exception as e:
        logger.warning(f"maintain_single_gossipsub_connection, Failed to add peer {peer_id}: {e}")


async def disconnect_peers(
    peer_ids: list[PeerID],
    host: IHost,
    gossipsub: GossipSub | None = None,
    dht: KadDHT | None = None,
    log_level: int = logging.DEBUG,
    reason: str = "because they are no longer registered on-chain",
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
            logger.log(log_level, f"Fully cleared peer {peer_id} from peerstore {reason}")
        except Exception as e:
            logger.debug(f"Failed to disconnect peer {peer_id}: {e}", exc_info=True)
        if dht:
            # Remove from DHT routing table
            try:
                dht.routing_table.remove_peer(peer_id)
                dht.host.get_peerstore().clear_peerdata(peer_id)
                logger.log(log_level, f"Removed peer {peer_id} from routing table and cleared peerdata")
            except Exception as e:
                logger.debug(f"Failed to remove peer {peer_id}: {e}", exc_info=True)
        if gossipsub:
            # Remove from GossipSub
            try:
                gossipsub.remove_peer(peer_id)
                logger.log(log_level, f"Removed peer {peer_id} from gossipsub {reason}")
            except Exception as e:
                logger.debug(f"Failed to remove peer {peer_id}: {e}", exc_info=True)


async def demonstrate_random_walk_discovery(dht: KadDHT, interval: int = 30) -> None:
    """Demonstrate Random Walk peer discovery with periodic statistics."""
    while True:
        logger.info(f"Routing table size: {dht.get_routing_table_size()}")
        logger.info(f"Connected peers: {len(dht.host.get_connected_peers())}")
        logger.info(f"Peerstore size: {len(dht.host.get_peerstore().peer_ids())}")

        if dht.get_routing_table_size() > 0:
            logger.info("Peers in routing table:")
            for peer_id in dht.routing_table.get_peer_ids():
                logger.info(f"  {peer_id}")

        await trio.sleep(interval)


async def basic_maintain_connections(
    host: IHost,
    telemetry: Telemetry | None = None,
    log_level: int = logging.DEBUG,
    gossipsub: GossipSub | None = None,
    pubsub: Pubsub | None = None,
    dht: KadDHT | None = None,
    abusive_peer_score_threshold: float = DEFAULT_ABUSIVE_PEER_SCORE_THRESHOLD,
    abusive_peer_behavior_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_BEHAVIOR_PENALTY_THRESHOLD,
    abusive_peer_invalid_message_threshold: float = DEFAULT_ABUSIVE_PEER_INVALID_MESSAGE_THRESHOLD,
    abusive_peer_control_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_CONTROL_PENALTY_THRESHOLD,
    abusive_peer_equivocation_penalty_threshold: float = DEFAULT_ABUSIVE_PEER_EQUIVOCATION_PENALTY_THRESHOLD,
    abusive_peer_backoff_duration: float = DEFAULT_ABUSIVE_PEER_BACKOFF_DURATION,
) -> None:
    """Maintain connections to ensure the host remains connected to healthy peers."""
    my_peer_id = host.get_id()
    abusive_peer_backoff_until: dict[PeerID, float] = {}

    while True:
        try:
            connected_peers = host.get_connected_peers()
            list_peers = host.get_peerstore().peers_with_addrs()

            logger.log(log_level, f"Connected peers: {connected_peers}")
            logger.log(log_level, f"List peers:      {list_peers}")

            all_peers = list(set(connected_peers + list_peers))
            for peer_id in all_peers:
                try:
                    peer_info = host.get_peerstore().peer_info(peer_id)
                    logger.log(log_level, f"Peer info addresses {peer_id}: {peer_info.addrs} \n\n")
                except Exception as e:
                    logger.debug(f"Failed to get peer info for {peer_id}: {e}", exc_info=True)

            disconnected_abusive_peers = await disconnect_abusive_gossipsub_peers(
                host,
                gossipsub,
                pubsub,
                dht,
                abusive_peer_backoff_until,
                local_peer_id=my_peer_id,
                score_threshold=abusive_peer_score_threshold,
                behavior_penalty_threshold=abusive_peer_behavior_penalty_threshold,
                invalid_message_threshold=abusive_peer_invalid_message_threshold,
                control_penalty_threshold=abusive_peer_control_penalty_threshold,
                equivocation_penalty_threshold=abusive_peer_equivocation_penalty_threshold,
                backoff_duration=abusive_peer_backoff_duration,
                telemetry=telemetry,
                log_level=log_level,
            )
            for peer_id in disconnected_abusive_peers:
                if peer_id in connected_peers:
                    connected_peers.remove(peer_id)
                if peer_id in list_peers:
                    list_peers.remove(peer_id)

            if len(connected_peers) < 20:
                logger.log(log_level, "Reconnecting to maintain peer connections...")

                # Find compatible peers
                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if filter_compatible_peer_info(peer_info) and peer_id not in abusive_peer_backoff_until:
                            compatible_peers.append(peer_id)
                    except Exception:
                        continue

                # Connect to random subset of compatible peers
                if compatible_peers:
                    random_peers = random.sample(compatible_peers, min(50, len(compatible_peers)))
                    for peer_id in random_peers:
                        if peer_id not in connected_peers:
                            try:
                                with trio.move_on_after(5):
                                    peer_info = host.get_peerstore().peer_info(peer_id)
                                    await host.connect(peer_info)
                                    logger.log(log_level, f"Connected to peer: {peer_id}")

                                    if telemetry:
                                        await telemetry.emit_async("peer_connected", peer_id=peer_id)
                            except Exception as e:
                                logger.debug(f"Failed to connect to {peer_id}: {e}")

            await trio.sleep(15)
        except Exception as e:
            logger.error(f"Error maintaining connections: {e}")
