from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
import inspect
import logging
from typing import Any, TypeAlias, cast

from libp2p.peer.id import ID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
import trio

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TopicPeerValidation:
    """
    Result returned by a topic peer validator.

    ``allowed=False`` removes the peer from the configured topic only. The
    optional ``reason`` is used for logs and membership pass results.
    """

    allowed: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class TopicPeerRemoval:
    """Details about one peer removed from a restricted topic."""

    peer_id: ID
    reason: str | None
    was_in_pubsub_topic: bool
    was_in_mesh: bool
    was_in_fanout: bool
    prune_sent: bool


@dataclass(frozen=True, slots=True)
class TopicMembershipResult:
    """Summary for a single topic membership pass."""

    topic: str
    checked_peers: tuple[ID, ...]
    allowed_peers: tuple[ID, ...]
    removed_peers: tuple[TopicPeerRemoval, ...]
    failed_peers: tuple[ID, ...]


TopicPeerValidator: TypeAlias = Callable[
    [str, ID],
    bool | TopicPeerValidation | Awaitable[bool | TopicPeerValidation],
]


@dataclass(frozen=True, slots=True)
class TopicMembershipConfig:
    """
    Configuration for one restricted Pubsub/GossipSub topic.

    Args:
        topic: Topic string to gatekeep.
        validator: Optional sync or async callable. It receives
            ``(topic, peer_id)`` and returns ``bool`` or ``TopicPeerValidation``.
            Leave this unset when subclassing ``TopicMembershipTemplate`` and
            overriding ``validate_peer``.
        reason: Default reason used when a validator returns ``False`` instead
            of a ``TopicPeerValidation`` with its own reason.
        emit_prune: Emit GossipSub PRUNE to peers removed from the mesh.
        add_backoff: Add GossipSub backoff before PRUNE so repeated GRAFTs are
            rejected for the normal prune backoff window.
        remove_on_validation_error: Remove a peer if custom validation raises.
            Defaults to ``False`` to avoid churn during temporary policy store
            failures.
        log_level: Log level for successful removal messages.

    """

    topic: str
    validator: TopicPeerValidator | None = None
    reason: str | None = None
    emit_prune: bool = True
    add_backoff: bool = True
    remove_on_validation_error: bool = False
    log_level: int = logging.DEBUG

    def __post_init__(self) -> None:
        """Validate the topic gate configuration."""
        if not self.topic:
            raise ValueError("TopicMembershipConfig.topic must be a non-empty string")


class TopicMembershipTemplate:
    """
    Gatekeep peers for one or more Pubsub/GossipSub topics.

    py-libp2p lets any peer advertise a topic subscription. This template gives
    applications hooks to enforce local policy for restricted topics: roles,
    stake, allowlists, subnet membership, or any other rule.

    Example: This can be used to ensure only specific roles should be subscribed
    to a topic, i.e., miners, providers, validators, trainers, etc.

    Enforcement is intentionally topic-scoped. Invalid peers are removed from
    ``pubsub.peer_topics[topic]``, ``gossipsub.mesh[topic]``, and
    ``gossipsub.fanout[topic]``. If the peer was in the GossipSub mesh, the
    cleaner also adds GossipSub backoff and emits PRUNE so the peer stops
    participating in this node's mesh for that topic. It does not blacklist,
    disconnect, or prevent the peer from being valid on other topics. GossipSub
    direct peers are router-wide rather than topic-scoped, so remove them from
    ``gossipsub.direct_peers`` separately only when that global behavior is
    desired.

    Provide one config per restricted topic:

        async def validate_role(topic: str, peer_id: ID) -> TopicPeerValidation:
            role = await role_registry.role_for_peer(peer_id)
            return TopicPeerValidation(role == "validator", f"role={role}")

        membership = TopicMembershipTemplate(
            pubsub=pubsub,
            gossipsub=gossipsub,
            configs=[
                TopicMembershipConfig(
                    topic="validator_votes",
                    validator=validate_role,
                    reason="validator-only topic",
                ),
                TopicMembershipConfig(
                    topic="reveals",
                    validator=validate_revealer,
                    reason="revealer-only topic",
                ),
            ],
        )
        membership.install_hooks()

    Or subclass and override ``validate_peer`` when policy needs direct access
    to instance state. The override receives the config and peer being checked.
    """

    def __init__(
        self,
        pubsub: Pubsub,
        gossipsub: GossipSub,
        configs: Sequence[TopicMembershipConfig],
    ) -> None:
        """Create a topic gatekeeper for the provided Pubsub/GossipSub pair."""
        if not configs:
            raise ValueError("TopicMembershipTemplate requires at least one TopicMembershipConfig")

        self.pubsub = pubsub
        self.gossipsub = gossipsub
        self.configs = tuple(configs)
        self._configs_by_topic = self._build_configs_by_topic(self.configs)
        self._allowed_topic_peers: dict[str, set[ID]] = {config.topic: set() for config in self.configs}
        self._denied_topic_peers: dict[str, set[ID]] = {config.topic: set() for config in self.configs}
        self._original_handle_subscription: Callable[..., Any] | None = None
        self._original_push_msg: Callable[..., Awaitable[Any]] | None = None
        self._original_handle_graft: Callable[..., Any] | None = None
        self._original_get_peers_to_send: Callable[..., Iterable[ID]] | None = None
        self._original_get_in_topic_gossipsub_peers_from_minus: Callable[..., list[ID]] | None = None

    def install_hooks(self) -> None:
        """
        Install event-driven topic gates on the attached Pubsub/GossipSub pair.

        The subscription hook validates a peer as soon as it advertises a
        restricted-topic subscription. The GossipSub hooks keep denied or
        unvalidated peers out of mesh/fanout selection and reject restricted
        GRAFT attempts with PRUNE.
        """
        if self._original_handle_subscription is None:
            self._install_subscription_hook()
        if self._original_push_msg is None and hasattr(self.pubsub, "push_msg"):
            self._install_publish_gate_hook()
        if self._original_handle_graft is None and hasattr(self.gossipsub, "handle_graft"):
            self._install_graft_hook()
        if self._original_get_peers_to_send is None and hasattr(self.gossipsub, "_get_peers_to_send"):
            self._install_peer_send_filter_hook()
        if self._original_get_in_topic_gossipsub_peers_from_minus is None and hasattr(
            self.gossipsub,
            "_get_in_topic_gossipsub_peers_from_minus",
        ):
            self._install_topic_selection_filter_hook()

    def uninstall_hooks(self) -> None:
        """Restore Pubsub/GossipSub methods replaced by ``install_hooks``."""
        if self._original_handle_subscription is not None:
            self.pubsub.handle_subscription = self._original_handle_subscription  # type: ignore[method-assign]
            self._original_handle_subscription = None
        if self._original_push_msg is not None:
            self.pubsub.push_msg = self._original_push_msg  # type: ignore[method-assign]
            self._original_push_msg = None
        if self._original_handle_graft is not None:
            self.gossipsub.handle_graft = self._original_handle_graft  # type: ignore[method-assign]
            self._original_handle_graft = None
        if self._original_get_peers_to_send is not None:
            self.gossipsub._get_peers_to_send = self._original_get_peers_to_send  # type: ignore[attr-defined]
            self._original_get_peers_to_send = None
        if self._original_get_in_topic_gossipsub_peers_from_minus is not None:
            self.gossipsub._get_in_topic_gossipsub_peers_from_minus = (  # type: ignore[attr-defined]
                self._original_get_in_topic_gossipsub_peers_from_minus
            )
            self._original_get_in_topic_gossipsub_peers_from_minus = None

    async def cleanup(self) -> tuple[TopicMembershipResult, ...]:
        """Run one membership pass for every configured topic."""
        results: list[TopicMembershipResult] = []
        for config in self.configs:
            results.append(await self.cleanup_topic(config.topic))
        return tuple(results)

    async def cleanup_topic(self, topic: str) -> TopicMembershipResult:
        """Run one membership pass for a single configured topic."""
        try:
            config = self._configs_by_topic[topic]
        except KeyError as error:
            raise ValueError(f"No TopicMembershipConfig registered for topic '{topic}'") from error

        return await self._cleanup_config(config)

    def config_for_topic(self, topic: str) -> TopicMembershipConfig:
        """Return the membership config for ``topic``."""
        try:
            return self._configs_by_topic[topic]
        except KeyError as error:
            raise ValueError(f"No TopicMembershipConfig registered for topic '{topic}'") from error

    def is_restricted_topic(self, topic: str) -> bool:
        """Return whether this template gatekeeps ``topic``."""
        return topic in self._configs_by_topic

    def get_topic_peers(self, topic: str) -> set[ID]:
        """
        Return peers known locally for ``topic`` across Pubsub and GossipSub.

        Pubsub tracks advertised subscriptions, while GossipSub separately
        tracks mesh and fanout peers used for forwarding.
        """
        peers: set[ID] = set(self.pubsub.peer_topics.get(topic, set()))
        peers.update(self.gossipsub.mesh.get(topic, set()))
        peers.update(self.gossipsub.fanout.get(topic, set()))
        return peers

    async def validate_peer(self, config: TopicMembershipConfig, peer_id: ID) -> bool | TopicPeerValidation:
        """
        Decide whether ``peer_id`` may stay in ``config.topic``.

        Subclasses can override this method. The default implementation calls
        the validator on the given config.
        """
        if config.validator is None:
            raise NotImplementedError(
                "Override validate_peer() or set validator= on every TopicMembershipConfig"
            )

        result = config.validator(config.topic, peer_id)
        if inspect.isawaitable(result):
            return await cast(Awaitable[bool | TopicPeerValidation], result)
        return result

    async def remove_peer_from_topic(
        self,
        topic: str,
        peer_id: ID,
        *,
        reason: str | None = None,
        emit_prune: bool = True,
        add_backoff: bool = True,
        force_prune: bool = False,
        log_level: int = logging.DEBUG,
    ) -> TopicPeerRemoval:
        """
        Remove a peer from local Pubsub/GossipSub state for this topic only.

        If the peer was in the mesh, PRUNE is emitted after local state and
        backoff are updated. ``force_prune`` also emits PRUNE when rejecting a
        GRAFT before the peer has been added to the local mesh.
        """
        topic_peers = self.pubsub.peer_topics.get(topic)
        mesh_peers = self.gossipsub.mesh.get(topic)
        fanout_peers = self.gossipsub.fanout.get(topic)

        was_in_pubsub_topic = topic_peers is not None and peer_id in topic_peers
        was_in_mesh = mesh_peers is not None and peer_id in mesh_peers
        was_in_fanout = fanout_peers is not None and peer_id in fanout_peers

        if topic_peers is not None:
            topic_peers.discard(peer_id)
        if mesh_peers is not None:
            mesh_peers.discard(peer_id)
        if fanout_peers is not None:
            fanout_peers.discard(peer_id)

        should_prune = was_in_mesh or force_prune

        if was_in_mesh:
            self._notify_scorer_peer_left(peer_id, topic)
        if should_prune:
            self._add_gossipsub_backoff(peer_id, topic, add_backoff)

        prune_sent = False
        if emit_prune and should_prune:
            try:
                await self.gossipsub.emit_prune(topic, peer_id, do_px=False, is_unsubscribe=False)
                prune_sent = True
            except Exception:
                logger.exception(
                    "Failed to emit PRUNE to peer %s for restricted topic '%s'",
                    peer_id.to_string(),
                    topic,
                )

        logger.log(
            log_level,
            "Removed peer %s from topic '%s'%s",
            peer_id.to_string(),
            topic,
            f": {reason}" if reason else "",
        )

        return TopicPeerRemoval(
            peer_id=peer_id,
            reason=reason,
            was_in_pubsub_topic=was_in_pubsub_topic,
            was_in_mesh=was_in_mesh,
            was_in_fanout=was_in_fanout,
            prune_sent=prune_sent,
        )

    async def enforce_peer(
        self,
        topic: str,
        peer_id: ID,
        *,
        force_prune: bool = False,
    ) -> TopicPeerRemoval | None:
        """
        Validate and enforce membership for one peer/topic pair.

        Returns the removal details when the peer is rejected, or ``None`` when
        the peer is allowed.
        """
        config = self.config_for_topic(topic)
        try:
            validation = await self._validate_peer(config, peer_id)
        except Exception:
            logger.exception(
                "Topic peer validation failed for peer %s on topic '%s'",
                peer_id.to_string(),
                config.topic,
            )
            self._mark_peer_denied(config.topic, peer_id)
            if not config.remove_on_validation_error and not force_prune:
                return None
            return await self.remove_peer_from_topic(
                config.topic,
                peer_id,
                reason="validation_error",
                emit_prune=config.emit_prune,
                add_backoff=config.add_backoff,
                force_prune=force_prune,
                log_level=config.log_level,
            )

        if validation.allowed:
            self._mark_peer_allowed(config.topic, peer_id)
            return None

        self._mark_peer_denied(config.topic, peer_id)
        return await self.remove_peer_from_topic(
            config.topic,
            peer_id,
            reason=validation.reason or config.reason,
            emit_prune=config.emit_prune,
            add_backoff=config.add_backoff,
            force_prune=force_prune,
            log_level=config.log_level,
        )

    async def _cleanup_config(self, config: TopicMembershipConfig) -> TopicMembershipResult:
        """Validate all locally known peers for one restricted topic."""
        checked_peers = tuple(sorted(self.get_topic_peers(config.topic), key=lambda peer_id: peer_id.to_string()))
        allowed_peers: list[ID] = []
        failed_peers: list[ID] = []
        removed_peers: list[TopicPeerRemoval] = []

        for peer_id in checked_peers:
            try:
                validation = await self._validate_peer(config, peer_id)
            except Exception:
                failed_peers.append(peer_id)
                logger.exception(
                    "Topic peer validation failed for peer %s on topic '%s'",
                    peer_id.to_string(),
                    config.topic,
                )
                if config.remove_on_validation_error:
                    removed_peers.append(
                        await self.remove_peer_from_topic(
                            config.topic,
                            peer_id,
                            reason="validation_error",
                            emit_prune=config.emit_prune,
                            add_backoff=config.add_backoff,
                            log_level=config.log_level,
                        )
                    )
                continue

            if validation.allowed:
                self._mark_peer_allowed(config.topic, peer_id)
                allowed_peers.append(peer_id)
                continue

            reason = validation.reason or config.reason
            self._mark_peer_denied(config.topic, peer_id)
            removed_peers.append(
                await self.remove_peer_from_topic(
                    config.topic,
                    peer_id,
                    reason=reason,
                    emit_prune=config.emit_prune,
                    add_backoff=config.add_backoff,
                    log_level=config.log_level,
                )
            )

        if removed_peers:
            logger.log(
                config.log_level,
                "Removed %d peer(s) from restricted topic '%s'",
                len(removed_peers),
                config.topic,
            )

        return TopicMembershipResult(
            topic=config.topic,
            checked_peers=checked_peers,
            allowed_peers=tuple(allowed_peers),
            removed_peers=tuple(removed_peers),
            failed_peers=tuple(failed_peers),
        )

    async def _validate_peer(self, config: TopicMembershipConfig, peer_id: ID) -> TopicPeerValidation:
        """Normalize a custom validator result into ``TopicPeerValidation``."""
        validation = await self.validate_peer(config, peer_id)
        if isinstance(validation, TopicPeerValidation):
            return validation
        return TopicPeerValidation(allowed=bool(validation))

    def _install_subscription_hook(self) -> None:
        """Patch Pubsub subscription handling to validate restricted-topic joins."""
        original = self.pubsub.handle_subscription
        self._original_handle_subscription = original

        def handle_subscription_with_topic_gate(origin_id: ID, sub_message: Any) -> None:
            """Record the peer subscription, then enforce topic membership."""
            original(origin_id, sub_message)
            topic = getattr(sub_message, "topicid", None)
            if not isinstance(topic, str) or not self.is_restricted_topic(topic):
                return
            if getattr(sub_message, "subscribe", False):
                self._schedule_peer_enforcement(topic, origin_id)
            else:
                self._mark_peer_unknown(topic, origin_id)

        self.pubsub.handle_subscription = handle_subscription_with_topic_gate  # type: ignore[method-assign]

    def _install_publish_gate_hook(self) -> None:
        """Patch inbound publish handling to drop denied restricted-topic messages."""
        original = self.pubsub.push_msg
        self._original_push_msg = original

        async def push_msg_with_topic_gate(msg_forwarder: ID, msg: Any) -> Any:
            """Drop messages from peers denied for any message topic."""
            for topic in tuple(getattr(msg, "topicIDs", ())):
                if await self._should_drop_message_from_peer(topic, msg_forwarder):
                    logger.log(
                        self.config_for_topic(topic).log_level,
                        "Dropping restricted-topic message from peer %s on topic '%s'",
                        msg_forwarder.to_string(),
                        topic,
                    )
                    return None
            return await original(msg_forwarder, msg)

        self.pubsub.push_msg = push_msg_with_topic_gate  # type: ignore[method-assign]

    def _install_graft_hook(self) -> None:
        """Patch GossipSub GRAFT handling to reject denied mesh entries."""
        original = self.gossipsub.handle_graft
        self._original_handle_graft = original

        async def handle_graft_with_topic_gate(graft_msg: Any, sender_peer_id: ID) -> None:
            """Validate restricted-topic GRAFTs before GossipSub accepts them."""
            topic = getattr(graft_msg, "topicID", None)
            if isinstance(topic, str) and self.is_restricted_topic(topic):
                removal = await self.enforce_peer(topic, sender_peer_id, force_prune=True)
                if removal is not None:
                    return
            await original(graft_msg, sender_peer_id)

        self.gossipsub.handle_graft = handle_graft_with_topic_gate  # type: ignore[method-assign]

    def _install_peer_send_filter_hook(self) -> None:
        """Patch GossipSub outbound peer selection to exclude denied peers."""
        original = self.gossipsub._get_peers_to_send
        self._original_get_peers_to_send = original

        def get_peers_to_send_with_topic_gate(
            topic_ids: Iterable[str],
            msg_forwarder: ID,
            origin: ID,
            msg_id: bytes | None = None,
        ) -> Iterable[ID]:
            """Yield only peers allowed for all selected outbound topics."""
            topics = tuple(topic_ids)
            for peer_id in original(topics, msg_forwarder, origin, msg_id):
                if self._can_send_to_peer_for_topics(peer_id, topics):
                    yield peer_id

        self.gossipsub._get_peers_to_send = get_peers_to_send_with_topic_gate  # type: ignore[attr-defined]

    def _install_topic_selection_filter_hook(self) -> None:
        """Patch mesh/fanout candidate selection to require topic approval."""
        original = self.gossipsub._get_in_topic_gossipsub_peers_from_minus
        self._original_get_in_topic_gossipsub_peers_from_minus = original

        def get_in_topic_gossipsub_peers_from_minus_with_topic_gate(
            topic: str,
            num_to_select: int,
            minus: Iterable[ID],
            backoff_check: bool = False,
        ) -> list[ID]:
            """Return only approved candidates for restricted-topic selection."""
            peers = original(topic, num_to_select, minus, backoff_check)
            if not self.is_restricted_topic(topic):
                return peers
            return [peer_id for peer_id in peers if self._can_select_peer_for_topic(topic, peer_id)]

        self.gossipsub._get_in_topic_gossipsub_peers_from_minus = (  # type: ignore[attr-defined]
            get_in_topic_gossipsub_peers_from_minus_with_topic_gate
        )

    def _schedule_peer_enforcement(self, topic: str, peer_id: ID) -> None:
        """Schedule async validation after a restricted-topic subscription."""
        manager = getattr(self.pubsub, "manager", None)
        run_task = getattr(manager, "run_task", None)
        if run_task is not None:
            run_task(self._enforce_peer_safe, topic, peer_id)
            return

        try:
            trio.lowlevel.spawn_system_task(self._enforce_peer_safe, topic, peer_id)
        except RuntimeError:
            logger.warning(
                "Could not schedule topic membership validation for peer %s on topic '%s'",
                peer_id.to_string(),
                topic,
            )

    async def _enforce_peer_safe(self, topic: str, peer_id: ID) -> None:
        """Run peer enforcement from hooks without letting errors escape."""
        try:
            await self.enforce_peer(topic, peer_id)
        except Exception:
            logger.exception(
                "Topic membership hook failed for peer %s on topic '%s'",
                peer_id.to_string(),
                topic,
            )

    def _can_send_to_peer_for_topics(self, peer_id: ID, topics: Iterable[str]) -> bool:
        """Return whether a peer may receive gossip for every topic."""
        return all(self._can_select_peer_for_topic(topic, peer_id) for topic in topics)

    def _can_select_peer_for_topic(self, topic: str, peer_id: ID) -> bool:
        """Return whether a peer may be selected for one topic."""
        if not self.is_restricted_topic(topic):
            return True
        if self._is_local_peer(peer_id):
            return True
        if peer_id in self._denied_topic_peers[topic]:
            return False
        return peer_id in self._allowed_topic_peers[topic]

    async def _should_drop_message_from_peer(self, topic: str, peer_id: ID) -> bool:
        """Return whether an inbound message should be dropped for this topic."""
        if not self.is_restricted_topic(topic) or self._is_local_peer(peer_id):
            return False
        if peer_id in self._denied_topic_peers[topic]:
            return True
        if peer_id in self._allowed_topic_peers[topic]:
            return False
        removal = await self.enforce_peer(topic, peer_id)
        return removal is not None or peer_id not in self._allowed_topic_peers[topic]

    def _is_local_peer(self, peer_id: ID) -> bool:
        """Return whether ``peer_id`` is the local Pubsub peer."""
        my_id = getattr(self.pubsub, "my_id", None)
        try:
            return my_id == peer_id
        except Exception:
            return False

    def _mark_peer_allowed(self, topic: str, peer_id: ID) -> None:
        """Cache that a peer is allowed for a restricted topic."""
        self._allowed_topic_peers[topic].add(peer_id)
        self._denied_topic_peers[topic].discard(peer_id)

    def _mark_peer_denied(self, topic: str, peer_id: ID) -> None:
        """Cache that a peer is denied for a restricted topic."""
        self._denied_topic_peers[topic].add(peer_id)
        self._allowed_topic_peers[topic].discard(peer_id)

    def _mark_peer_unknown(self, topic: str, peer_id: ID) -> None:
        """Clear cached membership state for a peer/topic pair."""
        self._allowed_topic_peers[topic].discard(peer_id)
        self._denied_topic_peers[topic].discard(peer_id)

    def _add_gossipsub_backoff(self, peer_id: ID, topic: str, add_backoff: bool) -> None:
        """Add GossipSub topic backoff for a rejected peer when enabled."""
        if not add_backoff:
            return

        add_back_off = getattr(self.gossipsub, "_add_back_off", None)
        if add_back_off is None:
            return

        try:
            add_back_off(peer_id, topic, False)
        except Exception:
            logger.exception(
                "Failed to add GossipSub backoff for peer %s on topic '%s'",
                peer_id.to_string(),
                topic,
            )

    def _notify_scorer_peer_left(self, peer_id: ID, topic: str) -> None:
        """Notify GossipSub scoring that a peer left a topic mesh."""
        scorer = getattr(self.gossipsub, "scorer", None)
        if scorer is None:
            return

        on_leave_mesh = getattr(scorer, "on_leave_mesh", None)
        if on_leave_mesh is None:
            return

        try:
            on_leave_mesh(peer_id, topic)
        except Exception:
            logger.exception(
                "Failed to notify GossipSub scorer that peer %s left topic '%s'",
                peer_id.to_string(),
                topic,
            )

    @staticmethod
    def _build_configs_by_topic(configs: Sequence[TopicMembershipConfig]) -> dict[str, TopicMembershipConfig]:
        """Index configs by topic and reject duplicate topic entries."""
        configs_by_topic: dict[str, TopicMembershipConfig] = {}
        duplicate_topics: set[str] = set()

        for config in configs:
            if config.topic in configs_by_topic:
                duplicate_topics.add(config.topic)
            configs_by_topic[config.topic] = config

        if duplicate_topics:
            duplicates = ", ".join(sorted(duplicate_topics))
            raise ValueError(f"Duplicate topic membership configuration: {duplicates}")

        return configs_by_topic
