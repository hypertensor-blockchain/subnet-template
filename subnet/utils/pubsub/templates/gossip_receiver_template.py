"""
Small py-libp2p GossipSub receiver template.

Developers only need to provide one ``GossipTopicConfig`` per topic:
the topic string, a message handler, and a py-libp2p topic validator.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import inspect
import logging
from typing import TypeAlias, cast

from libp2p.abc import ISubscriptionAPI
from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
from libp2p.pubsub.pubsub import Pubsub, ValidatorFn
import trio

logger = logging.getLogger(__name__)

GossipMessageHandler: TypeAlias = Callable[[ID, rpc_pb2.Message], Awaitable[None] | None]


def allow_all_validator(_peer_id: ID, _message: rpc_pb2.Message) -> bool:
    """Accept every message for topics that do not need custom validation."""
    return True


@dataclass(frozen=True, slots=True)
class GossipTopicConfig:
    """
    Configuration for one GossipSub topic.

    Args:
        topic: Topic string passed to ``Pubsub.subscribe``.
        topic_handler: Function called as ``handler(from_peer_id, message)``.
            Handlers may be synchronous or asynchronous.
        topic_validator: py-libp2p topic validator called by ``Pubsub`` before
            messages are delivered to subscribers.
        is_async_topic_validator: Set to ``True`` when ``topic_validator`` is an
            async validator.

    """

    topic: str
    topic_handler: GossipMessageHandler
    topic_validator: ValidatorFn | None
    is_async_topic_validator: bool = False


class GossipReceiverTemplate:
    """
    Register validators, subscribe to topics, and dispatch gossip messages.

    Use this template when you already have a configured py-libp2p ``Pubsub``
    instance and want one receive loop for one or more GossipSub topics. Provide
    one ``GossipTopicConfig`` per topic. Each config supplies:

    * ``topic``: the GossipSub topic string to subscribe to.
    * ``topic_handler``: a sync or async callable that receives
      ``(from_peer_id, message)`` after validation.
    * ``topic_validator``: a py-libp2p validator called before delivery. Use
      ``allow_all_validator`` for topics that do not need custom validation.

    Example:
        async def handle_vote(from_peer_id: ID, message: rpc_pb2.Message) -> None:
            payload = message.data.decode("utf-8")
            ...

        def validate_vote(from_peer_id: ID, message: rpc_pb2.Message) -> bool:
            return bool(message.data)

        termination_event = trio.Event()
        receiver = GossipReceiverTemplate(
            pubsub=pubsub,
            termination_event=termination_event,
            topics_config=[
                GossipTopicConfig(
                    topic="votes",
                    topic_handler=handle_vote,
                    topic_validator=validate_vote,
                ),
                GossipTopicConfig(
                    topic="peer_state",
                    topic_handler=handle_peer_state,
                    topic_validator=None,
                ),
            ],
        )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(receiver.run)
            ...
            termination_event.set()

    Set ``is_async_topic_validator=True`` on ``GossipTopicConfig`` when the
    validator is async. Topic strings must be non-empty and unique. When
    ``termination_event`` is set, ``run`` exits its receive loops and
    unsubscribes from every topic it subscribed to.

    """

    def __init__(
        self,
        pubsub: Pubsub,
        termination_event: trio.Event,
        topics_config: Sequence[GossipTopicConfig],
        *,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.topics_config = tuple(topics_config)
        self.log_level = log_level
        self._validate_topics_config()
        self._handlers: dict[str, GossipMessageHandler] = {
            config.topic: config.topic_handler for config in self.topics_config
        }
        self._subscribed_topics: set[str] = set()

    async def run(self) -> None:
        """
        Subscribe to configured topics and receive messages until shutdown.

        Example:
            ``nursery.start_soon(gossip_receiver.run)``

        """
        try:
            async with trio.open_nursery() as nursery:
                for config in self.topics_config:
                    if config.topic_validator is not None:
                        self.pubsub.set_topic_validator(
                            config.topic,
                            config.topic_validator,
                            is_async_validator=config.is_async_topic_validator,
                        )

                    subscription = await self.pubsub.subscribe(config.topic)
                    self._subscribed_topics.add(config.topic)
                    logger.log(self.log_level, "Subscribed to gossip topic '%s'", config.topic)
                    nursery.start_soon(self._receive_loop, config.topic, subscription)
        finally:
            await self._unsubscribe_all()

    async def _receive_loop(self, topic: str, subscription: ISubscriptionAPI) -> None:
        """Receive messages for one subscription."""
        logger.log(self.log_level, "Starting gossip receive loop for topic '%s'", topic)
        while not self.termination_event.is_set():
            try:
                message: rpc_pb2.Message | None = None
                with trio.move_on_after(1):
                    message = await subscription.get()
                if message is None:
                    continue

                await self._handle_message(message)
            except Exception:
                logger.exception("Error in gossip receive loop for topic '%s'", topic)
                await trio.sleep(1)

    async def _handle_message(self, message: rpc_pb2.Message) -> None:
        """Dispatch an incoming py-libp2p message to matching topic handlers."""
        from_peer_id = ID(message.from_id)
        matching_topics = [topic for topic in message.topicIDs if topic in self._handlers]
        if not matching_topics:
            logger.warning(
                "Ignoring gossip message from %s with no configured topic: %s",
                from_peer_id.to_string(),
                list(message.topicIDs),
            )
            return

        for topic in matching_topics:
            logger.log(
                self.log_level,
                "Dispatching gossip message from %s on topic '%s'",
                from_peer_id.to_string(),
                topic,
            )
            result = self._handlers[topic](from_peer_id, message)
            if inspect.isawaitable(result):
                await cast(Awaitable[None], result)

    def _validate_topics_config(self) -> None:
        if not self.topics_config:
            raise ValueError("GossipReceiverTemplate requires at least one GossipTopicConfig")

        seen_topics: set[str] = set()
        duplicate_topics: set[str] = set()
        for config in self.topics_config:
            if not config.topic:
                raise ValueError("GossipTopicConfig.topic must be a non-empty string")
            if config.topic in seen_topics:
                duplicate_topics.add(config.topic)
            seen_topics.add(config.topic)

        if duplicate_topics:
            duplicates = ", ".join(sorted(duplicate_topics))
            raise ValueError(f"Duplicate gossip topic configuration: {duplicates}")

    async def _unsubscribe_all(self) -> None:
        for topic in tuple(self._subscribed_topics):
            try:
                await self.pubsub.unsubscribe(topic)
            except Exception:
                logger.exception("Failed to unsubscribe from gossip topic '%s'", topic)
            finally:
                self._subscribed_topics.discard(topic)
