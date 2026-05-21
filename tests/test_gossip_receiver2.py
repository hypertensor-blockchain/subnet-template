from __future__ import annotations

from collections.abc import Callable

import pytest
from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
import trio

from subnet.utils.pubsub.templates.gossip_receiver_template import (
    GossipReceiverTemplate,
    GossipTopicConfig,
    allow_all_validator,
)


class FakeSubscription:
    def __init__(self, messages: list[rpc_pb2.Message]):
        self._messages = messages

    async def get(self) -> rpc_pb2.Message:
        if self._messages:
            return self._messages.pop(0)

        await trio.sleep_forever()
        raise AssertionError("unreachable")


class FakePubsub:
    def __init__(self, subscriptions: dict[str, FakeSubscription]):
        self.subscriptions = subscriptions
        self.validators: list[tuple[str, Callable[..., object], bool]] = []
        self.subscribed_topics: list[str] = []
        self.unsubscribed_topics: list[str] = []

    def set_topic_validator(self, topic: str, validator: Callable[..., object], is_async_validator: bool) -> None:
        self.validators.append((topic, validator, is_async_validator))

    async def subscribe(self, topic: str) -> FakeSubscription:
        self.subscribed_topics.append(topic)
        return self.subscriptions[topic]

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed_topics.append(topic)


def _message(topic: str, data: bytes = b"hello") -> rpc_pb2.Message:
    return rpc_pb2.Message(from_id=b"peer-1", data=data, topicIDs=[topic])


def test_topic_config_defaults_to_sync_validator() -> None:
    config = GossipTopicConfig("topic-a", lambda _peer_id, _message: None, allow_all_validator)

    assert config.topic == "topic-a"
    assert config.is_async_topic_validator is False


@pytest.mark.asyncio
async def test_handle_message_dispatches_to_async_topic_handler() -> None:
    received: list[tuple[str, bytes]] = []

    async def handler(peer_id: ID, message: rpc_pb2.Message) -> None:
        received.append((peer_id.to_string(), message.data))

    receiver = GossipReceiverTemplate(
        pubsub=FakePubsub({}),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        topics_config=[GossipTopicConfig("topic-a", handler, allow_all_validator)],
    )

    await receiver._handle_message(_message("topic-a", b"payload"))

    assert received == [(ID(b"peer-1").to_string(), b"payload")]


@pytest.mark.asyncio
async def test_handle_message_dispatches_to_sync_topic_handler() -> None:
    received: list[tuple[str, bytes]] = []

    def handler(peer_id: ID, message: rpc_pb2.Message) -> None:
        received.append((peer_id.to_string(), message.data))

    receiver = GossipReceiverTemplate(
        pubsub=FakePubsub({}),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        topics_config=[GossipTopicConfig("topic-a", handler, allow_all_validator)],
    )

    await receiver._handle_message(_message("topic-a", b"sync-payload"))

    assert received == [(ID(b"peer-1").to_string(), b"sync-payload")]


@pytest.mark.asyncio
async def test_handle_message_ignores_unconfigured_topics() -> None:
    called = False

    def handler(_peer_id: ID, _message: rpc_pb2.Message) -> None:
        nonlocal called
        called = True

    receiver = GossipReceiverTemplate(
        pubsub=FakePubsub({}),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        topics_config=[GossipTopicConfig("topic-a", handler, allow_all_validator)],
    )

    await receiver._handle_message(_message("topic-b"))

    assert called is False


@pytest.mark.trio
async def test_run_registers_validators_subscribes_dispatches_and_unsubscribes() -> None:
    termination_event = trio.Event()
    received: list[tuple[str, bytes]] = []

    async def handler(peer_id: ID, message: rpc_pb2.Message) -> None:
        received.append((peer_id.to_string(), message.data))
        termination_event.set()

    pubsub = FakePubsub({"topic-a": FakeSubscription([_message("topic-a", b"run-payload")])})
    config = GossipTopicConfig("topic-a", handler, allow_all_validator, is_async_topic_validator=False)
    receiver = GossipReceiverTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=termination_event,
        topics_config=[config],
    )

    await receiver.run()

    assert pubsub.validators == [("topic-a", allow_all_validator, False)]
    assert pubsub.subscribed_topics == ["topic-a"]
    assert pubsub.unsubscribed_topics == ["topic-a"]
    assert received == [(ID(b"peer-1").to_string(), b"run-payload")]


def test_receiver_rejects_duplicate_topics() -> None:
    with pytest.raises(ValueError, match="Duplicate gossip topic configuration: topic-a"):
        GossipReceiverTemplate(
            pubsub=FakePubsub({}),  # type: ignore[arg-type]
            termination_event=trio.Event(),
            topics_config=[
                GossipTopicConfig("topic-a", lambda _peer_id, _message: None, allow_all_validator),
                GossipTopicConfig("topic-a", lambda _peer_id, _message: None, allow_all_validator),
            ],
        )
