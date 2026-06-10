from __future__ import annotations

from collections.abc import Callable

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
import trio

from subnet.merkle_dag import (
    DagInventoryRequest,
    DagInventoryResponse,
    DagNodeGossip,
    InMemoryDagStorage,
    Libp2pKeyPairSigner,
    NodeIngestResult,
    NodeIngestStatus,
)
from subnet.merkle_dag.bases.gossip_dag_receiver import (
    DagGossipSubReceiver,
    GossipDagTopicConfig,
    GossipDagTopicContext,
)
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.payloads import MappingPayloadSchema
from subnet.merkle_dag.runtime import MerkleDagRuntime


class CounterPayloadSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("counter")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("value"), int):
            raise PayloadValidationError("counter payload requires an integer 'value'")


class DummyDB:
    def __init__(self):
        self.nested: dict[str, dict[str, object]] = {}

    def get_nested(self, key: str, nested_key: str):
        return self.nested.get(key, {}).get(nested_key)

    def set_nested(self, key: str, nested_key: str, value: object) -> None:
        self.nested.setdefault(key, {})[nested_key] = value


class FakeSubscription:
    def __init__(self, messages: list[rpc_pb2.Message]):
        self._messages = messages

    async def get(self) -> rpc_pb2.Message:
        if self._messages:
            return self._messages.pop(0)

        await trio.sleep_forever()
        raise AssertionError("unreachable")


class FakePubsub:
    def __init__(self, subscriptions: dict[str, FakeSubscription] | None = None):
        self.subscriptions = subscriptions or {}
        self.validators: list[tuple[str, Callable[..., object], bool]] = []
        self.subscribed_topics: list[str] = []
        self.unsubscribed_topics: list[str] = []
        self.published: list[tuple[str, bytes]] = []

    def set_topic_validator(self, topic: str, validator: Callable[..., object], is_async_validator: bool) -> None:
        self.validators.append((topic, validator, is_async_validator))

    async def subscribe(self, topic: str) -> FakeSubscription:
        self.subscribed_topics.append(topic)
        return self.subscriptions[topic]

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed_topics.append(topic)

    async def publish(self, topic: str, payload: bytes) -> None:
        self.published.append((topic, payload))


def _key_pair(seed_byte: int):
    return create_new_key_pair(bytes([seed_byte]) * 32)


def _peer_id(seed_byte: int) -> ID:
    return ID.from_pubkey(_key_pair(seed_byte).public_key)


async def _node_gossip(namespace: str, topic: str, seed_byte: int, value: int) -> tuple[ID, bytes, str]:
    key_pair = _key_pair(seed_byte)
    peer_id = ID.from_pubkey(key_pair.public_key)
    runtime = MerkleDagRuntime(
        db=DummyDB(),
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=peer_id,
        namespace=namespace,
        dag_topic=topic,
        storage=InMemoryDagStorage(),
    )
    node = await runtime.dag.create_node(
        schema_id="counter",
        payload={"value": value},
        parent_ids=(),
        signer=Libp2pKeyPairSigner(key_pair),
        author=peer_id.to_string(),
        created_at_ms=1000 + value,
    )
    gossip = DagNodeGossip(
        message_id=f"gossip-{namespace}-{value}",
        namespace=namespace,
        peer_id=peer_id.to_string(),
        node=node,
        created_at_ms=1000 + value,
    )
    return peer_id, runtime.codec.encode(gossip), node.header.node_id


def _message(peer_id: ID, topic: str, payload: bytes) -> rpc_pb2.Message:
    return rpc_pb2.Message(from_id=peer_id.to_bytes(), data=payload, topicIDs=[topic])


def test_receiver_builds_one_runtime_per_topic_namespace() -> None:
    storage_a = InMemoryDagStorage()
    storage_b = InMemoryDagStorage()
    receiver = DagGossipSubReceiver(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(1),
        topics_config=[
            GossipDagTopicConfig("topic-a", [CounterPayloadSchema()], storage=storage_a),
            GossipDagTopicConfig("topic-b", [CounterPayloadSchema()], namespace="namespace-b", storage=storage_b),
        ],
    )

    assert receiver.context_for_topic("topic-a").namespace == "topic-a"
    assert receiver.context_for_topic("topic-b").namespace == "namespace-b"
    assert receiver.context_for_namespace("topic-a").runtime.storage is storage_a
    assert receiver.context_for_namespace("namespace-b").runtime.storage is storage_b


@pytest.mark.asyncio
async def test_handle_message_stores_node_in_matching_topic_dag_and_calls_handler() -> None:
    storage = InMemoryDagStorage()
    db = DummyDB()
    handled: list[tuple[str, str, str]] = []

    async def handler(
        context: GossipDagTopicContext,
        from_peer_id: ID,
        message,
        result: NodeIngestResult | bool,
    ) -> None:
        assert isinstance(result, NodeIngestResult)
        handled.append((context.topic, from_peer_id.to_string(), result.status.value))

    receiver = DagGossipSubReceiver(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=db,  # type: ignore[arg-type]
        local_peer_id=_peer_id(2),
        topics_config=[
            GossipDagTopicConfig(
                "counter-topic",
                [CounterPayloadSchema()],
                topic_handler=handler,
                namespace="counter",
                storage=storage,
                latest_node_snapshot_db_key="counter-snapshots",
            )
        ],
    )
    remote_peer_id, payload, node_id = await _node_gossip("counter", "counter-topic", 3, 7)

    await receiver._handle_message(_message(remote_peer_id, "counter-topic", payload))

    assert await storage.get_node(node_id) is not None
    assert db.get_nested("counter-snapshots", remote_peer_id.to_string()) == {
        "peer_id": remote_peer_id.to_string(),
        "node_id": node_id,
        "created_at_ms": 1007,
    }
    assert handled == [("counter-topic", remote_peer_id.to_string(), NodeIngestStatus.ACCEPTED.value)]


@pytest.mark.trio
async def test_handle_message_orphans_child_when_parent_body_is_missing_and_schedules_sync() -> None:
    storage = InMemoryDagStorage()
    handled: list[str] = []

    async def handler(
        _context: GossipDagTopicContext,
        _from_peer_id: ID,
        _message,
        result: NodeIngestResult | bool,
    ) -> None:
        assert isinstance(result, NodeIngestResult)
        handled.append(result.status.value)

    receiver = DagGossipSubReceiver(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(12),
        topics_config=[
            GossipDagTopicConfig(
                "counter-topic",
                [CounterPayloadSchema()],
                topic_handler=handler,
                namespace="counter",
                storage=storage,
                sync_on_startup=False,
            )
        ],
    )

    remote_key_pair = _key_pair(13)
    remote_peer_id = ID.from_pubkey(remote_key_pair.public_key)
    remote_runtime = MerkleDagRuntime(
        db=DummyDB(),
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=remote_peer_id,
        namespace="counter",
        dag_topic="counter-topic",
        storage=InMemoryDagStorage(),
    )
    signer = Libp2pKeyPairSigner(remote_key_pair)
    parent = await remote_runtime.dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author=remote_peer_id.to_string(),
        created_at_ms=1013,
    )
    child = await remote_runtime.dag.create_node(
        schema_id="counter",
        payload={"value": 2},
        parent_ids=(parent.header.node_id,),
        signer=signer,
        author=remote_peer_id.to_string(),
        created_at_ms=1014,
    )
    await storage.put_header(parent.header)
    gossip = DagNodeGossip(
        message_id="gossip-counter-child",
        namespace="counter",
        peer_id=remote_peer_id.to_string(),
        node=child,
        created_at_ms=1014,
    )

    await receiver._handle_message(
        _message(remote_peer_id, "counter-topic", remote_runtime.codec.encode(gossip))
    )

    orphan = await storage.get_orphan(child.header.node_id)
    assert orphan is not None
    assert orphan.missing_parents == (parent.header.node_id,)
    assert handled == [NodeIngestStatus.ORPHAN.value]
    scheduler = receiver.context_for_topic("counter-topic").sync_scheduler
    assert scheduler is not None
    assert scheduler._signal_count == 1


@pytest.mark.asyncio
async def test_default_validator_accepts_valid_dag_gossip_and_rejects_wrong_namespace() -> None:
    receiver = DagGossipSubReceiver(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(4),
        topics_config=[
            GossipDagTopicConfig(
                "counter-topic",
                [CounterPayloadSchema()],
                namespace="counter",
                storage=InMemoryDagStorage(),
            )
        ],
    )
    context = receiver.context_for_topic("counter-topic")
    validator = receiver._default_topic_validator_for(context)
    remote_peer_id, payload, _node_id = await _node_gossip("counter", "counter-topic", 5, 8)
    wrong_namespace_peer_id, wrong_namespace_payload, _node_id = await _node_gossip(
        "other-counter",
        "counter-topic",
        6,
        9,
    )

    assert validator(remote_peer_id, _message(remote_peer_id, "counter-topic", payload)) is True
    assert (
        validator(
            wrong_namespace_peer_id,
            _message(wrong_namespace_peer_id, "counter-topic", wrong_namespace_payload),
        )
        is False
    )


@pytest.mark.asyncio
async def test_handle_sync_request_bytes_routes_by_namespace() -> None:
    receiver = DagGossipSubReceiver(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(7),
        topics_config=[
            GossipDagTopicConfig(
                "topic-a", [CounterPayloadSchema()], namespace="namespace-a", storage=InMemoryDagStorage()
            ),
            GossipDagTopicConfig(
                "topic-b", [CounterPayloadSchema()], namespace="namespace-b", storage=InMemoryDagStorage()
            ),
        ],
    )
    context_b = receiver.context_for_namespace("namespace-b")
    request = DagInventoryRequest(
        message_id="inventory-b",
        namespace="namespace-b",
        peer_id="peer-remote",
        known_heads=(),
        node_count=0,
        created_at_ms=1000,
    )

    response_bytes = await receiver.handle_sync_request_bytes("peer-remote", context_b.runtime.codec.encode(request))
    response = context_b.runtime.codec.decode(response_bytes)

    assert isinstance(response, DagInventoryResponse)
    assert response.namespace == "namespace-b"
    assert response.summary.namespace == "namespace-b"


@pytest.mark.asyncio
async def test_publish_heads_uses_topic_gossip_publisher() -> None:
    pubsub = FakePubsub()
    receiver = DagGossipSubReceiver(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(11),
        topics_config=[
            GossipDagTopicConfig(
                "counter-topic",
                [CounterPayloadSchema()],
                namespace="counter",
                storage=InMemoryDagStorage(),
            )
        ],
    )

    announcement = await receiver.publish_heads("counter-topic")

    assert announcement is not None
    assert announcement.namespace == "counter"
    assert len(pubsub.published) == 1
    topic, payload = pubsub.published[0]
    assert topic == "counter-topic"
    decoded = receiver.context_for_topic("counter-topic").runtime.codec.decode(payload)
    assert decoded == announcement


@pytest.mark.trio
async def test_run_registers_validators_subscribes_dispatches_and_unsubscribes() -> None:
    termination_event = trio.Event()
    storage = InMemoryDagStorage()
    handled: list[str] = []

    async def handler(
        _context: GossipDagTopicContext,
        _from_peer_id: ID,
        _message,
        _result: NodeIngestResult | bool,
    ) -> None:
        handled.append("called")
        termination_event.set()

    remote_peer_id, payload, node_id = await _node_gossip("counter", "counter-topic", 8, 10)
    pubsub = FakePubsub(
        {
            "counter-topic": FakeSubscription([_message(remote_peer_id, "counter-topic", payload)]),
        }
    )
    receiver = DagGossipSubReceiver(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=termination_event,
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(9),
        topics_config=[
            GossipDagTopicConfig(
                "counter-topic",
                [CounterPayloadSchema()],
                topic_handler=handler,
                namespace="counter",
                storage=storage,
                sync_on_startup=False,
            )
        ],
    )

    await receiver.run()

    assert [(topic, is_async) for topic, _validator, is_async in pubsub.validators] == [("counter-topic", False)]
    assert pubsub.subscribed_topics == ["counter-topic"]
    assert pubsub.unsubscribed_topics == ["counter-topic"]
    assert await storage.get_node(node_id) is not None
    assert handled == ["called"]


def test_receiver_allows_multiple_topics_to_share_namespace_runtime() -> None:
    storage = InMemoryDagStorage()
    receiver = DagGossipSubReceiver(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(10),
        topics_config=[
            GossipDagTopicConfig("topic-a", [CounterPayloadSchema()], namespace="shared", storage=storage),
            GossipDagTopicConfig("topic-b", [CounterPayloadSchema()], namespace="shared", storage=storage),
        ],
    )

    context_a = receiver.context_for_topic("topic-a")
    context_b = receiver.context_for_topic("topic-b")
    assert context_a.runtime is context_b.runtime
    assert context_a.sync_service is context_b.sync_service
    assert receiver.context_for_namespace("shared").runtime is context_a.runtime
