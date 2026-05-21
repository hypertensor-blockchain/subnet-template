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
from subnet.merkle_dag.bases.dag_gossip_system import (
    DagGossipSystem,
    DagGossipTopicConfig,
    DagGossipTopicContext,
)
from subnet.merkle_dag.bases.dag_publisher_base import DagNodePublishRequirements
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


class GaugePayloadSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("gauge")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("level"), int):
            raise PayloadValidationError("gauge payload requires an integer 'level'")


class DummyDB:
    def __init__(self):
        self.nested: dict[str, dict[str, object]] = {}

    def get_nested(self, key: str, nested_key: str):
        return self.nested.get(key, {}).get(nested_key)

    def set_nested(self, key: str, nested_key: str, value: object) -> None:
        self.nested.setdefault(key, {})[nested_key] = value


class FakeSubscription:
    def __init__(self, messages: list[rpc_pb2.Message] | None = None):
        self._messages = messages or []

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
        return self.subscriptions.setdefault(topic, FakeSubscription())

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed_topics.append(topic)

    async def publish(self, topic: str, payload: bytes) -> None:
        self.published.append((topic, payload))


def _key_pair(seed_byte: int):
    return create_new_key_pair(bytes([seed_byte]) * 32)


def _peer_id(seed_byte: int) -> ID:
    return ID.from_pubkey(_key_pair(seed_byte).public_key)


def _signer(seed_byte: int) -> Libp2pKeyPairSigner:
    return Libp2pKeyPairSigner(_key_pair(seed_byte))


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


def test_system_builds_one_context_per_topic_with_shared_publisher_runtime() -> None:
    storage_a = InMemoryDagStorage()
    storage_b = InMemoryDagStorage()
    system = DagGossipSystem(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(1),
        topics=[
            DagGossipTopicConfig(
                topic="topic-a",
                payload_schemas=[CounterPayloadSchema()],
                schema_id="counter",
                signer=_signer(1),
                storage=storage_a,
            ),
            DagGossipTopicConfig(
                topic="topic-b",
                payload_schemas=[CounterPayloadSchema()],
                namespace="namespace-b",
                storage=storage_b,
            ),
        ],
    )

    context_a = system.context_for_topic("topic-a")
    context_b = system.context_for_namespace("namespace-b")

    assert context_a.namespace == "topic-a"
    assert context_b.topic == "topic-b"
    assert context_a.runtime is context_a.publisher.runtime
    assert context_a.publisher.config is not None
    assert context_a.publisher.config.schema_id == "counter"
    assert context_b.runtime.storage is storage_b


@pytest.mark.asyncio
async def test_system_routes_shared_namespace_publish_by_schema_topic() -> None:
    pubsub = FakePubsub()
    storage = InMemoryDagStorage()
    key_pair = _key_pair(11)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic="counter-topic",
                namespace="shared",
                payload_schemas=[CounterPayloadSchema()],
                schema_id="counter",
                signer=Libp2pKeyPairSigner(key_pair),
                parent_schema_id="counter",
                storage=storage,
            ),
            DagGossipTopicConfig(
                topic="gauge-topic",
                namespace="shared",
                payload_schemas=[GaugePayloadSchema()],
                schema_id="gauge",
                signer=Libp2pKeyPairSigner(key_pair),
                parent_schema_id="gauge",
                storage=storage,
            ),
        ],
    )

    assert system.context_for_topic("counter-topic").runtime is system.context_for_topic("gauge-topic").runtime

    counter_result = await system.publish("shared", {"value": 1}, schema_id="counter", created_at_ms=1000)
    gauge_result = await system.publish("shared", {"level": 2}, schema_id="gauge", created_at_ms=1001)

    assert counter_result is not None
    assert gauge_result is not None
    assert [topic for topic, _payload in pubsub.published] == ["counter-topic", "gauge-topic"]
    context = system.context_for_namespace("shared")
    counter_node = await context.dag.get_node(counter_result.node_id)
    gauge_node = await context.dag.get_node(gauge_result.node_id)
    assert counter_node is not None
    assert gauge_node is not None
    assert counter_node.header.schema_id == "counter"
    assert gauge_node.header.schema_id == "gauge"
    assert counter_node.header.parent_ids == ()
    assert gauge_node.header.parent_ids == ()


@pytest.mark.asyncio
async def test_publish_writes_to_topic_dag_gossips_node_and_updates_snapshot() -> None:
    pubsub = FakePubsub()
    db = DummyDB()
    key_pair = _key_pair(2)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=db,  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic="counter-topic",
                namespace="counter",
                payload_schemas=[CounterPayloadSchema()],
                schema_id="counter",
                signer=Libp2pKeyPairSigner(key_pair),
                storage=InMemoryDagStorage(),
                latest_node_snapshot_db_key="counter-snapshots",
            )
        ],
    )

    result = await system.publish("counter", {"value": 1}, metadata={"label": "first"}, created_at_ms=1000)

    assert result is not None
    context = system.context_for_topic("counter-topic")
    assert await context.dag.get_node(result.node_id) is not None
    assert len(pubsub.published) == 1
    topic, payload = pubsub.published[0]
    assert topic == "counter-topic"
    decoded = context.runtime.codec.decode(payload)
    assert isinstance(decoded, DagNodeGossip)
    assert decoded.node.header.node_id == result.node_id
    assert db.get_nested("counter-snapshots", local_peer_id.to_string()) == {
        "peer_id": local_peer_id.to_string(),
        "node_id": result.node_id,
        "created_at_ms": 1000,
        "label": "first",
    }


@pytest.mark.asyncio
async def test_publish_rejects_unknown_namespace_without_pubsub_publish() -> None:
    pubsub = FakePubsub()
    system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(8),
        topics=[
            DagGossipTopicConfig(
                topic="counter-topic",
                namespace="counter",
                payload_schemas=[CounterPayloadSchema()],
                schema_id="counter",
                signer=_signer(8),
                storage=InMemoryDagStorage(),
            )
        ],
    )

    with pytest.raises(ValueError, match="No DAG namespace configured"):
        await system.publish("missing-namespace", {"value": 1})

    assert pubsub.published == []


@pytest.mark.asyncio
async def test_publish_accepts_explicit_requirements_without_template_defaults() -> None:
    pubsub = FakePubsub()
    key_pair = _key_pair(9)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic="counter-topic",
                namespace="counter",
                payload_schemas=[CounterPayloadSchema()],
                storage=InMemoryDagStorage(),
            )
        ],
    )
    requirements = DagNodePublishRequirements(
        schema_id="counter",
        payload={"value": 3},
        parent_ids=(),
        author=local_peer_id.to_string(),
        signer=Libp2pKeyPairSigner(key_pair),
        created_at_ms=1003,
    )

    result = await system.publish("counter", requirements)

    assert result is not None
    context = system.context_for_namespace("counter")
    assert await context.dag.get_node(result.node_id) is not None
    assert len(pubsub.published) == 1
    topic, payload = pubsub.published[0]
    assert topic == "counter-topic"
    decoded = context.runtime.codec.decode(payload)
    assert isinstance(decoded, DagNodeGossip)
    assert decoded.node.header.node_id == result.node_id


@pytest.mark.asyncio
async def test_receiver_side_ingests_remote_gossip_and_calls_system_handler() -> None:
    storage = InMemoryDagStorage()
    handled: list[tuple[str, str, str]] = []

    async def handler(
        context: DagGossipTopicContext,
        from_peer_id: ID,
        _message,
        result: NodeIngestResult | bool,
    ) -> None:
        assert isinstance(result, NodeIngestResult)
        handled.append((context.topic, from_peer_id.to_string(), result.status.value))

    system = DagGossipSystem(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(3),
        topics=[
            DagGossipTopicConfig(
                topic="counter-topic",
                namespace="counter",
                payload_schemas=[CounterPayloadSchema()],
                gossip_handler=handler,
                storage=storage,
            )
        ],
    )
    remote_peer_id, payload, node_id = await _node_gossip("counter", "counter-topic", 4, 7)

    await system.receiver._handle_message(_message(remote_peer_id, "counter-topic", payload))

    assert await storage.get_node(node_id) is not None
    assert handled == [("counter-topic", remote_peer_id.to_string(), NodeIngestStatus.ACCEPTED.value)]


@pytest.mark.asyncio
async def test_handle_sync_request_bytes_routes_by_namespace() -> None:
    system = DagGossipSystem(
        pubsub=FakePubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(5),
        topics=[
            DagGossipTopicConfig(
                "topic-a",
                [CounterPayloadSchema()],
                namespace="namespace-a",
                storage=InMemoryDagStorage(),
            ),
            DagGossipTopicConfig(
                "topic-b",
                [CounterPayloadSchema()],
                namespace="namespace-b",
                storage=InMemoryDagStorage(),
            ),
        ],
    )
    context_b = system.context_for_namespace("namespace-b")
    request = DagInventoryRequest(
        message_id="inventory-b",
        namespace="namespace-b",
        peer_id="peer-remote",
        known_heads=(),
        node_count=0,
        created_at_ms=1000,
    )

    response_bytes = await system.handle_sync_request_bytes("peer-remote", context_b.runtime.codec.encode(request))
    response = context_b.runtime.codec.decode(response_bytes)

    assert isinstance(response, DagInventoryResponse)
    assert response.namespace == "namespace-b"
    assert response.summary.namespace == "namespace-b"


@pytest.mark.trio
async def test_run_starts_receiver_without_auto_publishing() -> None:
    termination_event = trio.Event()
    pubsub = FakePubsub({"counter-topic": FakeSubscription()})
    system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=termination_event,
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=_peer_id(6),
        topics=[
            DagGossipTopicConfig(
                topic="counter-topic",
                namespace="counter",
                payload_schemas=[CounterPayloadSchema()],
                schema_id="counter",
                signer=_signer(6),
                storage=InMemoryDagStorage(),
            )
        ],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(system.run)
        await trio.sleep(0.01)
        termination_event.set()

    assert [(topic, is_async) for topic, _validator, is_async in pubsub.validators] == [("counter-topic", False)]
    assert pubsub.subscribed_topics == ["counter-topic"]
    assert pubsub.unsubscribed_topics == ["counter-topic"]
    assert pubsub.published == []


def test_publisher_options_require_schema_id_and_signer() -> None:
    with pytest.raises(ValueError, match="publisher options require both schema_id and signer"):
        DagGossipSystem(
            pubsub=FakePubsub(),  # type: ignore[arg-type]
            termination_event=trio.Event(),
            db=DummyDB(),  # type: ignore[arg-type]
            local_peer_id=_peer_id(7),
            topics=[
                DagGossipTopicConfig(
                    topic="counter-topic",
                    payload_schemas=[CounterPayloadSchema()],
                    author="local-peer",
                )
            ],
        )
