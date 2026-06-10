from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
import trio

from subnet.merkle_dag import DagNodeGossip, InMemoryDagStorage, Libp2pKeyPairSigner
from subnet.merkle_dag.bases.dag_publisher_base import (
    DagNodePublishRequirements,
    DagPublisher,
    DagPublisherConfig,
    DagPublishPreconditionError,
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


class DummyPubsub:
    def __init__(self):
        self.messages: list[tuple[str, bytes]] = []

    async def publish(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class DummyDB:
    def __init__(self):
        self.nested: dict[str, dict[str, object]] = {}

    def get_nested(self, key: str, nested_key: str):
        return self.nested.get(key, {}).get(nested_key)

    def set_nested(self, key: str, nested_key: str, value: object) -> None:
        self.nested.setdefault(key, {})[nested_key] = value


def _key_pair(seed_byte: int):
    return create_new_key_pair(bytes([seed_byte]) * 32)


def _peer_id(seed_byte: int) -> ID:
    return ID.from_pubkey(_key_pair(seed_byte).public_key)


def _signer(seed_byte: int) -> Libp2pKeyPairSigner:
    return Libp2pKeyPairSigner(_key_pair(seed_byte))


@pytest.mark.asyncio
async def test_publish_now_builds_node_and_gossips_node_message():
    pubsub = DummyPubsub()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([1]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    publisher = DagPublisher(
        pubsub=pubsub,
        termination_event=trio.Event(),
        db=DummyDB(),
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=local_peer_id,
        namespace="shared",
        dag_topic="dag-topic",
        storage=storage,
    )

    result = await publisher.publish_now(
        DagNodePublishRequirements(
            schema_id="counter",
            payload={"value": 1},
            parent_ids=(),
            author=local_peer_id.to_string(),
            signer=Libp2pKeyPairSigner(key_pair),
            created_at_ms=1000,
        )
    )

    assert await publisher.dag.get_node(result.node_id) is not None
    assert len(pubsub.messages) == 1
    topic, payload = pubsub.messages[0]
    assert topic == "dag-topic"
    gossiped = publisher.codec.decode(payload)
    assert isinstance(gossiped, DagNodeGossip)
    assert gossiped.node.header.node_id == result.node_id
    assert gossiped.peer_id == local_peer_id.to_string()
    assert result.gossip_message_id == gossiped.message_id


@pytest.mark.asyncio
async def test_publish_now_rejects_missing_parent_requirements():
    pubsub = DummyPubsub()
    key_pair = create_new_key_pair(bytes([3]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    publisher = DagPublisher(
        pubsub=pubsub,
        termination_event=trio.Event(),
        db=DummyDB(),
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=local_peer_id,
        namespace="shared",
        dag_topic="dag-topic",
        storage=InMemoryDagStorage(),
    )

    with pytest.raises(DagPublishPreconditionError):
        await publisher.publish_now(
            DagNodePublishRequirements(
                schema_id="counter",
                payload={"value": 2},
                parent_ids=("missing-parent",),
                author=local_peer_id.to_string(),
                signer=Libp2pKeyPairSigner(key_pair),
                created_at_ms=1001,
            )
        )

    assert pubsub.messages == []


def test_config_namespace_defaults_to_topic() -> None:
    config = DagPublisherConfig(
        topic="counter-topic",
        schema_id="counter",
        signer=_signer(1),
        payload_schemas=[CounterPayloadSchema()],
    )

    assert config.dag_namespace == "counter-topic"


@pytest.mark.asyncio
async def test_template_publish_builds_node_and_gossips_payload() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    local_peer_id = _peer_id(2)
    publisher = DagPublisher(
        pubsub=pubsub,
        termination_event=trio.Event(),
        db=db,
        local_peer_id=local_peer_id,
        config=DagPublisherConfig(
            topic="counter-topic",
            namespace="counter-topic",
            schema_id="counter",
            signer=_signer(2),
            payload_schemas=[CounterPayloadSchema()],
            storage=InMemoryDagStorage(),
            latest_node_snapshot_db_key="counter-snapshots",
        ),
    )

    result = await publisher.publish({"value": 1}, metadata={"label": "first"}, created_at_ms=1000)

    assert result is not None
    assert await publisher.dag.get_node(result.node_id) is not None
    assert len(pubsub.messages) == 1
    topic, payload = pubsub.messages[0]
    assert topic == "counter-topic"
    gossiped = publisher.codec.decode(payload)
    assert isinstance(gossiped, DagNodeGossip)
    assert gossiped.node.header.node_id == result.node_id
    assert db.get_nested("counter-snapshots", local_peer_id.to_string()) == {
        "peer_id": local_peer_id.to_string(),
        "node_id": result.node_id,
        "created_at_ms": 1000,
        "label": "first",
    }


@pytest.mark.asyncio
async def test_template_publish_uses_existing_runtime_storage() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    local_peer_id = _peer_id(3)
    runtime = MerkleDagRuntime(
        db=db,
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=local_peer_id,
        namespace="counter",
        dag_topic="counter-topic",
        storage=InMemoryDagStorage(),
    )
    publisher = DagPublisher.from_runtime(
        pubsub=pubsub,
        termination_event=trio.Event(),
        runtime=runtime,
        config=DagPublisherConfig(
            topic="counter-topic",
            namespace="counter",
            schema_id="counter",
            signer=_signer(3),
        ),
    )

    result = await publisher.publish({"value": 2}, created_at_ms=1001)

    assert result is not None
    assert publisher.runtime is runtime
    assert await runtime.storage.get_node(result.node_id) is not None


@pytest.mark.asyncio
async def test_template_publish_defaults_parent_ids_to_current_heads() -> None:
    pubsub = DummyPubsub()
    publisher = DagPublisher(
        pubsub=pubsub,
        termination_event=trio.Event(),
        db=DummyDB(),
        local_peer_id=_peer_id(4),
        config=DagPublisherConfig(
            topic="counter-topic",
            schema_id="counter",
            signer=_signer(4),
            payload_schemas=[CounterPayloadSchema()],
            storage=InMemoryDagStorage(),
        ),
    )

    first = await publisher.publish({"value": 1}, created_at_ms=1000)
    second = await publisher.publish({"value": 2}, created_at_ms=1001)

    assert first is not None
    assert second is not None
    second_node = await publisher.dag.get_node(second.node_id)
    assert second_node is not None
    assert second_node.header.parent_ids == (first.node_id,)


@pytest.mark.trio
async def test_template_run_publishes_from_payload_factory() -> None:
    pubsub = DummyPubsub()
    termination_event = trio.Event()
    publish_count = 0

    def payload_factory(_publisher: DagPublisher):
        return {"value": 5}

    async def after_publish(_publisher: DagPublisher, _result) -> None:
        nonlocal publish_count
        publish_count += 1
        termination_event.set()

    publisher = DagPublisher(
        pubsub=pubsub,
        termination_event=termination_event,
        db=DummyDB(),
        local_peer_id=_peer_id(5),
        config=DagPublisherConfig(
            topic="counter-topic",
            schema_id="counter",
            signer=_signer(5),
            payload_schemas=[CounterPayloadSchema()],
            payload_factory=payload_factory,
            after_publish=after_publish,
            publish_interval_seconds=0.01,
            storage=InMemoryDagStorage(),
        ),
    )

    await publisher.run()

    assert publish_count == 1
    assert len(pubsub.messages) == 1


@pytest.mark.asyncio
async def test_template_runtime_can_publish_to_config_topic() -> None:
    local_peer_id = _peer_id(6)
    pubsub = DummyPubsub()
    runtime = MerkleDagRuntime(
        db=DummyDB(),
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=local_peer_id,
        namespace="counter",
        dag_topic="counter-topic",
        storage=InMemoryDagStorage(),
    )

    publisher = DagPublisher.from_runtime(
        pubsub=pubsub,
        termination_event=trio.Event(),
        runtime=runtime,
        config=DagPublisherConfig(
            topic="other-topic",
            namespace="counter",
            schema_id="counter",
            signer=_signer(6),
        ),
    )

    result = await publisher.publish({"value": 6}, created_at_ms=1006)

    assert result is not None
    assert pubsub.messages[0][0] == "other-topic"
