from __future__ import annotations

from enum import Enum
from typing import Any

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
from pydantic import ConfigDict
import trio

from subnet.merkle_dag import (
    DagNode,
    DagNodeBody,
    DagNodeGossip,
    DagNodeHeader,
    InMemoryDagStorage,
    Libp2pKeyPairSigner,
)
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig
from subnet.merkle_dag.bases.dag_publisher_base import DagPublishResult
from subnet.merkle_dag.bases.dag_publisher_template import (
    CallableDagPublisherTemplate,
    DagPayloadTemplate,
    DagPublisherTemplate,
    DagPublisherTemplateSchema,
    DagRecord,
)
from subnet.merkle_dag.exceptions import PayloadValidationError

TEST_NAMESPACE = "counter-namespace"
TEST_SCHEMA_ID = "counter"
TEST_TOPIC = "counter-topic"
TEST_SNAPSHOT_KEY = "counter-snapshots"


class PayloadState(Enum):
    ONLINE = "online"


class TemplatePayload(DagPayloadTemplate):
    model_config = ConfigDict(extra="ignore", frozen=True)

    value: int
    state: PayloadState


class CounterDagSchema(DagPublisherTemplateSchema[dict[str, Any]]):
    def __init__(self, schema_id: str = TEST_SCHEMA_ID) -> None:
        super().__init__(schema_id)

    def validate_payload(self, payload) -> None:
        super().validate_payload(payload)
        if payload.get("kind") != self.schema_id:
            raise PayloadValidationError(f"counter payload requires kind={self.schema_id!r}")
        if not isinstance(payload.get("value"), int):
            raise PayloadValidationError("counter payload requires an integer 'value'")


class CounterDagPublisher(DagPublisherTemplate[dict[str, Any]]):
    def __init__(self, dag_system: DagGossipSystem) -> None:
        super().__init__(
            dag_system=dag_system,
            namespace=TEST_NAMESPACE,
            schema_id=TEST_SCHEMA_ID,
            snapshot_db_key=TEST_SNAPSHOT_KEY,
        )
        self.value = 0
        self.after_publish_calls: list[str] = []

    def build_payload(self) -> dict[str, Any]:
        self.value += 1
        return {
            "kind": self.schema_id,
            "value": self.value,
        }

    async def build_metadata(self, payload: dict[str, Any]):
        return {"value": payload["value"]}

    async def after_publish(self, payload: dict[str, Any], result: DagPublishResult) -> None:
        await super().after_publish(payload, result)
        self.after_publish_calls.append(result.node_id)


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


def _build_dag_system(
    *,
    pubsub: DummyPubsub,
    db: DummyDB,
    seed_byte: int = 1,
    storage: InMemoryDagStorage | None = None,
) -> tuple[DagGossipSystem, str]:
    key_pair = _key_pair(seed_byte)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    dag_system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=db,  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic=TEST_TOPIC,
                namespace=TEST_NAMESPACE,
                payload_schemas=[CounterDagSchema()],
                schema_id=TEST_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                parent_schema_id=TEST_SCHEMA_ID,
                storage=storage or InMemoryDagStorage(),
            )
        ],
    )
    return dag_system, local_peer_id.to_string()


def test_dag_payload_template_provides_metadata_and_json_helpers() -> None:
    payload = TemplatePayload(value=3, state=PayloadState.ONLINE)

    metadata = payload.to_metadata()
    json_payload = payload.to_json()

    assert metadata == {"value": 3, "state": "online"}
    assert TemplatePayload.from_metadata({**metadata, "ignored": True}) == payload
    assert TemplatePayload.from_json(json_payload) == payload


def test_template_schema_can_use_payload_type_for_default_conversion() -> None:
    schema = DagPublisherTemplateSchema(TEST_SCHEMA_ID, TemplatePayload)
    payload = {"value": 3, "state": "online", "ignored": True}

    schema.validate_payload(payload)

    expected = TemplatePayload(value=3, state=PayloadState.ONLINE)
    assert schema.data_from_payload(payload) == expected

    node = DagNode(
        header=DagNodeHeader(
            node_id="node-a",
            namespace=TEST_NAMESPACE,
            schema_id=TEST_SCHEMA_ID,
            parent_ids=(),
            body_hash="hash",
            body_size=1,
            author="peer-a",
            public_key="public-key",
            signature="signature",
            created_at_ms=1,
        ),
        body=DagNodeBody(node_id="node-a", payload=payload),
    )

    materialized = schema.materialize(node, ())

    assert materialized == DagRecord(node_id="node-a", peer_id="peer-a", data=expected)


def test_template_schema_payload_type_validation_errors_are_wrapped() -> None:
    schema = DagPublisherTemplateSchema(TEST_SCHEMA_ID, TemplatePayload)

    with pytest.raises(PayloadValidationError, match="invalid DAG template payload"):
        schema.validate_payload({"peer_id": "peer-a", "state": "online"})


@pytest.mark.asyncio
async def test_template_publish_builds_payload_stores_snapshot_and_gossips() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    dag_system, local_peer_id = _build_dag_system(pubsub=pubsub, db=db)
    publisher = CounterDagPublisher(dag_system)

    result = await publisher.publish()

    assert result is not None
    node = await publisher.dag.get_node(result.node_id)
    assert node is not None
    assert node.body.payload == {
        "kind": TEST_SCHEMA_ID,
        "value": 1,
    }
    assert node.header.metadata == {"value": 1}
    assert publisher.after_publish_calls == [result.node_id]
    assert db.get_nested(TEST_SNAPSHOT_KEY, local_peer_id)["node_id"] == result.node_id

    latest = await publisher.latest_local_peer_state()
    assert latest is not None
    assert isinstance(latest, DagRecord)
    assert latest.node_id == result.node_id
    assert latest.peer_id == local_peer_id
    assert latest.data == node.body.payload

    materialized = CounterDagSchema().materialize(node, ())
    assert isinstance(materialized, DagRecord)
    assert materialized.node_id == result.node_id
    assert materialized.peer_id == local_peer_id
    assert materialized.data == node.body.payload

    assert len(pubsub.messages) == 1
    topic, payload = pubsub.messages[0]
    assert topic == TEST_TOPIC
    gossiped = publisher.publisher.codec.decode(payload)
    assert isinstance(gossiped, DagNodeGossip)
    assert gossiped.node.header.node_id == result.node_id


@pytest.mark.asyncio
async def test_template_publish_payload_can_be_called_by_external_logic() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    dag_system, local_peer_id = _build_dag_system(pubsub=pubsub, db=db)
    publisher = CounterDagPublisher(dag_system)

    first = await publisher.publish_payload(
        {
            "kind": TEST_SCHEMA_ID,
            "value": 10,
        },
        metadata={"value": 10, "source": "external"},
        created_at_ms=1000,
    )
    second = await publisher.publish_payload(
        {
            "kind": TEST_SCHEMA_ID,
            "value": 11,
        },
        created_at_ms=1001,
    )

    assert first is not None
    assert second is not None
    second_node = await publisher.dag.get_node(second.node_id)
    assert second_node is not None
    assert second_node.header.parent_ids == (first.node_id,)
    assert second_node.header.metadata == {"value": 11}


@pytest.mark.asyncio
async def test_template_publish_uses_header_author_identity() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    dag_system, local_peer_id = _build_dag_system(pubsub=pubsub, db=db)
    publisher = CounterDagPublisher(dag_system)

    published = await publisher.publish_payload(
        {
            "kind": TEST_SCHEMA_ID,
            "value": 12,
        },
        metadata={"value": 12},
        created_at_ms=1002,
    )
    wrong_author = await publisher.publish_payload(
        {
            "kind": TEST_SCHEMA_ID,
            "value": 13,
        },
        metadata={"value": 13},
        author=f"{local_peer_id}-wrong",
        created_at_ms=1003,
    )

    assert published is not None
    assert wrong_author is None
    assert len(pubsub.messages) == 1


@pytest.mark.asyncio
async def test_callable_template_uses_factories_without_subclassing() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    dag_system, local_peer_id = _build_dag_system(pubsub=pubsub, db=db)
    after_publish_calls: list[str] = []

    async def after_publish(_payload: dict[str, Any], result: DagPublishResult) -> None:
        after_publish_calls.append(result.node_id)

    publisher = CallableDagPublisherTemplate(
        dag_system=dag_system,
        namespace=TEST_NAMESPACE,
        schema_id=TEST_SCHEMA_ID,
        snapshot_db_key=TEST_SNAPSHOT_KEY,
        payload_factory=lambda: {
            "kind": TEST_SCHEMA_ID,
            "value": 5,
        },
        metadata_factory=lambda payload: {"value": payload["value"]},
        after_publish_hook=after_publish,
    )

    result = await publisher.publish()

    assert result is not None
    assert after_publish_calls == [result.node_id]
    assert db.get_nested(TEST_SNAPSHOT_KEY, local_peer_id)["node_id"] == result.node_id
