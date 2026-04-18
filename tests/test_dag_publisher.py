from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
import trio

from subnet.merkle_dag import DagNodeGossip, InMemoryDagStorage, Libp2pKeyPairSigner
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.payloads import MappingPayloadSchema
from subnet.utils.gossipsub.dag_publisher import (
    DagNodePublishRequirements,
    DagPublisher,
    DagPublishPreconditionError,
)


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
    pass
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
