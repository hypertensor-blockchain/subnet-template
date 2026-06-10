from __future__ import annotations

import logging

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
import trio

from examples.dag.peer_state_dag_publisher import (
    PeerRole,
    PeerStateDagPublisher,
    PeerStateData,
    ServerState,
)
from subnet.merkle_dag import DagNodeGossip, InMemoryDagStorage, Libp2pKeyPairSigner
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig
from subnet.merkle_dag.bases.dag_publisher_template import DagPublisherTemplateSchema, DagRecord

TEST_DAG_NAMESPACE = "template-peer-state-dag"
TEST_SCHEMA_ID = "peer-state"
TEST_TOPIC = "template-peer-state-topic"
TEST_MULTIADDR = "/ip4/127.0.0.1/tcp/9001"


class DummyPubsub:
    def __init__(self):
        self.messages: list[tuple[str, bytes]] = []

    async def publish(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class DummyEpochData:
    def __init__(self, epoch: int):
        self.epoch = epoch


class DummyHypertensor:
    def __init__(self, epoch: int = 42):
        self._epoch = epoch

    def get_subnet_slot(self, subnet_id: int) -> int:
        return subnet_id

    def get_subnet_epoch_data(self, subnet_slot: int) -> DummyEpochData:
        return DummyEpochData(self._epoch)


class DummyDB:
    def __init__(self):
        self.values: dict[tuple[str, str], dict] = {}

    def get_nested(self, key: str, subkey: str, default=None):
        return self.values.get((key, subkey), default)

    def set_nested(self, key: str, subkey: str, value) -> None:
        self.values[(key, subkey)] = value


def _build_dag_system(
    *,
    pubsub: DummyPubsub,
    db: DummyDB,
    key_pair,
    local_peer_id: ID,
    storage: InMemoryDagStorage,
) -> DagGossipSystem:
    return DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=db,  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic=TEST_TOPIC,
                namespace=TEST_DAG_NAMESPACE,
                payload_schemas=[DagPublisherTemplateSchema(TEST_SCHEMA_ID, PeerStateData)],
                schema_id=TEST_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                parent_schema_id=TEST_SCHEMA_ID,
                storage=storage,
                latest_node_snapshot_db_key=TEST_TOPIC,
            )
        ],
    )


@pytest.mark.asyncio
async def test_template_peer_state_example_publishes_payload_data_and_reads_latest() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([1]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    dag_system = _build_dag_system(
        pubsub=pubsub,
        db=db,
        key_pair=key_pair,
        local_peer_id=local_peer_id,
        storage=storage,
    )
    publisher = PeerStateDagPublisher(
        dag_system=dag_system,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=DummyHypertensor(epoch=7),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        multiaddr=TEST_MULTIADDR,
        termination_event=trio.Event(),
    )

    result = await publisher.publish()

    assert result is not None
    node = await publisher.dag.get_node(result.node_id)
    assert node is not None
    assert "kind" not in node.body.payload
    assert "peer_id" not in node.body.payload
    assert node.header.author == local_peer_id.to_string()
    payload = publisher.data_from_dag_payload(node.body.payload)
    assert not hasattr(payload, "kind")
    assert not hasattr(payload, "peer_id")
    assert payload.epoch == 7
    assert payload.subnet_id == 1
    assert payload.subnet_node_id == 2
    assert payload.state is ServerState.JOINING
    assert payload.role is PeerRole.VALIDATOR
    assert payload.multiaddr == TEST_MULTIADDR
    assert node.header.metadata["epoch"] == 7
    assert node.header.metadata["multiaddr"] == TEST_MULTIADDR

    latest = await publisher.latest_local_peer_state()
    assert latest is not None
    assert isinstance(latest, DagRecord)
    assert latest.node_id == result.node_id
    assert latest.peer_id == local_peer_id.to_string()
    assert latest.data == payload

    materialized = DagPublisherTemplateSchema(TEST_SCHEMA_ID, PeerStateData).materialize(node, ())
    assert isinstance(materialized, DagRecord)
    assert materialized.node_id == result.node_id
    assert materialized.peer_id == local_peer_id.to_string()
    assert materialized.data == payload

    assert db.get_nested(TEST_TOPIC, local_peer_id.to_string())["node_id"] == result.node_id
    assert len(pubsub.messages) == 1
    topic, encoded = pubsub.messages[0]
    assert topic == TEST_TOPIC
    gossiped = publisher.publisher.codec.decode(encoded)
    assert isinstance(gossiped, DagNodeGossip)
    assert gossiped.node.header.node_id == result.node_id


@pytest.mark.asyncio
async def test_template_peer_state_example_can_publish_from_external_logic() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([2]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    dag_system = _build_dag_system(
        pubsub=pubsub,
        db=db,
        key_pair=key_pair,
        local_peer_id=local_peer_id,
        storage=storage,
    )
    publisher = PeerStateDagPublisher(
        dag_system=dag_system,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=DummyHypertensor(epoch=7),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        multiaddr=TEST_MULTIADDR,
        termination_event=trio.Event(),
    )

    result = await publisher.publish_peer_state(
        PeerStateData(
            uid="manual-update",
            epoch=99,
            subnet_id=3,
            subnet_node_id=4,
            state=ServerState.ONLINE,
            role=PeerRole.VALIDATOR,
            multiaddr="/ip4/127.0.0.1/tcp/9002",
        ),
        created_at_ms=1000,
    )

    assert result is not None
    node = await publisher.dag.get_node(result.node_id)
    assert node is not None
    assert "kind" not in node.body.payload
    assert "peer_id" not in node.body.payload
    assert node.header.author == local_peer_id.to_string()
    payload = publisher.data_from_dag_payload(node.body.payload)
    assert payload.epoch == 99
    assert payload.state is ServerState.ONLINE


@pytest.mark.asyncio
async def test_template_peer_state_example_can_publish_final_offline_state(caplog) -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([8]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    dag_system = _build_dag_system(
        pubsub=pubsub,
        db=db,
        key_pair=key_pair,
        local_peer_id=local_peer_id,
        storage=storage,
    )
    publisher = PeerStateDagPublisher(
        dag_system=dag_system,
        start_state=ServerState.ONLINE,
        start_role=PeerRole.VALIDATOR,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=DummyHypertensor(epoch=11),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        multiaddr=TEST_MULTIADDR,
        termination_event=trio.Event(),
    )
    await storage.mark_orphan("orphan-node", ("missing-parent",))

    with caplog.at_level(logging.INFO, logger="examples.dag.peer_state_dag_publisher"):
        result = await publisher.publish_offline_state(created_at_ms=1000)

    assert result is not None
    assert publisher.state is ServerState.OFFLINE
    assert publisher.skip_if_orphans is True
    assert "Announcing that this peer is going offline" in caplog.text

    node = await publisher.dag.get_node(result.node_id)
    assert node is not None
    payload = publisher.data_from_dag_payload(node.body.payload)
    assert payload.state is ServerState.OFFLINE
    assert payload.epoch == 11
    assert len(pubsub.messages) == 1


@pytest.mark.trio
async def test_template_peer_state_example_run_publishes_on_interval() -> None:
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([3]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    dag_system = _build_dag_system(
        pubsub=pubsub,
        db=db,
        key_pair=key_pair,
        local_peer_id=local_peer_id,
        storage=storage,
    )
    termination_event = trio.Event()
    publisher = PeerStateDagPublisher(
        dag_system=dag_system,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=DummyHypertensor(epoch=7),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        multiaddr=TEST_MULTIADDR,
        termination_event=termination_event,
        publish_interval_seconds=0.01,
    )

    with trio.fail_after(1):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(publisher.run)
            while len(pubsub.messages) < 2:
                await trio.sleep(0.01)
            termination_event.set()
            nursery.cancel_scope.cancel()

    assert len(pubsub.messages) >= 2
    latest = await publisher.latest_local_peer_state()
    assert latest is not None
    assert latest.peer_id == local_peer_id.to_string()
