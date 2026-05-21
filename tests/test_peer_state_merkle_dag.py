from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
import trio

from subnet.merkle_dag import InMemoryDagStorage, Libp2pKeyPairSigner
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig
from subnet.utils.dag.heartbeat_dag_publisher import HeartbeatDagPublisher, HeartbeatDagSchema
from subnet.utils.dag.peer_state_dag_publisher import (
    PeerRole,
    PeerStateDagPublisher,
    PeerStateDagSchema,
    PeerStateData,
    ServerState,
)
from subnet.utils.pubsub.topics import PEER_STATE_TOPIC

TEST_DAG_NAMESPACE = "general-dag"
TEST_SCHEMA_ID = "general-dag"
TEST_PEER_STATE_SCHEMA_ID = "peer-state"
TEST_HEARTBEAT_SCHEMA_ID = "heartbeat"
TEST_PEER_STATE_TOPIC = "peer-state-topic"
TEST_HEARTBEAT_TOPIC = "heartbeat-topic"


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
                topic="peer-state-dag",
                namespace=TEST_DAG_NAMESPACE,
                payload_schemas=[PeerStateDagSchema(TEST_SCHEMA_ID)],
                schema_id=TEST_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                parent_schema_id=TEST_SCHEMA_ID,
                storage=storage,
                latest_node_snapshot_db_key=PEER_STATE_TOPIC,
            )
        ],
    )


@pytest.mark.asyncio
async def test_publish_stores_peer_state_in_metadata_and_gossips_heads():
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
        termination_event=trio.Event(),
    )

    result = await publisher.publish()

    assert result is not None
    node = await publisher.dag.get_node(result.node_id)
    assert node is not None
    assert "peer_id" not in node.body.payload
    assert node.header.author == local_peer_id.to_string()
    assert node.body.payload["kind"] == TEST_SCHEMA_ID

    state = PeerStateData.from_metadata(node.header.metadata)
    assert state.epoch == 7
    assert state.subnet_id == 1
    assert state.subnet_node_id == 2
    assert state.state is ServerState.JOINING
    assert state.role is PeerRole.VALIDATOR
    assert db.get_nested(PEER_STATE_TOPIC, local_peer_id.to_string())["node_id"] == result.node_id

    assert len(pubsub.messages) == 1
    topic, _payload = pubsub.messages[0]
    assert topic == "peer-state-dag"


@pytest.mark.asyncio
async def test_publish_uses_dag_gossip_system_namespace_publish():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([5]) * 32)
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
        hypertensor=DummyHypertensor(epoch=13),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=trio.Event(),
    )

    result = await publisher.publish()

    assert result is not None
    context = dag_system.context_for_namespace(TEST_DAG_NAMESPACE)
    assert await context.dag.get_node(result.node_id) is not None
    assert publisher.dag is context.dag
    assert len(pubsub.messages) == 1
    topic, _payload = pubsub.messages[0]
    assert topic == "peer-state-dag"
    assert db.get_nested(PEER_STATE_TOPIC, local_peer_id.to_string())["node_id"] == result.node_id


@pytest.mark.trio
async def test_dag_system_peer_state_publisher_run_publishes_on_interval():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([6]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    termination_event = trio.Event()
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
        hypertensor=DummyHypertensor(epoch=17),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=termination_event,
        publish_interval_seconds=0.01,
    )

    async def stop_after_first_publish() -> None:
        while not pubsub.messages:
            await trio.sleep(0.001)
        termination_event.set()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(publisher.run)
        nursery.start_soon(stop_after_first_publish)

    assert len(pubsub.messages) == 1


@pytest.mark.asyncio
async def test_publish_links_new_status_to_shared_frontier():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair_a = create_new_key_pair(bytes([2]) * 32)
    peer_a = ID.from_pubkey(key_pair_a.public_key)
    dag_system_a = _build_dag_system(
        pubsub=pubsub,
        db=db,
        key_pair=key_pair_a,
        local_peer_id=peer_a,
        storage=storage,
    )
    publisher_a = PeerStateDagPublisher(
        dag_system=dag_system_a,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=3,
        subnet_node_id=4,
        hypertensor=DummyHypertensor(epoch=9),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=trio.Event(),
    )
    key_pair_b = create_new_key_pair(bytes([3]) * 32)
    peer_b = ID.from_pubkey(key_pair_b.public_key)
    dag_system_b = _build_dag_system(
        pubsub=pubsub,
        db=db,
        key_pair=key_pair_b,
        local_peer_id=peer_b,
        storage=storage,
    )
    publisher_b = PeerStateDagPublisher(
        dag_system=dag_system_b,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=3,
        subnet_node_id=5,
        hypertensor=DummyHypertensor(epoch=9),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=trio.Event(),
    )

    first_a = await publisher_a.publish()
    assert first_a is not None

    first_b = await publisher_b.publish()
    assert first_b is not None
    first_b_node = await publisher_b.dag.get_node(first_b.node_id)
    assert first_b_node is not None
    assert first_b_node.header.parent_ids == (first_a.node_id,)

    publisher_a.state = ServerState.ONLINE
    second_a = await publisher_a.publish()

    assert second_a is not None
    second_a_node = await publisher_a.dag.get_node(second_a.node_id)
    assert second_a_node is not None
    assert second_a_node.header.parent_ids == (first_b.node_id,)

    publisher_b.state = ServerState.ONLINE
    second_b = await publisher_b.publish()
    assert second_b is not None

    latest = await publisher_a.latest_local_peer_state()
    assert latest is not None
    assert latest.node_id == second_a.node_id
    assert latest.state.state is ServerState.ONLINE
    assert db.get_nested(PEER_STATE_TOPIC, peer_a.to_string())["node_id"] == second_a.node_id


@pytest.mark.asyncio
async def test_publish_skips_while_dag_has_unresolved_orphans():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([4]) * 32)
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
        subnet_id=4,
        subnet_node_id=6,
        hypertensor=DummyHypertensor(epoch=11),
        schema_id=TEST_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=trio.Event(),
    )

    await storage.mark_orphan("orphan-1", ("missing-parent-1",))

    skipped = await publisher.publish()

    assert skipped is None
    assert pubsub.messages == []
    assert db.get_nested(PEER_STATE_TOPIC, local_peer_id.to_string()) is None

    await storage.clear_orphan("orphan-1")

    published = await publisher.publish()

    assert published is not None
    assert len(pubsub.messages) == 1


@pytest.mark.asyncio
async def test_peer_state_and_heartbeat_publish_into_one_general_dag_namespace():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([7]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    dag_system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=db,  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic=TEST_PEER_STATE_TOPIC,
                namespace=TEST_DAG_NAMESPACE,
                payload_schemas=[PeerStateDagSchema(TEST_PEER_STATE_SCHEMA_ID)],
                schema_id=TEST_PEER_STATE_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                parent_schema_id=TEST_PEER_STATE_SCHEMA_ID,
                storage=storage,
            ),
            DagGossipTopicConfig(
                topic=TEST_HEARTBEAT_TOPIC,
                namespace=TEST_DAG_NAMESPACE,
                payload_schemas=[HeartbeatDagSchema(TEST_HEARTBEAT_SCHEMA_ID)],
                schema_id=TEST_HEARTBEAT_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                parent_schema_id=TEST_HEARTBEAT_SCHEMA_ID,
                storage=storage,
            ),
        ],
    )
    peer_state_publisher = PeerStateDagPublisher(
        dag_system=dag_system,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=5,
        subnet_node_id=7,
        hypertensor=DummyHypertensor(epoch=19),
        schema_id=TEST_PEER_STATE_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=trio.Event(),
    )
    heartbeat_publisher = HeartbeatDagPublisher(
        dag_system=dag_system,
        subnet_id=5,
        subnet_node_id=7,
        hypertensor=DummyHypertensor(epoch=19),
        schema_id=TEST_HEARTBEAT_SCHEMA_ID,
        namespace=TEST_DAG_NAMESPACE,
        termination_event=trio.Event(),
    )

    peer_state_result = await peer_state_publisher.publish()
    heartbeat_result = await heartbeat_publisher.publish()

    assert peer_state_result is not None
    assert heartbeat_result is not None
    peer_state_node = await dag_system.context_for_namespace(TEST_DAG_NAMESPACE).dag.get_node(peer_state_result.node_id)
    heartbeat_node = await dag_system.context_for_namespace(TEST_DAG_NAMESPACE).dag.get_node(heartbeat_result.node_id)
    assert peer_state_node is not None
    assert heartbeat_node is not None
    assert peer_state_node.header.namespace == TEST_DAG_NAMESPACE
    assert heartbeat_node.header.namespace == TEST_DAG_NAMESPACE
    assert peer_state_node.header.schema_id == TEST_PEER_STATE_SCHEMA_ID
    assert heartbeat_node.header.schema_id == TEST_HEARTBEAT_SCHEMA_ID
    assert peer_state_node.header.parent_ids == ()
    assert heartbeat_node.header.parent_ids == ()
    assert [topic for topic, _payload in pubsub.messages] == [TEST_PEER_STATE_TOPIC, TEST_HEARTBEAT_TOPIC]
