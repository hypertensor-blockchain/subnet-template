from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
import trio

from subnet.merkle_dag import InMemoryDagStorage
from subnet.utils.pubsub.peer_state_publisher import (
    PeerRole,
    PeerStateData,
    PeerStatePublisher,
    ServerState,
)
from subnet.utils.pubsub.topics import PEER_STATE_TOPIC


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


@pytest.mark.asyncio
async def test_publish_stores_peer_state_in_metadata_and_gossips_heads():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair = create_new_key_pair(bytes([1]) * 32)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    publisher = PeerStatePublisher(
        pubsub=pubsub,
        topic="peer-state-dag",
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=DummyHypertensor(epoch=7),
        db=db,
        key_pair=key_pair,
        local_peer_id=local_peer_id,
        storage=storage,
        termination_event=trio.Event(),
    )

    result = await publisher.publish()

    assert result is not None
    node = await publisher.dag.get_node(result.node_id)
    assert node is not None
    assert node.body.payload["peer_id"] == local_peer_id.to_string()
    assert node.body.payload["kind"] == "peer-state"

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
async def test_publish_links_new_status_to_shared_frontier():
    pubsub = DummyPubsub()
    db = DummyDB()
    storage = InMemoryDagStorage()
    key_pair_a = create_new_key_pair(bytes([2]) * 32)
    peer_a = ID.from_pubkey(key_pair_a.public_key)
    publisher_a = PeerStatePublisher(
        pubsub=pubsub,
        topic="peer-state-dag",
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=3,
        subnet_node_id=4,
        hypertensor=DummyHypertensor(epoch=9),
        db=db,
        key_pair=key_pair_a,
        local_peer_id=peer_a,
        storage=storage,
        termination_event=trio.Event(),
    )
    key_pair_b = create_new_key_pair(bytes([3]) * 32)
    peer_b = ID.from_pubkey(key_pair_b.public_key)
    publisher_b = PeerStatePublisher(
        pubsub=pubsub,
        topic="peer-state-dag",
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=3,
        subnet_node_id=5,
        hypertensor=DummyHypertensor(epoch=9),
        db=db,
        key_pair=key_pair_b,
        local_peer_id=peer_b,
        storage=storage,
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
    publisher = PeerStatePublisher(
        pubsub=pubsub,
        topic="peer-state-dag",
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=4,
        subnet_node_id=6,
        hypertensor=DummyHypertensor(epoch=11),
        db=db,
        key_pair=key_pair,
        local_peer_id=local_peer_id,
        storage=storage,
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
