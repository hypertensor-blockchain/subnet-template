from __future__ import annotations

from dataclasses import dataclass

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
import trio

from examples.commit_reveal.consensus.scores import Scoring
from examples.commit_reveal.dag import (
    COMMIT_REVEAL_DAG_NAMESPACE,
    COMMIT_SCHEMA_ID,
    COMMIT_TOPIC,
    DEFAULT_COMMIT_REVEAL_SCORE,
    REVEAL_SCHEMA_ID,
    REVEAL_TOPIC,
    CommitDagSchema,
    CommitRevealDagPublisher,
    RevealDagSchema,
    build_epoch_phase_topic_validator,
)
from subnet.hypertensor.chain_functions import EpochData
from subnet.merkle_dag import DagNodeGossip, InMemoryDagStorage, Libp2pKeyPairSigner
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig


@dataclass(frozen=True)
class FakePeerInfo:
    peer_id: str


@dataclass(frozen=True)
class FakeSubnetNode:
    subnet_node_id: int
    peer_info: FakePeerInfo


class FakeHypertensor:
    def __init__(self, nodes: list[FakeSubnetNode], *, epoch: int = 7, percent_complete: float = 0.25) -> None:
        self.nodes = nodes
        self.epoch = epoch
        self.percent_complete = percent_complete

    def get_subnet_slot(self, subnet_id: int) -> int:
        return 3

    def get_subnet_epoch_data(self, slot: int) -> EpochData:
        return EpochData(
            block=100,
            epoch=self.epoch,
            block_per_epoch=100,
            seconds_per_epoch=600,
            percent_complete=self.percent_complete,
            blocks_elapsed=int(100 * self.percent_complete),
            blocks_remaining=100 - int(100 * self.percent_complete),
            seconds_elapsed=int(600 * self.percent_complete),
            seconds_remaining=600 - int(600 * self.percent_complete),
        )

    def get_min_class_subnet_nodes_formatted(self, subnet_id, subnet_epoch, min_class):
        return self.nodes


class DummyPubsub:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []

    async def publish(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class DummyDB:
    def __init__(self) -> None:
        self.nested: dict[str, dict[str, object]] = {}

    def get_nested(self, key: str, nested_key: str):
        return self.nested.get(key, {}).get(nested_key)

    def set_nested(self, key: str, nested_key: str, value: object) -> None:
        self.nested.setdefault(key, {})[nested_key] = value


def _key_pair(seed_byte: int):
    return create_new_key_pair(bytes([seed_byte]) * 32)


def _peer_id(seed_byte: int) -> str:
    return ID.from_pubkey(_key_pair(seed_byte).public_key).to_string()


def _build_system(
    *,
    seed_byte: int,
    storage: InMemoryDagStorage,
    hypertensor: FakeHypertensor,
) -> tuple[DagGossipSystem, DummyPubsub]:
    key_pair = _key_pair(seed_byte)
    local_peer_id = ID.from_pubkey(key_pair.public_key)
    pubsub = DummyPubsub()
    system = DagGossipSystem(
        pubsub=pubsub,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        local_peer_id=local_peer_id,
        topics=[
            DagGossipTopicConfig(
                topic=COMMIT_TOPIC,
                namespace=COMMIT_REVEAL_DAG_NAMESPACE,
                payload_schemas=[CommitDagSchema()],
                schema_id=COMMIT_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                storage=storage,
                topic_validator=build_epoch_phase_topic_validator(
                    hypertensor,
                    1,
                    phase="commit",
                    schema_id=COMMIT_SCHEMA_ID,
                ),
            ),
            DagGossipTopicConfig(
                topic=REVEAL_TOPIC,
                namespace=COMMIT_REVEAL_DAG_NAMESPACE,
                payload_schemas=[RevealDagSchema()],
                schema_id=REVEAL_SCHEMA_ID,
                signer=Libp2pKeyPairSigner(key_pair),
                author=local_peer_id.to_string(),
                parent_schema_id=COMMIT_SCHEMA_ID,
                storage=storage,
                topic_validator=build_epoch_phase_topic_validator(
                    hypertensor,
                    1,
                    phase="reveal",
                    schema_id=REVEAL_SCHEMA_ID,
                ),
            ),
        ],
    )
    return system, pubsub


@pytest.mark.asyncio
async def test_commit_reveal_publisher_links_reveals_to_commits_and_scoring_means_valid_reveals() -> None:
    peer_1 = _peer_id(1)
    peer_2 = _peer_id(2)
    nodes = [
        FakeSubnetNode(subnet_node_id=1, peer_info=FakePeerInfo(peer_1)),
        FakeSubnetNode(subnet_node_id=2, peer_info=FakePeerInfo(peer_2)),
    ]
    hypertensor = FakeHypertensor(nodes)
    storage = InMemoryDagStorage()

    system_1, _pubsub_1 = _build_system(seed_byte=1, storage=storage, hypertensor=hypertensor)
    publisher_1 = CommitRevealDagPublisher(
        system_1,
        subnet_id=1,
        subnet_node_id=1,
        hypertensor=hypertensor,
    )
    commit_1 = await publisher_1.publish_commit(
        scores={peer_1: DEFAULT_COMMIT_REVEAL_SCORE, peer_2: DEFAULT_COMMIT_REVEAL_SCORE},
        nonce="peer-1-nonce",
        created_at_ms=1000,
    )

    system_2, _pubsub_2 = _build_system(seed_byte=2, storage=storage, hypertensor=hypertensor)
    publisher_2 = CommitRevealDagPublisher(
        system_2,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=hypertensor,
    )
    commit_2 = await publisher_2.publish_commit(
        scores={peer_1: DEFAULT_COMMIT_REVEAL_SCORE // 2, peer_2: DEFAULT_COMMIT_REVEAL_SCORE},
        nonce="peer-2-nonce",
        created_at_ms=1001,
    )

    assert commit_1 is not None
    assert commit_2 is not None

    hypertensor.percent_complete = 0.75
    reveal_1 = await publisher_1.publish_reveal(created_at_ms=2000)
    reveal_2 = await publisher_2.publish_reveal(created_at_ms=2001)

    assert reveal_1 is not None
    assert reveal_2 is not None
    reveal_node_1 = await storage.get_node(reveal_1.node_id)
    reveal_node_2 = await storage.get_node(reveal_2.node_id)
    assert reveal_node_1 is not None
    assert reveal_node_2 is not None
    assert reveal_node_1.header.parent_ids == (commit_1.node_id,)
    assert reveal_node_2.header.parent_ids == (commit_2.node_id,)

    scores = await Scoring(
        db=DummyDB(),  # type: ignore[arg-type]
        subnet_id=1,
        hypertensor=hypertensor,  # type: ignore[arg-type]
        storage=storage,
    ).get_scores(current_epoch=8)

    assert [(score.subnet_node_id, score.score) for score in scores] == [
        (1, (DEFAULT_COMMIT_REVEAL_SCORE + DEFAULT_COMMIT_REVEAL_SCORE // 2) // 2),
        (2, DEFAULT_COMMIT_REVEAL_SCORE),
    ]


@pytest.mark.asyncio
async def test_commit_reveal_publisher_links_new_commit_to_previous_local_commit() -> None:
    peer_1 = _peer_id(1)
    nodes = [FakeSubnetNode(subnet_node_id=1, peer_info=FakePeerInfo(peer_1))]
    hypertensor = FakeHypertensor(nodes, epoch=7, percent_complete=0.25)
    storage = InMemoryDagStorage()
    system, _pubsub = _build_system(seed_byte=1, storage=storage, hypertensor=hypertensor)
    publisher = CommitRevealDagPublisher(system, subnet_id=1, subnet_node_id=1, hypertensor=hypertensor)

    first_commit = await publisher.publish_commit(created_at_ms=1000)
    assert first_commit is not None
    first_node = await storage.get_node(first_commit.node_id)
    assert first_node is not None
    assert first_node.header.parent_ids == ()

    hypertensor.epoch = 8
    second_commit = await publisher.publish_commit(created_at_ms=2000)
    assert second_commit is not None
    second_node = await storage.get_node(second_commit.node_id)
    assert second_node is not None
    assert second_node.header.parent_ids == (first_commit.node_id,)


@pytest.mark.asyncio
async def test_commit_reveal_topic_validators_gate_epoch_phases_and_allow_late_reveals() -> None:
    peer_1 = _peer_id(1)
    nodes = [FakeSubnetNode(subnet_node_id=1, peer_info=FakePeerInfo(peer_1))]
    hypertensor = FakeHypertensor(nodes, epoch=7, percent_complete=0.25)
    storage = InMemoryDagStorage()
    system, pubsub = _build_system(seed_byte=1, storage=storage, hypertensor=hypertensor)
    publisher = CommitRevealDagPublisher(system, subnet_id=1, subnet_node_id=1, hypertensor=hypertensor)

    commit = await publisher.publish_commit(nonce="nonce", created_at_ms=1000)
    assert commit is not None
    commit_topic, commit_payload = pubsub.messages[-1]
    assert commit_topic == COMMIT_TOPIC

    commit_validator = build_epoch_phase_topic_validator(
        hypertensor,
        1,
        phase="commit",
        schema_id=COMMIT_SCHEMA_ID,
    )
    commit_message = _pubsub_message(ID.from_pubkey(_key_pair(1).public_key), commit_topic, commit_payload)
    assert commit_validator(ID.from_pubkey(_key_pair(1).public_key), commit_message)

    hypertensor.percent_complete = 0.75
    assert not commit_validator(ID.from_pubkey(_key_pair(1).public_key), commit_message)

    reveal = await publisher.publish_reveal(created_at_ms=2000)
    assert reveal is not None
    reveal_topic, reveal_payload = pubsub.messages[-1]
    assert reveal_topic == REVEAL_TOPIC
    decoded = system.context_for_namespace(COMMIT_REVEAL_DAG_NAMESPACE).runtime.codec.decode(reveal_payload)
    assert isinstance(decoded, DagNodeGossip)

    reveal_validator = build_epoch_phase_topic_validator(
        hypertensor,
        1,
        phase="reveal",
        schema_id=REVEAL_SCHEMA_ID,
    )
    reveal_message = _pubsub_message(ID.from_pubkey(_key_pair(1).public_key), reveal_topic, reveal_payload)
    assert reveal_validator(ID.from_pubkey(_key_pair(1).public_key), reveal_message)

    hypertensor.epoch = 8
    hypertensor.percent_complete = 0.10
    assert reveal_validator(ID.from_pubkey(_key_pair(1).public_key), reveal_message)


def _pubsub_message(peer_id: ID, topic: str, payload: bytes):
    from libp2p.pubsub.pb import rpc_pb2

    return rpc_pb2.Message(from_id=peer_id.to_bytes(), data=payload, topicIDs=[topic])
