from types import SimpleNamespace

import pytest

from subnet.consensus.scoring import Scoring
from subnet.hypertensor.chain_functions import SubnetNodeClass
from subnet.merkle_dag.models import DagNodeBody, DagNodeHeader
from subnet.merkle_dag.storage_rocksdb import RocksDBDagStorage
from subnet.utils.db.database import RocksDB
from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC, PEER_STATE_TOPIC


class FakeHypertensor:
    def __init__(self, included_nodes):
        self.included_nodes = included_nodes

    def get_min_class_subnet_nodes_formatted(self, subnet_id, subnet_epoch, min_class):
        assert subnet_id == 1
        assert subnet_epoch == 6
        assert min_class is SubnetNodeClass.Included
        return self.included_nodes


def _included_node(peer_id: str, subnet_node_id: int):
    return SimpleNamespace(
        subnet_node_id=subnet_node_id,
        peer_info=SimpleNamespace(peer_id=peer_id),
    )


async def _put_peer_state_node(
    storage: RocksDBDagStorage,
    *,
    node_id: str,
    peer_id: str,
    epoch: int,
    subnet_id: int,
    subnet_node_id: int,
    created_at_ms: int,
    schema_id: str = "peer-state",
    namespace: str = "general-dag",
) -> None:
    metadata = {
        "uid": node_id,
        "epoch": epoch,
        "subnet_id": subnet_id,
        "subnet_node_id": subnet_node_id,
        "state": 2,
        "role": 0,
        "multiaddr": None,
    }
    await storage.put_header(
        DagNodeHeader(
            node_id=node_id,
            namespace=namespace,
            schema_id=schema_id,
            parent_ids=(),
            body_hash="",
            body_size=0,
            author=peer_id,
            public_key="",
            signature="",
            created_at_ms=created_at_ms,
            metadata=metadata,
        )
    )
    await storage.put_body(
        DagNodeBody(
            node_id=node_id,
            payload={
                "kind": schema_id,
                "peer_id": peer_id,
            },
        )
    )


@pytest.mark.trio
async def test_get_scores_uses_previous_epoch_peer_state_dag_history(tmp_path):
    db = RocksDB(str(tmp_path / "scores"))
    try:
        storage = RocksDBDagStorage(db, namespace="general-dag")
        await _put_peer_state_node(
            storage,
            node_id="peer-a-epoch-5",
            peer_id="peer-a",
            epoch=5,
            subnet_id=1,
            subnet_node_id=101,
            created_at_ms=1000,
        )
        await _put_peer_state_node(
            storage,
            node_id="peer-a-epoch-6",
            peer_id="peer-a",
            epoch=6,
            subnet_id=1,
            subnet_node_id=101,
            created_at_ms=2000,
        )
        await _put_peer_state_node(
            storage,
            node_id="peer-b-epoch-6",
            peer_id="peer-b",
            epoch=6,
            subnet_id=1,
            subnet_node_id=102,
            created_at_ms=1000,
        )
        await _put_peer_state_node(
            storage,
            node_id="peer-c-wrong-node",
            peer_id="peer-c",
            epoch=5,
            subnet_id=1,
            subnet_node_id=999,
            created_at_ms=1000,
        )
        await _put_peer_state_node(
            storage,
            node_id="peer-d-not-included",
            peer_id="peer-d",
            epoch=5,
            subnet_id=1,
            subnet_node_id=104,
            created_at_ms=1000,
        )

        db.set_nested(
            PEER_STATE_TOPIC,
            "peer-a",
            {
                "peer_id": "peer-a",
                "node_id": "peer-a-epoch-6",
                "created_at_ms": 2000,
                "epoch": 6,
                "subnet_id": 1,
                "subnet_node_id": 101,
            },
        )
        db.nmap_set(HEARTBEAT_TOPIC, "5:peer-b", "legacy-heartbeat")

        scoring = Scoring(
            db=db,
            subnet_id=1,
            hypertensor=FakeHypertensor(
                [
                    _included_node("peer-a", 101),
                    _included_node("peer-b", 102),
                    _included_node("peer-c", 103),
                ]
            ),
        )

        scores = await scoring.get_scores(current_epoch=6)

        assert [(score.subnet_node_id, score.score) for score in scores] == [(101, int(1e18))]
    finally:
        db.close()
