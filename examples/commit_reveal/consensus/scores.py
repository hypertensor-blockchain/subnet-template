from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import Any, List

from examples.commit_reveal.dag import (
    COMMIT_REVEAL_DAG_NAMESPACE,
    COMMIT_SCHEMA_ID,
    REVEAL_SCHEMA_ID,
    CommitData,
    RevealData,
    commitment_for_scores,
)
from subnet.hypertensor.chain_data import SubnetNodeConsensusData
from subnet.hypertensor.chain_functions import Hypertensor, SubnetNodeClass
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag.interfaces import DagStorage
from subnet.merkle_dag.models import DagNode
from subnet.merkle_dag.storage_rocksdb import RocksDBDagStorage
from subnet.utils.db.database import RocksDB

logger = logging.getLogger(__name__)


class Scoring:
    """
    Deterministic commit-reveal scoring over the previous epoch's DAG nodes.

    Each included peer is expected to publish one commit before the epoch cutoff
    and one reveal after it. Valid reveals contribute their score map to the
    targets they scored. A peer with no valid self reveal receives score 0;
    otherwise its final score is the integer mean of all valid scores it
    received from eligible peers.
    """

    def __init__(
        self,
        db: RocksDB,
        subnet_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        *,
        dag_namespace: str = COMMIT_REVEAL_DAG_NAMESPACE,
        commit_schema_id: str = COMMIT_SCHEMA_ID,
        reveal_schema_id: str = REVEAL_SCHEMA_ID,
        storage: DagStorage | None = None,
        require_valid_self_reveal: bool = True,
    ) -> None:
        self.db = db
        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.dag_namespace = dag_namespace
        self.commit_schema_id = commit_schema_id
        self.reveal_schema_id = reveal_schema_id
        self.storage = storage
        self.require_valid_self_reveal = require_valid_self_reveal

    async def get_scores(self, current_epoch: int) -> List[SubnetNodeConsensusData]:
        previous_epoch = current_epoch - 1
        if previous_epoch < 0:
            return []

        included_nodes = self.hypertensor.get_min_class_subnet_nodes_formatted(
            subnet_id=self.subnet_id,
            subnet_epoch=previous_epoch,
            min_class=SubnetNodeClass.Included,
        )
        included_by_peer_id = {
            node.peer_info.peer_id: node
            for node in included_nodes
            if getattr(getattr(node, "peer_info", None), "peer_id", None)
        }
        if not included_by_peer_id:
            return []

        storage = self._storage()
        reveal_nodes = await self._latest_reveals_by_peer(
            storage=storage,
            epoch=previous_epoch,
            included_by_peer_id=included_by_peer_id,
        )
        received_scores: dict[str, list[int]] = {peer_id: [] for peer_id in included_by_peer_id}
        valid_reveal_peers: set[str] = set()

        for peer_id, included_node in included_by_peer_id.items():
            reveal_node = reveal_nodes.get(peer_id)
            if reveal_node is None:
                logger.debug("No reveal found for peer %s in epoch %s", peer_id, previous_epoch)
                continue

            reveal = await self._validated_reveal(
                storage=storage,
                reveal_node=reveal_node,
                expected_epoch=previous_epoch,
                expected_peer_id=peer_id,
                expected_subnet_node_id=included_node.subnet_node_id,
            )
            if reveal is None:
                continue

            valid_reveal_peers.add(peer_id)
            for target_peer_id, score in reveal.scores.items():
                if target_peer_id not in received_scores:
                    continue
                received_scores[target_peer_id].append(score)

        consensus_scores: list[SubnetNodeConsensusData] = []
        for peer_id, included_node in sorted(
            included_by_peer_id.items(),
            key=lambda item: item[1].subnet_node_id,
        ):
            if self.require_valid_self_reveal and peer_id not in valid_reveal_peers:
                score = 0
            else:
                score = self._mean(received_scores[peer_id])
            consensus_scores.append(
                SubnetNodeConsensusData(
                    subnet_node_id=included_node.subnet_node_id,
                    score=score,
                )
            )

        logger.info(
            "Commit-reveal consensus scores for subnet %s epoch %s: %s",
            self.subnet_id,
            previous_epoch,
            consensus_scores,
        )
        return consensus_scores

    async def _latest_reveals_by_peer(
        self,
        *,
        storage: DagStorage,
        epoch: int,
        included_by_peer_id: Mapping[str, Any],
    ) -> dict[str, DagNode]:
        latest: dict[str, DagNode] = {}
        for node_id in await storage.list_complete_node_ids():
            node = await storage.get_node(node_id)
            if node is None or node.header.schema_id != self.reveal_schema_id:
                continue
            try:
                reveal = RevealData.from_metadata(node.body.payload)
            except Exception:
                logger.debug("Ignoring malformed reveal node %s", node_id, exc_info=True)
                continue
            if reveal.epoch != epoch or reveal.subnet_id != self.subnet_id:
                continue
            if reveal.peer_id not in included_by_peer_id:
                continue
            if node.header.author != reveal.peer_id:
                continue

            previous = latest.get(reveal.peer_id)
            if previous is None or (node.header.created_at_ms, node.header.node_id) > (
                previous.header.created_at_ms,
                previous.header.node_id,
            ):
                latest[reveal.peer_id] = node
        return latest

    async def _validated_reveal(
        self,
        *,
        storage: DagStorage,
        reveal_node: DagNode,
        expected_epoch: int,
        expected_peer_id: str,
        expected_subnet_node_id: int,
    ) -> RevealData | None:
        try:
            reveal = RevealData.from_metadata(reveal_node.body.payload)
        except Exception:
            logger.debug("Rejecting malformed reveal node %s", reveal_node.header.node_id, exc_info=True)
            return None

        if reveal.epoch != expected_epoch:
            return None
        if reveal.subnet_id != self.subnet_id:
            return None
        if reveal.subnet_node_id != expected_subnet_node_id:
            return None
        if reveal.peer_id != expected_peer_id or reveal_node.header.author != expected_peer_id:
            return None
        if reveal_node.header.parent_ids != (reveal.commit_node_id,):
            return None

        commit_node = await storage.get_node(reveal.commit_node_id)
        if commit_node is None:
            logger.debug("Reveal %s is missing commit parent %s", reveal_node.header.node_id, reveal.commit_node_id)
            return None
        if commit_node.header.schema_id != self.commit_schema_id:
            return None
        if commit_node.header.author != expected_peer_id:
            return None

        try:
            commit = CommitData.from_metadata(commit_node.body.payload)
        except Exception:
            logger.debug("Rejecting malformed commit parent %s", commit_node.header.node_id, exc_info=True)
            return None

        if commit.epoch != reveal.epoch:
            return None
        if commit.subnet_id != reveal.subnet_id:
            return None
        if commit.subnet_node_id != reveal.subnet_node_id:
            return None
        if commit.peer_id != reveal.peer_id:
            return None

        expected_commitment = commitment_for_scores(
            epoch=reveal.epoch,
            subnet_id=reveal.subnet_id,
            subnet_node_id=reveal.subnet_node_id,
            peer_id=reveal.peer_id,
            scores=reveal.scores,
            nonce=reveal.nonce,
        )
        if commit.commitment != expected_commitment:
            logger.info(
                "Commit-reveal mismatch for peer %s epoch %s: commit=%s reveal=%s",
                expected_peer_id,
                expected_epoch,
                commit.commitment,
                expected_commitment,
            )
            return None

        return reveal

    def _storage(self) -> DagStorage:
        return self.storage or RocksDBDagStorage(self.db, namespace=self.dag_namespace)

    @staticmethod
    def _mean(values: list[int]) -> int:
        if not values:
            return 0
        return sum(values) // len(values)


__all__ = ["Scoring"]
