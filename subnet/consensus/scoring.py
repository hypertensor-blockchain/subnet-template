from collections.abc import Mapping
import logging
from typing import Any, List

from subnet.hypertensor.chain_data import SubnetNodeConsensusData
from subnet.hypertensor.chain_functions import Hypertensor, SubnetNodeClass
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag.models import DagNode
from subnet.merkle_dag.storage_rocksdb import RocksDBDagStorage
from subnet.utils.db.database import RocksDB

logger = logging.getLogger(__name__)

DEFAULT_PEER_STATE_DAG_NAMESPACE = "general-dag"
DEFAULT_PEER_STATE_SCHEMA_ID = "peer-state"
DEFAULT_CONSENSUS_SCORE = int(1e18)


class Scoring:
    """
    Template scoring class for subnet developers.

    Developers can replace the default logic in ``get_scores`` with their own
    deterministic scoring implementation as long as it returns a list of
    ``SubnetNodeConsensusData``.
    """

    def __init__(
        self,
        db: RocksDB,
        subnet_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        peer_state_dag_namespace: str = DEFAULT_PEER_STATE_DAG_NAMESPACE,
        peer_state_schema_id: str = DEFAULT_PEER_STATE_SCHEMA_ID,
    ):
        self.db = db
        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.peer_state_dag_namespace = peer_state_dag_namespace
        self.peer_state_schema_id = peer_state_schema_id

    async def get_scores(self, current_epoch: int) -> List[SubnetNodeConsensusData]:
        """
        Return consensus scores for the current epoch.

        Override this method to implement custom deterministic scoring. The
        default template gives every eligible node the same score and returns
        the data in the on-chain ``SubnetNodeConsensusData`` format.
        """
        included_nodes = self.hypertensor.get_min_class_subnet_nodes_formatted(
            subnet_id=self.subnet_id,
            subnet_epoch=current_epoch,
            min_class=SubnetNodeClass.Included,
        )

        included_by_peer_id = {
            node.peer_info.peer_id: node
            for node in included_nodes
            if getattr(getattr(node, "peer_info", None), "peer_id", None)
        }
        previous_epoch = current_epoch - 1
        peer_state_peer_ids = await self._peer_state_gossip_peer_ids_for_epoch(
            previous_epoch,
            included_by_peer_id,
        )

        subnet_node_ids = []
        for node in included_nodes:
            logger.debug(
                "Checking peer-state DAG for peer %s in epoch %s",
                node.peer_info.peer_id,
                previous_epoch,
            )

            if node.peer_info.peer_id not in peer_state_peer_ids:
                logger.debug(
                    "Peer-state DAG entry does not exist for node ID %s for epoch %s",
                    node.subnet_node_id,
                    previous_epoch,
                )
                continue

            subnet_node_ids.append(node.subnet_node_id)

        logger.info(f"Subnet node IDs: {subnet_node_ids}")

        consensus_score_list = [
            SubnetNodeConsensusData(subnet_node_id=node_id, score=DEFAULT_CONSENSUS_SCORE)
            for node_id in subnet_node_ids
        ]

        logger.debug(f"Consensus score list: {consensus_score_list}")

        return consensus_score_list

    async def _peer_state_gossip_peer_ids_for_epoch(
        self,
        epoch: int,
        included_by_peer_id: Mapping[str, Any],
    ) -> set[str]:
        """
        Return included peers with a complete peer-state DAG node for ``epoch``.

        The latest ``peer_state:<peer_id>`` snapshot only stores one node per
        peer, so scoring reads the immutable DAG history instead.
        """
        storage = RocksDBDagStorage(self.db, namespace=self.peer_state_dag_namespace)
        peer_ids: set[str] = set()

        for node_id in await storage.list_complete_node_ids():
            node = await storage.get_node(node_id)
            if node is None:
                continue
            if node.header.schema_id != self.peer_state_schema_id:
                continue

            peer_id = self._peer_id_from_peer_state_node(node)
            if peer_id is None or peer_id not in included_by_peer_id:
                continue
            if not self._peer_state_metadata_matches(
                node=node,
                peer_id=peer_id,
                epoch=epoch,
                included_node=included_by_peer_id[peer_id],
            ):
                continue

            peer_ids.add(peer_id)

        logger.info(
            "Peer-state DAG peers for subnet %s epoch %s: %s",
            self.subnet_id,
            epoch,
            sorted(peer_ids),
        )
        return peer_ids

    def _peer_state_metadata_matches(
        self,
        *,
        node: DagNode,
        peer_id: str,
        epoch: int,
        included_node: Any,
    ) -> bool:
        metadata = node.header.metadata
        metadata_epoch = self._metadata_int(metadata, "epoch")
        metadata_subnet_id = self._metadata_int(metadata, "subnet_id")
        metadata_subnet_node_id = self._metadata_int(metadata, "subnet_node_id")

        if metadata_epoch != epoch:
            return False
        if metadata_subnet_id != self.subnet_id:
            return False
        if metadata_subnet_node_id != included_node.subnet_node_id:
            logger.debug(
                "Ignoring peer-state DAG node %s from peer %s with subnet_node_id=%s; expected %s",
                node.header.node_id,
                peer_id,
                metadata_subnet_node_id,
                included_node.subnet_node_id,
            )
            return False
        return True

    def _peer_id_from_peer_state_node(self, node: DagNode) -> str | None:
        payload = node.body.payload
        if isinstance(payload, Mapping):
            payload_peer_id = payload.get("peer_id")
            if isinstance(payload_peer_id, str) and payload_peer_id:
                return payload_peer_id

        if node.header.author:
            return node.header.author

        return None

    @staticmethod
    def _metadata_int(metadata: Mapping[str, Any], key: str) -> int | None:
        value = metadata.get(key)
        if isinstance(value, bool) or value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
