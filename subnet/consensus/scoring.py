import logging
from typing import List

from subnet.hypertensor.chain_data import SubnetNodeConsensusData
from subnet.hypertensor.chain_functions import Hypertensor, SubnetNodeClass
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.db.database import RocksDB
from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC

logger = logging.getLogger(__name__)


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
    ):
        self.db = db
        self.subnet_id = subnet_id
        self.hypertensor = hypertensor

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

        subnet_node_ids = []
        for node in included_nodes:
            logger.debug(
                f"Checking is heartbeat exists under nmap key {HEARTBEAT_TOPIC}:{current_epoch - 1}:{node.peer_info.peer_id}"  # noqa: E501
            )

            exists = self.db.nmap_get(HEARTBEAT_TOPIC, f"{current_epoch - 1}:{node.peer_info.peer_id}") is not None
            if not exists:
                logger.debug(
                    f"Heartbeat does not exist for node ID {node.subnet_node_id} for epoch {current_epoch - 1}"
                )
                continue

            subnet_node_ids.append(node.subnet_node_id)

        logger.info(f"Subnet node IDs: {subnet_node_ids}")

        consensus_score_list = [
            SubnetNodeConsensusData(subnet_node_id=node_id, score=int(1e18)) for node_id in subnet_node_ids
        ]

        logger.debug(f"Consensus score list: {consensus_score_list}")

        return consensus_score_list
