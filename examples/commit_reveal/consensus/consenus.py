from __future__ import annotations

from examples.commit_reveal.consensus.scores import Scoring
from examples.commit_reveal.dag import COMMIT_REVEAL_DAG_NAMESPACE, COMMIT_SCHEMA_ID, REVEAL_SCHEMA_ID
from subnet.consensus.consensus_v2 import Consensus as BaseConsensus
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag.interfaces import DagStorage
from subnet.utils.db.database import RocksDB
from subnet.utils.hypertensor.subnet_info_tracker_v5 import SubnetInfoTracker


class Consensus(BaseConsensus):
    """
    Consensus runner that swaps the base template scoring for commit-reveal DAG scoring.

    The lifecycle, validator election, proposal, and attestation behavior are inherited
    from ``subnet.consensus.consensus_v2.Consensus``. Only ``self.scoring`` is replaced
    so ``run_consensus`` submits scores derived from the previous epoch's commit/reveal
    DAG.
    """

    def __init__(
        self,
        db: RocksDB,
        subnet_id: int,
        subnet_node_id: int,
        subnet_info_tracker: SubnetInfoTracker,
        hypertensor: Hypertensor | LocalMockHypertensor,
        *,
        dag_namespace: str = COMMIT_REVEAL_DAG_NAMESPACE,
        commit_schema_id: str = COMMIT_SCHEMA_ID,
        reveal_schema_id: str = REVEAL_SCHEMA_ID,
        storage: DagStorage | None = None,
        skip_activate_subnet: bool = False,
    ) -> None:
        super().__init__(
            db=db,
            subnet_id=subnet_id,
            subnet_node_id=subnet_node_id,
            subnet_info_tracker=subnet_info_tracker,
            hypertensor=hypertensor,
            skip_activate_subnet=skip_activate_subnet,
        )
        self.scoring = Scoring(
            db=db,
            subnet_id=subnet_id,
            hypertensor=hypertensor,
            dag_namespace=dag_namespace,
            commit_schema_id=commit_schema_id,
            reveal_schema_id=reveal_schema_id,
            storage=storage,
        )


__all__ = ["Consensus"]
