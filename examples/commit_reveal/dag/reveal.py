from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import logging
import secrets
from typing import Any, Literal, cast

from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
from pydantic import ConfigDict, field_validator
import trio

from subnet.hypertensor.chain_functions import EpochData, Hypertensor, SubnetNodeClass
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag import DagAnnouncement, DagNodeGossip, DagSyncMessageCodec
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem
from subnet.merkle_dag.bases.dag_publisher_base import DagPublishResult
from subnet.merkle_dag.bases.dag_publisher_template import (
    DagPayloadTemplate,
    DagPublisherTemplate,
    DagPublisherTemplateSchema,
)
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.telemetry.telemetry import Telemetry

from .commit import (
    COMMIT_REVEAL_DAG_NAMESPACE,
    COMMIT_SCHEMA_ID,
    COMMIT_TOPIC,
    DEFAULT_COMMIT_REVEAL_SCORE,
    CommitDagPublisher,
    CommitData,
    commitment_for_scores,
    normalize_scores,
)

logger = logging.getLogger(__name__)

REVEAL_TOPIC = "reveal-topic"
REVEAL_SCHEMA_ID = "reveal-schema"
COMMIT_REVEAL_CUTOFF_PERCENT = 0.5


class RevealData(DagPayloadTemplate):
    model_config = ConfigDict(extra="ignore", frozen=True)

    uid: str
    epoch: int
    subnet_id: int
    subnet_node_id: int
    peer_id: str
    commit_node_id: str
    scores: dict[str, int]
    nonce: str

    @field_validator("scores", mode="before")
    @classmethod
    def _validate_scores(cls, value: Any) -> dict[str, int]:
        if not isinstance(value, Mapping):
            raise ValueError("scores must be a mapping of peer_id to score")
        return normalize_scores(cast(Mapping[str, int], value))

    def model_post_init(self, __context: Any) -> None:
        assert self.uid, "uid is required"
        assert self.epoch >= 0, "epoch must be non-negative"
        assert self.subnet_id >= 0, "subnet_id must be non-negative"
        assert self.subnet_node_id >= 0, "subnet_node_id must be non-negative"
        assert self.peer_id, "peer_id is required"
        assert self.commit_node_id, "commit_node_id is required"
        assert self.nonce, "nonce is required"

    def commitment(self) -> str:
        return commitment_for_scores(
            epoch=self.epoch,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            peer_id=self.peer_id,
            scores=self.scores,
            nonce=self.nonce,
        )


class RevealDagSchema(DagPublisherTemplateSchema[RevealData]):
    def __init__(
        self,
        schema_id: str = REVEAL_SCHEMA_ID,
        *,
        commit_schema_id: str = COMMIT_SCHEMA_ID,
    ) -> None:
        super().__init__(schema_id, RevealData)
        self.commit_schema_id = commit_schema_id

    def validate_signer_peer(self, node, signer_peer_id: str) -> None:
        reveal = RevealData.from_metadata(node.body.payload)
        if reveal.peer_id != signer_peer_id:
            raise PayloadValidationError(
                f"reveal peer_id {reveal.peer_id!r} does not match signer peer {signer_peer_id!r}"
            )

    def validate_parent_links(self, node, parents) -> None:
        reveal = RevealData.from_metadata(node.body.payload)
        if len(parents) != 1:
            raise PayloadValidationError("reveal nodes must reference exactly one commit parent")

        commit_node = parents[0]
        if commit_node.header.schema_id != self.commit_schema_id:
            raise PayloadValidationError(
                f"reveal parent must have schema_id={self.commit_schema_id!r}, "
                f"got {commit_node.header.schema_id!r}"
            )
        if node.header.parent_ids != (commit_node.header.node_id,):
            raise PayloadValidationError("reveal commit parent must match header parent_ids")
        if reveal.commit_node_id != commit_node.header.node_id:
            raise PayloadValidationError("reveal commit_node_id must match its commit parent")
        if node.header.author != commit_node.header.author:
            raise PayloadValidationError("commit and reveal must be signed by the same peer")

        commit = CommitData.from_metadata(commit_node.body.payload)
        if commit.peer_id != reveal.peer_id:
            raise PayloadValidationError("commit and reveal peer_id values do not match")
        if commit.epoch != reveal.epoch:
            raise PayloadValidationError("commit and reveal epochs do not match")
        if commit.subnet_id != reveal.subnet_id:
            raise PayloadValidationError("commit and reveal subnet_id values do not match")
        if commit.subnet_node_id != reveal.subnet_node_id:
            raise PayloadValidationError("commit and reveal subnet_node_id values do not match")
        if commit.commitment != reveal.commitment():
            raise PayloadValidationError("reveal scores do not match the committed hash")


class EpochPhaseError(RuntimeError):
    pass


class EpochPhaseGate:
    def __init__(
        self,
        hypertensor: Hypertensor | LocalMockHypertensor,
        subnet_id: int,
        *,
        cutoff_percent: float = COMMIT_REVEAL_CUTOFF_PERCENT,
    ) -> None:
        self.hypertensor = hypertensor
        self.subnet_id = subnet_id
        self.cutoff_percent = cutoff_percent

    def epoch_data(self) -> EpochData:
        return self.hypertensor.get_subnet_epoch_data(self.hypertensor.get_subnet_slot(self.subnet_id))

    def commit_is_open(self, epoch: int) -> bool:
        epoch_data = self.epoch_data()
        return epoch == epoch_data.epoch and epoch_data.percent_complete < self.cutoff_percent

    def reveal_is_open(self, epoch: int) -> bool:
        epoch_data = self.epoch_data()
        if epoch < epoch_data.epoch:
            return True
        return epoch == epoch_data.epoch and epoch_data.percent_complete >= self.cutoff_percent

    def require_commit_open(self, epoch: int) -> None:
        if not self.commit_is_open(epoch):
            epoch_data = self.epoch_data()
            raise EpochPhaseError(
                f"commit phase is closed for epoch {epoch}; "
                f"current epoch={epoch_data.epoch} percent_complete={epoch_data.percent_complete:.3f}"
            )

    def require_reveal_open(self, epoch: int) -> None:
        if not self.reveal_is_open(epoch):
            epoch_data = self.epoch_data()
            raise EpochPhaseError(
                f"reveal phase is closed for epoch {epoch}; "
                f"current epoch={epoch_data.epoch} percent_complete={epoch_data.percent_complete:.3f}"
            )


def build_epoch_phase_topic_validator(
    hypertensor: Hypertensor | LocalMockHypertensor,
    subnet_id: int,
    *,
    phase: Literal["commit", "reveal"],
    schema_id: str,
    cutoff_percent: float = COMMIT_REVEAL_CUTOFF_PERCENT,
):
    gate = EpochPhaseGate(hypertensor, subnet_id, cutoff_percent=cutoff_percent)
    codec = DagSyncMessageCodec()

    def validate(_forwarder_peer_id: ID, message: rpc_pb2.Message) -> bool:
        try:
            decoded = codec.decode(message.data)
        except Exception:
            logger.debug("Rejecting undecodable commit-reveal DAG gossip", exc_info=True)
            return False

        if isinstance(decoded, DagAnnouncement):
            return True
        if not isinstance(decoded, DagNodeGossip):
            return False
        if decoded.node.header.schema_id != schema_id:
            return False

        payload_epoch = _payload_epoch(decoded.node.body.payload)
        if payload_epoch is None:
            return False

        current_epoch = hypertensor.get_subnet_epoch_data(hypertensor.get_subnet_slot(subnet_id)).epoch
        if payload_epoch != current_epoch:
            return False

        if phase == "commit":
            return gate.commit_is_open(payload_epoch)
        return gate.reveal_is_open(payload_epoch)

    return validate


def _payload_epoch(payload: Any) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("epoch")
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class RevealDagPublisher(DagPublisherTemplate[RevealData]):
    def build_payload(self) -> RevealData:
        raise RuntimeError("RevealDagPublisher requires caller-supplied RevealData")

    async def build_metadata(self, payload: RevealData) -> Mapping[str, Any] | None:
        return {
            "commit_node_id": payload.commit_node_id,
            "epoch": payload.epoch,
            "peer_id": payload.peer_id,
            "subnet_id": payload.subnet_id,
            "subnet_node_id": payload.subnet_node_id,
            "uid": payload.uid,
        }


@dataclass(slots=True)
class PendingCommit:
    epoch: int
    node_id: str
    payload: CommitData
    scores: dict[str, int]
    nonce: str
    revealed: bool = False


class CommitRevealDagPublisher:
    def __init__(
        self,
        dag_system: DagGossipSystem,
        *,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        namespace: str = COMMIT_REVEAL_DAG_NAMESPACE,
        commit_schema_id: str = COMMIT_SCHEMA_ID,
        reveal_schema_id: str = REVEAL_SCHEMA_ID,
        commit_snapshot_db_key: str | None = COMMIT_TOPIC,
        reveal_snapshot_db_key: str | None = REVEAL_TOPIC,
        telemetry: Telemetry | None = None,
        termination_event: trio.Event | None = None,
        publish_interval_seconds: float = 5.0,
        cutoff_percent: float = COMMIT_REVEAL_CUTOFF_PERCENT,
        log_level: int = logging.DEBUG,
    ) -> None:
        if publish_interval_seconds <= 0:
            raise ValueError("publish_interval_seconds must be greater than zero")
        self.dag_system = dag_system
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.namespace = namespace
        self.local_peer_id = dag_system.local_peer_id.to_string()
        self.telemetry = telemetry if telemetry is not None else dag_system.telemetry
        self.termination_event = termination_event if termination_event is not None else dag_system.termination_event
        self.publish_interval_seconds = publish_interval_seconds
        self.log_level = log_level
        self.phase_gate = EpochPhaseGate(hypertensor, subnet_id, cutoff_percent=cutoff_percent)
        self.commits = CommitDagPublisher(
            dag_system=dag_system,
            namespace=namespace,
            schema_id=commit_schema_id,
            snapshot_db_key=commit_snapshot_db_key,
            skip_if_orphans=False,
            telemetry=telemetry,
            log_level=log_level,
        )
        self.reveals = RevealDagPublisher(
            dag_system=dag_system,
            namespace=namespace,
            schema_id=reveal_schema_id,
            snapshot_db_key=reveal_snapshot_db_key,
            skip_if_orphans=False,
            telemetry=telemetry,
            log_level=log_level,
        )
        self._pending_by_epoch: dict[int, PendingCommit] = {}

    async def run(self) -> None:
        while not self.termination_event.is_set():
            await self.publish_for_current_phase()
            await trio.sleep(self.publish_interval_seconds)

    async def publish_for_current_phase(self) -> None:
        epoch_data = self.phase_gate.epoch_data()
        if epoch_data.percent_complete < self.phase_gate.cutoff_percent:
            if epoch_data.epoch not in self._pending_by_epoch:
                await self.publish_commit(epoch=epoch_data.epoch)
        else:
            await self.publish_reveal(epoch=epoch_data.epoch)

        for pending_epoch in sorted(self._pending_by_epoch):
            if pending_epoch < epoch_data.epoch:
                await self.publish_reveal(epoch=pending_epoch)

    async def publish_commit(
        self,
        *,
        epoch: int | None = None,
        scores: Mapping[str, int] | None = None,
        nonce: str | None = None,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        epoch_data = self.phase_gate.epoch_data()
        publish_epoch = epoch_data.epoch if epoch is None else epoch
        try:
            self.phase_gate.require_commit_open(publish_epoch)
        except EpochPhaseError as exc:
            logger.log(self.log_level, "Skipping commit publish: %s", exc)
            return None

        score_map = normalize_scores(scores or self.build_scores(publish_epoch))
        reveal_nonce = nonce or secrets.token_hex(16)
        payload = CommitData(
            uid=secrets.token_hex(16),
            epoch=publish_epoch,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            peer_id=self.local_peer_id,
            commitment=commitment_for_scores(
                epoch=publish_epoch,
                subnet_id=self.subnet_id,
                subnet_node_id=self.subnet_node_id,
                peer_id=self.local_peer_id,
                scores=score_map,
                nonce=reveal_nonce,
            ),
        )

        parent_ids = await self._previous_commit_parent_ids(publish_epoch)
        result = await self.commits.publish_payload(payload, parent_ids=parent_ids, created_at_ms=created_at_ms)
        if result is None:
            return None

        self._pending_by_epoch[publish_epoch] = PendingCommit(
            epoch=publish_epoch,
            node_id=result.node_id,
            payload=payload,
            scores=score_map,
            nonce=reveal_nonce,
        )
        logger.log(self.log_level, "Published commit %s for epoch %s", result.node_id, publish_epoch)
        return result

    async def _previous_commit_parent_ids(self, publish_epoch: int) -> tuple[str, ...]:
        latest_node = None
        latest_key: tuple[int, int, str] | None = None

        for node_id in await self.commits.dag.storage.list_complete_node_ids():
            node = await self.commits.dag.get_node(node_id)
            if node is None:
                continue
            if node.header.schema_id != self.commits.schema_id or node.header.author != self.local_peer_id:
                continue
            if await self.commits.dag.storage.get_orphan(node_id) is not None:
                continue

            try:
                commit = CommitData.from_metadata(node.body.payload)
            except Exception:
                logger.debug("Skipping malformed previous commit candidate %s", node_id, exc_info=True)
                continue

            if commit.peer_id != self.local_peer_id:
                continue
            if commit.subnet_id != self.subnet_id or commit.subnet_node_id != self.subnet_node_id:
                continue
            if commit.epoch >= publish_epoch:
                continue

            candidate_key = (commit.epoch, node.header.created_at_ms, node.header.node_id)
            if latest_key is None or candidate_key > latest_key:
                latest_key = candidate_key
                latest_node = node

        if latest_node is None:
            return ()
        return (latest_node.header.node_id,)

    async def publish_reveal(
        self,
        *,
        epoch: int | None = None,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        epoch_data = self.phase_gate.epoch_data()
        reveal_epoch = epoch_data.epoch if epoch is None else epoch
        pending = self._pending_by_epoch.get(reveal_epoch)
        if pending is None or pending.revealed:
            return None

        try:
            self.phase_gate.require_reveal_open(reveal_epoch)
        except EpochPhaseError as exc:
            logger.log(self.log_level, "Skipping reveal publish: %s", exc)
            return None

        payload = RevealData(
            uid=secrets.token_hex(16),
            epoch=reveal_epoch,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            peer_id=self.local_peer_id,
            commit_node_id=pending.node_id,
            scores=pending.scores,
            nonce=pending.nonce,
        )
        result = await self.reveals.publish_payload(
            payload,
            parent_ids=(pending.node_id,),
            created_at_ms=created_at_ms,
        )
        if result is None:
            return None

        pending.revealed = True
        logger.log(self.log_level, "Published reveal %s for epoch %s", result.node_id, reveal_epoch)
        return result

    def build_scores(self, epoch: int) -> dict[str, int]:
        nodes = self.hypertensor.get_min_class_subnet_nodes_formatted(
            subnet_id=self.subnet_id,
            subnet_epoch=epoch,
            min_class=SubnetNodeClass.Included,
        )
        scores = {
            node.peer_info.peer_id: DEFAULT_COMMIT_REVEAL_SCORE
            for node in nodes
            if getattr(getattr(node, "peer_info", None), "peer_id", None)
        }
        if not scores:
            scores[self.local_peer_id] = DEFAULT_COMMIT_REVEAL_SCORE
        return normalize_scores(scores)


__all__ = [
    "COMMIT_REVEAL_CUTOFF_PERCENT",
    "REVEAL_SCHEMA_ID",
    "REVEAL_TOPIC",
    "CommitRevealDagPublisher",
    "EpochPhaseError",
    "EpochPhaseGate",
    "PendingCommit",
    "RevealDagPublisher",
    "RevealDagSchema",
    "RevealData",
    "build_epoch_phase_topic_validator",
]
