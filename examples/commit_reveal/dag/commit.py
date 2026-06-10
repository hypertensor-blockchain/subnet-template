from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import ConfigDict

from subnet.merkle_dag import CanonicalJSONSerializer, SHA256Hasher
from subnet.merkle_dag.bases.dag_publisher_template import (
    DagPayloadTemplate,
    DagPublisherTemplate,
    DagPublisherTemplateSchema,
)
from subnet.merkle_dag.exceptions import PayloadValidationError

COMMIT_REVEAL_DAG_NAMESPACE = "commit-reveal"
COMMIT_TOPIC = "commit-topic"
COMMIT_SCHEMA_ID = "commit-schema"
DEFAULT_COMMIT_REVEAL_SCORE = int(1e18)

_SERIALIZER = CanonicalJSONSerializer()
_HASHER = SHA256Hasher()


def normalize_scores(scores: Mapping[str, int]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for peer_id, score in scores.items():
        if not isinstance(peer_id, str) or not peer_id:
            raise ValueError("score keys must be non-empty peer id strings")
        if isinstance(score, bool):
            raise ValueError("scores must be integers")
        int_score = int(score)
        if int_score < 0 or int_score > DEFAULT_COMMIT_REVEAL_SCORE:
            raise ValueError(f"scores must be between 0 and {DEFAULT_COMMIT_REVEAL_SCORE}")
        normalized[peer_id] = int_score
    return {peer_id: normalized[peer_id] for peer_id in sorted(normalized)}


def commitment_payload(
    *,
    epoch: int,
    subnet_id: int,
    subnet_node_id: int,
    peer_id: str,
    scores: Mapping[str, int],
    nonce: str,
) -> dict[str, Any]:
    return {
        "epoch": int(epoch),
        "nonce": str(nonce),
        "peer_id": str(peer_id),
        "scores": normalize_scores(scores),
        "subnet_id": int(subnet_id),
        "subnet_node_id": int(subnet_node_id),
    }


def commitment_for_scores(
    *,
    epoch: int,
    subnet_id: int,
    subnet_node_id: int,
    peer_id: str,
    scores: Mapping[str, int],
    nonce: str,
) -> str:
    return _HASHER.digest(
        _SERIALIZER.serialize(
            commitment_payload(
                epoch=epoch,
                subnet_id=subnet_id,
                subnet_node_id=subnet_node_id,
                peer_id=peer_id,
                scores=scores,
                nonce=nonce,
            )
        )
    )


class CommitData(DagPayloadTemplate):
    model_config = ConfigDict(extra="ignore", frozen=True)

    uid: str
    epoch: int
    subnet_id: int
    subnet_node_id: int
    peer_id: str
    commitment: str

    def model_post_init(self, __context: Any) -> None:
        assert self.uid, "uid is required"
        assert self.epoch >= 0, "epoch must be non-negative"
        assert self.subnet_id >= 0, "subnet_id must be non-negative"
        assert self.subnet_node_id >= 0, "subnet_node_id must be non-negative"
        assert self.peer_id, "peer_id is required"
        assert self.commitment, "commitment is required"


class CommitDagSchema(DagPublisherTemplateSchema[CommitData]):
    def __init__(
        self,
        schema_id: str = COMMIT_SCHEMA_ID,
        *,
        commit_schema_id: str | None = None,
    ) -> None:
        super().__init__(schema_id, CommitData)
        self.commit_schema_id = commit_schema_id or schema_id

    def validate_signer_peer(self, node, signer_peer_id: str) -> None:
        commit = CommitData.from_metadata(node.body.payload)
        if commit.peer_id != signer_peer_id:
            raise PayloadValidationError(
                f"commit peer_id {commit.peer_id!r} does not match signer peer {signer_peer_id!r}"
            )

    def validate_parent_links(self, node, parents) -> None:
        if len(parents) > 1:
            raise PayloadValidationError("commit nodes can reference at most one previous commit parent")
        if not parents:
            return

        commit = CommitData.from_metadata(node.body.payload)
        parent_node = parents[0]
        if parent_node.header.schema_id != self.commit_schema_id:
            raise PayloadValidationError(
                f"commit parent must have schema_id={self.commit_schema_id!r}, "
                f"got {parent_node.header.schema_id!r}"
            )
        if node.header.author != parent_node.header.author:
            raise PayloadValidationError("commit parent must be signed by the same peer")

        parent_commit = CommitData.from_metadata(parent_node.body.payload)
        if parent_commit.peer_id != commit.peer_id:
            raise PayloadValidationError("commit parent peer_id does not match child commit")
        if parent_commit.subnet_id != commit.subnet_id:
            raise PayloadValidationError("commit parent subnet_id does not match child commit")
        if parent_commit.subnet_node_id != commit.subnet_node_id:
            raise PayloadValidationError("commit parent subnet_node_id does not match child commit")
        if parent_commit.epoch >= commit.epoch:
            raise PayloadValidationError("commit parent must be from an earlier epoch")


class CommitDagPublisher(DagPublisherTemplate[CommitData]):
    def build_payload(self) -> CommitData:
        raise RuntimeError("CommitDagPublisher requires caller-supplied CommitData")

    async def build_metadata(self, payload: CommitData) -> Mapping[str, Any] | None:
        return {
            "commitment": payload.commitment,
            "epoch": payload.epoch,
            "peer_id": payload.peer_id,
            "subnet_id": payload.subnet_id,
            "subnet_node_id": payload.subnet_node_id,
            "uid": payload.uid,
        }


__all__ = [
    "COMMIT_REVEAL_DAG_NAMESPACE",
    "COMMIT_SCHEMA_ID",
    "COMMIT_TOPIC",
    "DEFAULT_COMMIT_REVEAL_SCORE",
    "CommitDagPublisher",
    "CommitDagSchema",
    "CommitData",
    "commitment_for_scores",
    "commitment_payload",
    "normalize_scores",
]
