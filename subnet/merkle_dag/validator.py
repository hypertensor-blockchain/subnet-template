"""Validation pipeline for Merkle DAG headers, bodies, and activated nodes."""

from __future__ import annotations

from binascii import Error as BinasciiError
from collections.abc import Sequence
import time
from typing import Callable

from libp2p.peer.id import ID
from libp2p.records.pubkey import unmarshal_public_key

from subnet.merkle_dag.exceptions import (
    HashMismatchError,
    ParentValidationError,
    SignatureVerificationError,
    SourcePeerMismatchError,
    TimestampValidationError,
)
from subnet.merkle_dag.interfaces import DomainValidator, HashProvider, SignatureVerifier
from subnet.merkle_dag.models import DagNode, DagNodeBody, DagNodeHeader
from subnet.merkle_dag.payloads import PayloadSchemaRegistry
from subnet.merkle_dag.serialization import CanonicalJSONSerializer


class DagValidator:
    """Validates all local and remote Merkle DAG content before acceptance."""

    def __init__(
        self,
        serializer: CanonicalJSONSerializer,
        hasher: HashProvider,
        schema_registry: PayloadSchemaRegistry,
        signature_verifier: SignatureVerifier,
        domain_validators: Sequence[DomainValidator] | None = None,
        max_future_skew_ms: int | None = 60_000,
        now_ms: Callable[[], int] | None = None,
    ):
        self._serializer = serializer
        self._hasher = hasher
        self._schemas = schema_registry
        self._signature_verifier = signature_verifier
        self._domain_validators = tuple(domain_validators or ())
        self._max_future_skew_ms = max_future_skew_ms
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))

    def header_signing_bytes(self, header: DagNodeHeader) -> bytes:
        """Return the bytes covered by hashing and signature verification."""
        return self._serializer.serialize(header.unsigned_primitive())

    def payload_bytes(self, payload: object) -> bytes:
        """Return canonical payload bytes."""
        return self._serializer.serialize(payload)

    def compute_body_hash(self, payload: object) -> tuple[str, int]:
        """Return the canonical payload hash and size."""
        payload_bytes = self.payload_bytes(payload)
        return self._hasher.digest(payload_bytes), len(payload_bytes)

    def compute_node_id(self, header: DagNodeHeader) -> str:
        """Return the expected content hash for a header."""
        return self._hasher.digest(self.header_signing_bytes(header))

    def validate_header(self, header: DagNodeHeader) -> None:
        """Validate a header independently of parent availability."""
        self._schemas.require(header.schema_id)

        if list(header.parent_ids) != sorted(header.parent_ids):
            raise ParentValidationError("Parent ids must be sorted lexicographically")
        if len(set(header.parent_ids)) != len(header.parent_ids):
            raise ParentValidationError("Parent ids must be unique")
        if header.node_id in header.parent_ids:
            raise ParentValidationError("Nodes cannot reference themselves as parents")
        if header.body_size < 0:
            raise ParentValidationError("Body size must be non-negative")

        expected_node_id = self.compute_node_id(header)
        if expected_node_id != header.node_id:
            raise HashMismatchError(f"Header hash mismatch for node {header.node_id}")

        try:
            signature = bytes.fromhex(header.signature)
            public_key = bytes.fromhex(header.public_key)
        except (ValueError, BinasciiError) as exc:
            raise SignatureVerificationError("Invalid signature encoding") from exc

        if not self._signature_verifier.verify(self.header_signing_bytes(header), signature, public_key):
            raise SignatureVerificationError(f"Signature verification failed for node {header.node_id}")

        self.validate_header_author(header)

    def validate_body(self, header: DagNodeHeader, body: DagNodeBody) -> None:
        """Validate a body against its header and payload schema."""
        if body.node_id != header.node_id:
            raise HashMismatchError("Body node id does not match header node id")

        body_hash, body_size = self.compute_body_hash(body.payload)
        if body_hash != header.body_hash:
            raise HashMismatchError(f"Body hash mismatch for node {header.node_id}")
        if body_size != header.body_size:
            raise HashMismatchError(f"Body size mismatch for node {header.node_id}")

        schema = self._schemas.require(header.schema_id)
        schema.validate_payload(body.payload)

    def header_signer_peer_id(self, header: DagNodeHeader) -> str:
        """Return the libp2p peer id derived from the header's signing key."""
        try:
            public_key = unmarshal_public_key(bytes.fromhex(header.public_key))
        except (ValueError, BinasciiError) as exc:
            raise SourcePeerMismatchError("Invalid public key encoding") from exc
        return ID.from_pubkey(public_key).to_string()

    def validate_header_source_peer(self, header: DagNodeHeader, source_peer: str) -> None:
        """Validate that a transport sender matches the header's signed identity."""
        signed_peer_id = self.header_signer_peer_id(header)
        if signed_peer_id != source_peer:
            raise SourcePeerMismatchError(
                f"Transport sender {source_peer} does not match signed node identity {signed_peer_id}"
            )

    def validate_header_author(self, header: DagNodeHeader) -> None:
        """Validate that the stored header author matches the signed identity."""
        signed_peer_id = self.header_signer_peer_id(header)
        if header.author != signed_peer_id:
            raise SourcePeerMismatchError(
                f"Header author {header.author} does not match signed node identity {signed_peer_id}"
            )

    def validate_remote_header(self, header: DagNodeHeader) -> None:
        """Validate remote-only timestamp sanity constraints."""
        if self._max_future_skew_ms is None:
            return

        now_ms = self._now_ms()
        max_allowed_ms = now_ms + self._max_future_skew_ms
        if header.created_at_ms > max_allowed_ms:
            raise TimestampValidationError(
                f"Node {header.node_id} created_at_ms {header.created_at_ms} exceeds local allowance {max_allowed_ms}"
            )

    def validate_node(self, node: DagNode) -> None:
        """Validate a complete node independently of parents."""
        self.validate_header(node.header)
        self.validate_body(node.header, node.body)
        schema = self._schemas.require(node.header.schema_id)
        schema.validate_signer_peer(node, self.header_signer_peer_id(node.header))

    def validate_activation(self, node: DagNode, parents: Sequence[DagNode]) -> None:
        """Validate a complete node once all parent nodes are available."""
        if any(parent.header.namespace != node.header.namespace for parent in parents):
            raise ParentValidationError("Parent namespace mismatch")

        schema = self._schemas.require(node.header.schema_id)
        schema.validate_parent_links(node, tuple(parents))
        for validator in self._domain_validators:
            validator.validate(node, tuple(parents))
