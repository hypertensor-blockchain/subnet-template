"""Payload schema registration and helpers."""

from __future__ import annotations

from typing import Any

from subnet.merkle_dag.exceptions import PayloadValidationError, SchemaNotFoundError
from subnet.merkle_dag.interfaces import PayloadSchema
from subnet.merkle_dag.models import DagNode
from subnet.merkle_dag.serialization import CanonicalJSONSerializer
from subnet.merkle_dag.types import JSONValue


class PayloadSchemaRegistry:
    """Registry of payload schemas used for validation and materialization."""

    def __init__(self, schemas: list[PayloadSchema] | None = None):
        self._schemas: dict[str, PayloadSchema] = {}
        for schema in schemas or []:
            self.register(schema)

    def register(self, schema: PayloadSchema) -> None:
        """Register or replace a schema."""
        self._schemas[schema.schema_id] = schema

    def require(self, schema_id: str) -> PayloadSchema:
        """Return a schema or raise if it is not registered."""
        schema = self._schemas.get(schema_id)
        if schema is None:
            raise SchemaNotFoundError(f"Unknown payload schema: {schema_id}")
        return schema

    def canonicalize(self, schema_id: str, payload: Any) -> JSONValue:
        """Canonicalize and validate a payload for the given schema."""
        schema = self.require(schema_id)
        canonical_payload = schema.canonicalize_payload(payload)
        schema.validate_payload(canonical_payload)
        return canonical_payload


class MappingPayloadSchema:
    """Convenience base class for dict-like payload schemas."""

    def __init__(self, schema_id: str):
        self.schema_id = schema_id
        self._serializer = CanonicalJSONSerializer()

    def canonicalize_payload(self, payload: Any) -> JSONValue:
        """Normalize payloads through the canonical serializer."""
        return self._serializer.normalize(payload)

    def validate_payload(self, payload: JSONValue) -> None:
        """Default payload validation requiring an object payload."""
        if not isinstance(payload, dict):
            raise PayloadValidationError(f"Schema '{self.schema_id}' requires a mapping payload")

    def validate_parent_links(self, node: DagNode, parents: tuple[DagNode, ...]) -> None:
        """Default parent-link validation is a no-op."""

    def validate_signer_peer(self, node: DagNode, signer_peer_id: str) -> None:
        """Default signer-identity validation is a no-op."""

    def materialize(self, node: DagNode, parent_states: tuple[Any, ...]) -> Any:
        """Default materialization returns the node payload."""
        return node.body.payload
