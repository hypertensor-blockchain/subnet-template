"""Immutable domain models and wire messages for the Merkle DAG subsystem."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from subnet.merkle_dag.types import JSONValue


def _sorted_mapping(value: dict[str, JSONValue] | None) -> dict[str, JSONValue]:
    """Return a shallow key-sorted mapping copy."""
    if value is None:
        return {}
    return {key: value[key] for key in sorted(value)}


@dataclass(frozen=True)
class DagNodeHeader:
    """Immutable node header used for identity, verification, and reconciliation."""

    node_id: str
    namespace: str
    schema_id: str
    parent_ids: tuple[str, ...]
    body_hash: str
    body_size: int
    author: str
    public_key: str
    signature: str
    created_at_ms: int
    version: int = 1
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parent_ids", tuple(self.parent_ids))
        object.__setattr__(self, "metadata", _sorted_mapping(self.metadata))

    def unsigned_primitive(self) -> dict[str, JSONValue]:
        """Return the header fields covered by hashing and signatures."""
        return {
            "author": self.author,
            "body_hash": self.body_hash,
            "body_size": self.body_size,
            "created_at_ms": self.created_at_ms,
            "metadata": dict(self.metadata),
            "namespace": self.namespace,
            "parent_ids": list(self.parent_ids),
            "public_key": self.public_key,
            "schema_id": self.schema_id,
            "version": self.version,
        }

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the full header representation."""
        payload = self.unsigned_primitive()
        payload["node_id"] = self.node_id
        payload["signature"] = self.signature
        return payload

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagNodeHeader":
        """Construct a header from a JSON-compatible mapping."""
        return cls(
            node_id=str(value["node_id"]),
            namespace=str(value["namespace"]),
            schema_id=str(value["schema_id"]),
            parent_ids=tuple(str(parent) for parent in value.get("parent_ids", [])),
            body_hash=str(value["body_hash"]),
            body_size=int(value["body_size"]),
            author=str(value["author"]),
            public_key=str(value["public_key"]),
            signature=str(value["signature"]),
            created_at_ms=int(value["created_at_ms"]),
            version=int(value.get("version", 1)),
            metadata=_sorted_mapping(dict(value.get("metadata", {}))),
        )


@dataclass(frozen=True)
class DagNodeBody:
    """Immutable node payload body stored separately from the header."""

    node_id: str
    payload: JSONValue

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible body representation."""
        return {
            "node_id": self.node_id,
            "payload": self.payload,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagNodeBody":
        """Construct a body from a JSON-compatible mapping."""
        return cls(node_id=str(value["node_id"]), payload=value["payload"])


@dataclass(frozen=True)
class DagNode:
    """A complete immutable DAG node containing header and body."""

    header: DagNodeHeader
    body: DagNodeBody

    def to_snapshot(self) -> "DagNodeSnapshot":
        """Convert to a transfer snapshot."""
        return DagNodeSnapshot(header=self.header, body=self.body)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible node representation."""
        return {
            "header": self.header.to_primitive(),
            "body": self.body.to_primitive(),
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagNode":
        """Construct a complete node from a JSON-compatible mapping."""
        return cls(
            header=DagNodeHeader.from_primitive(dict(value["header"])),
            body=DagNodeBody.from_primitive(dict(value["body"])),
        )


@dataclass(frozen=True)
class DagNodeSnapshot:
    """Transfer object for header-only or header-plus-body fetches."""

    header: DagNodeHeader
    body: DagNodeBody | None = None

    def to_node(self) -> DagNode:
        """Convert a complete snapshot into a `DagNode`."""
        if self.body is None:
            raise ValueError("Snapshot does not contain a body")
        return DagNode(header=self.header, body=self.body)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible snapshot representation."""
        payload: dict[str, JSONValue] = {"header": self.header.to_primitive()}
        payload["body"] = None if self.body is None else self.body.to_primitive()
        return payload

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagNodeSnapshot":
        """Construct a snapshot from a JSON-compatible mapping."""
        body_value = value.get("body")
        return cls(
            header=DagNodeHeader.from_primitive(dict(value["header"])),
            body=None if body_value is None else DagNodeBody.from_primitive(dict(body_value)),
        )


class NodeIngestStatus(str, Enum):
    """Outcomes for node/header/body ingestion."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    ORPHAN = "orphan"
    PENDING_BODY = "pending_body"
    REJECTED = "rejected"


@dataclass(frozen=True)
class NodeIngestResult:
    """Result returned by Merkle DAG ingestion methods."""

    node_id: str
    status: NodeIngestStatus
    missing_parents: tuple[str, ...] = ()
    resolved_nodes: tuple[str, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class OrphanRecord:
    """Tracks a node waiting on missing parents."""

    node_id: str
    missing_parents: tuple[str, ...]
    updated_at_ms: int

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible orphan representation."""
        return {
            "missing_parents": list(self.missing_parents),
            "node_id": self.node_id,
            "updated_at_ms": self.updated_at_ms,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "OrphanRecord":
        """Construct an orphan record from a JSON-compatible mapping."""
        return cls(
            node_id=str(value["node_id"]),
            missing_parents=tuple(str(parent) for parent in value.get("missing_parents", [])),
            updated_at_ms=int(value["updated_at_ms"]),
        )


@dataclass(frozen=True)
class DagSummary:
    """Lightweight inventory summary exchanged during reconciliation."""

    namespace: str
    head_ids: tuple[str, ...]
    node_count: int
    orphan_count: int
    generated_at_ms: int

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible summary representation."""
        return {
            "generated_at_ms": self.generated_at_ms,
            "head_ids": list(self.head_ids),
            "namespace": self.namespace,
            "node_count": self.node_count,
            "orphan_count": self.orphan_count,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagSummary":
        """Construct a summary from a JSON-compatible mapping."""
        return cls(
            namespace=str(value["namespace"]),
            head_ids=tuple(str(node_id) for node_id in value.get("head_ids", [])),
            node_count=int(value["node_count"]),
            orphan_count=int(value["orphan_count"]),
            generated_at_ms=int(value["generated_at_ms"]),
        )


@dataclass(frozen=True)
class PeerSyncState:
    """Cached sync metadata about a remote peer."""

    peer_id: str
    summary: DagSummary
    updated_at_ms: int

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible peer sync state."""
        return {
            "peer_id": self.peer_id,
            "summary": self.summary.to_primitive(),
            "updated_at_ms": self.updated_at_ms,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "PeerSyncState":
        """Construct peer sync state from a JSON-compatible mapping."""
        return cls(
            peer_id=str(value["peer_id"]),
            summary=DagSummary.from_primitive(dict(value["summary"])),
            updated_at_ms=int(value["updated_at_ms"]),
        )


class SyncMessageKind(str, Enum):
    """Wire message kinds for gossip announcements and direct sync calls."""

    NODE_GOSSIP = "node_gossip"
    ANNOUNCEMENT = "announcement"
    INVENTORY_REQUEST = "inventory_request"
    INVENTORY_RESPONSE = "inventory_response"
    FETCH_REQUEST = "fetch_request"
    FETCH_RESPONSE = "fetch_response"


@dataclass(frozen=True)
class DagNodeGossip:
    """GossipSub message carrying a complete DAG node for live replication."""

    message_id: str
    namespace: str
    peer_id: str
    node: DagNode
    created_at_ms: int

    kind: SyncMessageKind = field(default=SyncMessageKind.NODE_GOSSIP, init=False)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible wire representation."""
        return {
            "created_at_ms": self.created_at_ms,
            "kind": self.kind.value,
            "message_id": self.message_id,
            "namespace": self.namespace,
            "node": self.node.to_primitive(),
            "peer_id": self.peer_id,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagNodeGossip":
        """Construct a node gossip message from a JSON-compatible mapping."""
        return cls(
            message_id=str(value["message_id"]),
            namespace=str(value["namespace"]),
            peer_id=str(value["peer_id"]),
            node=DagNode.from_primitive(dict(value["node"])),
            created_at_ms=int(value["created_at_ms"]),
        )


@dataclass(frozen=True)
class DagAnnouncement:
    """Lightweight GossipSub announcement of current DAG heads."""

    message_id: str
    namespace: str
    peer_id: str
    head_ids: tuple[str, ...]
    node_count: int
    created_at_ms: int

    kind: SyncMessageKind = field(default=SyncMessageKind.ANNOUNCEMENT, init=False)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible wire representation."""
        return {
            "created_at_ms": self.created_at_ms,
            "head_ids": list(self.head_ids),
            "kind": self.kind.value,
            "message_id": self.message_id,
            "namespace": self.namespace,
            "node_count": self.node_count,
            "peer_id": self.peer_id,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagAnnouncement":
        """Construct an announcement from a JSON-compatible mapping."""
        return cls(
            message_id=str(value["message_id"]),
            namespace=str(value["namespace"]),
            peer_id=str(value["peer_id"]),
            head_ids=tuple(str(head_id) for head_id in value.get("head_ids", [])),
            node_count=int(value["node_count"]),
            created_at_ms=int(value["created_at_ms"]),
        )


@dataclass(frozen=True)
class DagInventoryRequest:
    """Direct request asking a peer for its current DAG summary."""

    message_id: str
    namespace: str
    peer_id: str
    known_heads: tuple[str, ...]
    node_count: int
    created_at_ms: int

    kind: SyncMessageKind = field(default=SyncMessageKind.INVENTORY_REQUEST, init=False)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible wire representation."""
        return {
            "created_at_ms": self.created_at_ms,
            "kind": self.kind.value,
            "known_heads": list(self.known_heads),
            "message_id": self.message_id,
            "namespace": self.namespace,
            "node_count": self.node_count,
            "peer_id": self.peer_id,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagInventoryRequest":
        """Construct an inventory request from a JSON-compatible mapping."""
        return cls(
            message_id=str(value["message_id"]),
            namespace=str(value["namespace"]),
            peer_id=str(value["peer_id"]),
            known_heads=tuple(str(head_id) for head_id in value.get("known_heads", [])),
            node_count=int(value["node_count"]),
            created_at_ms=int(value["created_at_ms"]),
        )


@dataclass(frozen=True)
class DagInventoryResponse:
    """Direct response carrying a peer's current DAG summary."""

    message_id: str
    namespace: str
    peer_id: str
    summary: DagSummary
    created_at_ms: int

    kind: SyncMessageKind = field(default=SyncMessageKind.INVENTORY_RESPONSE, init=False)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible wire representation."""
        return {
            "created_at_ms": self.created_at_ms,
            "kind": self.kind.value,
            "message_id": self.message_id,
            "namespace": self.namespace,
            "peer_id": self.peer_id,
            "summary": self.summary.to_primitive(),
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagInventoryResponse":
        """Construct an inventory response from a JSON-compatible mapping."""
        return cls(
            message_id=str(value["message_id"]),
            namespace=str(value["namespace"]),
            peer_id=str(value["peer_id"]),
            summary=DagSummary.from_primitive(dict(value["summary"])),
            created_at_ms=int(value["created_at_ms"]),
        )


@dataclass(frozen=True)
class DagFetchRequest:
    """Direct request asking a peer for headers, bodies, and optionally ancestors."""

    message_id: str
    namespace: str
    peer_id: str
    node_ids: tuple[str, ...]
    include_bodies: bool
    max_ancestor_depth: int
    created_at_ms: int

    kind: SyncMessageKind = field(default=SyncMessageKind.FETCH_REQUEST, init=False)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible wire representation."""
        return {
            "created_at_ms": self.created_at_ms,
            "include_bodies": self.include_bodies,
            "kind": self.kind.value,
            "max_ancestor_depth": self.max_ancestor_depth,
            "message_id": self.message_id,
            "namespace": self.namespace,
            "node_ids": list(self.node_ids),
            "peer_id": self.peer_id,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagFetchRequest":
        """Construct a fetch request from a JSON-compatible mapping."""
        return cls(
            message_id=str(value["message_id"]),
            namespace=str(value["namespace"]),
            peer_id=str(value["peer_id"]),
            node_ids=tuple(str(node_id) for node_id in value.get("node_ids", [])),
            include_bodies=bool(value["include_bodies"]),
            max_ancestor_depth=int(value["max_ancestor_depth"]),
            created_at_ms=int(value["created_at_ms"]),
        )


@dataclass(frozen=True)
class DagFetchResponse:
    """Direct response carrying headers/bodies for requested nodes and ancestors."""

    message_id: str
    namespace: str
    peer_id: str
    nodes: tuple[DagNodeSnapshot, ...]
    not_found: tuple[str, ...]
    created_at_ms: int

    kind: SyncMessageKind = field(default=SyncMessageKind.FETCH_RESPONSE, init=False)

    def to_primitive(self) -> dict[str, JSONValue]:
        """Return the JSON-compatible wire representation."""
        return {
            "created_at_ms": self.created_at_ms,
            "kind": self.kind.value,
            "message_id": self.message_id,
            "namespace": self.namespace,
            "nodes": [node.to_primitive() for node in self.nodes],
            "not_found": list(self.not_found),
            "peer_id": self.peer_id,
        }

    @classmethod
    def from_primitive(cls, value: dict[str, Any]) -> "DagFetchResponse":
        """Construct a fetch response from a JSON-compatible mapping."""
        return cls(
            message_id=str(value["message_id"]),
            namespace=str(value["namespace"]),
            peer_id=str(value["peer_id"]),
            nodes=tuple(DagNodeSnapshot.from_primitive(dict(item)) for item in value.get("nodes", [])),
            not_found=tuple(str(node_id) for node_id in value.get("not_found", [])),
            created_at_ms=int(value["created_at_ms"]),
        )
