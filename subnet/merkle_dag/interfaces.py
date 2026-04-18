"""Protocols for the transport-agnostic Merkle DAG subsystem."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Any, Protocol

from subnet.merkle_dag.models import (
    DagAnnouncement,
    DagFetchRequest,
    DagFetchResponse,
    DagInventoryRequest,
    DagInventoryResponse,
    DagNode,
    DagNodeBody,
    DagNodeGossip,
    DagNodeHeader,
    OrphanRecord,
    PeerSyncState,
)
from subnet.merkle_dag.types import JSONValue


class CanonicalSerializer(Protocol):
    """Serializes JSON-compatible data deterministically."""

    def normalize(self, value: Any) -> JSONValue:
        """Normalize arbitrary JSON-compatible input."""

    def serialize(self, value: Any) -> bytes:
        """Serialize a value to canonical bytes."""

    def deserialize(self, payload: bytes) -> JSONValue:
        """Deserialize canonical bytes into a JSON value."""


class HashProvider(Protocol):
    """Computes deterministic content hashes."""

    @property
    def algorithm(self) -> str:
        """Hash algorithm identifier."""

    def digest(self, payload: bytes) -> str:
        """Return a stable digest for the supplied bytes."""


class Signer(Protocol):
    """Signs serialized node headers."""

    def public_key_bytes(self) -> bytes:
        """Return the serialized public key."""

    def sign(self, payload: bytes) -> bytes:
        """Sign the supplied bytes."""


class SignatureVerifier(Protocol):
    """Verifies detached signatures."""

    def verify(self, payload: bytes, signature: bytes, public_key: bytes) -> bool:
        """Return whether the detached signature is valid."""


class PayloadSchema(Protocol):
    """Validates, canonicalizes, and optionally materializes payloads."""

    schema_id: str

    def canonicalize_payload(self, payload: Any) -> JSONValue:
        """Normalize the raw payload into a JSON-compatible value."""

    def validate_payload(self, payload: JSONValue) -> None:
        """Raise if the canonical payload is invalid."""

    def validate_parent_links(self, node: DagNode, parents: Sequence[DagNode]) -> None:
        """Raise if the node and parent combination is invalid."""

    def validate_signer_peer(self, node: DagNode, signer_peer_id: str) -> None:
        """Raise if the node's signed peer identity is not allowed to publish this payload."""

    def materialize(self, node: DagNode, parent_states: Sequence[Any]) -> Any:
        """Materialize a state value from a node and its parent states."""


class DomainValidator(Protocol):
    """Validates domain-specific node semantics after structural checks."""

    def validate(self, node: DagNode, parents: Sequence[DagNode]) -> None:
        """Raise if the node is domain-invalid."""


class DagStorage(Protocol):
    """Abstract node/index storage used by the DAG engine."""

    async def has_header(self, node_id: str) -> bool:
        """Return whether a header exists."""

    async def has_body(self, node_id: str) -> bool:
        """Return whether a body exists."""

    async def get_header(self, node_id: str) -> DagNodeHeader | None:
        """Return the stored header if present."""

    async def get_body(self, node_id: str) -> DagNodeBody | None:
        """Return the stored body if present."""

    async def get_node(self, node_id: str) -> DagNode | None:
        """Return a complete node if both header and body are present."""

    async def put_header(self, header: DagNodeHeader) -> None:
        """Store a node header."""

    async def put_body(self, body: DagNodeBody) -> None:
        """Store a node body."""

    async def get_heads(self) -> tuple[str, ...]:
        """Return the current accepted head set."""

    async def add_head(self, node_id: str) -> None:
        """Add a node to the head set."""

    async def remove_head(self, node_id: str) -> None:
        """Remove a node from the head set."""

    async def mark_orphan(self, node_id: str, missing_parents: Sequence[str]) -> None:
        """Track an orphan waiting on missing parents."""

    async def clear_orphan(self, node_id: str) -> None:
        """Remove orphan tracking for a node."""

    async def get_orphan(self, node_id: str) -> OrphanRecord | None:
        """Return orphan metadata for a node if present."""

    async def list_orphans(self) -> tuple[OrphanRecord, ...]:
        """Return all orphan records currently waiting on missing parents."""

    async def get_children(self, parent_id: str) -> tuple[str, ...]:
        """Return known children for a parent."""

    async def add_child(self, parent_id: str, child_id: str) -> None:
        """Track a parent-to-child relationship."""

    async def get_waiting_children(self, parent_id: str) -> tuple[str, ...]:
        """Return orphaned children waiting on a parent."""

    async def mark_seen_announcement(self, message_id: str) -> bool:
        """Return False if this announcement was already processed."""

    async def count_orphans(self) -> int:
        """Return the number of nodes currently waiting on missing parents."""

    async def count_complete_nodes(self) -> int:
        """Return the number of complete nodes stored locally."""

    async def list_complete_node_ids(self) -> tuple[str, ...]:
        """Return the set of node ids that have both header and body."""

    async def get_peer_state(self, peer_id: str) -> PeerSyncState | None:
        """Return the last known sync state for a peer."""

    async def set_peer_state(self, state: PeerSyncState) -> None:
        """Persist sync metadata for a peer."""


SyncRequest = DagInventoryRequest | DagFetchRequest
SyncResponse = DagInventoryResponse | DagFetchResponse
SyncMessage = DagNodeGossip | DagAnnouncement | SyncRequest | SyncResponse


class GossipPublisher(Protocol):
    """Publishes lightweight sync messages over an existing gossip layer."""

    async def publish(self, topic: str, payload: bytes) -> None:
        """Publish raw bytes onto the configured gossip topic."""


class PeerRequestClient(Protocol):
    """Issues direct request-response sync calls to peers."""

    async def request(self, peer_id: str, message: SyncRequest) -> SyncResponse:
        """Send a typed request and return the typed response."""


class PeerSetProvider(Protocol):
    """Lists peers that should participate in reconciliation."""

    async def list_peer_ids(self) -> tuple[str, ...]:
        """Return currently known peer ids."""


PeerRequestCallable = Callable[[str, SyncRequest], Awaitable[SyncResponse]]
PeerListCallable = Callable[[], Awaitable[Iterable[str]] | Iterable[str]]
