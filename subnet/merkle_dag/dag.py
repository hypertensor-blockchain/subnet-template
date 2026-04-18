"""Core Merkle DAG storage, validation, and orphan-resolution engine."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
import logging
import time
from typing import Any

from subnet.merkle_dag.interfaces import DagStorage, Signer
from subnet.merkle_dag.models import (
    DagNode,
    DagNodeBody,
    DagNodeHeader,
    DagNodeSnapshot,
    DagSummary,
    NodeIngestResult,
    NodeIngestStatus,
)
from subnet.merkle_dag.payloads import PayloadSchemaRegistry
from subnet.merkle_dag.serialization import CanonicalJSONSerializer
from subnet.merkle_dag.validator import DagValidator

logger = logging.getLogger(__name__)


class MerkleDag:
    """Validates, stores, and reconciles immutable DAG nodes."""

    def __init__(
        self,
        namespace: str,
        storage: DagStorage,
        validator: DagValidator,
        schema_registry: PayloadSchemaRegistry,
        serializer: CanonicalJSONSerializer,
    ):
        self.namespace = namespace
        self.storage = storage
        self.validator = validator
        self.schema_registry = schema_registry
        self.serializer = serializer

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    async def has_header(self, node_id: str) -> bool:
        """Return whether a header is stored locally."""
        return await self.storage.has_header(node_id)

    async def has_body(self, node_id: str) -> bool:
        """Return whether a body is stored locally."""
        return await self.storage.has_body(node_id)

    async def get_header(self, node_id: str) -> DagNodeHeader | None:
        """Return a node header if present."""
        return await self.storage.get_header(node_id)

    async def get_body(self, node_id: str) -> DagNodeBody | None:
        """Return a node body if present."""
        return await self.storage.get_body(node_id)

    async def get_node(self, node_id: str) -> DagNode | None:
        """Return a complete node if both header and body exist."""
        return await self.storage.get_node(node_id)

    async def get_heads(self) -> tuple[str, ...]:
        """Return the current accepted head set."""
        return await self.storage.get_heads()

    async def create_node(
        self,
        schema_id: str,
        payload: Any,
        parent_ids: Sequence[str],
        signer: Signer,
        author: str,
        *,
        created_at_ms: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DagNode:
        """Build a fully signed node from canonical payload content."""
        canonical_payload = self.schema_registry.canonicalize(schema_id, payload)
        body_hash, body_size = self.validator.compute_body_hash(canonical_payload)
        header_seed = DagNodeHeader(
            node_id="",
            namespace=self.namespace,
            schema_id=schema_id,
            parent_ids=tuple(sorted(set(parent_ids))),
            body_hash=body_hash,
            body_size=body_size,
            author=author,
            public_key=signer.public_key_bytes().hex(),
            signature="",
            created_at_ms=created_at_ms if created_at_ms is not None else self._now_ms(),
            metadata=self.serializer.normalize(metadata or {}),
        )
        node_id = self.validator.compute_node_id(header_seed)
        signature = signer.sign(self.validator.header_signing_bytes(header_seed)).hex()
        header = DagNodeHeader(
            node_id=node_id,
            namespace=self.namespace,
            schema_id=schema_id,
            parent_ids=header_seed.parent_ids,
            body_hash=body_hash,
            body_size=body_size,
            author=author,
            public_key=header_seed.public_key,
            signature=signature,
            created_at_ms=header_seed.created_at_ms,
            metadata=header_seed.metadata,
        )
        body = DagNodeBody(node_id=node_id, payload=canonical_payload)
        return DagNode(header=header, body=body)

    async def add_node(
        self,
        node: DagNode,
        *,
        from_peer: str | None = None,
        validate_remote_timestamp: bool = False,
    ) -> NodeIngestResult:
        """Validate and store a complete node."""
        self.validator.validate_node(node)
        if validate_remote_timestamp:
            self.validator.validate_remote_header(node.header)

        if node.header.namespace != self.namespace:
            return NodeIngestResult(
                node_id=node.header.node_id,
                status=NodeIngestStatus.REJECTED,
                detail=f"Unexpected namespace '{node.header.namespace}'",
            )

        existing = await self.storage.get_node(node.header.node_id)
        if existing is not None:
            return NodeIngestResult(node_id=node.header.node_id, status=NodeIngestStatus.DUPLICATE)

        await self.storage.put_header(node.header)
        await self.storage.put_body(node.body)
        for parent_id in node.header.parent_ids:
            await self.storage.add_child(parent_id, node.header.node_id)

        missing_parents = await self._missing_parents(node.header.parent_ids)
        if missing_parents:
            await self.storage.mark_orphan(node.header.node_id, missing_parents)
            logger.info(
                "Stored orphan node",
                extra={
                    "node_id": node.header.node_id,
                    "missing_parents": missing_parents,
                    "from_peer": from_peer,
                },
            )
            return NodeIngestResult(
                node_id=node.header.node_id,
                status=NodeIngestStatus.ORPHAN,
                missing_parents=missing_parents,
            )

        resolved_nodes = await self._activate_with_descendants(node.header.node_id)
        return NodeIngestResult(
            node_id=node.header.node_id,
            status=NodeIngestStatus.ACCEPTED,
            resolved_nodes=resolved_nodes,
            detail="stored",
        )

    async def add_snapshot(
        self,
        snapshot: DagNodeSnapshot,
        *,
        from_peer: str | None = None,
        validate_remote_timestamp: bool = False,
    ) -> NodeIngestResult:
        """Validate and store a fetched snapshot."""
        self.validator.validate_header(snapshot.header)
        if validate_remote_timestamp:
            self.validator.validate_remote_header(snapshot.header)

        if snapshot.header.namespace != self.namespace:
            return NodeIngestResult(
                node_id=snapshot.header.node_id,
                status=NodeIngestStatus.REJECTED,
                detail=f"Unexpected namespace '{snapshot.header.namespace}'",
            )

        if snapshot.body is not None:
            return await self.add_node(
                DagNode(header=snapshot.header, body=snapshot.body),
                from_peer=from_peer,
                validate_remote_timestamp=validate_remote_timestamp,
            )

        if await self.storage.has_header(snapshot.header.node_id):
            return NodeIngestResult(node_id=snapshot.header.node_id, status=NodeIngestStatus.DUPLICATE)

        await self.storage.put_header(snapshot.header)
        for parent_id in snapshot.header.parent_ids:
            await self.storage.add_child(parent_id, snapshot.header.node_id)

        missing_parents = await self._missing_parents(snapshot.header.parent_ids)
        if missing_parents:
            await self.storage.mark_orphan(snapshot.header.node_id, missing_parents)
            return NodeIngestResult(
                node_id=snapshot.header.node_id,
                status=NodeIngestStatus.ORPHAN,
                missing_parents=missing_parents,
            )

        return NodeIngestResult(node_id=snapshot.header.node_id, status=NodeIngestStatus.PENDING_BODY)

    async def add_body(self, body: DagNodeBody) -> NodeIngestResult:
        """Store a body for a previously known header."""
        header = await self.storage.get_header(body.node_id)
        if header is None:
            return NodeIngestResult(
                node_id=body.node_id,
                status=NodeIngestStatus.REJECTED,
                detail="Body arrived before header",
            )

        self.validator.validate_body(header, body)

        if await self.storage.has_body(body.node_id):
            return NodeIngestResult(node_id=body.node_id, status=NodeIngestStatus.DUPLICATE)

        await self.storage.put_body(body)

        missing_parents = await self._missing_parents(header.parent_ids)
        if missing_parents:
            await self.storage.mark_orphan(body.node_id, missing_parents)
            return NodeIngestResult(
                node_id=body.node_id,
                status=NodeIngestStatus.ORPHAN,
                missing_parents=missing_parents,
            )

        resolved_nodes = await self._activate_with_descendants(body.node_id)
        return NodeIngestResult(
            node_id=body.node_id,
            status=NodeIngestStatus.ACCEPTED,
            resolved_nodes=resolved_nodes,
        )

    async def summary(self) -> DagSummary:
        """Return a lightweight DAG inventory summary."""
        heads = await self.storage.get_heads()
        return DagSummary(
            namespace=self.namespace,
            head_ids=heads,
            node_count=await self.storage.count_complete_nodes(),
            orphan_count=await self.storage.count_orphans(),
            generated_at_ms=self._now_ms(),
        )

    async def snapshots_for_fetch(
        self,
        node_ids: Sequence[str],
        *,
        include_bodies: bool,
        max_ancestor_depth: int,
    ) -> tuple[DagNodeSnapshot, ...]:
        """Build a deterministic fetch response for the requested nodes and ancestors."""
        ordered: list[DagNodeSnapshot] = []
        visited: set[str] = set()

        async def visit(node_id: str, depth: int) -> None:
            if node_id in visited or depth < 0:
                return
            header = await self.storage.get_header(node_id)
            if header is None:
                return
            visited.add(node_id)
            if depth > 0:
                for parent_id in header.parent_ids:
                    await visit(parent_id, depth - 1)
            body = await self.storage.get_body(node_id) if include_bodies else None
            ordered.append(DagNodeSnapshot(header=header, body=body))

        for node_id in sorted(set(node_ids)):
            await visit(node_id, max_ancestor_depth)
        return tuple(ordered)

    async def _missing_parents(self, parent_ids: Sequence[str]) -> tuple[str, ...]:
        missing = [parent_id for parent_id in parent_ids if not await self.storage.has_header(parent_id)]
        return tuple(sorted(missing))

    async def _activate_with_descendants(self, node_id: str) -> tuple[str, ...]:
        queue: deque[str] = deque([node_id])
        activated: list[str] = []

        while queue:
            current_node_id = queue.popleft()
            node = await self.storage.get_node(current_node_id)
            if node is None:
                continue

            missing_parents = await self._missing_parents(node.header.parent_ids)
            if missing_parents:
                await self.storage.mark_orphan(current_node_id, missing_parents)
                continue

            parents: list[DagNode] = []
            for parent_id in node.header.parent_ids:
                parent = await self.storage.get_node(parent_id)
                if parent is None:
                    await self.storage.mark_orphan(current_node_id, (parent_id,))
                    parents = []
                    break
                parents.append(parent)
            if len(parents) != len(node.header.parent_ids):
                continue

            self.validator.validate_activation(node, parents)

            await self.storage.clear_orphan(current_node_id)
            await self.storage.add_head(current_node_id)
            for parent_id in node.header.parent_ids:
                await self.storage.remove_head(parent_id)

            if current_node_id not in activated:
                activated.append(current_node_id)

            waiting_children = await self.storage.get_waiting_children(current_node_id)
            for child_id in waiting_children:
                queue.append(child_id)

        return tuple(activated)
