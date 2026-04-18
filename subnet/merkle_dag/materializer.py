"""Helpers for replaying and materializing state from DAG nodes."""

from __future__ import annotations

from typing import Any

from subnet.merkle_dag.dag import MerkleDag
from subnet.merkle_dag.models import DagNode


class DagStateMaterializer:
    """Topologically replays reachable nodes and materializes per-node state."""

    def __init__(self, dag: MerkleDag):
        self._dag = dag

    async def topological_order(self, head_ids: tuple[str, ...] | None = None) -> tuple[DagNode, ...]:
        """Return a deterministic topological ordering for the selected heads."""
        order: list[DagNode] = []
        visited: set[str] = set()
        heads = head_ids or await self._dag.get_heads()

        async def visit(node_id: str) -> None:
            if node_id in visited:
                return
            visited.add(node_id)
            header = await self._dag.get_header(node_id)
            if header is None:
                return
            for parent_id in header.parent_ids:
                await visit(parent_id)
            node = await self._dag.get_node(node_id)
            if node is not None:
                order.append(node)

        for head_id in sorted(heads):
            await visit(head_id)
        return tuple(order)

    async def materialize(self, head_ids: tuple[str, ...] | None = None) -> dict[str, Any]:
        """Materialize a value for every reachable node using its schema handler."""
        states: dict[str, Any] = {}
        for node in await self.topological_order(head_ids):
            schema = self._dag.schema_registry.require(node.header.schema_id)
            parent_states = tuple(states[parent_id] for parent_id in node.header.parent_ids if parent_id in states)
            states[node.header.node_id] = schema.materialize(node, parent_states)
        return states
