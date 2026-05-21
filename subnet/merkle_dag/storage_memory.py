"""In-memory storage backend for the Merkle DAG subsystem."""

from __future__ import annotations

import time

from subnet.merkle_dag.models import DagNode, DagNodeBody, DagNodeHeader, OrphanRecord, PeerSyncState


class _NullAsyncLock:
    """No-op async lock for the single-process in-memory backend."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class InMemoryDagStorage:
    """In-memory storage implementation for tests and local development."""

    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self._headers: dict[str, DagNodeHeader] = {}
        self._bodies: dict[str, DagNodeBody] = {}
        self._heads: set[str] = set()
        self._orphans: dict[str, OrphanRecord] = {}
        self._waiting_children: dict[str, set[str]] = {}
        self._seen_announcements: set[str] = set()
        self._peer_states: dict[str, PeerSyncState] = {}
        self._lock = _NullAsyncLock()

    async def has_header(self, node_id: str) -> bool:
        async with self._lock:
            return node_id in self._headers

    async def has_body(self, node_id: str) -> bool:
        async with self._lock:
            return node_id in self._bodies

    async def get_header(self, node_id: str) -> DagNodeHeader | None:
        async with self._lock:
            return self._headers.get(node_id)

    async def get_body(self, node_id: str) -> DagNodeBody | None:
        async with self._lock:
            return self._bodies.get(node_id)

    async def get_node(self, node_id: str) -> DagNode | None:
        async with self._lock:
            header = self._headers.get(node_id)
            body = self._bodies.get(node_id)
            if header is None or body is None:
                return None
            return DagNode(header=header, body=body)

    async def put_header(self, header: DagNodeHeader) -> None:
        async with self._lock:
            self._headers[header.node_id] = header

    async def put_body(self, body: DagNodeBody) -> None:
        async with self._lock:
            self._bodies[body.node_id] = body

    async def get_heads(self) -> tuple[str, ...]:
        async with self._lock:
            return tuple(sorted(self._heads))

    async def add_head(self, node_id: str) -> None:
        async with self._lock:
            self._heads.add(node_id)

    async def remove_head(self, node_id: str) -> None:
        async with self._lock:
            self._heads.discard(node_id)

    async def mark_orphan(self, node_id: str, missing_parents: tuple[str, ...] | list[str]) -> None:
        async with self._lock:
            previous = self._orphans.get(node_id)
            if previous is not None:
                for parent_id in previous.missing_parents:
                    waiting = self._waiting_children.get(parent_id)
                    if waiting is not None:
                        waiting.discard(node_id)
                        if not waiting:
                            self._waiting_children.pop(parent_id, None)
            missing_tuple = tuple(sorted(set(missing_parents)))
            self._orphans[node_id] = OrphanRecord(
                node_id=node_id,
                missing_parents=missing_tuple,
                updated_at_ms=int(time.time() * 1000),
            )
            for parent_id in missing_tuple:
                self._waiting_children.setdefault(parent_id, set()).add(node_id)

    async def clear_orphan(self, node_id: str) -> None:
        async with self._lock:
            record = self._orphans.pop(node_id, None)
            if record is None:
                return
            for parent_id in record.missing_parents:
                waiting = self._waiting_children.get(parent_id)
                if waiting is None:
                    continue
                waiting.discard(node_id)
                if not waiting:
                    self._waiting_children.pop(parent_id, None)

    async def get_orphan(self, node_id: str) -> OrphanRecord | None:
        async with self._lock:
            return self._orphans.get(node_id)

    async def list_orphans(self) -> tuple[OrphanRecord, ...]:
        async with self._lock:
            return tuple(self._orphans[node_id] for node_id in sorted(self._orphans))

    async def get_waiting_children(self, parent_id: str) -> tuple[str, ...]:
        async with self._lock:
            return tuple(sorted(self._waiting_children.get(parent_id, set())))

    async def mark_seen_announcement(self, message_id: str) -> bool:
        async with self._lock:
            if message_id in self._seen_announcements:
                return False
            self._seen_announcements.add(message_id)
            return True

    async def count_orphans(self) -> int:
        async with self._lock:
            return len(self._orphans)

    async def count_complete_nodes(self) -> int:
        async with self._lock:
            return len(set(self._headers).intersection(self._bodies))

    async def list_complete_node_ids(self) -> tuple[str, ...]:
        async with self._lock:
            return tuple(sorted(set(self._headers).intersection(self._bodies)))

    async def get_peer_state(self, peer_id: str) -> PeerSyncState | None:
        async with self._lock:
            return self._peer_states.get(peer_id)

    async def set_peer_state(self, state: PeerSyncState) -> None:
        async with self._lock:
            self._peer_states[state.peer_id] = state
