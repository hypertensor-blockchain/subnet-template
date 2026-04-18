"""RocksDB-backed storage for Merkle DAG state and indexes."""

from __future__ import annotations

import json
import time
from typing import Any

import trio

from subnet.merkle_dag.models import DagNode, DagNodeBody, DagNodeHeader, OrphanRecord, PeerSyncState
from subnet.merkle_dag.serialization import CanonicalJSONSerializer
from subnet.utils.db.database import RocksDB


class RocksDBDagStorage:
    """Persists DAG state using the repository's existing RocksDB wrapper."""

    HEADS_MAP = "dag_heads"
    HEADERS_MAP = "dag_headers"
    BODIES_MAP = "dag_bodies"
    ORPHANS_MAP = "dag_orphans"
    SEEN_MAP = "dag_seen_announcements"
    PEER_STATE_MAP = "dag_peer_state"
    CHILDREN_KEY = "dag_children"
    WAITING_KEY = "dag_waiting"

    def __init__(
        self,
        db: RocksDB,
        serializer: CanonicalJSONSerializer | None = None,
        *,
        namespace: str = "default",
    ):
        self._db = db
        self._serializer = serializer or CanonicalJSONSerializer()
        self._namespace = namespace
        self._lock = trio.Lock()

    def _scope(self, name: str) -> str:
        return f"{name}:{self._namespace}"

    def _dump(self, value: Any) -> str:
        return self._serializer.serialize(value).decode("utf-8")

    def _load_json(self, value: str | None, default: Any) -> Any:
        if value is None:
            return default
        return json.loads(value)

    def _load_list(self, key: str, item_key: str) -> list[str]:
        raw = self._db.get_nested(key, item_key)
        return sorted(set(str(item) for item in self._load_json(raw, [])))

    def _save_list(self, key: str, item_key: str, values: list[str]) -> None:
        self._db.set_nested(key, item_key, self._dump(sorted(set(values))))

    async def has_header(self, node_id: str) -> bool:
        async with self._lock:
            return await trio.to_thread.run_sync(self._db.nmap_exists, self._scope(self.HEADERS_MAP), node_id)

    async def has_body(self, node_id: str) -> bool:
        async with self._lock:
            return await trio.to_thread.run_sync(self._db.nmap_exists, self._scope(self.BODIES_MAP), node_id)

    async def get_header(self, node_id: str) -> DagNodeHeader | None:
        async with self._lock:
            raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.HEADERS_MAP), node_id)
        if raw is None:
            return None
        return DagNodeHeader.from_primitive(json.loads(str(raw)))

    async def get_body(self, node_id: str) -> DagNodeBody | None:
        async with self._lock:
            raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.BODIES_MAP), node_id)
        if raw is None:
            return None
        return DagNodeBody.from_primitive(json.loads(str(raw)))

    async def get_node(self, node_id: str) -> DagNode | None:
        async with self._lock:
            header_raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.HEADERS_MAP), node_id)
            body_raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.BODIES_MAP), node_id)
        if header_raw is None or body_raw is None:
            return None
        return DagNode(
            header=DagNodeHeader.from_primitive(json.loads(str(header_raw))),
            body=DagNodeBody.from_primitive(json.loads(str(body_raw))),
        )

    async def put_header(self, header: DagNodeHeader) -> None:
        async with self._lock:
            await trio.to_thread.run_sync(
                self._db.nmap_set,
                self._scope(self.HEADERS_MAP),
                header.node_id,
                self._dump(header.to_primitive()),
            )

    async def put_body(self, body: DagNodeBody) -> None:
        async with self._lock:
            await trio.to_thread.run_sync(
                self._db.nmap_set,
                self._scope(self.BODIES_MAP),
                body.node_id,
                self._dump(body.to_primitive()),
            )

    async def get_heads(self) -> tuple[str, ...]:
        async with self._lock:
            values = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.HEADS_MAP))
        return tuple(sorted(str(node_id) for node_id in values))

    async def add_head(self, node_id: str) -> None:
        async with self._lock:
            await trio.to_thread.run_sync(self._db.nmap_set, self._scope(self.HEADS_MAP), node_id, "1")

    async def remove_head(self, node_id: str) -> None:
        async with self._lock:
            await trio.to_thread.run_sync(self._db.nmap_delete, self._scope(self.HEADS_MAP), node_id)

    async def mark_orphan(self, node_id: str, missing_parents: tuple[str, ...] | list[str]) -> None:
        async with self._lock:
            previous_raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.ORPHANS_MAP), node_id)
            if previous_raw is not None:
                previous = OrphanRecord.from_primitive(json.loads(str(previous_raw)))
                for parent_id in previous.missing_parents:
                    waiting = self._load_list(self._scope(self.WAITING_KEY), parent_id)
                    if node_id in waiting:
                        waiting.remove(node_id)
                        self._save_list(self._scope(self.WAITING_KEY), parent_id, waiting)

            record = OrphanRecord(
                node_id=node_id,
                missing_parents=tuple(sorted(set(missing_parents))),
                updated_at_ms=int(time.time() * 1000),
            )
            await trio.to_thread.run_sync(
                self._db.nmap_set,
                self._scope(self.ORPHANS_MAP),
                node_id,
                self._dump(record.to_primitive()),
            )
            for parent_id in record.missing_parents:
                waiting = self._load_list(self._scope(self.WAITING_KEY), parent_id)
                waiting.append(node_id)
                self._save_list(self._scope(self.WAITING_KEY), parent_id, waiting)

    async def clear_orphan(self, node_id: str) -> None:
        async with self._lock:
            raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.ORPHANS_MAP), node_id)
            if raw is None:
                return
            record = OrphanRecord.from_primitive(json.loads(str(raw)))
            await trio.to_thread.run_sync(self._db.nmap_delete, self._scope(self.ORPHANS_MAP), node_id)
            for parent_id in record.missing_parents:
                waiting = self._load_list(self._scope(self.WAITING_KEY), parent_id)
                if node_id in waiting:
                    waiting.remove(node_id)
                    self._save_list(self._scope(self.WAITING_KEY), parent_id, waiting)

    async def get_orphan(self, node_id: str) -> OrphanRecord | None:
        async with self._lock:
            raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.ORPHANS_MAP), node_id)
        if raw is None:
            return None
        return OrphanRecord.from_primitive(json.loads(str(raw)))

    async def list_orphans(self) -> tuple[OrphanRecord, ...]:
        async with self._lock:
            raw_orphans = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.ORPHANS_MAP))
        orphans = [
            OrphanRecord.from_primitive(json.loads(str(raw_record)))
            for raw_record in raw_orphans.values()
        ]
        return tuple(sorted(orphans, key=lambda orphan: orphan.node_id))

    async def get_children(self, parent_id: str) -> tuple[str, ...]:
        async with self._lock:
            return tuple(self._load_list(self._scope(self.CHILDREN_KEY), parent_id))

    async def add_child(self, parent_id: str, child_id: str) -> None:
        async with self._lock:
            children = self._load_list(self._scope(self.CHILDREN_KEY), parent_id)
            children.append(child_id)
            self._save_list(self._scope(self.CHILDREN_KEY), parent_id, children)

    async def get_waiting_children(self, parent_id: str) -> tuple[str, ...]:
        async with self._lock:
            return tuple(self._load_list(self._scope(self.WAITING_KEY), parent_id))

    async def mark_seen_announcement(self, message_id: str) -> bool:
        async with self._lock:
            exists = await trio.to_thread.run_sync(self._db.nmap_exists, self._scope(self.SEEN_MAP), message_id)
            if exists:
                return False
            await trio.to_thread.run_sync(self._db.nmap_set, self._scope(self.SEEN_MAP), message_id, "1")
            return True

    async def count_orphans(self) -> int:
        async with self._lock:
            orphans = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.ORPHANS_MAP))
        return len(orphans)

    async def count_complete_nodes(self) -> int:
        async with self._lock:
            headers = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.HEADERS_MAP))
            bodies = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.BODIES_MAP))
        return len(set(headers).intersection(bodies))

    async def list_complete_node_ids(self) -> tuple[str, ...]:
        async with self._lock:
            headers = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.HEADERS_MAP))
            bodies = await trio.to_thread.run_sync(self._db.nmap_get_all, self._scope(self.BODIES_MAP))
        return tuple(sorted(set(str(node_id) for node_id in headers).intersection(str(node_id) for node_id in bodies)))

    async def get_peer_state(self, peer_id: str) -> PeerSyncState | None:
        async with self._lock:
            raw = await trio.to_thread.run_sync(self._db.nmap_get, self._scope(self.PEER_STATE_MAP), peer_id)
        if raw is None:
            return None
        return PeerSyncState.from_primitive(json.loads(str(raw)))

    async def set_peer_state(self, state: PeerSyncState) -> None:
        async with self._lock:
            await trio.to_thread.run_sync(
                self._db.nmap_set,
                self._scope(self.PEER_STATE_MAP),
                state.peer_id,
                self._dump(state.to_primitive()),
            )
