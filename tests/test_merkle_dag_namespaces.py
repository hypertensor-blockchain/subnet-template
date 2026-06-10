from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID

from subnet.merkle_dag import Libp2pKeyPairSigner, NodeIngestStatus
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.payloads import MappingPayloadSchema
from subnet.merkle_dag.runtime import MerkleDagRuntime
import subnet.merkle_dag.storage_rocksdb as storage_rocksdb
from subnet.utils.db.database import RocksDB


class CounterPayloadSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("counter")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("value"), int):
            raise PayloadValidationError("counter payload requires an integer 'value'")


def _peer_id(seed_byte: int) -> ID:
    return ID.from_pubkey(create_new_key_pair(bytes([seed_byte]) * 32).public_key)


def _signer(seed_byte: int) -> Libp2pKeyPairSigner:
    return Libp2pKeyPairSigner(create_new_key_pair(bytes([seed_byte]) * 32))


class _NullAsyncLock:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


async def _run_sync(func, *args):
    return func(*args)


def _patch_rocksdb_storage_for_asyncio(monkeypatch) -> None:
    monkeypatch.setattr(storage_rocksdb.trio, "Lock", lambda: _NullAsyncLock())
    monkeypatch.setattr(storage_rocksdb.trio.to_thread, "run_sync", _run_sync)


@pytest.mark.asyncio
async def test_runtime_default_storage_keeps_heads_and_counts_isolated_by_namespace(tmp_path, monkeypatch):
    _patch_rocksdb_storage_for_asyncio(monkeypatch)
    db = RocksDB(str(tmp_path / "dag-namespaces"))
    runtime_a = MerkleDagRuntime(
        db=db,
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(1),
        namespace="namespace-a",
        dag_topic="dag-a",
    )
    runtime_b = MerkleDagRuntime(
        db=db,
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(2),
        namespace="namespace-b",
        dag_topic="dag-b",
    )

    node_a = await runtime_a.dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=_signer(11),
        author=_peer_id(11).to_string(),
        created_at_ms=1001,
    )
    node_b = await runtime_b.dag.create_node(
        schema_id="counter",
        payload={"value": 2},
        parent_ids=(),
        signer=_signer(12),
        author=_peer_id(12).to_string(),
        created_at_ms=1002,
    )

    assert (await runtime_a.dag.add_node(node_a)).status is NodeIngestStatus.ACCEPTED
    assert (await runtime_b.dag.add_node(node_b)).status is NodeIngestStatus.ACCEPTED

    assert await runtime_a.dag.get_heads() == (node_a.header.node_id,)
    assert await runtime_b.dag.get_heads() == (node_b.header.node_id,)
    assert await runtime_a.storage.list_complete_node_ids() == (node_a.header.node_id,)
    assert await runtime_b.storage.list_complete_node_ids() == (node_b.header.node_id,)

    summary_a = await runtime_a.dag.summary()
    summary_b = await runtime_b.dag.summary()
    assert summary_a.namespace == "namespace-a"
    assert summary_a.head_ids == (node_a.header.node_id,)
    assert summary_a.node_count == 1
    assert summary_a.orphan_count == 0
    assert summary_b.namespace == "namespace-b"
    assert summary_b.head_ids == (node_b.header.node_id,)
    assert summary_b.node_count == 1
    assert summary_b.orphan_count == 0


@pytest.mark.asyncio
async def test_runtime_default_storage_keeps_orphans_and_waiting_children_isolated_by_namespace(tmp_path, monkeypatch):
    _patch_rocksdb_storage_for_asyncio(monkeypatch)
    db = RocksDB(str(tmp_path / "dag-namespaces-orphans"))
    runtime_a = MerkleDagRuntime(
        db=db,
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(3),
        namespace="namespace-a",
        dag_topic="dag-a",
    )
    runtime_b = MerkleDagRuntime(
        db=db,
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(4),
        namespace="namespace-b",
        dag_topic="dag-b",
    )

    parent_a = await runtime_a.dag.create_node(
        schema_id="counter",
        payload={"value": 10},
        parent_ids=(),
        signer=_signer(13),
        author=_peer_id(13).to_string(),
        created_at_ms=1010,
    )
    child_a = await runtime_a.dag.create_node(
        schema_id="counter",
        payload={"value": 11},
        parent_ids=(parent_a.header.node_id,),
        signer=_signer(14),
        author=_peer_id(14).to_string(),
        created_at_ms=1011,
    )

    child_result = await runtime_a.dag.add_node(child_a)

    assert child_result.status is NodeIngestStatus.ORPHAN
    assert child_result.missing_parents == (parent_a.header.node_id,)
    assert await runtime_a.storage.count_orphans() == 1
    assert await runtime_b.storage.count_orphans() == 0
    assert await runtime_a.storage.get_waiting_children(parent_a.header.node_id) == (child_a.header.node_id,)
    assert await runtime_b.storage.get_waiting_children(parent_a.header.node_id) == ()

    parent_result = await runtime_a.dag.add_node(parent_a)

    assert parent_result.status is NodeIngestStatus.ACCEPTED
    assert child_a.header.node_id in parent_result.resolved_nodes
    assert await runtime_a.storage.count_orphans() == 0
    assert await runtime_b.storage.count_orphans() == 0
    assert await runtime_a.storage.get_waiting_children(parent_a.header.node_id) == ()
    assert await runtime_b.storage.get_waiting_children(parent_a.header.node_id) == ()
