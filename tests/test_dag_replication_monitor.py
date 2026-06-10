from __future__ import annotations

import json

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
from rocksdict import Rdict

from subnet.cli.dag.dag_replication_monitor import (
    PeerDagSnapshot,
    PeerPathSpec,
    compare_snapshots,
    comparison_to_dict,
    load_peer_snapshot,
    parse_duration,
    parse_peer_specs,
    render_comparison,
    sample,
)
from subnet.merkle_dag import InMemoryDagStorage, Libp2pKeyPairSigner
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.payloads import MappingPayloadSchema
from subnet.merkle_dag.runtime import MerkleDagRuntime


class CounterPayloadSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("counter")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("value"), int):
            raise PayloadValidationError("counter payload requires an integer 'value'")


class DummyDB:
    def get_nested(self, _key: str, _nested_key: str):
        return None

    def set_nested(self, _key: str, _nested_key: str, _value: object) -> None:
        return None


def _key_pair(seed_byte: int):
    return create_new_key_pair(bytes([seed_byte]) * 32)


def _peer_id(seed_byte: int) -> ID:
    return ID.from_pubkey(_key_pair(seed_byte).public_key)


def test_parse_peer_specs_keeps_exact_paths() -> None:
    specs = parse_peer_specs(["boot=/tmp/boot", "/tmp/alith", "/tmp/alith"])

    assert specs[0] == PeerPathSpec(label="boot", path="/tmp/boot")
    assert specs[1] == PeerPathSpec(label="alith", path="/tmp/alith")
    assert specs[2] == PeerPathSpec(label="alith-2", path="/tmp/alith")


def test_parse_duration_supports_seconds_minutes_and_hours() -> None:
    assert parse_duration("15") == 15
    assert parse_duration("15s") == 15
    assert parse_duration("2m") == 120
    assert parse_duration("1.5h") == 5400


def test_compare_snapshots_finds_missing_and_divergent_nodes() -> None:
    peer_a = PeerDagSnapshot(
        label="a",
        path="/tmp/a",
        namespace="general-dag",
        generated_at_ms=1000,
        header_ids=frozenset({"node-1", "node-2"}),
        body_ids=frozenset({"node-1", "node-2"}),
        content_digests={"node-1": "digest-a", "node-2": "digest-2"},
        created_at_ms_by_node={"node-1": 1000, "node-2": 2000},
    )
    peer_b = PeerDagSnapshot(
        label="b",
        path="/tmp/b",
        namespace="general-dag",
        generated_at_ms=1000,
        header_ids=frozenset({"node-1"}),
        body_ids=frozenset({"node-1"}),
        content_digests={"node-1": "digest-b"},
        created_at_ms_by_node={"node-1": 1000},
    )

    comparison = compare_snapshots((peer_a, peer_b), "general-dag")

    assert comparison.union_complete_node_ids == frozenset({"node-1", "node-2"})
    assert comparison.common_complete_node_ids == frozenset({"node-1"})
    assert comparison.missing_by_peer == {"a": (), "b": ("node-2",)}
    assert comparison.divergent_node_ids == ("node-1",)
    assert not comparison.ok
    rendered = render_comparison(comparison)
    assert "node-2 @ 1970-01-01T00:00:02+00:00" in rendered
    report = comparison_to_dict(comparison)
    assert report is not None
    assert report["missing_details_by_peer"]["b"] == [
        {
            "node_id": "node-2",
            "created_at_ms": 2000,
            "created_at": "1970-01-01T00:00:02+00:00",
        }
    ]


def test_sample_skips_missing_exact_peer_paths_until_initialized(tmp_path) -> None:
    exact_path = tmp_path / "dorothy-db"
    comparison = sample(
        parse_peer_specs([f"dorothy={exact_path}"]),
        "general-dag",
    )

    assert comparison.snapshots == ()
    assert len(comparison.skipped_peers) == 1
    assert comparison.skipped_peers[0].label == "dorothy"
    assert comparison.skipped_peers[0].path == str(exact_path)
    assert not comparison.ok
    rendered = render_comparison(comparison)
    assert "No initialized peer databases found yet." in rendered
    assert "Skipped peer paths:" in rendered
    assert "dorothy:" in rendered


def test_sample_adds_peer_after_exact_path_is_initialized(tmp_path) -> None:
    exact_path = str(tmp_path / "dorothy-db")
    specs = parse_peer_specs([f"dorothy={exact_path}"])

    before = sample(specs, "general-dag")
    assert before.snapshots == ()
    assert before.skipped_peers[0].label == "dorothy"

    store = Rdict(exact_path)
    store.close()

    after = sample(specs, "general-dag")
    assert [snapshot.label for snapshot in after.snapshots] == ["dorothy"]
    assert after.skipped_peers == ()


@pytest.mark.asyncio
async def test_load_peer_snapshot_reads_same_rocksdb_maps_as_dag_storage(tmp_path) -> None:
    namespace = "general-dag"
    key_pair = _key_pair(7)
    local_peer_id = _peer_id(7)
    runtime = MerkleDagRuntime(
        db=DummyDB(),  # type: ignore[arg-type]
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=local_peer_id,
        namespace=namespace,
        storage=InMemoryDagStorage(namespace=namespace),
    )
    node = await runtime.dag.create_node(
        schema_id="counter",
        payload={"value": 7},
        parent_ids=(),
        signer=Libp2pKeyPairSigner(key_pair),
        author=local_peer_id.to_string(),
        created_at_ms=1000,
    )

    exact_path = str(tmp_path / "peer-a-db")
    store = Rdict(exact_path)
    store[f"nmap:dag_headers:{namespace}:{node.header.node_id}"] = json.dumps(node.header.to_primitive())
    store[f"nmap:dag_bodies:{namespace}:{node.body.node_id}"] = json.dumps(node.body.to_primitive())
    store[f"nmap:dag_heads:{namespace}:{node.header.node_id}"] = "1"
    store.close()

    snapshot = load_peer_snapshot(
        PeerPathSpec(label="peer-a", path=exact_path),
        namespace,
    )

    assert snapshot.errors == ()
    assert snapshot.issues == {}
    assert snapshot.complete_node_ids == frozenset({node.header.node_id})
    assert snapshot.head_ids == frozenset({node.header.node_id})
    assert snapshot.content_digests[node.header.node_id].startswith("sha256:")
    assert snapshot.created_at_ms_by_node[node.header.node_id] == 1000
