"""Read-only CLI monitor for comparing DAG replication across peer databases."""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any

from rocksdict import AccessType, Rdict

from subnet.merkle_dag.crypto import SHA256Hasher
from subnet.merkle_dag.models import DagNodeBody, DagNodeHeader, OrphanRecord
from subnet.merkle_dag.serialization import CanonicalJSONSerializer
from subnet.merkle_dag.storage_rocksdb import RocksDBDagStorage

_SERIALIZER = CanonicalJSONSerializer()
_HASHER = SHA256Hasher()
_DB_SEPARATOR = ":"
_DASHBOARD_ENTER = "\033[?1049h\033[H"
_DASHBOARD_EXIT = "\033[?1049l"
_DASHBOARD_REFRESH = "\033[H\033[2J\033[3J"


@dataclass(frozen=True)
class PeerPathSpec:
    """One labeled peer database path supplied to the monitor."""

    label: str
    path: str


@dataclass(frozen=True)
class SkippedPeerPath:
    """A configured peer path that is not ready to inspect yet."""

    label: str
    path: str
    reason: str


@dataclass(frozen=True)
class PeerDagSnapshot:
    """A read-only view of one peer's local DAG indexes at one instant."""

    label: str
    path: str
    namespace: str
    generated_at_ms: int
    header_ids: frozenset[str] = frozenset()
    body_ids: frozenset[str] = frozenset()
    head_ids: frozenset[str] = frozenset()
    orphan_missing_parents: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    header_digests: Mapping[str, str] = field(default_factory=dict)
    body_digests: Mapping[str, str] = field(default_factory=dict)
    content_digests: Mapping[str, str] = field(default_factory=dict)
    created_at_ms_by_node: Mapping[str, int] = field(default_factory=dict)
    parent_ids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    issues: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    errors: tuple[str, ...] = ()

    @property
    def complete_node_ids(self) -> frozenset[str]:
        """Node ids with both a header and a body."""
        return self.header_ids.intersection(self.body_ids)

    @property
    def header_only_ids(self) -> frozenset[str]:
        """Node ids with a header but no body."""
        return self.header_ids.difference(self.body_ids)

    @property
    def body_only_ids(self) -> frozenset[str]:
        """Node ids with a body but no header."""
        return self.body_ids.difference(self.header_ids)

    @property
    def orphan_ids(self) -> frozenset[str]:
        """Node ids tracked as waiting on missing parents."""
        return frozenset(self.orphan_missing_parents)

    @property
    def issue_count(self) -> int:
        """Total local structural/integrity issue count."""
        return len(self.errors) + sum(len(ids) for ids in self.issues.values())


@dataclass(frozen=True)
class DagComparison:
    """Cross-peer comparison for a set of snapshots."""

    namespace: str
    generated_at_ms: int
    snapshots: tuple[PeerDagSnapshot, ...]
    union_complete_node_ids: frozenset[str]
    common_complete_node_ids: frozenset[str]
    missing_by_peer: Mapping[str, tuple[str, ...]]
    divergent_node_ids: tuple[str, ...]
    ok: bool
    skipped_peers: tuple[SkippedPeerPath, ...] = ()


@dataclass
class MonitorStats:
    """Aggregated report data for a long-running monitor session."""

    namespace: str
    started_at_ms: int
    sample_count: int = 0
    ok_samples: int = 0
    first_ok_at_ms: int | None = None
    last_comparison: DagComparison | None = None
    worst_missing_count_by_peer: dict[str, int] = field(default_factory=dict)
    worst_issue_count_by_peer: dict[str, int] = field(default_factory=dict)
    ever_divergent_node_ids: set[str] = field(default_factory=set)
    ever_missing_node_ids_by_peer: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    ever_skipped_peer_labels: set[str] = field(default_factory=set)

    def observe(self, comparison: DagComparison) -> None:
        """Fold one comparison sample into the aggregate report state."""
        self.sample_count += 1
        self.last_comparison = comparison

        if comparison.ok:
            self.ok_samples += 1
            if self.first_ok_at_ms is None:
                self.first_ok_at_ms = comparison.generated_at_ms

        self.ever_divergent_node_ids.update(comparison.divergent_node_ids)
        self.ever_skipped_peer_labels.update(skipped.label for skipped in comparison.skipped_peers)
        for snapshot in comparison.snapshots:
            missing = comparison.missing_by_peer.get(snapshot.label, ())
            self.ever_missing_node_ids_by_peer[snapshot.label].update(missing)
            self.worst_missing_count_by_peer[snapshot.label] = max(
                self.worst_missing_count_by_peer.get(snapshot.label, 0),
                len(missing),
            )
            self.worst_issue_count_by_peer[snapshot.label] = max(
                self.worst_issue_count_by_peer.get(snapshot.label, 0),
                snapshot.issue_count,
            )

    def to_dict(self, *, finished_at_ms: int | None = None) -> dict[str, Any]:
        """Return JSON-compatible aggregate report data."""
        finished_at_ms = finished_at_ms or _now_ms()
        last = comparison_to_dict(self.last_comparison) if self.last_comparison is not None else None
        return {
            "namespace": self.namespace,
            "started_at_ms": self.started_at_ms,
            "started_at": _format_time(self.started_at_ms),
            "finished_at_ms": finished_at_ms,
            "finished_at": _format_time(finished_at_ms),
            "duration_seconds": round((finished_at_ms - self.started_at_ms) / 1000, 3),
            "sample_count": self.sample_count,
            "ok_samples": self.ok_samples,
            "first_ok_at_ms": self.first_ok_at_ms,
            "first_ok_at": _format_time(self.first_ok_at_ms) if self.first_ok_at_ms is not None else None,
            "worst_missing_count_by_peer": dict(sorted(self.worst_missing_count_by_peer.items())),
            "worst_issue_count_by_peer": dict(sorted(self.worst_issue_count_by_peer.items())),
            "ever_divergent_node_ids": sorted(self.ever_divergent_node_ids),
            "ever_missing_node_ids_by_peer": {
                label: sorted(node_ids) for label, node_ids in sorted(self.ever_missing_node_ids_by_peer.items())
            },
            "ever_skipped_peer_labels": sorted(self.ever_skipped_peer_labels),
            "last": last,
        }


def _now_ms() -> int:
    return int(time.time() * 1000)


def _format_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def _scope(map_name: str, namespace: str) -> str:
    return f"{map_name}:{namespace}"


def _json_value(raw: Any) -> Any:
    if isinstance(raw, bytes | bytearray):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict | list | int | float | bool) or raw is None:
        return raw
    return json.loads(str(raw))


def _digest(value: Any) -> str:
    return _HASHER.digest(_SERIALIZER.serialize(value))


def _issue_map(issues: Mapping[str, set[str]]) -> dict[str, tuple[str, ...]]:
    return {category: tuple(sorted(ids)) for category, ids in sorted(issues.items()) if ids}


def parse_peer_specs(values: Sequence[str]) -> tuple[PeerPathSpec, ...]:
    """Parse CLI path specs in either `/path` or `label=/path` form."""
    specs: list[PeerPathSpec] = []
    used_labels: dict[str, int] = {}

    for index, value in enumerate(values, start=1):
        if "=" in value:
            label, path = value.split("=", 1)
            label = label.strip() or f"peer{index}"
        else:
            path = value
            label = Path(path.rstrip("/")).name or f"peer{index}"

        duplicate_count = used_labels.get(label, 0)
        used_labels[label] = duplicate_count + 1
        if duplicate_count:
            label = f"{label}-{duplicate_count + 1}"

        specs.append(PeerPathSpec(label=label, path=str(Path(path).expanduser())))

    return tuple(specs)


def _nmap_get_all(store: Rdict, nmap: str) -> dict[str, Any]:
    prefix = f"nmap{_DB_SEPARATOR}{nmap}{_DB_SEPARATOR}"
    results = {}
    for key in store.keys():
        if isinstance(key, str) and key.startswith(prefix):
            actual_key = key[len(prefix) :]
            results[actual_key] = store[key]
    return results


def load_peer_snapshot(spec: PeerPathSpec, namespace: str) -> PeerDagSnapshot:
    """Open one exact RocksDB path read-only and inspect its DAG namespace maps."""
    generated_at_ms = _now_ms()
    try:
        store = Rdict(spec.path, access_type=AccessType.read_only())
        try:
            raw_headers = _nmap_get_all(store, _scope(RocksDBDagStorage.HEADERS_MAP, namespace))
            raw_bodies = _nmap_get_all(store, _scope(RocksDBDagStorage.BODIES_MAP, namespace))
            raw_heads = _nmap_get_all(store, _scope(RocksDBDagStorage.HEADS_MAP, namespace))
            raw_orphans = _nmap_get_all(store, _scope(RocksDBDagStorage.ORPHANS_MAP, namespace))
        finally:
            store.close()
    except Exception as exc:
        return PeerDagSnapshot(
            label=spec.label,
            path=spec.path,
            namespace=namespace,
            generated_at_ms=generated_at_ms,
            errors=(f"could not open/read {spec.path} read-only: {exc}",),
        )

    issues: dict[str, set[str]] = defaultdict(set)
    parsed_headers: dict[str, DagNodeHeader] = {}
    parsed_bodies: dict[str, DagNodeBody] = {}
    header_digests: dict[str, str] = {}
    body_digests: dict[str, str] = {}
    content_digests: dict[str, str] = {}
    created_at_ms_by_node: dict[str, int] = {}
    parent_ids: dict[str, tuple[str, ...]] = {}
    orphan_missing_parents: dict[str, tuple[str, ...]] = {}

    header_ids = frozenset(str(node_id) for node_id in raw_headers)
    body_ids = frozenset(str(node_id) for node_id in raw_bodies)
    head_ids = frozenset(str(node_id) for node_id in raw_heads)

    for node_id, raw_header in raw_headers.items():
        node_id = str(node_id)
        try:
            header_value = dict(_json_value(raw_header))
            header = DagNodeHeader.from_primitive(header_value)
            parsed_headers[node_id] = header
            parent_ids[node_id] = header.parent_ids
            created_at_ms_by_node[node_id] = header.created_at_ms
            header_digests[node_id] = _digest(header.to_primitive())
        except Exception:
            issues["unparseable_header"].add(node_id)
            continue

        if header.node_id != node_id:
            issues["header_key_mismatch"].add(node_id)
        if header.namespace != namespace:
            issues["namespace_mismatch"].add(node_id)
        if list(header.parent_ids) != sorted(header.parent_ids):
            issues["unsorted_parent_ids"].add(node_id)
        if len(set(header.parent_ids)) != len(header.parent_ids):
            issues["duplicate_parent_ids"].add(node_id)
        if header.node_id in header.parent_ids:
            issues["self_parent"].add(node_id)

        expected_node_id = _digest(header.unsigned_primitive())
        if expected_node_id != header.node_id:
            issues["node_id_hash_mismatch"].add(node_id)

    for node_id, raw_body in raw_bodies.items():
        node_id = str(node_id)
        try:
            body_value = dict(_json_value(raw_body))
            body = DagNodeBody.from_primitive(body_value)
            parsed_bodies[node_id] = body
            body_digests[node_id] = _digest(body.to_primitive())
        except Exception:
            issues["unparseable_body"].add(node_id)
            continue

        if body.node_id != node_id:
            issues["body_key_mismatch"].add(node_id)

    for node_id, raw_orphan in raw_orphans.items():
        node_id = str(node_id)
        try:
            orphan_value = dict(_json_value(raw_orphan))
            orphan = OrphanRecord.from_primitive(orphan_value)
            orphan_missing_parents[node_id] = orphan.missing_parents
        except Exception:
            issues["unparseable_orphan_record"].add(node_id)
            continue

        if orphan.node_id != node_id:
            issues["orphan_key_mismatch"].add(node_id)

    complete_node_ids = header_ids.intersection(body_ids)
    for node_id in header_ids.difference(body_ids):
        issues["header_without_body"].add(node_id)
    for node_id in body_ids.difference(header_ids):
        issues["body_without_header"].add(node_id)
    for node_id in orphan_missing_parents:
        issues["orphan"].add(node_id)
    for node_id in head_ids.difference(complete_node_ids):
        issues["head_without_complete_node"].add(node_id)

    for node_id in complete_node_ids:
        header = parsed_headers.get(node_id)
        body = parsed_bodies.get(node_id)
        if header is None or body is None:
            continue

        body_bytes = _SERIALIZER.serialize(body.payload)
        body_hash = _HASHER.digest(body_bytes)
        if header.body_hash != body_hash:
            issues["body_hash_mismatch"].add(node_id)
        if header.body_size != len(body_bytes):
            issues["body_size_mismatch"].add(node_id)

        for parent_id in header.parent_ids:
            if parent_id not in complete_node_ids:
                issues["missing_parent"].add(node_id)

        content_digests[node_id] = _digest({"body": body.to_primitive(), "header": header.to_primitive()})

    active_node_ids = complete_node_ids.difference(orphan_missing_parents)
    referenced_parents = {
        parent_id
        for node_id in active_node_ids
        for parent_id in parent_ids.get(node_id, ())
        if parent_id in active_node_ids
    }
    derived_heads = active_node_ids.difference(referenced_parents)
    for node_id in derived_heads.difference(head_ids):
        issues["missing_head_index"].add(node_id)
    for node_id in head_ids.difference(derived_heads):
        issues["extra_head_index"].add(node_id)

    return PeerDagSnapshot(
        label=spec.label,
        path=spec.path,
        namespace=namespace,
        generated_at_ms=generated_at_ms,
        header_ids=header_ids,
        body_ids=body_ids,
        head_ids=head_ids,
        orphan_missing_parents=orphan_missing_parents,
        header_digests=header_digests,
        body_digests=body_digests,
        content_digests=content_digests,
        created_at_ms_by_node=created_at_ms_by_node,
        parent_ids=parent_ids,
        issues=_issue_map(issues),
    )


def compare_snapshots(
    snapshots: Sequence[PeerDagSnapshot],
    namespace: str,
    *,
    skipped_peers: Sequence[SkippedPeerPath] = (),
) -> DagComparison:
    """Compare peer snapshots and compute replication gaps."""
    generated_at_ms = _now_ms()
    complete_sets = [snapshot.complete_node_ids for snapshot in snapshots]
    union_complete_node_ids = frozenset().union(*complete_sets) if complete_sets else frozenset()
    common_complete_node_ids = (
        frozenset.intersection(*complete_sets) if complete_sets else frozenset()
    )
    missing_by_peer = {
        snapshot.label: tuple(sorted(union_complete_node_ids.difference(snapshot.complete_node_ids)))
        for snapshot in snapshots
    }

    divergent_node_ids: list[str] = []
    for node_id in sorted(union_complete_node_ids):
        digests = {
            snapshot.content_digests[node_id]
            for snapshot in snapshots
            if node_id in snapshot.content_digests
        }
        if len(digests) > 1:
            divergent_node_ids.append(node_id)

    ok = bool(snapshots) and (
        all(not snapshot.errors and snapshot.issue_count == 0 for snapshot in snapshots)
        and all(not missing for missing in missing_by_peer.values())
        and not divergent_node_ids
    )

    return DagComparison(
        namespace=namespace,
        generated_at_ms=generated_at_ms,
        snapshots=tuple(snapshots),
        union_complete_node_ids=union_complete_node_ids,
        common_complete_node_ids=common_complete_node_ids,
        missing_by_peer=missing_by_peer,
        divergent_node_ids=tuple(divergent_node_ids),
        ok=ok,
        skipped_peers=tuple(skipped_peers),
    )


def _skip_reason(spec: PeerPathSpec) -> str | None:
    if not Path(spec.path).exists():
        return f"{spec.path} does not exist yet"
    return None


def sample(specs: Sequence[PeerPathSpec], namespace: str) -> DagComparison:
    """Read all peer paths once and return the cross-peer comparison."""
    active_specs: list[PeerPathSpec] = []
    skipped_peers: list[SkippedPeerPath] = []

    for spec in specs:
        reason = _skip_reason(spec)
        if reason is None:
            active_specs.append(spec)
        else:
            skipped_peers.append(
                SkippedPeerPath(
                    label=spec.label,
                    path=spec.path,
                    reason=reason,
                )
            )

    snapshots = tuple(load_peer_snapshot(spec, namespace) for spec in active_specs)
    return compare_snapshots(snapshots, namespace, skipped_peers=skipped_peers)


def snapshot_to_dict(snapshot: PeerDagSnapshot) -> dict[str, Any]:
    """Return a JSON-compatible peer snapshot summary."""
    return {
        "label": snapshot.label,
        "path": snapshot.path,
        "namespace": snapshot.namespace,
        "generated_at_ms": snapshot.generated_at_ms,
        "generated_at": _format_time(snapshot.generated_at_ms),
        "header_count": len(snapshot.header_ids),
        "body_count": len(snapshot.body_ids),
        "complete_node_count": len(snapshot.complete_node_ids),
        "head_count": len(snapshot.head_ids),
        "orphan_count": len(snapshot.orphan_ids),
        "header_only_count": len(snapshot.header_only_ids),
        "body_only_count": len(snapshot.body_only_ids),
        "issue_count": snapshot.issue_count,
        "errors": list(snapshot.errors),
        "issues": {category: list(ids) for category, ids in snapshot.issues.items()},
        "created_at_ms_by_node": dict(sorted(snapshot.created_at_ms_by_node.items())),
        "complete_node_ids": sorted(snapshot.complete_node_ids),
        "head_ids": sorted(snapshot.head_ids),
        "orphan_missing_parents": {
            node_id: list(missing_parents)
            for node_id, missing_parents in sorted(snapshot.orphan_missing_parents.items())
        },
    }


def skipped_peer_to_dict(skipped_peer: SkippedPeerPath) -> dict[str, str]:
    """Return a JSON-compatible skipped peer record."""
    return {
        "label": skipped_peer.label,
        "path": skipped_peer.path,
        "reason": skipped_peer.reason,
    }


def _created_at_ms_for_node(comparison: DagComparison, node_id: str) -> int | None:
    created_at_values = [
        snapshot.created_at_ms_by_node[node_id]
        for snapshot in comparison.snapshots
        if node_id in snapshot.created_at_ms_by_node
    ]
    if not created_at_values:
        return None
    return min(created_at_values)


def missing_node_to_dict(comparison: DagComparison, node_id: str) -> dict[str, Any]:
    """Return JSON-compatible missing-node detail."""
    created_at_ms = _created_at_ms_for_node(comparison, node_id)
    return {
        "node_id": node_id,
        "created_at_ms": created_at_ms,
        "created_at": _format_time(created_at_ms) if created_at_ms is not None else None,
    }


def comparison_to_dict(comparison: DagComparison | None) -> dict[str, Any] | None:
    """Return a JSON-compatible comparison report."""
    if comparison is None:
        return None
    return {
        "namespace": comparison.namespace,
        "generated_at_ms": comparison.generated_at_ms,
        "generated_at": _format_time(comparison.generated_at_ms),
        "ok": comparison.ok,
        "union_complete_node_count": len(comparison.union_complete_node_ids),
        "common_complete_node_count": len(comparison.common_complete_node_ids),
        "divergent_node_ids": list(comparison.divergent_node_ids),
        "missing_by_peer": {label: list(ids) for label, ids in sorted(comparison.missing_by_peer.items())},
        "missing_details_by_peer": {
            label: [missing_node_to_dict(comparison, node_id) for node_id in ids]
            for label, ids in sorted(comparison.missing_by_peer.items())
        },
        "peers": [snapshot_to_dict(snapshot) for snapshot in comparison.snapshots],
        "skipped_peers": [skipped_peer_to_dict(skipped) for skipped in comparison.skipped_peers],
    }


def _short_node_id(node_id: str, width: int = 18) -> str:
    if len(node_id) <= width:
        return node_id
    return f"{node_id[:width]}..."


def _format_node_ids(node_ids: Sequence[str], limit: int) -> str:
    if not node_ids:
        return ""
    visible = [_short_node_id(node_id) for node_id in node_ids[:limit]]
    remaining = len(node_ids) - len(visible)
    if remaining > 0:
        visible.append(f"+{remaining} more")
    return ", ".join(visible)


def _format_missing_node_ids(comparison: DagComparison, node_ids: Sequence[str], limit: int) -> str:
    if not node_ids:
        return ""

    visible = []
    for node_id in node_ids[:limit]:
        created_at_ms = _created_at_ms_for_node(comparison, node_id)
        if created_at_ms is None:
            visible.append(_short_node_id(node_id))
        else:
            visible.append(f"{_short_node_id(node_id)} @ {_format_time(created_at_ms)}")

    remaining = len(node_ids) - len(visible)
    if remaining > 0:
        visible.append(f"+{remaining} more")
    return ", ".join(visible)


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    if not rows:
        return "No initialized peer databases found yet."

    str_rows = [[str(cell) for cell in row] for row in rows]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in str_rows))
        for index in range(len(headers))
    ]
    lines = ["  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))]
    lines.append("  ".join("-" * width for width in widths))
    for row in str_rows:
        lines.append("  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row)))
    return "\n".join(lines)


def _peer_status(snapshot: PeerDagSnapshot, comparison: DagComparison) -> str:
    if snapshot.errors:
        return "UNREADABLE"
    missing = comparison.missing_by_peer.get(snapshot.label, ())
    has_divergent = any(node_id in snapshot.complete_node_ids for node_id in comparison.divergent_node_ids)
    if has_divergent:
        return "DIVERGED"
    if snapshot.issue_count:
        return "CHECK"
    if missing:
        return "LAGGING"
    return "OK"


def render_comparison(comparison: DagComparison, *, node_limit: int = 12) -> str:
    """Render a compact terminal view of the current comparison."""
    rows = []
    for snapshot in comparison.snapshots:
        rows.append(
            [
                snapshot.label,
                len(snapshot.complete_node_ids),
                len(snapshot.head_ids),
                len(snapshot.orphan_ids),
                len(snapshot.header_only_ids),
                len(snapshot.body_only_ids),
                len(comparison.missing_by_peer.get(snapshot.label, ())),
                snapshot.issue_count,
                _peer_status(snapshot, comparison),
            ]
        )

    status = "OK" if comparison.ok else "WAITING" if not comparison.snapshots else "CHECK"
    lines = [
        (
            f"DAG replication monitor  namespace={comparison.namespace}  "
            f"sampled={_format_time(comparison.generated_at_ms)}"
        ),
        (
            f"status={status}  union_complete={len(comparison.union_complete_node_ids)}  "
            f"replicated_on_all={len(comparison.common_complete_node_ids)}  "
            f"divergent={len(comparison.divergent_node_ids)}  skipped={len(comparison.skipped_peers)}"
        ),
        "",
        _table(
            ["peer", "complete", "heads", "orphans", "hdr-only", "body-only", "missing", "issues", "status"],
            rows,
        ),
    ]

    if comparison.skipped_peers:
        lines.append("")
        lines.append("Skipped peer paths:")
        for skipped in comparison.skipped_peers:
            lines.append(f"  {skipped.label}: {skipped.reason}")

    missing_rows = [
        (label, ids)
        for label, ids in sorted(comparison.missing_by_peer.items())
        if ids
    ]
    if missing_rows:
        lines.append("")
        lines.append("Missing complete node ids vs union:")
        for label, ids in missing_rows:
            lines.append(f"  {label}: {len(ids)} ({_format_missing_node_ids(comparison, ids, node_limit)})")

    if comparison.divergent_node_ids:
        lines.append("")
        lines.append(
            "Divergent node ids: "
            f"{_format_node_ids(comparison.divergent_node_ids, node_limit)}"
        )

    issue_snapshots = [snapshot for snapshot in comparison.snapshots if snapshot.errors or snapshot.issues]
    if issue_snapshots:
        lines.append("")
        lines.append("Local DAG issues:")
        for snapshot in issue_snapshots:
            for error in snapshot.errors:
                lines.append(f"  {snapshot.label}: {error}")
            for category, ids in sorted(snapshot.issues.items()):
                lines.append(f"  {snapshot.label}: {category}={len(ids)} ({_format_node_ids(ids, node_limit)})")

    return "\n".join(lines)


def append_jsonl(path: str, comparison: DagComparison) -> None:
    """Append one comparison sample as JSONL."""
    with Path(path).open("a", encoding="utf-8") as file:
        file.write(json.dumps(comparison_to_dict(comparison), sort_keys=True) + "\n")


def write_report(path: str, stats: MonitorStats) -> None:
    """Write the aggregate report as JSON."""
    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_duration(value: str) -> float:
    """Parse durations like `10`, `10s`, `5m`, or `4h` into seconds."""
    normalized = value.strip().lower()
    if not normalized:
        raise argparse.ArgumentTypeError("duration cannot be empty")

    multiplier = 1.0
    if normalized[-1].isalpha():
        unit = normalized[-1]
        normalized = normalized[:-1]
        if unit == "s":
            multiplier = 1.0
        elif unit == "m":
            multiplier = 60.0
        elif unit == "h":
            multiplier = 3600.0
        else:
            raise argparse.ArgumentTypeError("duration unit must be s, m, or h")

    try:
        seconds = float(normalized) * multiplier
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid duration: {value!r}") from exc

    if seconds < 0:
        raise argparse.ArgumentTypeError("duration must be non-negative")
    return seconds


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare namespace-scoped Merkle DAG storage across exact peer RocksDB paths.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
python -m subnet.cli.dag.dag_replication_monitor --namespace general-dag boot=/tmp/boot_db alith=/tmp/alith_db

python -m subnet.cli.dag.dag_replication_monitor \\
--namespace general-dag --watch --interval 10s --duration 4h \\
--jsonl /tmp/dag-samples.jsonl --report /tmp/dag-report.json \\
bootstrap=/tmp/bootstrap_db \\
alith=/tmp/alith_db \\
baltathar=/tmp/baltathar_db \\
charleth=/tmp/charleth_db \\
dorothy=/tmp/dorothy_db

Paths are exact RocksDB directories. The monitor does not add, remove, or infer
any suffixes.
""",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Exact peer RocksDB paths. Use label=/path for stable peer names.",
    )
    parser.add_argument(
        "--namespace",
        default="general-dag",
        help="DAG namespace to inspect (default: general-dag, used by run_server_example).",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Refresh until Ctrl-C or --duration expires.",
    )
    parser.add_argument(
        "--interval",
        type=parse_duration,
        default=10.0,
        help="Refresh interval for --watch, e.g. 5s, 1m (default: 10s).",
    )
    parser.add_argument(
        "--duration",
        type=parse_duration,
        default=0.0,
        help="Total watch time, e.g. 30m or 4h. Implies --watch when > 0.",
    )
    parser.add_argument(
        "--report",
        help="Write final aggregate JSON report to this path.",
    )
    parser.add_argument(
        "--jsonl",
        help="Append every sample as JSONL to this path.",
    )
    parser.add_argument(
        "--node-limit",
        type=int,
        default=12,
        help="Maximum node ids to show per issue in terminal output (default: 12).",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Append every watch sample instead of using the live one-table dashboard.",
    )
    return parser.parse_args(argv)


def _sleep_until_next_sample(interval_seconds: float, deadline: float | None) -> bool:
    if deadline is not None and time.monotonic() >= deadline:
        return False

    sleep_seconds = max(interval_seconds, 0.0)
    if deadline is not None:
        sleep_seconds = min(sleep_seconds, max(deadline - time.monotonic(), 0.0))
        if sleep_seconds <= 0:
            return False

    time.sleep(sleep_seconds)
    return True


def _log_skipped_peer_transitions(
    comparison: DagComparison,
    previous_skipped_labels: set[str],
) -> set[str]:
    current_skipped = {skipped.label: skipped for skipped in comparison.skipped_peers}
    current_labels = set(current_skipped)

    for label in sorted(current_labels.difference(previous_skipped_labels)):
        skipped = current_skipped[label]
        print(f"Skipping peer path until initialized: {label} ({skipped.reason})", file=sys.stderr)

    for label in sorted(previous_skipped_labels.difference(current_labels)):
        print(f"Peer path initialized, adding to DAG visual: {label}", file=sys.stderr)

    return current_labels


def _write_terminal_frame(rendered: str, *, refresh: bool) -> None:
    if refresh:
        sys.stdout.write(_DASHBOARD_REFRESH)
        sys.stdout.write(rendered)
        sys.stdout.write("\n")
        sys.stdout.flush()
        return

    print(rendered, flush=True)


def run_monitor(args: argparse.Namespace) -> int:
    """Run the monitor from parsed CLI arguments."""
    specs = parse_peer_specs(args.paths)
    watch = args.watch or args.duration > 0
    deadline = time.monotonic() + args.duration if args.duration > 0 else None
    stats = MonitorStats(namespace=args.namespace, started_at_ms=_now_ms())
    previous_skipped_labels: set[str] = set()
    use_dashboard = watch and not args.no_clear and sys.stdout.isatty()
    last_rendered: str | None = None
    interrupted = False

    try:
        if use_dashboard:
            sys.stdout.write(_DASHBOARD_ENTER)
            sys.stdout.flush()

        while True:
            comparison = sample(specs, args.namespace)
            stats.observe(comparison)
            previous_skipped_labels = _log_skipped_peer_transitions(comparison, previous_skipped_labels)
            if args.jsonl:
                append_jsonl(args.jsonl, comparison)

            last_rendered = render_comparison(comparison, node_limit=max(args.node_limit, 1))
            _write_terminal_frame(
                last_rendered,
                refresh=use_dashboard,
            )

            if not watch or not _sleep_until_next_sample(args.interval, deadline):
                break
            if args.no_clear:
                print()
    except KeyboardInterrupt:
        interrupted = True
    finally:
        if use_dashboard:
            sys.stdout.write(_DASHBOARD_EXIT)
            sys.stdout.flush()
            if last_rendered is not None:
                print(last_rendered, flush=True)
        if interrupted:
            print("\nStopping monitor after Ctrl-C.", file=sys.stderr)
        if args.report:
            write_report(args.report, stats)
            print(f"Final DAG replication report written to {args.report}", file=sys.stderr)

    return 0 if stats.last_comparison is not None and stats.last_comparison.ok else 1


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point."""
    raise SystemExit(run_monitor(parse_args(argv)))


if __name__ == "__main__":
    main()
