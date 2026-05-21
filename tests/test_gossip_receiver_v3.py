from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
import trio

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagAnnouncement,
    DagInventoryRequest,
    DagInventoryResponse,
    InMemoryDagStorage,
)
from subnet.merkle_dag.models import DagSummary, PeerSyncState
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.merkle_dag.sync_service import MerkleDagSyncService


class CounterPayloadSchema:
    schema_id = "counter"

    def canonicalize_payload(self, payload):
        return CanonicalJSONSerializer().normalize(payload)

    def validate_payload(self, payload):
        if not isinstance(payload, dict):
            raise TypeError("payload must be a mapping")

    def validate_parent_links(self, node, parents) -> None:
        return None

    def validate_signer_peer(self, node, signer_peer_id: str) -> None:
        return None

    def materialize(self, node, parent_states):
        return node.body.payload


class DummyDB:
    pass


class DummyPubsub:
    async def subscribe(self, topic: str):
        return None


@dataclass
class FakeCoordinator:
    local_peer_id: str
    reconciled: list[str]
    storage: FakeStorage | None = None

    async def reconcile_with_peer(self, peer_id: str) -> None:
        self.reconciled.append(peer_id)
        if self.storage is None:
            return

        head_id = f"{peer_id}-head"
        await self.storage.set_peer_state(
            PeerSyncState(
                peer_id=peer_id,
                summary=DagSummary(
                    namespace="shared",
                    head_ids=(head_id,),
                    node_count=1,
                    orphan_count=0,
                    generated_at_ms=1000,
                ),
                updated_at_ms=1000,
            )
        )
        self.storage.complete_nodes.add(head_id)


class FakeStorage:
    def __init__(self):
        self.complete_nodes: set[str] = set()
        self.peer_states: dict[str, PeerSyncState] = {}
        self.orphan_count = 0

    async def count_complete_nodes(self) -> int:
        return len(self.complete_nodes)

    async def count_orphans(self) -> int:
        return self.orphan_count

    async def has_header(self, node_id: str) -> bool:
        return node_id in self.complete_nodes

    async def has_body(self, node_id: str) -> bool:
        return node_id in self.complete_nodes

    async def get_peer_state(self, peer_id: str) -> PeerSyncState | None:
        return self.peer_states.get(peer_id)

    async def set_peer_state(self, state: PeerSyncState) -> None:
        self.peer_states[state.peer_id] = state


class FakeRuntime:
    def __init__(self, local_peer_id: str):
        self.local_peer_id = local_peer_id
        self.codec = None
        self.storage = FakeStorage()
        self.dag = SimpleNamespace(storage=self.storage)
        self.coordinator = FakeCoordinator(local_peer_id=local_peer_id, reconciled=[], storage=self.storage)


class FakePeerProvider:
    def __init__(self, peer_ids: tuple[str, ...]):
        self._peer_ids = peer_ids

    async def list_peer_ids(self) -> tuple[str, ...]:
        return self._peer_ids


class FakeBootstrapPeerProvider(FakePeerProvider):
    def __init__(
        self,
        peer_ids: tuple[str, ...],
        connected_sequences: tuple[tuple[str, ...], ...],
    ):
        super().__init__(peer_ids)
        self._connected_sequences = connected_sequences
        self._connected_calls = 0

    async def list_connected_peer_ids(self) -> tuple[str, ...]:
        index = min(self._connected_calls, len(self._connected_sequences) - 1)
        self._connected_calls += 1
        return self._connected_sequences[index]


class FakeLoopCoordinator(FakeCoordinator):
    def __init__(self, local_peer_id: str, termination_event: trio.Event):
        super().__init__(local_peer_id=local_peer_id, reconciled=[])
        self._termination_event = termination_event

    async def reconcile_with_peer(self, peer_id: str) -> None:
        self.reconciled.append(peer_id)
        self._termination_event.set()


class FakeLoopRuntime(FakeRuntime):
    def __init__(self, local_peer_id: str, termination_event: trio.Event):
        super().__init__(local_peer_id)
        self.coordinator = FakeLoopCoordinator(local_peer_id=local_peer_id, termination_event=termination_event)


class FakeBootstrapCoordinator(FakeCoordinator):
    def __init__(self, local_peer_id: str, storage: FakeStorage, remote_heads: dict[str, tuple[str, ...]]):
        super().__init__(local_peer_id=local_peer_id, reconciled=[])
        self._storage = storage
        self._remote_heads = remote_heads
        self._reconcile_count: dict[str, int] = {}

    async def reconcile_with_peer(self, peer_id: str) -> None:
        await super().reconcile_with_peer(peer_id)
        heads = self._remote_heads[peer_id]
        self._reconcile_count[peer_id] = self._reconcile_count.get(peer_id, 0) + 1
        await self._storage.set_peer_state(
            PeerSyncState(
                peer_id=peer_id,
                summary=DagSummary(
                    namespace="shared",
                    head_ids=heads,
                    node_count=len(heads),
                    orphan_count=0,
                    generated_at_ms=1000 + self._reconcile_count[peer_id],
                ),
                updated_at_ms=2000 + self._reconcile_count[peer_id],
            )
        )
        if self._reconcile_count[peer_id] >= 2:
            self._storage.complete_nodes.update(heads)


class FakeBootstrapRuntime(FakeRuntime):
    def __init__(self, local_peer_id: str, remote_heads: dict[str, tuple[str, ...]]):
        super().__init__(local_peer_id)
        self.coordinator = FakeBootstrapCoordinator(local_peer_id, self.storage, remote_heads)


def _peer_id(seed_byte: int) -> ID:
    return ID.from_pubkey(create_new_key_pair(bytes([seed_byte]) * 32).public_key)


def _runtime(seed_byte: int, *, request_client=None) -> MerkleDagRuntime:
    return MerkleDagRuntime(
        db=DummyDB(),
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(seed_byte),
        namespace="shared",
        dag_topic="dag-topic",
        storage=InMemoryDagStorage(),
        request_client=request_client,
    )


@pytest.mark.asyncio
async def test_receiver_delegates_matching_announcement():
    peer_id = _peer_id(1)
    receiver = GossipReceiverV2(
        gossipsub=None,  # type: ignore[arg-type]
        pubsub=DummyPubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(9),
        namespace="shared",
        dag_topic="dag-topic",
        storage=InMemoryDagStorage(),
    )
    announcement = DagAnnouncement(
        message_id="announcement-1",
        namespace="shared",
        peer_id=peer_id.to_string(),
        head_ids=("head-1",),
        node_count=1,
        created_at_ms=1000,
    )
    message = rpc_pb2.Message(
        from_id=peer_id.to_bytes(),
        data=receiver.codec.encode(announcement),
        topicIDs=["dag-topic"],
    )

    await receiver._handle_message(message)

    peer_state = await receiver.storage.get_peer_state(peer_id.to_string())
    assert peer_state is not None
    assert peer_state.summary.namespace == "shared"
    assert peer_state.summary.head_ids == ("head-1",)
    assert peer_state.summary.node_count == 1


@pytest.mark.asyncio
async def test_receiver_rejects_mismatched_sender_and_claimed_peer():
    receiver = GossipReceiverV2(
        gossipsub=None,  # type: ignore[arg-type]
        pubsub=DummyPubsub(),  # type: ignore[arg-type]
        termination_event=trio.Event(),
        db=DummyDB(),  # type: ignore[arg-type]
        payload_schemas=[CounterPayloadSchema()],
        local_peer_id=_peer_id(10),
        namespace="shared",
        dag_topic="dag-topic",
        storage=InMemoryDagStorage(),
    )
    sender = _peer_id(2)
    claimed = _peer_id(3)
    announcement = DagAnnouncement(
        message_id="announcement-2",
        namespace="shared",
        peer_id=claimed.to_string(),
        head_ids=("head-2",),
        node_count=2,
        created_at_ms=1001,
    )
    message = rpc_pb2.Message(
        from_id=sender.to_bytes(),
        data=receiver.codec.encode(announcement),
        topicIDs=["dag-topic"],
    )

    await receiver._handle_message(message)

    assert await receiver.storage.get_peer_state(sender.to_string()) is None


@pytest.mark.asyncio
async def test_sync_service_handles_inventory_request_bytes():
    runtime = _runtime(6)
    service = MerkleDagSyncService(runtime=runtime, termination_event=trio.Event())
    request = DagInventoryRequest(
        message_id="inventory-2",
        namespace="shared",
        peer_id="peer-remote",
        known_heads=(),
        node_count=0,
        created_at_ms=1002,
    )

    raw_response = await service.handle_sync_request_bytes("peer-remote", runtime.codec.encode(request))
    response = runtime.codec.decode(raw_response)

    assert isinstance(response, DagInventoryResponse)
    assert response.summary.namespace == "shared"


@pytest.mark.asyncio
async def test_sync_service_reconcile_once_uses_peer_provider_and_skips_local_peer():
    local_peer_id = _peer_id(7).to_string()
    runtime = FakeRuntime(local_peer_id)
    service = MerkleDagSyncService(
        runtime=runtime,
        termination_event=trio.Event(),
        peer_provider=FakePeerProvider((local_peer_id, "peer-a", "peer-b")),
    )

    await service.reconcile_once()

    assert runtime.coordinator.reconciled == ["peer-a", "peer-b"]


@pytest.mark.asyncio
async def test_sync_service_sync_dag_waits_for_connected_peers_then_reconciles(monkeypatch):
    local_peer_id = _peer_id(21).to_string()
    runtime = FakeRuntime(local_peer_id)
    provider = FakeBootstrapPeerProvider(
        ("peer-a", "peer-b", "peer-c"),
        (("peer-a",), ("peer-a", "peer-b")),
    )
    service = MerkleDagSyncService(
        runtime=runtime,
        termination_event=trio.Event(),
        peer_provider=provider,
    )

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("subnet.merkle_dag.sync_service.trio.sleep", fake_sleep)

    reconciled_peers = await service.sync_dag(
        min_peer_count=2,
        wait_timeout=0.03,
        poll_interval=0.01,
        settle_time=0.0,
    )

    assert reconciled_peers == ("peer-a", "peer-b", "peer-c")
    assert runtime.coordinator.reconciled == ["peer-a", "peer-b", "peer-c"]


@pytest.mark.asyncio
async def test_sync_service_sync_dag_waits_for_frontier_closure(monkeypatch):
    local_peer_id = _peer_id(31).to_string()
    runtime = FakeBootstrapRuntime(local_peer_id, {"peer-a": ("head-a",)})
    provider = FakeBootstrapPeerProvider(
        ("peer-a",),
        (("peer-a",),),
    )
    service = MerkleDagSyncService(
        runtime=runtime,
        termination_event=trio.Event(),
        peer_provider=provider,
    )

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("subnet.merkle_dag.sync_service.trio.sleep", fake_sleep)

    reconciled_peers = await service.sync_dag(
        min_peer_count=1,
        wait_timeout=0.03,
        poll_interval=0.01,
        settle_time=0.0,
    )

    assert reconciled_peers == ("peer-a",)
    assert runtime.coordinator.reconciled == ["peer-a", "peer-a"]
    assert "head-a" in runtime.storage.complete_nodes


@pytest.mark.asyncio
async def test_sync_service_run_owns_anti_entropy_loop(monkeypatch):
    termination_event = trio.Event()
    local_peer_id = _peer_id(8).to_string()
    runtime = FakeLoopRuntime(local_peer_id, termination_event)
    service = MerkleDagSyncService(
        runtime=runtime,
        termination_event=termination_event,
        peer_provider=FakePeerProvider(("peer-remote",)),
        enable_periodic_reconciliation=True,
        reconciliation_interval=0.01,
    )

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("subnet.merkle_dag.sync_service.trio.sleep", fake_sleep)
    await service.run()

    assert runtime.coordinator.reconciled == ["peer-remote"]
