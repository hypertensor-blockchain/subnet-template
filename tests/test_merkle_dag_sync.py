from __future__ import annotations

from dataclasses import dataclass
import logging

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagFetchRequest,
    DagSyncMessageCodec,
    DagValidator,
    InMemoryDagStorage,
    Libp2pKeyPairSigner,
    Libp2pSignatureVerifier,
    MerkleDag,
    MerkleDagSyncCoordinator,
    PayloadSchemaRegistry,
    SHA256Hasher,
)
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.payloads import MappingPayloadSchema


class CounterPayloadSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("counter")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("value"), int):
            raise PayloadValidationError("counter payload requires an integer 'value'")


class RecordingPublisher:
    def __init__(self):
        self.messages: list[tuple[str, bytes]] = []

    async def publish(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class InMemoryRequestNetwork:
    def __init__(self):
        self.coordinators: dict[str, MerkleDagSyncCoordinator] = {}
        self.calls: list[tuple[str, str, str]] = []

    def register(self, peer_id: str, coordinator: MerkleDagSyncCoordinator) -> None:
        self.coordinators[peer_id] = coordinator


class InMemoryPeerRequestClient:
    def __init__(self, local_peer_id: str, network: InMemoryRequestNetwork):
        self._local_peer_id = local_peer_id
        self._network = network

    async def request(self, peer_id: str, message):
        self._network.calls.append((self._local_peer_id, peer_id, type(message).__name__))
        return await self._network.coordinators[peer_id].handle_request(self._local_peer_id, message)


class RecordingInMemoryPeerRequestClient(InMemoryPeerRequestClient):
    def __init__(self, local_peer_id: str, network: InMemoryRequestNetwork):
        super().__init__(local_peer_id, network)
        self.messages: list[object] = []

    async def request(self, peer_id: str, message):
        self.messages.append(message)
        return await super().request(peer_id, message)


@dataclass
class PeerFixture:
    peer_id: str
    signer: Libp2pKeyPairSigner
    publisher: RecordingPublisher
    dag: MerkleDag
    coordinator: MerkleDagSyncCoordinator
    codec: DagSyncMessageCodec
    request_client: InMemoryPeerRequestClient


def _build_peer(
    _peer_label: str,
    seed_byte: int,
    network: InMemoryRequestNetwork,
    *,
    request_client_cls=InMemoryPeerRequestClient,
) -> PeerFixture:
    key_pair = create_new_key_pair(bytes([seed_byte]) * 32)
    peer_id = ID.from_pubkey(key_pair.public_key).to_string()
    signer = Libp2pKeyPairSigner(key_pair)
    serializer = CanonicalJSONSerializer()
    schema_registry = PayloadSchemaRegistry([CounterPayloadSchema()])
    validator = DagValidator(
        serializer=serializer,
        hasher=SHA256Hasher(),
        schema_registry=schema_registry,
        signature_verifier=Libp2pSignatureVerifier(),
    )
    dag = MerkleDag(
        namespace="shared",
        storage=InMemoryDagStorage(),
        validator=validator,
        schema_registry=schema_registry,
        serializer=serializer,
    )
    publisher = RecordingPublisher()
    codec = DagSyncMessageCodec(serializer)
    request_client = request_client_cls(peer_id, network)
    coordinator = MerkleDagSyncCoordinator(
        dag=dag,
        local_peer_id=peer_id,
        topic="shared-topic",
        codec=codec,
        gossip_publisher=publisher,
        request_client=request_client,
    )
    network.register(peer_id, coordinator)
    return PeerFixture(
        peer_id=peer_id,
        signer=signer,
        publisher=publisher,
        dag=dag,
        coordinator=coordinator,
        codec=codec,
        request_client=request_client,
    )


async def _append_node(peer: PeerFixture, value: int, parent_ids: tuple[str, ...] = ()) -> str:
    node = await peer.dag.create_node(
        schema_id="counter",
        payload={"value": value},
        parent_ids=parent_ids,
        signer=peer.signer,
        author=peer.peer_id,
        created_at_ms=1000 + value,
    )
    await peer.coordinator.ingest_local_node(node)
    return node.header.node_id


@pytest.mark.asyncio
async def test_duplicate_announcement_dedup():
    network = InMemoryRequestNetwork()
    source = _build_peer("peer-a", 1, network)
    target = _build_peer("peer-b", 2, network)

    head_id = await _append_node(source, 1)
    announcement = await source.coordinator.publish_heads()
    assert announcement is not None

    await target.coordinator.handle_announcement(announcement, source_peer=source.peer_id)
    first_call_count = len(network.calls)
    await target.coordinator.handle_announcement(announcement, source_peer=source.peer_id)

    assert len(network.calls) == first_call_count
    assert await target.dag.get_node(head_id) is not None


@pytest.mark.asyncio
async def test_convergence_after_reconciliation():
    network = InMemoryRequestNetwork()
    source = _build_peer("peer-source", 3, network)
    target = _build_peer("peer-target", 4, network)

    root_id = await _append_node(source, 1)
    head_id = await _append_node(source, 2, (root_id,))

    await target.coordinator.reconcile_with_peer(source.peer_id)

    assert await target.dag.get_node(root_id) is not None
    assert await target.dag.get_node(head_id) is not None
    assert await target.dag.get_heads() == await source.dag.get_heads()


@pytest.mark.asyncio
async def test_late_join_catch_up(caplog: pytest.LogCaptureFixture):
    network = InMemoryRequestNetwork()
    source = _build_peer("peer-source", 5, network)
    late = _build_peer("peer-late", 6, network)

    root_id = await _append_node(source, 1)
    middle_id = await _append_node(source, 2, (root_id,))
    head_id = await _append_node(source, 3, (middle_id,))

    with caplog.at_level(logging.INFO, logger="subnet.merkle_dag.sync"):
        await late.coordinator.reconcile_with_peer(source.peer_id)

    assert await late.dag.get_node(root_id) is not None
    assert await late.dag.get_node(middle_id) is not None
    assert await late.dag.get_node(head_id) is not None
    assert await late.dag.get_heads() == await source.dag.get_heads()
    assert await late.dag.storage.count_complete_nodes() == await source.dag.storage.count_complete_nodes()

    sync_store_records = [
        record for record in caplog.records if getattr(record, "event", None) == "peer_sync_dag_node_stored"
    ]
    assert {record.node_id for record in sync_store_records} == {root_id, middle_id, head_id}
    assert {record.namespace for record in sync_store_records} == {"shared"}
    assert {record.schema_id for record in sync_store_records} == {"counter"}
    assert {record.topic for record in sync_store_records} == {"shared-topic"}
    assert {record.from_peer for record in sync_store_records} == {source.peer_id}
    assert all("topic=shared-topic" in record.message for record in sync_store_records)
    assert all("namespace=shared" in record.message for record in sync_store_records)
    assert all("schema_id=counter" in record.message for record in sync_store_records)


@pytest.mark.asyncio
async def test_fetch_missing_requests_frontier_only_and_walks_back_recursively():
    network = InMemoryRequestNetwork()
    source = _build_peer("peer-source", 11, network)
    late = _build_peer("peer-late", 12, network, request_client_cls=RecordingInMemoryPeerRequestClient)

    root_id = await _append_node(source, 1)
    middle_id = await _append_node(source, 2, (root_id,))
    head_id = await _append_node(source, 3, (middle_id,))

    await late.coordinator.fetch_missing(source.peer_id, (head_id,))

    assert await late.dag.get_node(root_id) is not None
    assert await late.dag.get_node(middle_id) is not None
    assert await late.dag.get_node(head_id) is not None

    fetch_messages = [message for message in late.request_client.messages if isinstance(message, DagFetchRequest)]
    assert [message.node_ids for message in fetch_messages] == [
        (head_id,),
        (middle_id,),
        (root_id,),
    ]
    assert all(message.max_ancestor_depth == 0 for message in fetch_messages)


@pytest.mark.asyncio
async def test_anti_entropy_summary_reconciliation():
    network = InMemoryRequestNetwork()
    peer_a = _build_peer("peer-a", 7, network)
    peer_b = _build_peer("peer-b", 8, network)

    root_id = await _append_node(peer_a, 1)
    await peer_b.coordinator.reconcile_with_peer(peer_a.peer_id)

    left_head = await _append_node(peer_a, 2, (root_id,))
    right_head = await _append_node(peer_b, 3, (root_id,))

    await peer_a.coordinator.reconcile_with_peer(peer_b.peer_id)
    await peer_b.coordinator.reconcile_with_peer(peer_a.peer_id)

    assert set(await peer_a.dag.get_heads()) == {left_head, right_head}
    assert set(await peer_b.dag.get_heads()) == {left_head, right_head}
    assert await peer_a.dag.storage.count_complete_nodes() == await peer_b.dag.storage.count_complete_nodes()
