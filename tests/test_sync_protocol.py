from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair as create_ed25519_key_pair
from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
from multiaddr import Multiaddr
import trio
import varint

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagFetchRequest,
    DagFetchResponse,
    DagNode,
    DagNodeBody,
    DagNodeGossip,
    DagNodeHeader,
    DagNodeSnapshot,
    InMemoryDagStorage,
    NodeIngestResult,
    NodeIngestStatus,
)
from subnet.merkle_dag.exceptions import TimestampValidationError
from subnet.merkle_dag.sync_scheduler import SyncScheduler
from subnet.merkle_dag.sync_service import MerkleDagSyncService
from subnet.protocols.sync_protocol import (
    MAX_STREAM_WRITE_LEN,
    MerkleDagSyncProtocol,
    PeerStateDagPeerSetProvider,
    SyncProtocolPeerRequestClient,
)
from subnet.utils.gossipsub.peer_status_gossip_receiver import PeerStatusGossipReceiver
from subnet.utils.pubsub.peer_state_publisher import PeerRole, PeerStateData, ServerState
from subnet.utils.pubsub.topics import PEER_STATE_TOPIC


class DummyDB:
    def __init__(self):
        self.values: dict[tuple[str, str], dict] = {}

    def get_all_under_key(self, key: str) -> dict[str, dict]:
        return {
            subkey: value
            for (prefix, subkey), value in self.values.items()
            if prefix == key
        }

    def get_nested(self, key: str, subkey: str, default=None):
        return self.values.get((key, subkey), default)

    def set_nested(self, key: str, subkey: str, value) -> None:
        self.values[(key, subkey)] = value


class DummyPeerStore:
    def __init__(self, peer_infos: dict[object, object] | None = None):
        self._peer_infos = peer_infos or {}

    def peer_ids(self):
        return list(self._peer_infos)

    def peer_info(self, peer_id):
        return self._peer_infos[peer_id]


class DummyPeerInfo:
    def __init__(self, addrs=()):
        self.addrs = tuple(addrs)


class DummyRoutingTable:
    def __init__(self, peer_infos: dict[object, object] | None = None):
        self._peer_infos = peer_infos or {}

    def get_peer_ids(self):
        return list(self._peer_infos)

    def get_peer_info(self, peer_id):
        return self._peer_infos.get(peer_id)


class DummyDHT:
    def __init__(self, peer_infos: dict[object, object] | None = None):
        self.routing_table = DummyRoutingTable(peer_infos)


class DummyNetwork:
    def __init__(self, connections: dict[object, list[object]] | None = None):
        self._connections = connections or {}

    def get_connections(self, peer_id):
        return self._connections.get(peer_id, [])


class DummyMuxedConn:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id


class FakeStream:
    def __init__(self, *, incoming: bytes = b"", peer_id: str = "peer-remote"):
        self._incoming = bytearray(incoming)
        self.written = bytearray()
        self.write_calls: list[bytes] = []
        self.closed = False
        self.muxed_conn = DummyMuxedConn(peer_id)

    async def read(self, size: int = -1) -> bytes:
        if not self._incoming:
            return b""
        if size < 0:
            size = len(self._incoming)
        data = bytes(self._incoming[:size])
        del self._incoming[:size]
        return data

    async def write(self, payload: bytes) -> None:
        self.write_calls.append(payload)
        self.written.extend(payload)

    async def close(self) -> None:
        self.closed = True


class FakeHost:
    def __init__(
        self,
        *,
        stream: FakeStream | None = None,
        local_peer_id: str = "peer-local",
        peerstore: DummyPeerStore | None = None,
        network: DummyNetwork | None = None,
        connected_peer_ids: tuple[object, ...] | None = None,
    ):
        self.stream = stream
        self.local_peer_id = local_peer_id
        self.handler = None
        self.connected = []
        self.opened_streams = []
        self._peerstore = peerstore or DummyPeerStore()
        self._network = network or DummyNetwork()
        self._connected_peer_ids = connected_peer_ids

    def set_stream_handler(self, _protocol_id, handler) -> None:
        self.handler = handler

    def get_peerstore(self):
        return self._peerstore

    def get_network(self):
        return self._network

    def get_id(self) -> str:
        return self.local_peer_id

    def get_connected_peers(self):
        if self._connected_peer_ids is not None:
            return list(self._connected_peer_ids)
        return list(getattr(self._network, "_connections", {}).keys())

    async def connect(self, info) -> None:
        self.connected.append(info)

    async def new_stream(self, peer_id, protocols):
        self.opened_streams.append((peer_id, tuple(protocols)))
        return self.stream


class FakeProtocolHost:
    def get_id(self) -> str:
        return "peer-local"


class FakeProtocol:
    def __init__(self, codec, responses):
        self.host = FakeProtocolHost()
        self._codec = codec
        self._responses = responses
        self.calls: list[str] = []

    async def request_bytes(self, peer_id: str, payload: bytes) -> bytes:
        self.calls.append(peer_id)
        return self._responses[peer_id](payload)

    async def list_known_peer_ids(self) -> tuple[str, ...]:
        return ("peer-a", "peer-b")


class FakeReceiverValidator:
    def validate_header_source_peer(self, _header, _peer_id: str) -> None:
        return None


class FakeReceiverDag:
    def __init__(self, ingest_result: NodeIngestResult | Exception):
        self._ingest_result = ingest_result

    async def add_node(self, _node: DagNode, **_kwargs) -> NodeIngestResult:
        if isinstance(self._ingest_result, Exception):
            raise self._ingest_result
        return self._ingest_result


class FakeReceiverCoordinator:
    def __init__(self):
        self.fetch_calls: list[tuple[str, tuple[str, ...]]] = []

    async def fetch_missing(self, peer_id: str, node_ids: tuple[str, ...]) -> None:
        self.fetch_calls.append((peer_id, tuple(node_ids)))


class FakeSyncScheduler:
    def __init__(self):
        self.calls: list[str | None] = []

    async def schedule(self, peer_id: str | None = None) -> None:
        self.calls.append(peer_id)


class FakeReceiverCodec:
    def __init__(self, decoded: DagNodeGossip):
        self._decoded = decoded

    def decode(self, _payload: bytes) -> DagNodeGossip:
        return self._decoded


class FakeReceiverRuntime:
    def __init__(self, decoded: DagNodeGossip, ingest_result: NodeIngestResult | Exception):
        self.local_peer_id = "peer-local"
        self.namespace = "peer-state"
        self.validator = FakeReceiverValidator()
        self.dag = FakeReceiverDag(ingest_result)
        self.coordinator = FakeReceiverCoordinator()
        self.codec = FakeReceiverCodec(decoded)


class FakeLoopCoordinator:
    def __init__(self, local_peer_id: str):
        self.local_peer_id = local_peer_id
        self.reconciled: list[str] = []

    async def reconcile_with_peer(self, peer_id: str) -> None:
        self.reconciled.append(peer_id)


class FakeLoopRuntime:
    def __init__(self, local_peer_id: str):
        self.local_peer_id = local_peer_id
        self.coordinator = FakeLoopCoordinator(local_peer_id)
        self.codec = None


class FakePeerProvider:
    def __init__(self, peer_ids: tuple[str, ...]):
        self._peer_ids = peer_ids

    async def list_peer_ids(self) -> tuple[str, ...]:
        return self._peer_ids


class FakeSchedulerCoordinator:
    def __init__(self):
        self.fetch_calls: list[tuple[str, tuple[str, ...]]] = []

    async def fetch_missing(self, peer_id: str, node_ids: tuple[str, ...]) -> None:
        self.fetch_calls.append((peer_id, tuple(node_ids)))


class FakeSchedulerDag:
    def __init__(self, storage: InMemoryDagStorage):
        self.storage = storage


class FakeSchedulerRuntime:
    def __init__(self, local_peer_id: str, storage: InMemoryDagStorage):
        self.local_peer_id = local_peer_id
        self.dag = FakeSchedulerDag(storage)
        self.coordinator = FakeSchedulerCoordinator()


def _peer_id_obj(seed_byte: int) -> ID:
    return ID.from_pubkey(create_ed25519_key_pair(bytes([seed_byte]) * 32).public_key)


def _peer_id(seed_byte: int) -> str:
    return _peer_id_obj(seed_byte).to_string()


def _frame(payload: bytes) -> bytes:
    return varint.encode(len(payload)) + payload


def _read_frame(payload: bytes) -> bytes:
    prefix = bytearray()
    index = 0
    while True:
        prefix.append(payload[index])
        index += 1
        if prefix[-1] & 0x80 == 0:
            break
    length = varint.decode_bytes(bytes(prefix))
    return payload[index : index + length]


def _peer_state_node(node_id: str, peer_id: str, created_at_ms: int, multiaddr: str) -> DagNode:
    return DagNode(
        header=DagNodeHeader(
            node_id=node_id,
            namespace="peer-state",
            schema_id="peer-state",
            parent_ids=(),
            body_hash="body-hash",
            body_size=1,
            author=peer_id,
            public_key="00",
            signature="00",
            created_at_ms=created_at_ms,
            metadata={
                "uid": f"peer-state-{created_at_ms}",
                "epoch": created_at_ms,
                "subnet_id": 1,
                "subnet_node_id": 1,
                "state": 2,
                "role": 0,
                "multiaddr": multiaddr,
            },
        ),
        body=DagNodeBody(
            node_id=node_id,
            payload={"kind": "peer-state", "peer_id": peer_id},
        ),
    )


@pytest.mark.asyncio
async def test_request_client_falls_back_to_other_known_peers_for_missing_nodes():
    serializer = CanonicalJSONSerializer()
    from subnet.merkle_dag import DagSyncMessageCodec

    codec = DagSyncMessageCodec(serializer)
    snapshot = DagNodeSnapshot(
        header=DagNodeHeader(
            node_id="node-1",
            namespace="shared",
            schema_id="counter",
            parent_ids=(),
            body_hash="body-hash",
            body_size=1,
            author="peer-b",
            public_key="00",
            signature="00",
            created_at_ms=1000,
        ),
        body=DagNodeBody(node_id="node-1", payload={"value": 1}),
    )

    def primary_response(payload: bytes) -> bytes:
        request = codec.decode(payload)
        assert isinstance(request, DagFetchRequest)
        return codec.encode(
            DagFetchResponse(
                message_id=request.message_id,
                namespace=request.namespace,
                peer_id="peer-a",
                nodes=(),
                not_found=("node-1",),
                created_at_ms=1001,
            )
        )

    def fallback_response(payload: bytes) -> bytes:
        request = codec.decode(payload)
        assert isinstance(request, DagFetchRequest)
        assert request.node_ids == ("node-1",)
        return codec.encode(
            DagFetchResponse(
                message_id=request.message_id,
                namespace=request.namespace,
                peer_id="peer-b",
                nodes=(snapshot,),
                not_found=(),
                created_at_ms=1002,
            )
        )

    protocol = FakeProtocol(codec, {"peer-a": primary_response, "peer-b": fallback_response})
    client = SyncProtocolPeerRequestClient(protocol, codec)

    response = await client.request(
        "peer-a",
        DagFetchRequest(
            message_id="fetch-1",
            namespace="shared",
            peer_id="peer-local",
            node_ids=("node-1",),
            include_bodies=True,
            max_ancestor_depth=8,
            created_at_ms=999,
        ),
    )

    assert isinstance(response, DagFetchResponse)
    assert protocol.calls == ["peer-a", "peer-b"]
    assert response.not_found == ()
    assert tuple(item.header.node_id for item in response.nodes) == ("node-1",)


def test_peer_state_data_round_trips_multiaddr():
    data = PeerStateData(
        uid="peer-state-1",
        epoch=7,
        subnet_id=1,
        subnet_node_id=2,
        state=ServerState.ONLINE,
        role=PeerRole.VALIDATOR,
        multiaddr=Multiaddr("/ip4/127.0.0.1/tcp/9002"),
    )

    metadata = data.to_metadata()
    restored = PeerStateData.from_metadata(metadata)

    assert metadata["multiaddr"] == "/ip4/127.0.0.1/tcp/9002"
    assert restored.multiaddr == Multiaddr("/ip4/127.0.0.1/tcp/9002")


@pytest.mark.asyncio
async def test_sync_protocol_request_bytes_resolves_peer_multiaddr_from_cached_db_state():
    remote_peer_id = _peer_id(7)
    db = DummyDB()
    db.set_nested(
        PEER_STATE_TOPIC,
        remote_peer_id,
        {
            "peer_id": remote_peer_id,
            "node_id": "peer-state-2",
            "created_at_ms": 1001,
            "state": ServerState.ONLINE.value,
            "multiaddr": "/ip4/127.0.0.1/tcp/9002",
        },
    )
    protocol = MerkleDagSyncProtocol(FakeHost(), db)

    calls = {}

    async def fake_call_remote(destination, payload, *, peer_id=None):
        calls["destination"] = str(destination)
        calls["payload"] = payload
        calls["peer_id"] = peer_id
        return b"response-bytes"

    protocol.call_remote = fake_call_remote  # type: ignore[method-assign]
    provider = PeerStateDagPeerSetProvider(protocol)

    response = await protocol.request_bytes(remote_peer_id, b"request-bytes")

    assert response == b"response-bytes"
    assert calls == {
        "destination": f"/ip4/127.0.0.1/tcp/9002/p2p/{remote_peer_id}",
        "payload": b"request-bytes",
        "peer_id": remote_peer_id,
    }
    assert await provider.list_peer_ids() == (remote_peer_id,)


@pytest.mark.asyncio
async def test_sync_protocol_request_bytes_resolves_peer_multiaddr_from_dht_routing_table():
    remote_peer = _peer_id_obj(21)
    remote_peer_id = remote_peer.to_string()
    dht = DummyDHT({remote_peer: DummyPeerInfo((Multiaddr("/ip4/127.0.0.1/tcp/9011"),))})
    protocol = MerkleDagSyncProtocol(FakeHost(), DummyDB(), dht=dht)

    calls = {}

    async def fake_call_remote(destination, payload, *, peer_id=None):
        calls["destination"] = str(destination)
        calls["payload"] = payload
        calls["peer_id"] = peer_id
        return b"response-bytes"

    protocol.call_remote = fake_call_remote  # type: ignore[method-assign]

    response = await protocol.request_bytes(remote_peer_id, b"request-bytes")

    assert response == b"response-bytes"
    assert calls == {
        "destination": f"/ip4/127.0.0.1/tcp/9011/p2p/{remote_peer_id}",
        "payload": b"request-bytes",
        "peer_id": remote_peer_id,
    }


@pytest.mark.asyncio
async def test_sync_protocol_request_bytes_allows_joining_peer_for_direct_sync():
    joining_peer_id = _peer_id(18)
    db = DummyDB()
    db.set_nested(
        PEER_STATE_TOPIC,
        joining_peer_id,
        {
            "peer_id": joining_peer_id,
            "node_id": "peer-state-joining",
            "created_at_ms": 1002,
            "state": ServerState.JOINING.value,
            "multiaddr": "/ip4/127.0.0.1/tcp/9003",
        },
    )
    protocol = MerkleDagSyncProtocol(FakeHost(), db)

    calls = {}

    async def fake_call_remote(destination, payload, *, peer_id=None):
        calls["destination"] = str(destination)
        calls["payload"] = payload
        calls["peer_id"] = peer_id
        return b"response-bytes"

    protocol.call_remote = fake_call_remote  # type: ignore[method-assign]
    provider = PeerStateDagPeerSetProvider(protocol)

    response = await protocol.request_bytes(joining_peer_id, b"request-bytes")

    assert response == b"response-bytes"
    assert calls == {
        "destination": f"/ip4/127.0.0.1/tcp/9003/p2p/{joining_peer_id}",
        "payload": b"request-bytes",
        "peer_id": joining_peer_id,
    }
    assert await protocol.resolve_peer_multiaddr(joining_peer_id) is None
    assert await provider.list_peer_ids() == ()


@pytest.mark.asyncio
async def test_sync_protocol_ignores_non_online_cached_peer_states():
    online_peer_id = _peer_id(11)
    joining_peer_id = _peer_id(12)
    db = DummyDB()
    db.set_nested(
        PEER_STATE_TOPIC,
        online_peer_id,
        {
            "peer_id": online_peer_id,
            "node_id": "peer-state-online",
            "created_at_ms": 1001,
            "state": ServerState.ONLINE.value,
            "multiaddr": "/ip4/127.0.0.1/tcp/9002",
        },
    )
    db.set_nested(
        PEER_STATE_TOPIC,
        joining_peer_id,
        {
            "peer_id": joining_peer_id,
            "node_id": "peer-state-joining",
            "created_at_ms": 1002,
            "state": ServerState.JOINING.value,
            "multiaddr": "/ip4/127.0.0.1/tcp/9003",
        },
    )
    protocol = MerkleDagSyncProtocol(FakeHost(), db)
    provider = PeerStateDagPeerSetProvider(protocol)

    assert await protocol.resolve_peer_multiaddr(online_peer_id) == Multiaddr(
        f"/ip4/127.0.0.1/tcp/9002/p2p/{online_peer_id}"
    )
    assert await protocol.resolve_peer_multiaddr(joining_peer_id) is None
    assert await provider.list_peer_ids() == (online_peer_id,)


@pytest.mark.asyncio
async def test_sync_protocol_request_bytes_falls_back_to_existing_connection_without_cached_multiaddr():
    remote_peer = _peer_id_obj(19)
    remote_peer_id = remote_peer.to_string()
    response_stream = FakeStream(incoming=_frame(b"pong"))
    network = DummyNetwork({remote_peer: [object()]})
    host = FakeHost(stream=response_stream, network=network)
    protocol = MerkleDagSyncProtocol(host, DummyDB())

    response = await protocol.request_bytes(remote_peer_id, b"ping")

    assert response == b"pong"
    assert _read_frame(bytes(response_stream.written)) == b"ping"
    assert host.connected == []
    assert len(host.opened_streams) == 1
    opened_peer_id, protocols = host.opened_streams[0]
    assert opened_peer_id == remote_peer
    assert protocols == ("/subnet/merkle_dag_sync_protocol/1.0.0",)


@pytest.mark.asyncio
async def test_sync_protocol_list_known_peer_ids_includes_connected_peers_without_addresses():
    connected_peer = _peer_id_obj(20)
    protocol = MerkleDagSyncProtocol(
        FakeHost(connected_peer_ids=(connected_peer,), peerstore=DummyPeerStore({connected_peer: DummyPeerInfo(())})),
        DummyDB(),
    )
    provider = PeerStateDagPeerSetProvider(protocol)

    assert await protocol.list_known_peer_ids() == (connected_peer.to_string(),)
    assert await provider.list_connected_peer_ids() == (connected_peer.to_string(),)


@pytest.mark.asyncio
async def test_sync_protocol_list_known_peer_ids_includes_dht_routing_table_peers():
    dht_peer = _peer_id_obj(22)
    dht = DummyDHT({dht_peer: DummyPeerInfo((Multiaddr("/ip4/127.0.0.1/tcp/9012"),))})
    protocol = MerkleDagSyncProtocol(FakeHost(), DummyDB(), dht=dht)

    assert await protocol.list_known_peer_ids() == (dht_peer.to_string(),)


@pytest.mark.asyncio
async def test_sync_protocol_list_known_peer_ids_skips_undialable_peerstore_entries():
    dialable_peer = _peer_id_obj(16)
    undialable_peer = _peer_id_obj(17)
    peerstore = DummyPeerStore(
        {
            dialable_peer: DummyPeerInfo((Multiaddr("/ip4/127.0.0.1/tcp/9006"),)),
            undialable_peer: DummyPeerInfo(()),
        }
    )
    protocol = MerkleDagSyncProtocol(FakeHost(peerstore=peerstore), DummyDB())

    assert await protocol.list_known_peer_ids() == (dialable_peer.to_string(),)


@pytest.mark.asyncio
async def test_peer_status_receiver_stores_latest_peer_state_in_plain_db():
    remote_peer_id = _peer_id(10)
    db = DummyDB()
    receiver = PeerStatusGossipReceiver(
        pubsub=None,  # type: ignore[arg-type]
        termination_event=None,  # type: ignore[arg-type]
        runtime=None,  # type: ignore[arg-type]
        db=db,
    )

    newer = DagNodeGossip(
        message_id="gossip-2",
        namespace="peer-state",
        peer_id=remote_peer_id,
        node=_peer_state_node("peer-state-2", remote_peer_id, 1001, "/ip4/127.0.0.1/tcp/9002"),
        created_at_ms=1001,
    )
    older = DagNodeGossip(
        message_id="gossip-1",
        namespace="peer-state",
        peer_id=remote_peer_id,
        node=_peer_state_node("peer-state-1", remote_peer_id, 1000, "/ip4/127.0.0.1/tcp/9001"),
        created_at_ms=1000,
    )

    receiver._store_peer_state_snapshot(remote_peer_id, newer)
    receiver._store_peer_state_snapshot(remote_peer_id, older)

    assert db.get_all_under_key(PEER_STATE_TOPIC) == {
        remote_peer_id: {
            "peer_id": remote_peer_id,
            "node_id": "peer-state-2",
            "created_at_ms": 1001,
            "uid": "peer-state-1001",
            "epoch": 1001,
            "subnet_id": 1,
            "subnet_node_id": 1,
            "state": 2,
            "role": 0,
            "multiaddr": "/ip4/127.0.0.1/tcp/9002",
        }
    }


@pytest.mark.asyncio
async def test_peer_status_receiver_notifies_sync_scheduler_when_orphaned():
    remote_peer = _peer_id_obj(13)
    remote_peer_id = remote_peer.to_string()
    decoded = DagNodeGossip(
        message_id="gossip-orphan",
        namespace="peer-state",
        peer_id=remote_peer_id,
        node=_peer_state_node("peer-state-3", remote_peer_id, 1003, "/ip4/127.0.0.1/tcp/9004"),
        created_at_ms=1003,
    )
    runtime = FakeReceiverRuntime(
        decoded,
        NodeIngestResult(
            node_id="peer-state-3",
            status=NodeIngestStatus.ORPHAN,
            missing_parents=("missing-parent-1", "missing-parent-2"),
        ),
    )
    sync_scheduler = FakeSyncScheduler()
    receiver = PeerStatusGossipReceiver(
        pubsub=None,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        runtime=runtime,  # type: ignore[arg-type]
        db=DummyDB(),
        sync_scheduler=sync_scheduler,  # type: ignore[arg-type]
    )

    await receiver._handle_message(
        rpc_pb2.Message(
            from_id=remote_peer.to_bytes(),
            data=b"encoded-node",
            topicIDs=[PEER_STATE_TOPIC],
        )
    )

    assert sync_scheduler.calls == [remote_peer_id]
    assert runtime.coordinator.fetch_calls == []


@pytest.mark.asyncio
async def test_peer_status_receiver_fetches_missing_parents_inline_without_scheduler():
    remote_peer = _peer_id_obj(15)
    remote_peer_id = remote_peer.to_string()
    decoded = DagNodeGossip(
        message_id="gossip-orphan-inline",
        namespace="peer-state",
        peer_id=remote_peer_id,
        node=_peer_state_node("peer-state-4", remote_peer_id, 1004, "/ip4/127.0.0.1/tcp/9005"),
        created_at_ms=1004,
    )
    runtime = FakeReceiverRuntime(
        decoded,
        NodeIngestResult(
            node_id="peer-state-4",
            status=NodeIngestStatus.ORPHAN,
            missing_parents=("missing-parent-inline",),
        ),
    )
    receiver = PeerStatusGossipReceiver(
        pubsub=None,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        runtime=runtime,  # type: ignore[arg-type]
        db=DummyDB(),
    )

    await receiver._handle_message(
        rpc_pb2.Message(
            from_id=remote_peer.to_bytes(),
            data=b"encoded-node",
            topicIDs=[PEER_STATE_TOPIC],
        )
    )

    assert runtime.coordinator.fetch_calls == [
        (remote_peer_id, ("missing-parent-inline",)),
    ]


@pytest.mark.asyncio
async def test_peer_status_receiver_rejects_future_dated_node():
    remote_peer = _peer_id_obj(16)
    remote_peer_id = remote_peer.to_string()
    decoded = DagNodeGossip(
        message_id="gossip-future",
        namespace="peer-state",
        peer_id=remote_peer_id,
        node=_peer_state_node("peer-state-future", remote_peer_id, 31_536_000_000, "/ip4/127.0.0.1/tcp/9006"),
        created_at_ms=31_536_000_000,
    )
    runtime = FakeReceiverRuntime(decoded, TimestampValidationError("future timestamp"))
    receiver = PeerStatusGossipReceiver(
        pubsub=None,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        runtime=runtime,  # type: ignore[arg-type]
        db=DummyDB(),
    )

    await receiver._handle_message(
        rpc_pb2.Message(
            from_id=remote_peer.to_bytes(),
            data=b"encoded-node",
            topicIDs=[PEER_STATE_TOPIC],
        )
    )

    assert receiver.db.get_all_under_key(PEER_STATE_TOPIC) == {}
    assert runtime.coordinator.fetch_calls == []


@pytest.mark.asyncio
async def test_sync_scheduler_sync_once_fetches_current_orphans_from_preferred_peer():
    storage = InMemoryDagStorage()
    await storage.mark_orphan("orphan-1", ("missing-parent-1", "missing-parent-2"))
    runtime = FakeSchedulerRuntime("peer-local", storage)
    scheduler = SyncScheduler(
        runtime=runtime,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        batch_window=0,
        sync_on_startup=False,
    )

    await scheduler.sync_once(("peer-preferred",))

    assert runtime.coordinator.fetch_calls == [
        ("peer-preferred", ("missing-parent-1", "missing-parent-2")),
    ]


@pytest.mark.trio
async def test_sync_scheduler_run_fetches_startup_orphans_with_available_peer():
    storage = InMemoryDagStorage()
    await storage.mark_orphan("orphan-startup", ("missing-parent-startup",))
    termination_event = trio.Event()
    runtime = FakeSchedulerRuntime("peer-local", storage)

    async def fetch_missing(peer_id: str, node_ids: tuple[str, ...]) -> None:
        runtime.coordinator.fetch_calls.append((peer_id, tuple(node_ids)))
        termination_event.set()

    runtime.coordinator.fetch_missing = fetch_missing  # type: ignore[method-assign]
    scheduler = SyncScheduler(
        runtime=runtime,  # type: ignore[arg-type]
        termination_event=termination_event,
        peer_provider=FakePeerProvider(("peer-a",)),
        batch_window=0,
    )

    await scheduler.run()

    assert runtime.coordinator.fetch_calls == [
        ("peer-a", ("missing-parent-startup",)),
    ]


@pytest.mark.asyncio
async def test_sync_service_run_is_noop_by_default():
    local_peer_id = _peer_id(14)
    runtime = FakeLoopRuntime(local_peer_id)
    service = MerkleDagSyncService(
        runtime=runtime,  # type: ignore[arg-type]
        termination_event=trio.Event(),
        peer_provider=FakePeerProvider(("peer-remote",)),
    )

    await service.run()

    assert runtime.coordinator.reconciled == []


@pytest.mark.asyncio
async def test_sync_protocol_handles_and_sends_framed_messages():
    remote_peer_id = _peer_id(8)
    inbound_stream = FakeStream(incoming=_frame(b"hello"), peer_id=remote_peer_id)
    protocol = MerkleDagSyncProtocol(
        FakeHost(),
        DummyDB(),
        request_handler=lambda peer_id, payload: payload.upper() + b":" + peer_id.encode("utf-8"),
    )

    await protocol._handle_incoming_stream(inbound_stream)

    assert _read_frame(bytes(inbound_stream.written)) == f"HELLO:{remote_peer_id}".encode("utf-8")
    assert inbound_stream.closed is True


@pytest.mark.asyncio
async def test_sync_protocol_call_remote_writes_and_reads_framed_payloads():
    remote_peer_id = _peer_id(9)
    response_stream = FakeStream(incoming=_frame(b"pong"))
    host = FakeHost(stream=response_stream)
    protocol = MerkleDagSyncProtocol(host, DummyDB())
    destination = Multiaddr(f"/ip4/127.0.0.1/tcp/9002/p2p/{remote_peer_id}")

    response = await protocol.call_remote(destination, b"ping")

    assert response == b"pong"
    assert _read_frame(bytes(response_stream.written)) == b"ping"
    assert len(host.connected) == 1
    assert len(host.opened_streams) == 1


@pytest.mark.asyncio
async def test_sync_protocol_write_frame_chunks_large_payloads():
    stream = FakeStream()
    protocol = MerkleDagSyncProtocol(FakeHost(), DummyDB())
    payload = b"x" * (MAX_STREAM_WRITE_LEN + 123)

    await protocol._write_frame(stream, payload)

    assert b"".join(stream.write_calls) == varint.encode(len(payload)) + payload
    assert max(len(chunk) for chunk in stream.write_calls) <= MAX_STREAM_WRITE_LEN
