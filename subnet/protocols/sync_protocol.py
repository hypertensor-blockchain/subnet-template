"""Libp2p request-response transport for Merkle DAG anti-entropy sync."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
import inspect
import logging
from typing import Any

from libp2p.abc import IHost, INetStream
from libp2p.peer.id import ID
from libp2p.tools.utils import info_from_p2p_addr
from multiaddr import Multiaddr
import varint

from subnet.merkle_dag import (
    DagAnnouncement,
    DagFetchRequest,
    DagFetchResponse,
    DagInventoryRequest,
    DagInventoryResponse,
    DagSyncMessageCodec,
)
from subnet.merkle_dag.interfaces import PeerRequestClient, PeerSetProvider
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB
from subnet.utils.pubsub.peer_state_publisher import ServerState
from subnet.utils.pubsub.topics import PEER_STATE_TOPIC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("merkle_dag_sync_protocol/1.0.0")

PROTOCOL_ID = "/subnet/merkle_dag_sync_protocol/1.0.0"
MAX_READ_LEN = 2**32 - 1
MAX_STREAM_WRITE_LEN = 60 * 1024

SyncRequestHandler = Callable[[str, bytes], Awaitable[bytes | str] | bytes | str]
PeerListCallable = Callable[[], Awaitable[Iterable[str]] | Iterable[str]]


class MerkleDagSyncProtocol:
    """Serve and issue framed DAG sync requests over a libp2p stream."""

    def __init__(
        self,
        host: IHost,
        db: RocksDB,
        dht: Any | None = None,
        telemetry: Telemetry | None = None,
        request_handler: SyncRequestHandler | None = None,
    ):
        """
        Initialize the MerkleDagSyncProtocol.

        Args:
            host: The libp2p host instance.
            db: The RocksDB instance backing the shared DAG state.
            dht: Optional Kademlia DHT used to inspect the routing table.
            telemetry: Optional telemetry sink.
            request_handler: Optional bytes handler for inbound sync requests.

        """
        self.host = host
        self.db = db
        self.dht = dht
        self.telemetry = telemetry
        self._request_handler = request_handler

        self.host.set_stream_handler(PROTOCOL_ID, self._handle_incoming_stream)
        logger.info("MerkleDagSyncProtocol initialized with protocol ID: %s", PROTOCOL_ID)

    @staticmethod
    def _coerce_peer_id(peer_id: object) -> str:
        """Normalize libp2p peer IDs and test doubles to a string form."""
        to_string = getattr(peer_id, "to_string", None)
        if callable(to_string):
            return to_string()
        return str(peer_id)

    def set_request_handler(self, handler: SyncRequestHandler | None) -> None:
        """Update the inbound sync request handler."""
        self._request_handler = handler

    async def call_remote(
        self,
        destination: Multiaddr,
        msg: bytes | str,
        *,
        peer_id: str | None = None,
    ) -> bytes:
        """Call a remote peer and return the framed unary response bytes."""
        payload = msg.encode("utf-8") if isinstance(msg, str) else msg
        stream_peer_id = peer_id
        stream = None

        try:
            dial_addr = self._normalize_dial_addr(destination, peer_id=peer_id)
            info = info_from_p2p_addr(dial_addr)
            stream_peer_id = self._coerce_peer_id(info.peer_id)

            await self.host.connect(info)
            stream = await self.host.new_stream(info.peer_id, [PROTOCOL_ID])
            await self._write_frame(stream, payload)
            response = await self._read_frame(stream)

            return response
        except Exception:
            logger.exception("Failed DAG sync request to peer %s", stream_peer_id)
            raise
        finally:
            if stream is not None:
                try:
                    await stream.close()
                except Exception:
                    logger.debug("Failed to close DAG sync stream to %s", stream_peer_id, exc_info=True)

    async def request_bytes(self, peer_id: str, payload: bytes) -> bytes:
        """Resolve a peer's published multiaddr and issue a raw sync request."""
        if await self._has_live_connection(peer_id):
            return await self._call_connected_peer(peer_id, payload)

        destination = await self._resolve_request_destination(peer_id)
        if destination is not None:
            return await self.call_remote(destination, payload, peer_id=peer_id)

        raise ValueError(f"No sync multiaddr known for peer {peer_id!r}")

    async def resolve_peer_multiaddr(self, peer_id: str) -> Multiaddr | None:
        """Resolve a peer to a dialable multiaddr using libp2p sources first."""
        destination = self._resolve_from_peerstore(peer_id)
        if destination is not None:
            return destination

        destination = self._resolve_from_dht(peer_id)
        if destination is not None:
            return destination

        latest_state_addrs = await self._peer_state_addrs()
        return latest_state_addrs.get(peer_id)

    async def _resolve_request_destination(self, peer_id: str) -> Multiaddr | None:
        """
        Resolve a peer destination for an explicit direct sync request.

        Direct fetches may target a peer that is still JOINING but just gossiped
        a node we need. For that case, allow the cached peer-state snapshot to
        supply a dial address regardless of `state`. General peer discovery
        still stays ONLINE-only via `_peer_state_addrs()`.
        """
        destination = self._resolve_from_peerstore(peer_id)
        if destination is not None:
            return destination

        destination = self._resolve_from_dht(peer_id)
        if destination is not None:
            return destination

        return self._resolve_from_cached_peer_state(peer_id, require_online=False)

    async def list_known_peer_ids(self) -> tuple[str, ...]:
        """Return peer ids known via host connections, DHT, peerstore, or peer-state cache."""
        peer_state_addrs = await self._peer_state_addrs()
        connected_peer_ids = set(await self.list_connected_peer_ids())

        peer_ids = set(peer_state_addrs.keys())
        peer_ids.update(connected_peer_ids)
        peer_ids.update(self._list_dht_peer_ids())
        try:
            for peer_id in self.host.get_peerstore().peer_ids():
                normalized_peer_id = self._coerce_peer_id(peer_id)
                if self._resolve_from_peerstore(normalized_peer_id) is None:
                    continue
                peer_ids.add(normalized_peer_id)
        except Exception:
            logger.debug("Failed to enumerate peerstore peer ids", exc_info=True)
        dialable_or_connected = {
            peer_id
            for peer_id in peer_ids
            if peer_id in connected_peer_ids
            or self._resolve_from_peerstore(peer_id) is not None
            or self._resolve_from_dht(peer_id) is not None
            or peer_id in peer_state_addrs
        }
        return tuple(sorted(dialable_or_connected))

    async def list_connected_peer_ids(self) -> tuple[str, ...]:
        """Return peers with an active libp2p connection to this host."""
        try:
            return tuple(sorted(self._coerce_peer_id(peer_id) for peer_id in self.host.get_connected_peers()))
        except Exception:
            logger.debug("Failed to enumerate connected peer ids", exc_info=True)
            return ()

    async def _handle_incoming_stream(self, stream: INetStream) -> None:
        """Read one framed request, serve it, and write one framed response."""
        from_peer = self._coerce_peer_id(stream.muxed_conn.peer_id)

        try:
            request_bytes = await self._read_frame(stream)
            if self._request_handler is None:
                raise RuntimeError("No DAG sync request handler configured")

            response = self._request_handler(from_peer, request_bytes)
            if inspect.isawaitable(response):
                response = await response
            response_bytes = response.encode("utf-8") if isinstance(response, str) else response

            await self._write_frame(stream, response_bytes)
        except Exception:
            logger.exception("Error handling DAG sync stream from %s", from_peer)
        finally:
            try:
                await stream.close()
            except Exception:
                logger.debug("Failed to close inbound DAG sync stream from %s", from_peer, exc_info=True)

    def _resolve_from_peerstore(self, peer_id: str) -> Multiaddr | None:
        try:
            peerstore = self.host.get_peerstore()
            try:
                peer_id_obj = ID.from_string(peer_id)
                destination = self._first_addr_from_peer_info(peerstore.peer_info(peer_id_obj), peer_id=peer_id)
                if destination is not None:
                    return destination
            except Exception:
                logger.debug("Direct peerstore lookup failed for peer %s", peer_id, exc_info=True)
            for known_peer_id in peerstore.peer_ids():
                if self._coerce_peer_id(known_peer_id) != peer_id:
                    continue
                destination = self._first_addr_from_peer_info(peerstore.peer_info(known_peer_id), peer_id=peer_id)
                if destination is not None:
                    return destination
        except Exception:
            logger.debug("Failed to resolve peer %s from peerstore", peer_id, exc_info=True)
        return None

    def _resolve_from_dht(self, peer_id: str) -> Multiaddr | None:
        if self.dht is None:
            return None

        try:
            peer_id_obj = ID.from_string(peer_id)
        except Exception:
            logger.debug("Failed to parse peer id %s while resolving from DHT", peer_id, exc_info=True)
            return None

        try:
            routing_table = getattr(self.dht, "routing_table", None)
            get_peer_info = None if routing_table is None else getattr(routing_table, "get_peer_info", None)
            if callable(get_peer_info):
                destination = self._first_addr_from_peer_info(get_peer_info(peer_id_obj), peer_id=peer_id)
                if destination is not None:
                    return destination
        except Exception:
            logger.debug("Failed to resolve peer %s from DHT routing table", peer_id, exc_info=True)

        return None

    def _list_dht_peer_ids(self) -> tuple[str, ...]:
        if self.dht is None:
            return ()

        try:
            routing_table = getattr(self.dht, "routing_table", None)
            get_peer_ids = None if routing_table is None else getattr(routing_table, "get_peer_ids", None)
            if not callable(get_peer_ids):
                return ()
            return tuple(sorted(self._coerce_peer_id(peer_id) for peer_id in get_peer_ids()))
        except Exception:
            logger.debug("Failed to enumerate DHT routing-table peer ids", exc_info=True)
            return ()

    async def _peer_state_addrs(self) -> dict[str, Multiaddr]:
        try:
            states = self.db.get_all_under_key(PEER_STATE_TOPIC)
        except Exception:
            logger.debug("Failed to list peer-state snapshots from DB", exc_info=True)
            return {}

        addrs: dict[str, Multiaddr] = {}
        for peer_id, state in states.items():
            if not isinstance(peer_id, str) or not peer_id:
                continue
            resolved = self._resolve_from_cached_state_record(peer_id, state, require_online=True)
            if resolved is not None:
                addrs[peer_id] = resolved

        return addrs

    def _resolve_from_cached_peer_state(self, peer_id: str, *, require_online: bool) -> Multiaddr | None:
        try:
            state = self.db.get_nested(PEER_STATE_TOPIC, peer_id)
        except Exception:
            logger.debug("Failed to read cached peer-state snapshot for peer %s", peer_id, exc_info=True)
            return None
        return self._resolve_from_cached_state_record(peer_id, state, require_online=require_online)

    def _resolve_from_cached_state_record(
        self,
        peer_id: str,
        state: object,
        *,
        require_online: bool,
    ) -> Multiaddr | None:
        if not isinstance(state, dict):
            return None
        if require_online and not self._is_online_state(state.get("state")):
            return None

        raw_multiaddr = state.get("multiaddr")
        if raw_multiaddr in (None, ""):
            return None

        try:
            return self._normalize_dial_addr(raw_multiaddr, peer_id=peer_id)
        except Exception:
            logger.warning("Ignoring invalid cached peer-state multiaddr for peer %s: %r", peer_id, raw_multiaddr)
            return None

    def _first_addr_from_peer_info(self, peer_info: object, *, peer_id: str) -> Multiaddr | None:
        if peer_info is None:
            return None
        addrs = sorted(getattr(peer_info, "addrs", ()) or (), key=str)
        for addr in addrs:
            return self._normalize_dial_addr(addr, peer_id=peer_id)
        return None

    def _is_online_state(self, state: object) -> bool:
        if state is ServerState.ONLINE:
            return True
        if isinstance(state, int):
            return state == ServerState.ONLINE.value
        if isinstance(state, str):
            normalized = state.strip().lower()
            return normalized in {ServerState.ONLINE.name.lower(), str(ServerState.ONLINE.value)}
        return False

    def _normalize_dial_addr(self, destination: Multiaddr | str, *, peer_id: str | None) -> Multiaddr:
        addr = str(destination).rstrip("/")
        if "/ipfs/" in addr:
            addr = addr.replace("/ipfs/", "/p2p/")
        if "/p2p/" not in addr:
            if peer_id is None:
                raise ValueError(f"Destination {addr!r} is missing a /p2p/<peer_id> component")
            addr = f"{addr}/p2p/{peer_id}"
        return Multiaddr(addr)

    async def _read_frame(self, stream: INetStream) -> bytes:
        length_prefix = b""
        while True:
            byte = await stream.read(1)
            if not byte:
                raise EOFError("Stream closed while reading message length")
            length_prefix += byte
            if byte[0] & 0x80 == 0:
                break

        msg_length = varint.decode_bytes(length_prefix)
        if msg_length > MAX_READ_LEN:
            raise ValueError(f"Frame length {msg_length} exceeds maximum {MAX_READ_LEN}")
        return await self._read_exact(stream, msg_length)

    async def _read_exact(self, stream: INetStream, size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining > 0:
            chunk = await stream.read(remaining)
            if not chunk:
                raise EOFError("Stream closed before full frame was received")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    async def _call_connected_peer(self, peer_id: str, payload: bytes) -> bytes:
        """Open a sync stream to an already-connected peer when no dial addr is cached."""
        stream = None
        peer_id_obj = ID.from_string(peer_id)

        try:
            stream = await self.host.new_stream(peer_id_obj, [PROTOCOL_ID])
            await self._write_frame(stream, payload)
            response = await self._read_frame(stream)

            return response
        except Exception:
            logger.exception("Failed DAG sync request to already-connected peer %s", peer_id)
            raise
        finally:
            if stream is not None:
                try:
                    await stream.close()
                except Exception:
                    logger.debug("Failed to close DAG sync stream to %s", peer_id, exc_info=True)

    async def _has_live_connection(self, peer_id: str) -> bool:
        try:
            network = self.host.get_network()
            connections = network.get_connections(ID.from_string(peer_id))
            return bool(connections)
        except Exception:
            logger.debug("Failed to inspect live connections for peer %s", peer_id, exc_info=True)
            return False

    async def _write_frame(self, stream: INetStream, payload: bytes) -> None:
        await stream.write(varint.encode(len(payload)))
        for offset in range(0, len(payload), MAX_STREAM_WRITE_LEN):
            await stream.write(payload[offset : offset + MAX_STREAM_WRITE_LEN])


class SyncProtocolPeerRequestClient(PeerRequestClient):
    """Typed DAG sync request client backed by `MerkleDagSyncProtocol`."""

    def __init__(
        self,
        protocol: MerkleDagSyncProtocol,
        codec: DagSyncMessageCodec | None = None,
        *,
        fallback_peer_ids: PeerListCallable | None = None,
    ):
        self._protocol = protocol
        self._codec = codec or DagSyncMessageCodec()
        self._fallback_peer_ids = fallback_peer_ids

    async def request(
        self,
        peer_id: str,
        message: DagInventoryRequest | DagFetchRequest,
    ) -> DagInventoryResponse | DagFetchResponse:
        """Encode a request, send it over libp2p, and decode the typed response."""
        response = await self._send_request(peer_id, message)
        if isinstance(message, DagFetchRequest) and isinstance(response, DagFetchResponse):
            return await self._fetch_missing_from_fallback_peers(peer_id, message, response)
        return response

    async def _send_request(
        self,
        peer_id: str,
        message: DagInventoryRequest | DagFetchRequest,
    ) -> DagInventoryResponse | DagFetchResponse:
        raw_request = self._codec.encode(message)
        raw_response = await self._protocol.request_bytes(peer_id, raw_request)
        decoded = self._codec.decode(raw_response)
        if isinstance(decoded, DagAnnouncement):
            raise TypeError("Unexpected gossip announcement on request-response channel")
        if not isinstance(decoded, DagInventoryResponse | DagFetchResponse):
            raise TypeError(f"Unexpected DAG sync response type: {type(decoded)!r}")
        return decoded

    async def _fetch_missing_from_fallback_peers(
        self,
        primary_peer_id: str,
        request: DagFetchRequest,
        response: DagFetchResponse,
    ) -> DagFetchResponse:
        if not response.not_found:
            return response

        ordered_nodes = {snapshot.header.node_id: snapshot for snapshot in response.nodes}
        unresolved = tuple(sorted(set(response.not_found)))

        for fallback_peer_id in await self._list_fallback_peers(primary_peer_id):
            if not unresolved:
                break

            try:
                fallback_response = await self._send_request(
                    fallback_peer_id,
                    DagFetchRequest(
                        message_id=request.message_id,
                        namespace=request.namespace,
                        peer_id=request.peer_id,
                        node_ids=unresolved,
                        include_bodies=request.include_bodies,
                        max_ancestor_depth=request.max_ancestor_depth,
                        created_at_ms=request.created_at_ms,
                    ),
                )
            except Exception:
                logger.warning(
                    "Fallback DAG fetch for %s via %s failed",
                    unresolved,
                    fallback_peer_id,
                    exc_info=True,
                )
                continue

            if not isinstance(fallback_response, DagFetchResponse):
                raise TypeError(f"Expected DagFetchResponse, got {type(fallback_response)!r}")

            for snapshot in fallback_response.nodes:
                ordered_nodes.setdefault(snapshot.header.node_id, snapshot)
            unresolved = tuple(sorted(set(fallback_response.not_found)))

        return DagFetchResponse(
            message_id=response.message_id,
            namespace=response.namespace,
            peer_id=response.peer_id,
            nodes=tuple(ordered_nodes.values()),
            not_found=unresolved,
            created_at_ms=response.created_at_ms,
        )

    async def _list_fallback_peers(self, primary_peer_id: str) -> tuple[str, ...]:
        if self._fallback_peer_ids is None:
            peers = await self._protocol.list_known_peer_ids()
        else:
            peers = self._fallback_peer_ids()
            if inspect.isawaitable(peers):
                peers = await peers
            peers = tuple(str(peer_id) for peer_id in peers)

        local_peer_id = None
        try:
            local_peer_id = self._protocol._coerce_peer_id(self._protocol.host.get_id())
        except Exception:
            logger.debug("Failed to determine local peer id for fallback filtering", exc_info=True)

        excluded = {primary_peer_id}
        if local_peer_id is not None:
            excluded.add(local_peer_id)
        return tuple(peer_id for peer_id in peers if peer_id not in excluded)


class PeerStateDagPeerSetProvider(PeerSetProvider):
    """Peer provider backed by the locally cached peer-state DB snapshot."""

    def __init__(self, protocol: MerkleDagSyncProtocol):
        self._protocol = protocol

    async def list_peer_ids(self) -> tuple[str, ...]:
        """Return currently known peer ids from peer-state DAG metadata."""
        return await self._protocol.list_known_peer_ids()

    async def list_connected_peer_ids(self) -> tuple[str, ...]:
        """Return peers that are currently connected to the local host."""
        return await self._protocol.list_connected_peer_ids()
