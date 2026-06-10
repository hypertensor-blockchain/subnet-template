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
from subnet.utils.logging_config import configure_logging

# Configure logging
configure_logging()
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
        pubsub: Any | None = None,
        gossipsub: Any | None = None,
        telemetry: Telemetry | None = None,
        request_handler: SyncRequestHandler | None = None,
    ):
        """
        Initialize the MerkleDagSyncProtocol.

        Args:
            host: The libp2p host instance.
            db: The RocksDB instance backing the shared DAG state.
            dht: Optional Kademlia DHT used to inspect the routing table.
            pubsub: Optional libp2p Pubsub service used to inspect gossip peers.
            gossipsub: Optional GossipSub router used to inspect mesh/fanout peers.
            telemetry: Optional telemetry sink.
            request_handler: Optional bytes handler for inbound sync requests.

        """
        self.host = host
        self.db = db
        self.dht = dht
        self.pubsub = pubsub
        self.gossipsub = gossipsub
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
        """Resolve a peer through libp2p state and issue a raw sync request."""
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

        destination = await self._resolve_from_dht(peer_id)
        if destination is not None:
            return destination

        return self._resolve_from_gossip(peer_id)

    async def _resolve_request_destination(self, peer_id: str) -> Multiaddr | None:
        """
        Resolve a peer destination for an explicit direct sync request.

        Direct fetches use libp2p-native address sources only. Templates built
        on this protocol should not need to publish a peer-state DAG just to
        make sync dialable.
        """
        destination = self._resolve_from_peerstore(peer_id)
        if destination is not None:
            return destination

        destination = await self._resolve_from_dht(peer_id)
        if destination is not None:
            return destination

        return self._resolve_from_gossip(peer_id)

    async def list_known_peer_ids(self) -> tuple[str, ...]:
        """Return peer ids known via libp2p host, peerstore, DHT, or gossip."""
        return await self.list_libp2p_peer_ids()

    async def list_libp2p_peer_ids(self) -> tuple[str, ...]:
        """Return peer ids found in libp2p host, DHT, and gossip state."""
        connected_peer_ids = set(await self.list_connected_peer_ids())
        peer_ids = set(connected_peer_ids)
        peer_ids.update(self._list_dht_peer_ids())
        peer_ids.update(self._list_gossip_peer_ids())
        peer_ids.update(self._list_peerstore_peer_ids(require_addrs=True))

        local_peer_id = self._local_peer_id()
        if local_peer_id is not None:
            peer_ids.discard(local_peer_id)

        reachable_peers = {
            peer_id
            for peer_id in peer_ids
            if peer_id in connected_peer_ids
            or self._resolve_from_peerstore(peer_id) is not None
            or self._resolve_from_dht_routing_table(peer_id) is not None
            or self._resolve_from_gossip(peer_id) is not None
            or peer_id in self._list_gossip_connected_peer_ids()
        }
        return tuple(sorted(reachable_peers))

    async def list_connected_peer_ids(self) -> tuple[str, ...]:
        """Return peers with an active libp2p connection to this host."""
        peer_ids: set[str] = set()
        try:
            peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in self.host.get_connected_peers())
        except Exception:
            logger.debug("Failed to enumerate connected peer ids", exc_info=True)
        try:
            get_live_peers = getattr(self.host, "get_live_peers", None)
            if callable(get_live_peers):
                peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in get_live_peers())
        except Exception:
            logger.debug("Failed to enumerate live peer ids", exc_info=True)
        peer_ids.update(self._list_network_connected_peer_ids())
        peer_ids.update(self._list_gossip_connected_peer_ids())

        local_peer_id = self._local_peer_id()
        if local_peer_id is not None:
            peer_ids.discard(local_peer_id)
        return tuple(sorted(peer_ids))

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
                destination = self._resolve_from_peerstore_entry(peerstore, peer_id_obj, peer_id=peer_id)
                if destination is not None:
                    return destination
            except Exception:
                logger.debug("Direct peerstore lookup failed for peer %s", peer_id, exc_info=True)
            for known_peer_id in peerstore.peer_ids():
                if self._coerce_peer_id(known_peer_id) != peer_id:
                    continue
                destination = self._resolve_from_peerstore_entry(peerstore, known_peer_id, peer_id=peer_id)
                if destination is not None:
                    return destination
        except Exception:
            logger.debug("Failed to resolve peer %s from peerstore", peer_id, exc_info=True)
        return None

    def _resolve_from_peerstore_entry(
        self,
        peerstore: object,
        peer_id_obj: object,
        *,
        peer_id: str,
    ) -> Multiaddr | None:
        addrs = getattr(peerstore, "addrs", None)
        if callable(addrs):
            try:
                destination = self._first_addr_from_addrs(addrs(peer_id_obj), peer_id=peer_id)
                if destination is not None:
                    return destination
            except Exception:
                logger.debug("Peerstore addrs lookup failed for peer %s", peer_id, exc_info=True)

        peer_info = getattr(peerstore, "peer_info", None)
        if callable(peer_info):
            try:
                return self._first_addr_from_peer_info(peer_info(peer_id_obj), peer_id=peer_id)
            except Exception:
                logger.debug("Peerstore peer_info lookup failed for peer %s", peer_id, exc_info=True)
        return None

    def _list_peerstore_peer_ids(self, *, require_addrs: bool) -> tuple[str, ...]:
        try:
            peerstore = self.host.get_peerstore()
        except Exception:
            logger.debug("Failed to get host peerstore", exc_info=True)
            return ()

        peer_id_objects: list[object] = []
        peers_with_addrs = getattr(peerstore, "peers_with_addrs", None)
        if require_addrs and callable(peers_with_addrs):
            try:
                peer_id_objects.extend(peers_with_addrs())
            except Exception:
                logger.debug("Failed to enumerate peerstore peers with addresses", exc_info=True)

        if not peer_id_objects:
            try:
                peer_id_objects.extend(peerstore.peer_ids())
            except Exception:
                logger.debug("Failed to enumerate peerstore peer ids", exc_info=True)
                return ()

        peer_ids: set[str] = set()
        for peer_id_obj in peer_id_objects:
            peer_id = self._coerce_peer_id(peer_id_obj)
            if require_addrs and self._resolve_from_peerstore(peer_id) is None:
                continue
            peer_ids.add(peer_id)

        return tuple(sorted(peer_ids))

    async def _resolve_from_dht(self, peer_id: str) -> Multiaddr | None:
        destination = self._resolve_from_dht_routing_table(peer_id)
        if destination is not None:
            return destination

        if self.dht is None:
            return None

        try:
            peer_id_obj = ID.from_string(peer_id)
        except Exception:
            logger.debug("Failed to parse peer id %s while resolving from DHT", peer_id, exc_info=True)
            return None

        find_peer = getattr(self.dht, "find_peer", None)
        if callable(find_peer):
            try:
                peer_info = find_peer(peer_id_obj)
                if inspect.isawaitable(peer_info):
                    peer_info = await peer_info
                destination = self._first_addr_from_peer_info(peer_info, peer_id=peer_id)
                if destination is not None:
                    return destination
            except Exception:
                logger.debug("DHT find_peer failed for peer %s", peer_id, exc_info=True)

        return None

    def _resolve_from_dht_routing_table(self, peer_id: str) -> Multiaddr | None:
        if self.dht is None:
            return None

        try:
            peer_id_obj = ID.from_string(peer_id)
        except Exception:
            logger.debug("Failed to parse peer id %s while resolving from DHT routing table", peer_id, exc_info=True)
            return None

        try:
            routing_table = getattr(self.dht, "routing_table", None)
            get_peer_info = None if routing_table is None else getattr(routing_table, "get_peer_info", None)
            if callable(get_peer_info):
                for peer_id_candidate in (peer_id_obj, peer_id):
                    destination = self._first_addr_from_peer_info(
                        get_peer_info(peer_id_candidate),
                        peer_id=peer_id,
                    )
                    if destination is not None:
                        return destination
        except Exception:
            logger.debug("Failed to resolve peer %s from DHT routing table", peer_id, exc_info=True)

        return None

    def _list_dht_peer_ids(self) -> tuple[str, ...]:
        if self.dht is None:
            return ()

        peer_ids: set[str] = set()
        try:
            routing_table = getattr(self.dht, "routing_table", None)
            get_peer_ids = None if routing_table is None else getattr(routing_table, "get_peer_ids", None)
            if callable(get_peer_ids):
                peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in get_peer_ids())

            peer_ids_method = None if routing_table is None else getattr(routing_table, "peer_ids", None)
            if callable(peer_ids_method):
                peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in peer_ids_method())

            get_peer_infos = None if routing_table is None else getattr(routing_table, "get_peer_infos", None)
            if callable(get_peer_infos):
                for peer_info in get_peer_infos():
                    peer_info_id = getattr(peer_info, "peer_id", None)
                    if peer_info_id is not None:
                        peer_ids.add(self._coerce_peer_id(peer_info_id))
        except Exception:
            logger.debug("Failed to enumerate DHT routing-table peer ids", exc_info=True)
            return ()

        return tuple(sorted(peer_ids))

    def _resolve_from_gossip(self, peer_id: str) -> Multiaddr | None:
        for source in self._iter_gossip_sources():
            direct_peers = getattr(source, "direct_peers", None)
            if not isinstance(direct_peers, dict):
                continue

            for direct_peer_id, peer_info in direct_peers.items():
                peer_info_id = getattr(peer_info, "peer_id", direct_peer_id)
                if self._coerce_peer_id(peer_info_id) != peer_id and self._coerce_peer_id(direct_peer_id) != peer_id:
                    continue
                destination = self._first_addr_from_peer_info(peer_info, peer_id=peer_id)
                if destination is not None:
                    return destination
        return None

    def _list_gossip_peer_ids(self) -> tuple[str, ...]:
        peer_ids: set[str] = set()

        for source in self._iter_gossip_sources():
            for attr in ("peers", "peer_protocol", "direct_peers"):
                mapping = getattr(source, attr, None)
                if isinstance(mapping, dict):
                    peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in mapping)
                    if attr == "direct_peers":
                        for peer_info in mapping.values():
                            peer_info_id = getattr(peer_info, "peer_id", None)
                            if peer_info_id is not None:
                                peer_ids.add(self._coerce_peer_id(peer_info_id))

            peer_topics = getattr(source, "peer_topics", None)
            if isinstance(peer_topics, dict):
                for topic_peers in peer_topics.values():
                    peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in topic_peers or ())

            for attr in ("mesh", "fanout"):
                topic_peers_by_topic = getattr(source, attr, None)
                if isinstance(topic_peers_by_topic, dict):
                    for topic_peers in topic_peers_by_topic.values():
                        peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in topic_peers or ())

        return tuple(sorted(peer_ids))

    def _list_gossip_connected_peer_ids(self) -> tuple[str, ...]:
        peer_ids: set[str] = set()
        for source in self._iter_gossip_sources():
            peers = getattr(source, "peers", None)
            if isinstance(peers, dict):
                peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in peers)
            peer_protocol = getattr(source, "peer_protocol", None)
            if isinstance(peer_protocol, dict):
                peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in peer_protocol)
        return tuple(sorted(peer_ids))

    def _list_network_connected_peer_ids(self) -> tuple[str, ...]:
        try:
            network = self.host.get_network()
        except Exception:
            logger.debug("Failed to get host network for connected peer enumeration", exc_info=True)
            return ()

        peer_ids: set[str] = set()
        try:
            get_connections_map = getattr(network, "get_connections_map", None)
            if callable(get_connections_map):
                peer_ids.update(
                    self._coerce_peer_id(peer_id)
                    for peer_id, connections in get_connections_map().items()
                    if connections
                )
        except Exception:
            logger.debug("Failed to enumerate network connections map", exc_info=True)

        try:
            connections = getattr(network, "connections", None)
            if isinstance(connections, dict):
                peer_ids.update(
                    self._coerce_peer_id(peer_id)
                    for peer_id, peer_connections in connections.items()
                    if peer_connections
                )
        except Exception:
            logger.debug("Failed to enumerate network connections", exc_info=True)

        try:
            legacy_connections = getattr(network, "connections_legacy", None)
            if isinstance(legacy_connections, dict):
                peer_ids.update(self._coerce_peer_id(peer_id) for peer_id in legacy_connections)
        except Exception:
            logger.debug("Failed to enumerate legacy network connections", exc_info=True)

        return tuple(sorted(peer_ids))

    def _iter_gossip_sources(self) -> tuple[object, ...]:
        sources: list[object] = []
        seen: set[int] = set()
        queue: list[object | None] = [self.pubsub, self.gossipsub]

        while queue:
            source = queue.pop(0)
            if source is None or id(source) in seen:
                continue

            seen.add(id(source))
            sources.append(source)
            queue.append(getattr(source, "pubsub", None))
            queue.append(getattr(source, "router", None))

        return tuple(sources)

    def _first_addr_from_peer_info(self, peer_info: object, *, peer_id: str) -> Multiaddr | None:
        if peer_info is None:
            return None
        return self._first_addr_from_addrs(getattr(peer_info, "addrs", ()) or (), peer_id=peer_id)

    def _first_addr_from_addrs(self, addrs: Iterable[Multiaddr | str], *, peer_id: str) -> Multiaddr | None:
        addrs = sorted(addrs or (), key=str)
        for addr in addrs:
            return self._normalize_dial_addr(addr, peer_id=peer_id)
        return None

    def _local_peer_id(self) -> str | None:
        try:
            return self._coerce_peer_id(self.host.get_id())
        except Exception:
            logger.debug("Failed to determine local peer id", exc_info=True)
            return None

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
        if peer_id in set(await self.list_connected_peer_ids()):
            return True

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


class DagPeerSetProvider(PeerSetProvider):
    """Peer provider backed by libp2p host, DHT, peerstore, and gossip state."""

    def __init__(self, protocol: MerkleDagSyncProtocol):
        self._protocol = protocol

    async def list_peer_ids(self) -> tuple[str, ...]:
        """Return currently known libp2p peer ids."""
        return await self._protocol.list_known_peer_ids()

    async def list_connected_peer_ids(self) -> tuple[str, ...]:
        """Return peers that are currently connected to the local host."""
        return await self._protocol.list_connected_peer_ids()
