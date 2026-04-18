"""Networking adapters and peer-id helpers for DAG sync integration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import inspect

from libp2p.abc import IHost

from subnet.merkle_dag import (
    DagAnnouncement,
    DagFetchRequest,
    DagFetchResponse,
    DagInventoryRequest,
    DagInventoryResponse,
    DagSyncMessageCodec,
)
from subnet.merkle_dag.interfaces import PeerRequestClient, PeerSetProvider


class BytesPeerRequestClientAdapter(PeerRequestClient):
    """Bridge typed DAG sync requests onto an existing bytes request-response API."""

    def __init__(
        self,
        request_bytes: Callable[[str, bytes], Awaitable[bytes] | bytes],
        codec: DagSyncMessageCodec,
    ):
        self._request_bytes = request_bytes
        self._codec = codec

    async def request(
        self,
        peer_id: str,
        message: DagInventoryRequest | DagFetchRequest,
    ) -> DagInventoryResponse | DagFetchResponse:
        """Encode a request, send it, and decode the typed response."""
        raw_request = self._codec.encode(message)
        raw_response = self._request_bytes(peer_id, raw_request)
        if inspect.isawaitable(raw_response):
            raw_response = await raw_response
        decoded = self._codec.decode(raw_response)
        if isinstance(decoded, DagAnnouncement):
            raise TypeError("Unexpected gossip announcement on request-response channel")
        return decoded


class HostPeerSetProvider(PeerSetProvider):
    """List peers from an existing libp2p host peerstore."""

    def __init__(self, host: IHost):
        self._host = host

    async def list_peer_ids(self) -> tuple[str, ...]:
        """Return peer ids currently known to the host peerstore."""
        return tuple(sorted(peer_id.to_string() for peer_id in self._host.get_peerstore().peer_ids()))
