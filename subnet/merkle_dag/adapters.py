"""Small callable-based adapters for integrating the sync coordinator."""

from __future__ import annotations

import inspect

from subnet.merkle_dag.interfaces import (
    GossipPublisher,
    PeerListCallable,
    PeerRequestCallable,
    PeerRequestClient,
    PeerSetProvider,
    SyncRequest,
    SyncResponse,
)


class CallableGossipPublisher(GossipPublisher):
    """Wraps an async callable as a `GossipPublisher`."""

    def __init__(self, publish_callable):
        self._publish_callable = publish_callable

    async def publish(self, topic: str, payload: bytes) -> None:
        """Publish raw bytes through the wrapped callable."""
        result = self._publish_callable(topic, payload)
        if inspect.isawaitable(result):
            await result


class CallablePeerRequestClient(PeerRequestClient):
    """Wraps an async callable as a typed request client."""

    def __init__(self, request_callable: PeerRequestCallable):
        self._request_callable = request_callable

    async def request(self, peer_id: str, message: SyncRequest) -> SyncResponse:
        """Dispatch the request through the wrapped callable."""
        return await self._request_callable(peer_id, message)


class CallablePeerSetProvider(PeerSetProvider):
    """Wraps a sync or async callable that returns peer ids."""

    def __init__(self, peers_callable: PeerListCallable):
        self._peers_callable = peers_callable

    async def list_peer_ids(self) -> tuple[str, ...]:
        """Return the current peer id set."""
        result = self._peers_callable()
        if inspect.isawaitable(result):
            peers = await result
        else:
            peers = result
        return tuple(sorted(str(peer_id) for peer_id in peers))
