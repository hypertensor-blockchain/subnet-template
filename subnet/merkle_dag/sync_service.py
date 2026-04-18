"""
Lifecycle wrapper around request handling and anti-entropy for one DAG runtime.

This service does not implement DAG reconciliation itself. The real sync logic
lives in `runtime.coordinator`. This class exists to own the long-running
background loop, expose a small bytes-based handler for the libp2p
request-response transport, and keep the server/bootstrap layer thin.

There are two distinct modes of operation:

1. passive / request-driven:
   remote peers ask us for inventory or node data, and
   `handle_sync_request_bytes()` decodes the request, delegates to the
   coordinator, and returns encoded response bytes.
2. optional active / periodic anti-entropy:
   `run()` can start a background loop that wakes up on a timer and calls
   `reconcile_once()` against discovered peers. This is how a node can recover
   missed announcements, catch up after restart, or repair divergence after a
   partition.

In the current server wiring, the service is primarily request-driven. Actual
fetches happen when reconciliation determines that local state is behind or
missing data. Orphan-triggered missing-parent recovery is typically delegated
to `SyncScheduler`, while periodic anti-entropy remains available but disabled
by default.
"""

from __future__ import annotations

from collections.abc import Sequence
import inspect
import logging
import time

import trio

from subnet.merkle_dag import DagAnnouncement, DagFetchRequest, DagInventoryRequest
from subnet.merkle_dag.interfaces import PeerSetProvider
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.telemetry.telemetry import Telemetry

logger = logging.getLogger(__name__)


class MerkleDagSyncService:
    """
    Own sync request handling and the anti-entropy loop for one DAG runtime.

    The server creates one instance per `MerkleDagRuntime` and keeps it alive as
    part of normal process startup. The service is intentionally small:

    - `handle_sync_request_bytes()` serves inbound inventory/fetch requests.
    - `reconcile_once()` performs one outbound comparison pass against peers.
    - `run()` can repeatedly call `reconcile_once()` on a timer until shutdown.

    Important behavior:

    - By default, this service is request-driven and does not poll peers.
    - Periodic reconciliation is optional and disabled unless explicitly enabled.
    - Real node/body transfers only happen when the coordinator detects unknown
      heads, missing ancestors, or other summary differences that require
      convergence.
    - If `peer_provider` is `None`, `run()` exits immediately and the service
      becomes request-only.
    """

    def __init__(
        self,
        runtime: MerkleDagRuntime,
        termination_event: trio.Event,
        *,
        peer_provider: PeerSetProvider | None = None,
        telemetry: Telemetry | None = None,
        enable_periodic_reconciliation: bool = False,
        reconciliation_interval: float = 30.0,
        log_level: int = logging.INFO,
    ):
        self.runtime = runtime
        self.termination_event = termination_event
        self.peer_provider = peer_provider
        self.telemetry = telemetry
        self.enable_periodic_reconciliation = enable_periodic_reconciliation
        self.reconciliation_interval = reconciliation_interval
        self.log_level = log_level

    async def handle_announcement(self, announcement: DagAnnouncement, *, source_peer: str) -> bool:
        """
        Process one gossip announcement from a peer.

        This is the event-driven path, separate from the periodic loop. A
        received announcement can cause the coordinator to reconcile
        immediately if the remote peer appears to have unknown heads or a
        larger node count. If the announcement reveals no new information, this
        method can return after lightweight bookkeeping only.
        """
        processed = await self.runtime.coordinator.handle_announcement(announcement, source_peer=source_peer)
        # if processed and self.telemetry:
        #     await self.telemetry.emit_async(
        #         "dag_announcement_received",
        #         peer_id=source_peer,
        #         head_ids=list(announcement.head_ids),
        #         node_count=announcement.node_count,
        #     )
        return processed

    async def reconcile_once(self, peer_ids: Sequence[str] | None = None) -> None:
        """
        Perform one anti-entropy pass against supplied or discovered peers.

        This method asks the coordinator to compare local state against each
        target peer. The coordinator first performs an inventory/summary
        exchange and only issues fetch requests when it finds missing heads,
        ancestors, or otherwise incomplete local data.

        So this method always performs a reconciliation attempt, but it does
        not guarantee that any node transfer will occur.
        """
        peers = tuple(peer_ids) if peer_ids is not None else await self._list_peers()
        for peer_id in peers:
            if peer_id == self.runtime.local_peer_id:
                continue
            try:
                await self.runtime.coordinator.reconcile_with_peer(peer_id)
            except Exception:
                logger.exception("DAG reconciliation failed for peer %s", peer_id)
                # if self.telemetry:
                #     await self.telemetry.emit_async("dag_reconcile_failed", peer_id=peer_id)

    async def bootstrap_join_sync(
        self,
        *,
        min_peer_count: int = 2,
        wait_timeout: float = 30.0,
        poll_interval: float = 1.0,
        settle_time: float = 3.0,
    ) -> tuple[str, ...]:
        """
        Wait for peers to connect, then perform one startup reconciliation pass.

        This is the join/bootstrap path for a node that starts with an empty or
        stale local DAG. Unlike orphan-driven repair, this does not wait for a
        missing node to be observed first. The method:

        - waits until at least `min_peer_count` peers are connected, or until
          `wait_timeout` elapses,
        - gives peer discovery a short `settle_time` window to populate
          peerstore/routing information,
        - then keeps reconciling against the connected and otherwise known
          peers until local storage contains the advertised remote heads and
          their closure.

        It returns the peer ids that were targeted for the startup reconcile.
        """
        if self.peer_provider is None:
            return ()

        deadline = time.monotonic() + max(wait_timeout, 0.0)
        connected_peers = await self._wait_for_connected_peers(
            min_peer_count=min_peer_count,
            wait_timeout=max(deadline - time.monotonic(), 0.0),
            poll_interval=poll_interval,
        )
        if not connected_peers:
            return ()

        if settle_time > 0 and not self.termination_event.is_set():
            await trio.sleep(settle_time)

        attempted_peers: set[str] = set()
        last_converged_peer_set: tuple[str, ...] | None = None

        while not self.termination_event.is_set():
            startup_peers = await self._list_startup_peers(await self._list_connected_peers())
            converged = bool(startup_peers) and await self._startup_sync_converged(startup_peers)

            if startup_peers and not converged:
                await self.reconcile_once(startup_peers)
                attempted_peers.update(startup_peers)
                converged = await self._startup_sync_converged(startup_peers)

            if startup_peers and converged:
                if last_converged_peer_set == startup_peers:
                    return tuple(sorted(attempted_peers))
                last_converged_peer_set = startup_peers
            else:
                last_converged_peer_set = None

            await trio.sleep(max(poll_interval, 0.0))

        return tuple(sorted(attempted_peers))

    async def handle_sync_request_bytes(self, from_peer: str, payload: bytes) -> bytes:
        """
        Handle one direct sync request on the request-response channel.

        The transport layer passes raw bytes here after a remote peer opens a
        sync stream. This method:

        - decodes the request,
        - verifies that it is a supported inventory or fetch message,
        - delegates response construction to the coordinator,
        - and re-encodes the typed response for the transport.

        This path is only active when another peer explicitly asks us for DAG
        state; it does not run on a timer.
        """
        message = self.runtime.codec.decode(payload)
        if not isinstance(message, DagInventoryRequest | DagFetchRequest):
            raise TypeError(f"Unexpected sync request type: {type(message)!r}")
        response = await self.runtime.coordinator.handle_request(from_peer, message)
        # if self.telemetry:
        #     await self.telemetry.emit_async(
        #         "dag_sync_request_served",
        #         peer_id=from_peer,
        #         request_type=type(message).__name__,
        #     )
        return self.runtime.codec.encode(response)

    async def run(self) -> None:
        """Optionally start the periodic anti-entropy loop for this runtime."""
        if not self.enable_periodic_reconciliation or self.peer_provider is None:
            return
        await self._anti_entropy_loop()

    async def _anti_entropy_loop(self) -> None:
        """Sleep, reconcile, and repeat until shutdown."""
        while not self.termination_event.is_set():
            try:
                await trio.sleep(self.reconciliation_interval)
                await self.reconcile_once()
            except Exception:
                logger.exception("Error in DAG anti-entropy loop")
                await trio.sleep(1)

    async def _list_peers(self) -> tuple[str, ...]:
        if self.peer_provider is None:
            return ()
        return await self.peer_provider.list_peer_ids()

    async def _list_connected_peers(self) -> tuple[str, ...]:
        if self.peer_provider is None:
            return ()

        list_connected = getattr(self.peer_provider, "list_connected_peer_ids", None)
        if callable(list_connected):
            connected = list_connected()
            if inspect.isawaitable(connected):
                connected = await connected
            return tuple(str(peer_id) for peer_id in connected)

        return await self._list_peers()

    async def _wait_for_connected_peers(
        self,
        *,
        min_peer_count: int,
        wait_timeout: float,
        poll_interval: float,
    ) -> tuple[str, ...]:
        if min_peer_count <= 0:
            return await self._list_connected_peers()

        deadline = time.monotonic() + max(wait_timeout, 0.0)
        connected = await self._list_connected_peers()

        while len(connected) < min_peer_count and not self.termination_event.is_set():
            if time.monotonic() >= deadline:
                break
            await trio.sleep(max(poll_interval, 0.0))
            connected = await self._list_connected_peers()

        return connected

    async def _list_startup_peers(self, connected_peers: Sequence[str]) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()

        for peer_id in connected_peers:
            if peer_id == self.runtime.local_peer_id or peer_id in seen:
                continue
            seen.add(peer_id)
            ordered.append(peer_id)

        for peer_id in await self._list_peers():
            if peer_id == self.runtime.local_peer_id or peer_id in seen:
                continue
            seen.add(peer_id)
            ordered.append(peer_id)

        return tuple(ordered)

    async def _local_complete_node_count(self) -> int | None:
        dag = getattr(self.runtime, "dag", None)
        storage = None if dag is None else getattr(dag, "storage", None)
        count_complete_nodes = None if storage is None else getattr(storage, "count_complete_nodes", None)
        if not callable(count_complete_nodes):
            return None

        count = count_complete_nodes()
        if inspect.isawaitable(count):
            count = await count
        if count is None:
            return None
        return int(count)

    async def _startup_sync_converged(self, peer_ids: Sequence[str]) -> bool:
        dag = getattr(self.runtime, "dag", None)
        storage = None if dag is None else getattr(dag, "storage", None)
        if storage is None or not peer_ids:
            return False

        orphan_count = await self._storage_int_call(storage, "count_orphans")
        if orphan_count is not None and orphan_count > 0:
            return False

        local_node_count = await self._local_complete_node_count()

        for peer_id in peer_ids:
            peer_state = await self._storage_call(storage, "get_peer_state", peer_id)
            if peer_state is None:
                return False

            summary = getattr(peer_state, "summary", None)
            if summary is None:
                return False

            remote_node_count = getattr(summary, "node_count", None)
            if local_node_count is not None and remote_node_count is not None:
                if local_node_count < int(remote_node_count):
                    return False

            for head_id in getattr(summary, "head_ids", ()) or ():
                if not await self._storage_has_complete_node(storage, str(head_id)):
                    return False

        return True

    async def _storage_has_complete_node(self, storage: object, node_id: str) -> bool:
        has_header = await self._storage_bool_call(storage, "has_header", node_id)
        has_body = await self._storage_bool_call(storage, "has_body", node_id)
        return has_header and has_body

    async def _storage_call(self, storage: object, method_name: str, *args):
        method = getattr(storage, method_name, None)
        if not callable(method):
            return None

        result = method(*args)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _storage_bool_call(self, storage: object, method_name: str, *args) -> bool:
        result = await self._storage_call(storage, method_name, *args)
        return bool(result)

    async def _storage_int_call(self, storage: object, method_name: str, *args) -> int | None:
        result = await self._storage_call(storage, method_name, *args)
        if result is None:
            return None
        return int(result)
