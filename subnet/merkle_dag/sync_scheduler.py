"""Event-driven scheduler for fetching missing Merkle DAG nodes."""

from __future__ import annotations

from collections.abc import Sequence
import logging
from typing import TYPE_CHECKING

import trio

from subnet.merkle_dag.interfaces import PeerSetProvider
from subnet.telemetry.telemetry import Telemetry

if TYPE_CHECKING:
    from subnet.merkle_dag.runtime import MerkleDagRuntime

logger = logging.getLogger(__name__)


class SyncScheduler:
    """
    Batch orphan-driven DAG sync requests outside the gossip receive path.

    Gossip receivers can notify this scheduler when they store an orphan. The
    scheduler then wakes up, inspects the DAG storage's orphan index, batches
    any still-missing parent ids, and asks the sync coordinator to fetch them.

    This keeps the receive loop fast:

    - gossip ingestion still checks for missing parents immediately
    - outbound sync is moved to a dedicated background task
    - multiple orphan notifications can be coalesced into one fetch pass

    The scheduler is event-driven. It does not poll peers continuously. It only
    reaches out when:

    - startup finds unresolved orphans already in storage, or
    - a caller explicitly schedules a sync pass after detecting missing data
    """

    def __init__(
        self,
        runtime: MerkleDagRuntime,
        termination_event: trio.Event,
        *,
        peer_provider: PeerSetProvider | None = None,
        telemetry: Telemetry | None = None,
        batch_window: float = 0.05,
        sync_on_startup: bool = True,
        log_level: int = logging.INFO,
    ):
        self.runtime = runtime
        self.termination_event = termination_event
        self.peer_provider = peer_provider
        self.telemetry = telemetry
        self.batch_window = batch_window
        self.sync_on_startup = sync_on_startup
        self.log_level = log_level

        self._condition = trio.Condition()
        self._signal_count = 0
        self._preferred_peers: set[str] = set()

    async def schedule(self, peer_id: str | None = None) -> None:
        """Queue one sync pass and optionally prefer a specific peer first."""
        async with self._condition:
            if peer_id and peer_id != self.runtime.local_peer_id:
                self._preferred_peers.add(peer_id)
            self._signal_count += 1
            self._condition.notify_all()

    async def run(self) -> None:
        """Wait for orphan notifications and fetch missing nodes in batches."""
        if self.sync_on_startup:
            await self.schedule()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._notify_on_termination)

            observed_signals = 0
            while not self.termination_event.is_set():
                observed_signals, preferred_peers = await self._wait_for_signal(observed_signals)
                if self.termination_event.is_set():
                    break

                if self.batch_window > 0:
                    await trio.sleep(self.batch_window)
                    observed_signals, preferred_peers = await self._drain_signals(
                        observed_signals,
                        preferred_peers,
                    )

                await self.sync_once(preferred_peers)

            nursery.cancel_scope.cancel()

    async def sync_once(self, preferred_peer_ids: Sequence[str] = ()) -> None:
        """Inspect current orphan state and fetch any still-missing parent nodes."""
        missing_node_ids = await self._missing_node_ids()
        if not missing_node_ids:
            return

        candidate_peers = await self._candidate_peer_ids(preferred_peer_ids)
        if not candidate_peers:
            logger.log(
                self.log_level,
                "Missing DAG nodes remain unresolved because no candidate peers are available: %s",
                missing_node_ids,
            )
            return

        for peer_id in candidate_peers:
            if not missing_node_ids:
                break

            try:
                logger.log(
                    self.log_level,
                    "Attempting DAG missing-node sync from peer %s for %s",
                    peer_id,
                    missing_node_ids,
                )
                await self.runtime.coordinator.fetch_missing(peer_id, missing_node_ids)
            except Exception:
                logger.exception("Failed DAG missing-node sync from peer %s", peer_id)
                # if self.telemetry:
                #     await self.telemetry.emit_async(
                #         "dag_missing_node_sync_failed",
                #         peer_id=peer_id,
                #         missing_node_ids=list(missing_node_ids),
                #     )

            missing_node_ids = await self._missing_node_ids()

        # if missing_node_ids and self.telemetry:
        #     await self.telemetry.emit_async(
        #         "dag_missing_node_sync_pending",
        #         missing_node_ids=list(missing_node_ids),
        #     )

    async def _notify_on_termination(self) -> None:
        await self.termination_event.wait()
        async with self._condition:
            self._condition.notify_all()

    async def _wait_for_signal(self, observed_signals: int) -> tuple[int, tuple[str, ...]]:
        async with self._condition:
            while self._signal_count == observed_signals and not self.termination_event.is_set():
                await self._condition.wait()
            return self._consume_pending_locked()

    async def _drain_signals(
        self,
        observed_signals: int,
        preferred_peer_ids: Sequence[str],
    ) -> tuple[int, tuple[str, ...]]:
        async with self._condition:
            if self._signal_count == observed_signals:
                return observed_signals, tuple(sorted(set(preferred_peer_ids)))

            merged_peers = set(preferred_peer_ids)
            merged_peers.update(self._preferred_peers)
            self._preferred_peers.clear()
            return self._signal_count, tuple(sorted(merged_peers))

    def _consume_pending_locked(self) -> tuple[int, tuple[str, ...]]:
        observed_signals = self._signal_count
        preferred_peers = tuple(sorted(self._preferred_peers))
        self._preferred_peers.clear()
        return observed_signals, preferred_peers

    async def _candidate_peer_ids(self, preferred_peer_ids: Sequence[str]) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()

        for peer_id in preferred_peer_ids:
            if peer_id == self.runtime.local_peer_id or peer_id in seen:
                continue
            seen.add(peer_id)
            ordered.append(peer_id)

        if self.peer_provider is not None:
            for peer_id in await self.peer_provider.list_peer_ids():
                if peer_id == self.runtime.local_peer_id or peer_id in seen:
                    continue
                seen.add(peer_id)
                ordered.append(peer_id)

        return tuple(ordered)

    async def _missing_node_ids(self) -> tuple[str, ...]:
        storage = self.runtime.dag.storage
        missing_node_ids: set[str] = set()

        for orphan in await storage.list_orphans():
            for node_id in orphan.missing_parents:
                if await storage.has_header(node_id) and await storage.has_body(node_id):
                    continue
                missing_node_ids.add(node_id)

        return tuple(sorted(missing_node_ids))
