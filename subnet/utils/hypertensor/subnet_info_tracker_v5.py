from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
import logging
import time
from typing import Any, Dict, Optional

from libp2p.peer.id import ID as PeerID
import trio

from subnet.hypertensor.chain_data import AllSubnetBootnodes, OverwatchNodeInfo, SubnetNodeInfo
from subnet.hypertensor.chain_functions import (
    EpochData,
    Hypertensor,
    SubnetNodeClass,
    subnet_node_class_to_enum,
)
from subnet.hypertensor.config import BLOCK_SECS
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger("subnet-info-tracker-v5")


@dataclass
class _RefreshResult:
    slot: int | None = None
    epoch_length: int | None = None
    epoch_data: EpochData | None = None
    nodes: list[SubnetNodeInfo] | None = None
    overwatch_nodes: list[OverwatchNodeInfo] | None = None
    bootnodes: AllSubnetBootnodes | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


class SubnetInfoTracker:
    """
    Tracks subnet chain data without blocking the Trio loop.

    The tracker keeps epoch data, subnet nodes, overwatch nodes, and bootnodes
    refreshed in the background. It performs one scheduled refresh per epoch;
    callers can still trigger an immediate refresh when they know the cache
    needs updating.
    """

    def __init__(
        self,
        termination_event: trio.Event,
        subnet_id: int,
        subnet_slot: int,
        hypertensor: Hypertensor | LocalMockHypertensor | None = None,
        updates_per_epoch: int = 1,
        start_fresh_epoch: bool = True,
        poll_interval: float = BLOCK_SECS,
        history_epochs: int = 3,
    ) -> None:
        if hypertensor is None:
            raise ValueError("SubnetInfoTracker requires hypertensor")

        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.start_fresh_epoch = start_fresh_epoch
        self.termination_event = termination_event
        # Steady-state refreshes are intentionally once per epoch. The old
        # interval knobs are kept only for constructor compatibility and failure
        # retry timing; explicit trigger_update/request_update can still refresh
        # sooner when another component knows the cache needs updating.
        self.updates_per_epoch = 1
        self.retry_interval = max(float(BLOCK_SECS), poll_interval)
        self.history_epochs = max(1, history_epochs)

        self.slot = subnet_slot
        self.epoch_length: int | None = None
        self.epoch_data: Optional[EpochData] = None
        self.nodes: Optional[list[SubnetNodeInfo]] = None
        self.bootnodes: Optional[AllSubnetBootnodes] = None
        self.overwatch_nodes: Optional[list[OverwatchNodeInfo]] = None

        self.overwatch_nodes_by_epoch: Dict[int, list[OverwatchNodeInfo]] = {}
        self.bootnodes_by_epoch: Dict[int, AllSubnetBootnodes] = {}

        self.previous_interval_timestamp: Optional[float] = None
        self.last_refreshed_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.started = False

        self._refresh_lock = trio.Lock()
        self._state_condition = trio.Condition()
        self._refresh_requested = trio.Event()
        self._refresh_count = 0
        self._epoch_revision = 0
        self._running = False

    async def run(self) -> None:
        """
        Run the background tracker loop.

        Call this with ``nursery.start_soon(tracker.run)``. Blocking chain RPCs
        are executed in worker threads, so this loop does not block Trio while
        waiting for the chain. After each successful refresh it sleeps until the
        next epoch boundary instead of polling the chain every block.
        """
        logger.info("Starting subnet info tracker v5 subnet_id=%s", self.subnet_id)
        self._running = True
        try:
            refresh_ok = await self.refresh()
            self.started = True

            while not self.termination_event.is_set():
                seconds_to_sleep = self._seconds_until_next_epoch_refresh() if refresh_ok else self.retry_interval
                await self._sleep_until_refresh_requested(seconds_to_sleep)
                if self.termination_event.is_set():
                    break

                refresh_ok = await self.refresh()
        except trio.Cancelled:
            raise
        except Exception as e:
            logger.warning("SubnetInfoTracker v5 stopped after error: %s", e, exc_info=True)
        finally:
            self._running = False

    async def refresh(self) -> bool:
        """
        Refresh all tracked chain data now.

        This method is safe to await from the Trio loop; blocking chain calls run
        in a worker thread and concurrent refresh requests are coalesced by a
        lock.
        """
        async with self._refresh_lock:
            result = await trio.to_thread.run_sync(self._fetch_full_data_sync)
            await self._apply_refresh_result(result)
            return result.ok

    async def trigger_update(self, wait: bool = True) -> bool:
        """
        Trigger a refresh.

        When ``wait`` is true, the caller waits for the refresh to finish. When
        false, the background loop is woken and this method returns immediately.
        """
        if wait:
            return await self.refresh()

        self.request_update()
        return True

    def request_update(self) -> None:
        """Wake the background loop so it refreshes as soon as possible."""
        self._refresh_requested.set()

    async def wait_for_epoch_change(
        self,
        after_epoch: int | None = None,
        stop_events: tuple[trio.Event, ...] = (),
    ) -> EpochData | None:
        """
        Wait until the tracker observes a different epoch.

        If ``after_epoch`` is omitted, the current cached epoch is used. The
        returned epoch data is from the refresh that observed the change.
        """
        if after_epoch is None:
            after_epoch = self.get_current_epoch()

        async with self._state_condition:
            while not self.termination_event.is_set() and not self._any_event_set(stop_events):
                if self.epoch_data is not None and (after_epoch is None or self.epoch_data.epoch != after_epoch):
                    return self.epoch_data
                with trio.move_on_after(1.0):
                    await self._state_condition.wait()
        return None

    async def watch_epoch_changes(
        self,
        start_epoch: int | None = None,
        stop_events: tuple[trio.Event, ...] = (),
    ) -> AsyncIterator[EpochData]:
        """
        Yield epoch data each time the tracker observes a new epoch.

        Example:
            ``async for epoch_data in tracker.watch_epoch_changes(): ...``

        """
        last_epoch = self.get_current_epoch() if start_epoch is None else start_epoch
        while not self.termination_event.is_set() and not self._any_event_set(stop_events):
            epoch_data = await self.wait_for_epoch_change(last_epoch, stop_events=stop_events)
            if epoch_data is None:
                return
            last_epoch = epoch_data.epoch
            yield epoch_data

    async def get_epoch_data(self, force: bool = False) -> EpochData | None:
        if force:
            await self.refresh()
        return self.epoch_data

    def get_current_epoch(self) -> int | None:
        if self.epoch_data is None:
            return None

        seconds_per_epoch = max(1.0, float(self.epoch_data.seconds_per_epoch))
        seconds_since_epoch_start = float(self.epoch_data.seconds_elapsed) + self.get_seconds_since_previous_interval()
        return self.epoch_data.epoch + int(seconds_since_epoch_start // seconds_per_epoch)

    def get_subnet_slot(self) -> int | None:
        return self.slot

    def get_epoch_length(self) -> int | None:
        if self.epoch_length is None:
            try:
                epoch_length = self.hypertensor.get_epoch_length()
                if epoch_length is None or epoch_length == "None":
                    return None
                self.epoch_length = int(str(epoch_length))
            except Exception as e:
                logger.warning("Consensus get_epoch_length=%s", e, exc_info=True)
        return self.epoch_length

    async def get_nodes(
        self, classification: SubnetNodeClass, start_epoch: int | None = None, force: bool = False
    ) -> list[SubnetNodeInfo]:
        if force:
            await self.refresh()

        if self.nodes is None or self.epoch_data is None:
            return []

        if start_epoch is None:
            start_epoch = self.get_current_epoch()
            if start_epoch is None:
                return []

        return self._filter_nodes(self.nodes, classification, start_epoch)

    def get_overwatch_nodes(self) -> list[OverwatchNodeInfo]:
        return self.overwatch_nodes or []

    def get_bootnodes(self) -> AllSubnetBootnodes | None:
        return self.bootnodes

    async def is_node(self, peer_id: PeerID, force: bool = False) -> bool:
        all_peer_ids = await self.get_all_peer_ids(force)
        target = self._to_peer_id(peer_id)
        return target is not None and any(self._same_peer_id(target, candidate) for candidate in all_peer_ids)

    async def get_peer_id_node_id(self, peer_id: PeerID, force: bool = False) -> int:
        nodes = await self.get_nodes(SubnetNodeClass.Registered, force=force)
        return self._get_peer_id_node_id_from_nodes(peer_id, nodes)

    def get_peer_id_node_id_sync(self, peer_id: PeerID, force: bool = False) -> int:
        """
        Return the node ID from cached data.

        A synchronous method cannot await a non-blocking chain refresh. When
        ``force`` is true, it requests a background refresh and still returns the
        best currently cached answer.
        """
        if force:
            self.request_update()
        return self._get_peer_id_node_id_from_nodes(peer_id, self.nodes or [])

    async def get_all_peer_ids(self, force: bool = False) -> list[PeerID]:
        """
        Return all known subnet, bootnode, and overwatch peer IDs.
        """
        if force:
            await self.refresh()

        all_ids: list[PeerID] = []
        seen: set[str] = set()

        for raw_peer_id in self._iter_all_peer_ids_raw():
            peer_id = self._to_peer_id(raw_peer_id)
            if peer_id is None:
                continue
            key = str(peer_id)
            if key in seen:
                continue
            seen.add(key)
            all_ids.append(peer_id)

        return all_ids

    def get_seconds_since_previous_interval(self) -> float:
        if self.previous_interval_timestamp is None:
            return 0.0
        return time.time() - self.previous_interval_timestamp

    def get_seconds_remaining_until_next_epoch(self) -> float:
        if self.epoch_data is None:
            return float(BLOCK_SECS)

        return max(
            0.0,
            float(self.epoch_data.seconds_remaining) - self.get_seconds_since_previous_interval(),
        )

    def is_ready(self) -> bool:
        return (
            self.epoch_data is not None
            and self.nodes is not None
            and self.overwatch_nodes is not None
            and self.bootnodes is not None
        )

    @property
    def refresh_count(self) -> int:
        return self._refresh_count

    @property
    def epoch_revision(self) -> int:
        return self._epoch_revision

    @property
    def is_running(self) -> bool:
        return self._running

    def _fetch_full_data_sync(self) -> _RefreshResult:
        result = _RefreshResult()

        try:
            epoch_length = self.hypertensor.get_epoch_length()
            if epoch_length is not None and epoch_length != "None":
                result.epoch_length = int(str(epoch_length))
        except Exception as e:
            result.errors.append(f"epoch_length: {e}")

        result.slot = self._resolve_slot_sync(result.errors)

        if result.slot is not None:
            try:
                result.epoch_data = self.hypertensor.get_subnet_epoch_data(result.slot)
            except Exception as e:
                result.errors.append(f"epoch_data: {e}")

        try:
            result.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)
            if result.nodes is None:
                result.errors.append("nodes: returned None")
        except Exception as e:
            result.errors.append(f"nodes: {e}")

        try:
            result.overwatch_nodes = self.hypertensor.get_all_overwatch_nodes_info_formatted()
            if result.overwatch_nodes is None:
                result.errors.append("overwatch_nodes: returned None")
        except Exception as e:
            result.errors.append(f"overwatch_nodes: {e}")

        try:
            result.bootnodes = self.hypertensor.get_bootnodes_formatted(self.subnet_id)
            if result.bootnodes is None:
                result.errors.append("bootnodes: returned None")
        except Exception as e:
            result.errors.append(f"bootnodes: {e}")

        return result

    async def _apply_refresh_result(self, result: _RefreshResult) -> None:
        now = time.time()
        old_epoch = self.epoch_data.epoch if self.epoch_data is not None else None

        async with self._state_condition:
            if result.slot is not None:
                self.slot = result.slot

            if result.epoch_length is not None:
                self.epoch_length = result.epoch_length

            if result.epoch_data is not None:
                self.epoch_data = result.epoch_data
                self.epoch_length = result.epoch_data.block_per_epoch
                self.previous_interval_timestamp = now

            epoch = self.epoch_data.epoch if self.epoch_data is not None else None

            if result.nodes is not None:
                self.nodes = result.nodes

            if result.overwatch_nodes is not None:
                self.overwatch_nodes = result.overwatch_nodes
                if epoch is not None:
                    self.overwatch_nodes_by_epoch[epoch] = result.overwatch_nodes

            if result.bootnodes is not None:
                self.bootnodes = result.bootnodes
                if epoch is not None:
                    self.bootnodes_by_epoch[epoch] = result.bootnodes

            if epoch is not None:
                self._prune_epoch_history(epoch)

            self.last_refreshed_at = now
            self.last_error = "; ".join(result.errors) if result.errors else None
            self._refresh_count += 1

            if epoch is not None and epoch != old_epoch:
                self._epoch_revision += 1
                logger.info("SubnetInfoTracker observed epoch=%s subnet_id=%s", epoch, self.subnet_id)

            self._state_condition.notify_all()

        if result.errors:
            logger.warning("SubnetInfoTracker refresh completed with errors: %s", self.last_error)

    def _resolve_slot_sync(self, errors: list[str]) -> int | None:
        if self.slot is not None:
            return self.slot

        try:
            slot = self.hypertensor.get_subnet_slot(self.subnet_id)
            if slot is None or slot == "None":
                return None
            return int(str(slot))
        except Exception as e:
            errors.append(f"subnet_slot: {e}")
            return None

    def _seconds_until_next_epoch_refresh(self) -> float:
        if self.epoch_data is None:
            return self._seconds_per_epoch()

        seconds_remaining = self.get_seconds_remaining_until_next_epoch()
        if seconds_remaining > 0:
            return seconds_remaining

        if self.last_error is not None:
            return self.retry_interval

        return self._seconds_per_epoch()

    async def _sleep_until_refresh_requested(self, seconds: float) -> bool:
        seconds = max(0.0, seconds)
        event = self._refresh_requested

        with trio.move_on_after(seconds):
            await event.wait()

        if event.is_set():
            self._refresh_requested = trio.Event()
            return True
        return False

    def _seconds_per_epoch(self) -> float:
        if self.epoch_data is not None:
            return float(self.epoch_data.seconds_per_epoch)

        if self.epoch_length is not None:
            return float(self.epoch_length * BLOCK_SECS)

        return float(BLOCK_SECS)

    def _filter_nodes(
        self, nodes: list[SubnetNodeInfo], classification: SubnetNodeClass, start_epoch: int
    ) -> list[SubnetNodeInfo]:
        filtered_nodes = []
        for node in nodes:
            try:
                node_class = subnet_node_class_to_enum(node.classification["node_class"])
                node_start_epoch = node.classification["start_epoch"]
            except Exception:
                continue

            if node_class.value >= classification.value and node_start_epoch <= start_epoch:
                filtered_nodes.append(node)

        return filtered_nodes

    def _get_peer_id_node_id_from_nodes(self, peer_id: Any, nodes: list[SubnetNodeInfo]) -> int:
        target = self._to_peer_id(peer_id)
        if target is None:
            return 0

        for node in nodes:
            for node_peer_id in self._iter_node_peer_ids(node):
                if self._same_peer_id(target, node_peer_id):
                    return node.subnet_node_id
        return 0

    def _iter_all_peer_ids_raw(self) -> Iterator[Any]:
        if self.nodes is not None:
            for node in self.nodes:
                yield from self._iter_node_peer_ids_raw(node)

        if self.bootnodes is not None:
            for entries_name in ("subnet_bootnodes", "node_bootnodes", "registered_bootnodes"):
                entries = getattr(self.bootnodes, entries_name, None)
                if entries is None:
                    continue
                for peer_id, _ in entries:
                    yield peer_id

        if self.overwatch_nodes is not None:
            for overwatch_node in self.overwatch_nodes:
                peer_id = self._get_overwatch_peer_id_by_subnet(overwatch_node.peer_ids, self.subnet_id)
                if peer_id is not None:
                    yield peer_id

    def _iter_node_peer_ids(self, node: SubnetNodeInfo) -> Iterator[PeerID]:
        for raw_peer_id in self._iter_node_peer_ids_raw(node):
            peer_id = self._to_peer_id(raw_peer_id)
            if peer_id is not None:
                yield peer_id

    def _iter_node_peer_ids_raw(self, node: SubnetNodeInfo) -> Iterator[Any]:
        for peer_info in (node.peer_info, node.bootnode_peer_info, node.client_peer_info):
            peer_id = getattr(peer_info, "peer_id", None)
            if peer_id is not None and peer_id != "":
                yield peer_id

    def _get_overwatch_peer_id_by_subnet(self, tuples: Any, subnet_id: int) -> Any | None:
        if tuples is None:
            return None

        for net_id, peer_id in tuples:
            if net_id == subnet_id:
                return peer_id
        return None

    def _prune_epoch_history(self, current_epoch: int) -> None:
        minimum_epoch = current_epoch - (self.history_epochs - 1)
        for mapping in (self.overwatch_nodes_by_epoch, self.bootnodes_by_epoch):
            for epoch in list(mapping):
                if epoch < minimum_epoch:
                    mapping.pop(epoch, None)

    @staticmethod
    def _same_peer_id(left: PeerID, right: PeerID) -> bool:
        return str(left) == str(right)

    @staticmethod
    def _to_peer_id(peer_id: Any) -> PeerID | None:
        if peer_id is None or peer_id == "":
            return None
        if isinstance(peer_id, PeerID):
            return peer_id
        if isinstance(peer_id, bytes):
            return PeerID(peer_id)
        if isinstance(peer_id, bytearray):
            return PeerID(bytes(peer_id))
        if isinstance(peer_id, (list, tuple)) and all(isinstance(item, int) for item in peer_id):
            return PeerID(bytes(peer_id))

        try:
            return PeerID.from_base58(str(peer_id))
        except Exception:
            return None

    @staticmethod
    def _any_event_set(events: tuple[trio.Event, ...]) -> bool:
        return any(event.is_set() for event in events)
