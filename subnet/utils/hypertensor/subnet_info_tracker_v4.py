import logging
import threading
import time
from typing import Dict, Optional

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
logger = logging.getLogger("subnet-info-tracker-v4")


class SubnetInfoTracker:
    """
    Tracks subnet info in a separate thread to avoid blocking the main Trio loop.

    Tracks:
        - Subnet epoch data
        - Subnet nodes info
    """

    def __init__(
        self,
        termination_event: trio.Event,
        subnet_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        updates_per_epoch: int = 5,
        start_fresh_epoch: bool = True,
    ):
        self.updates_per_epoch = updates_per_epoch

        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.start_fresh_epoch = start_fresh_epoch
        self.termination_event = termination_event  # Main trio termination event
        self.epoch_data: Optional[EpochData] = None
        self.slot: int | None = None
        self.nodes: Optional[list[SubnetNodeInfo]] = None
        self.nodes_v2: Dict[int, list[SubnetNodeInfo]] = {}  # epoch -> nodes mapping
        self.bootnodes: Optional[AllSubnetBootnodes] = None
        self.overwatch_nodes: Optional[list[OverwatchNodeInfo]] = None

        # Timing variables
        self.interval_percentage: float = 1 / (self.updates_per_epoch + 1)
        # Note: We fetch epoch_length dynamically in the thread to avoid blocking __init__
        self.interval_duration: float = 10.0  # Default, updated in loop
        self.previous_interval_timestamp: Optional[float] = None
        self.started = False

        # Thread control
        self._thread_stop_event = threading.Event()

    async def run(self) -> None:
        """
        Main entry point - spawns the sync blocking loop in a separate thread.
        """
        try:
            # Run the blocking method in a separate thread
            # Cancellable by the main trio loop
            await trio.to_thread.run_sync(self._run_sync_epoch_blocking, self._thread_stop_event)
        except trio.Cancelled:
            # Signal the thread to stop if trio cancels this task
            self._thread_stop_event.set()
            raise

    def _run_sync_epoch_blocking(self, stop_event: threading.Event) -> None:
        """
        Background loop running in a thread.
        Uses blocking blocking IO (time.sleep, synchronous RPC) safely.
        """
        logger.info("Starting threaded subnet info tracker (v4)")
        last_epoch = None

        # Initialize dynamic interval duration
        try:
            epoch_length = self.hypertensor.get_epoch_length()
            self.interval_duration = float(BLOCK_SECS * int(str(epoch_length))) * self.interval_percentage
        except Exception as e:
            logger.warning(f"Failed to get initial epoch length: {e}")

        while not stop_event.is_set() and not self.termination_event.is_set():
            try:
                # 1. Get Slot (Blocking)
                slot = self.get_subnet_slot()
                if slot is None:
                    time.sleep(BLOCK_SECS)
                    continue

                # 2. Get Epoch Data (Blocking)
                # We fetch just the epoch number first to check for changes
                try:
                    current_epoch_datum = self.hypertensor.get_subnet_epoch_data(slot)
                    current_epoch = current_epoch_datum.epoch
                except Exception:
                    time.sleep(1.0)
                    continue

                # 3. Handle Epoch Change
                if current_epoch != last_epoch:
                    logger.info(f"🆕 Epoch Tracker {current_epoch} (Threaded)")
                    last_epoch = current_epoch

                    # Perform full update (Blocking)
                    self._update_data_blocking()

                    # Sleep logic for updates within epoch
                    if self.interval_duration < self.get_seconds_remaining_until_next_epoch():
                        # logger.info(f"Sleeping for {self.interval_duration} seconds for next update")
                        self._cancellable_sleep(self.interval_duration, stop_event)

                    # Nested loop for multiple updates per epoch
                    while (
                        not stop_event.is_set()
                        and not self.termination_event.is_set()
                        and self.epoch_data.epoch == current_epoch
                        and self.interval_duration < self.get_seconds_remaining_until_next_epoch()
                    ):
                        # logger.info(f"Updating subnet info for epoch {current_epoch}")
                        self._update_data_blocking()

                        if self.interval_duration <= self.get_seconds_remaining_until_next_epoch():
                            # logger.info(f"(nested) Sleeping for {self.interval_duration} seconds")
                            self._cancellable_sleep(self.interval_duration, stop_event)

                # 4. Wait for next epoch
                # We re-fetch to get accurate remaining time
                try:
                    subnet_epoch_data = self.hypertensor.get_subnet_epoch_data(slot)
                    seconds_remaining = subnet_epoch_data.seconds_remaining

                    # Sleep until next epoch
                    # We add a small buffer or check frequently to avoid oversleeping
                    # But since we are in a thread, we can just sleep.
                    # check stop_event frequently
                    self._cancellable_sleep(max(0.1, seconds_remaining), stop_event)

                except Exception:
                    time.sleep(1.0)

            except Exception as e:
                logger.warning(f"Error in threaded loop: {e}", exc_info=True)
                time.sleep(1.0)

    def _cancellable_sleep(self, duration: float, stop_event: threading.Event):
        """Sleeps for duration but wakes up if stop_event is set."""
        # Wait returns True if the flag is set, False on timeout
        # We want to sleep (timeout), so we ignore the return unless it's True (stop)
        stop_event.wait(timeout=duration)

    def _update_data_blocking(self):
        """Blocking data update sequence."""
        self.update_epoch_data()
        self.update_nodes()  # Crucial: This was missing in v3!
        self.update_overwatch_nodes()
        self.update_bootnodes()

    def update_epoch_data(self) -> EpochData | None:
        try:
            self.epoch_data = self.hypertensor.get_subnet_epoch_data(self.slot)
            self.previous_interval_timestamp = time.time()
            return self.epoch_data
        except Exception as e:
            logger.warning(e, exc_info=True)
            return None

    def update_nodes(self) -> list[SubnetNodeInfo] | None:
        try:
            # This is a HEAVY blocking call (2-6s usually)
            self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)
            if self.nodes is not None:
                if len(self.nodes) > 0:
                    self.nodes_v2[self.epoch_data.epoch] = self.nodes

            # Cleanup old data
            if self.nodes_v2.get(self.epoch_data.epoch - 2):
                self.nodes_v2.pop(self.epoch_data.epoch - 2)
            return self.nodes
        except Exception as e:
            logger.warning(e, exc_info=True)
            return None

    def update_overwatch_nodes(self) -> list[SubnetNodeInfo] | None:
        try:
            self.overwatch_nodes = self.hypertensor.get_all_overwatch_nodes_info_formatted()
            return self.overwatch_nodes
        except Exception as e:
            logger.warning(e, exc_info=True)
            return None

    def update_bootnodes(self) -> list[SubnetNodeInfo] | None:
        try:
            self.bootnodes = self.hypertensor.get_bootnodes_formatted(self.subnet_id)
            return self.bootnodes
        except Exception as e:
            logger.warning(e, exc_info=True)
            return None

    async def get_epoch_data(self, force: bool = False) -> EpochData | None:
        # Note: 'force' is ignored in threaded mode as updates happen autonomously
        return self.epoch_data

    def get_subnet_slot(self) -> int | None:
        if self.slot is None or self.slot == "None":  # noqa: E711
            try:
                slot = self.hypertensor.get_subnet_slot(self.subnet_id)
                if slot == None or slot == "None":  # noqa: E711
                    return None
                self.slot = int(str(slot))
                logger.debug(f"Subnet running in slot {self.slot}")
            except Exception as e:
                logger.warning(f"Consensus get_subnet_slot={e}", exc_info=True)
        return self.slot

    async def get_nodes(
        self, classification: SubnetNodeClass, start_epoch: int | None = None, force: bool = False
    ) -> list[SubnetNodeInfo]:
        if self.nodes is None or self.epoch_data is None:
            return []

        if start_epoch is None:
            start_epoch = self.epoch_data.epoch

        return [
            node
            for node in self.nodes
            if subnet_node_class_to_enum(node.classification["node_class"]).value >= classification.value
            and node.classification["start_epoch"] <= start_epoch
        ]

    async def get_nodes_v2(
        self, on_epoch: int, classification: SubnetNodeClass, start_epoch: int | None = None
    ) -> list[SubnetNodeInfo]:
        """
        Only call if it's certain the epoch is soon or current
        """
        if start_epoch is None and self.epoch_data:
            start_epoch = self.epoch_data.epoch

        # Wait for data availability
        # We loop here because the thread might be busy updating
        while self.nodes_v2.get(on_epoch) is None:
            # logger.info(f"Waiting for epoch {on_epoch} to be synced")
            await trio.sleep(1.0)

        return [
            node
            for node in self.nodes_v2[on_epoch]
            if subnet_node_class_to_enum(node.classification["node_class"]).value >= classification.value
            and node.classification["start_epoch"] <= (start_epoch or on_epoch)
        ]

    async def is_node(self, peer_id: PeerID, force: bool = False) -> bool:
        all_peer_ids = await self.get_all_peer_ids(force)
        return peer_id in all_peer_ids

    async def get_peer_id_node_id(self, peer_id: PeerID, force: bool = False) -> int:
        nodes = await self.get_nodes(SubnetNodeClass.Registered, force=force)
        for node in nodes:
            if peer_id.__eq__(node.peer_info.peer_id):
                return node.subnet_node_id
        return 0

    def get_peer_id_node_id_sync(self, peer_id: PeerID, force: bool = False) -> int:
        # Note: force is ignored as we read cached state
        if self.nodes is None:
            # Fallback if accessed before thread starts (rare)
            return 0

        for node in self.nodes:
            if peer_id.__eq__(node.peer_info.peer_id):
                return node.subnet_node_id
        return 0

    async def get_all_peer_ids(self, force: bool = False) -> list[PeerID]:
        # 'force' ignored
        all_ids = []

        try:
            if self.nodes is not None:
                for node in self.nodes:
                    p_infos = [node.peer_info, node.bootnode_peer_info, node.client_peer_info]
                    for p_info in p_infos:
                        if p_info is not None and p_info.peer_id != "":
                            pid_raw = p_info.peer_id
                            try:
                                if isinstance(pid_raw, PeerID):
                                    all_ids.append(pid_raw)
                                else:
                                    all_ids.append(PeerID.from_base58(str(pid_raw)))
                            except Exception:
                                continue
        except Exception as e:
            logger.error(f"get_all_peer_ids: Exception: {e}")

        try:
            if self.bootnodes is not None and self.bootnodes.subnet_bootnodes is not None:
                for peer_id, _ in self.bootnodes.subnet_bootnodes:
                    if peer_id is not None and peer_id != "":
                        try:
                            if isinstance(peer_id, PeerID):
                                all_ids.append(peer_id)
                            else:
                                all_ids.append(PeerID.from_base58(str(peer_id)))
                        except Exception:
                            continue
        except Exception as e:
            logger.error(f"get_all_peer_ids: Exception: {e}")

        try:
            if self.overwatch_nodes is not None and len(self.overwatch_nodes) > 0:
                for overwatch_node in self.overwatch_nodes:
                    if overwatch_node.peer_ids is None or len(overwatch_node.peer_ids) == 0:
                        continue
                    peer_id_raw = self._get_overwatch_peer_id_by_subnet(overwatch_node.peer_ids, self.subnet_id)
                    if peer_id_raw is not None and peer_id_raw != "":
                        try:
                            if isinstance(peer_id_raw, PeerID):
                                all_ids.append(peer_id_raw)
                            else:
                                all_ids.append(PeerID.from_base58(str(peer_id_raw)))
                        except Exception:
                            continue
        except Exception as e:
            logger.error(f"get_all_peer_ids: Exception: {e}")

        return all_ids

    def _get_overwatch_peer_id_by_subnet(self, tuples, subnet_id):
        for net_id, peer_id in tuples:
            if net_id == subnet_id:
                return peer_id
        return None

    def get_seconds_since_previous_interval(self) -> float:
        if self.previous_interval_timestamp is None:
            return 0
        return time.time() - self.previous_interval_timestamp

    def get_seconds_remaining_until_next_epoch(self) -> float:
        if self.epoch_data is None:
            return BLOCK_SECS

        true_seconds_remaining = max(
            0.0,
            self.epoch_data.seconds_remaining - self.get_seconds_since_previous_interval(),
        )

        return true_seconds_remaining
