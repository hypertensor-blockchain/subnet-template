import logging
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("subnet-info-tracker")


class SubnetInfoTracker:
    """
    Tracks subnet info and updates at each interval.

    Tracks:
        - Subnet epoch data
        - Subnet nodes info

    Other updates can be integrated such as the subnet node info RPC query to always have the most updated subnet info.

    A central class for tracking the subnets epoch data across multiple components without having to call the
    Hypertensor RPC multiple times from multiple components.
    """

    def __init__(
        self,
        termination_event: trio.Event,
        subnet_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        epoch_update_intervals: list[float] = [0.0],
        start_fresh_epoch: bool = True,
    ):
        for interval in epoch_update_intervals:
            if interval < 0 or interval > 1:
                raise ValueError("Epoch update interval must be between 0 and 1")

        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.epoch_update_intervals = epoch_update_intervals
        self.start_fresh_epoch = start_fresh_epoch
        self.termination_event = termination_event
        self.epoch_data: Optional[EpochData] = None
        self.slot: int | None = None
        self.nodes: Optional[list[SubnetNodeInfo]] = None
        self.nodes_v2: Dict[int, list[SubnetNodeInfo]] = {}  # epoch -> nodes mapping
        self.bootnodes: Optional[AllSubnetBootnodes] = None
        self.overwatch_nodes: Optional[list[OverwatchNodeInfo]] = None
        self.previous_interval: Optional[float] = None
        self.previous_interval_epoch: Dict[float, int] = {}
        for interval in self.epoch_update_intervals:
            self.previous_interval_epoch[interval] = 0

        self.started = False

    async def run(self) -> None:
        """
        Main entry point - starts sync loop and receive loops for all topics.

        Call this with: nursery.start_soon(gossip.run)
        """
        async with trio.open_nursery() as nursery:
            # Start the sync loop
            nursery.start_soon(self._run_sync_epoch)

    async def _run_sync_epoch(self) -> None:
        """Background loop that keeps epoch_data synced at each interval."""
        logger.info("Starting subnet info tracker")
        while not self.termination_event.is_set():
            try:
                seconds_to_wait = await self._update_data()
                await trio.sleep(seconds_to_wait)

            except Exception as e:
                logger.exception(f"Error in sync loop, error={e}", exc_info=True)
                await trio.sleep(BLOCK_SECS)

    async def _update_data(self) -> int:
        """
        Sync with blockchain and update epoch data.

        Returns:
            int: seconds to sleep

        """
        # Sync with blockchain
        slot = await self._get_subnet_slot(self.subnet_id)
        if slot is None:
            return BLOCK_SECS

        self.epoch_data = self.hypertensor.get_subnet_epoch_data(slot)
        self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)
        if self.nodes is not None or len(self.nodes) > 0:
            self.nodes_v2[self.epoch_data.epoch] = self.nodes

        self.overwatch_nodes = self.hypertensor.get_all_overwatch_nodes_info_formatted()
        self.bootnodes = self.hypertensor.get_bootnodes_formatted(self.subnet_id)
        self.previous_interval_timestamp = int(time.time())

        # Start on fresh epoch
        if not self.started and self.start_fresh_epoch:
            logger.info(
                f"SubnetInfoTracker starting in {self.epoch_data.seconds_remaining} seconds"  # noqa: E501
            )
            logger.info(f"SubnetInfoTracker total nodes={len(self.nodes)}")
            self.started = True
            return self.epoch_data.seconds_remaining

        pct = self.epoch_data.percent_complete
        logger.info(f"Synced: epoch={self.epoch_data.epoch}, pct={pct:.2%}")
        logger.debug(f"SubnetInfoTracker total nodes={len(self.nodes)}")

        # Always ensure we're on a new epoch for each interval
        if self.epoch_data.epoch == self.previous_interval_epoch.get(pct):
            logger.debug(
                f"SubnetInfoTracker waiting for next interval to match epoch {self.previous_interval_epoch[pct]}"  # noqa: E501
            )
            return 1

        self.previous_interval_epoch[pct] = self.epoch_data.epoch
        self.previous_interval = pct

        # Find next epoch update interval
        # next_epoch_update_interval = None
        # for d in self.epoch_update_intervals:
        #     if pct < d:
        #         next_epoch_update_interval = d
        #         break

        next_epoch_update_interval = self._next_epoch_update_interval(pct)

        seconds_to_wait = self._seconds_to_wait(next_epoch_update_interval, pct)
        # seconds_to_wait = max(BLOCK_SECS, seconds_to_wait)
        seconds_to_wait = min(seconds_to_wait, self.epoch_data.seconds_remaining)
        return seconds_to_wait

    def _seconds_to_wait(self, next_epoch_update_interval: float, current_pct: float) -> int:
        # Calculate seconds to sleep
        if next_epoch_update_interval is not None:
            pct_remaining = next_epoch_update_interval - current_pct
            return pct_remaining * self.epoch_data.seconds_per_epoch

        # No more epoch update intervals this epoch, wait until epoch ends
        return self.epoch_data.seconds_remaining

    def _next_epoch_update_interval(self, current_pct: float | None) -> float | None:
        if current_pct is None:
            return None
        for d in self.epoch_update_intervals:
            if current_pct < d:
                return d
        return None

    async def _get_subnet_slot(self, subnet_id: int) -> int | None:
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

    async def get_nodes(self, classification: SubnetNodeClass, force: bool = False) -> list[SubnetNodeInfo]:
        if force:
            await self._update_data()

        if self.nodes is None or self.epoch_data is None:
            return []

        return [
            node
            for node in self.nodes
            if subnet_node_class_to_enum(node.classification["node_class"]).value >= classification.value
            and node.classification["start_epoch"] <= self.epoch_data.epoch
        ]

    async def get_nodes_v2(self, epoch: int, classification: SubnetNodeClass) -> list[SubnetNodeInfo]:
        """
        Only call if it's certain the epoch is soon or current
        """
        while self.nodes_v2.get(epoch) is None:
            logger.info(f"Waiting for epoch {epoch} to be synced")
            await trio.sleep(1.0)

        return [
            node
            for node in self.nodes_v2[epoch]
            if subnet_node_class_to_enum(node.classification["node_class"]).value >= classification.value
            and node.classification["start_epoch"] <= epoch
        ]

    async def is_node(self, peer_id: PeerID, force: bool = False) -> bool:
        """
        Returns True if the peer_id is a node in the subnet via self.nodes
        """
        all_peer_ids = await self.get_all_peer_ids(force)
        return peer_id in all_peer_ids

    async def get_peer_id_node_id(self, peer_id: PeerID, force: bool = False) -> int:
        """
        Returns the node_id of the peer_id in the subnet via self.nodes
        """
        nodes = await self.get_nodes(SubnetNodeClass.Registered, force)
        for node in nodes:
            if peer_id.__eq__(node.peer_id):
                return node.subnet_node_id
        return 0

    def get_peer_id_node_id_sync(self, peer_id: PeerID, force: bool = False) -> int:
        """
        Returns the node_id of the peer_id in the subnet via self.nodes
        """
        if force:
            self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)
        else:
            if self.nodes is None:
                self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)

        for node in self.nodes:
            if peer_id.__eq__(node.peer_id):
                return node.subnet_node_id
        return 0

    async def get_all_peer_ids(self, force: bool = False) -> list[PeerID]:
        """
        Returns a list of all peer_ids, client_peer_ids, and bootnode_peer_ids, and subnet bootnode peer IDs in the
        subnet via self.nodes
        """
        if force:
            await self._update_data()

        all_ids = []

        try:
            if self.nodes is not None:
                for node in self.nodes:
                    for pid_raw in [node.peer_id, node.client_peer_id, node.bootnode_peer_id]:
                        if pid_raw is not None and pid_raw != "":  # Check for empty strings for LocalMockHypertensor
                            try:
                                # Convert to PeerID format
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
                    if peer_id is not None and peer_id != "":  # Check for empty strings for LocalMockHypertensor
                        try:
                            # Convert to PeerID format
                            if isinstance(peer_id, PeerID):
                                all_ids.append(peer_id)
                            else:
                                all_ids.append(PeerID.from_base58(str(peer_id)))
                        except Exception:
                            continue
        except Exception as e:
            logger.error(f"get_all_peer_ids: Exception: {e}")

        # Get overwatch node peer IDs matching the current subnet
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

    def get_seconds_since_previous_interval(self) -> int:
        if self.previous_interval_timestamp is None:
            return 0
        return int(time.time()) - self.previous_interval_timestamp

    def get_seconds_remaining_until_next_epoch(self) -> int:
        if self.epoch_data is None:
            return BLOCK_SECS

        true_seconds_remaining = max(
            1,
            self.epoch_data.seconds_remaining - self.get_seconds_since_previous_interval(),
        )

        return true_seconds_remaining
