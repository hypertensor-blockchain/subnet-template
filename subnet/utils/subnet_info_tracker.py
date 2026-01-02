import logging
import time
from typing import Dict, Optional

import trio

from libp2p.peer.id import ID as PeerID
from subnet.hypertensor.chain_data import SubnetNodeInfo
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
    ):
        for interval in epoch_update_intervals:
            if interval < 0 or interval > 1:
                raise ValueError("Epoch update interval must be between 0 and 1")

        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.epoch_update_intervals = epoch_update_intervals
        self.termination_event = termination_event
        self.epoch_data: Optional[EpochData] = None
        self.slot: int | None = None
        self.nodes: Optional[list[SubnetNodeInfo]] = None
        self.previous_interval: Optional[float] = None
        self.previous_interval_epoch: Dict[float, int] = {}
        for interval in self.epoch_update_intervals:
            self.previous_interval_epoch[interval] = 0

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
        logger.info("Starting sync loop")
        started = False
        while not self.termination_event.is_set():
            try:
                # Sync with blockchain
                slot = await self._get_subnet_slot(self.subnet_id)
                if slot is None:
                    await trio.sleep(BLOCK_SECS)
                    continue

                epoch_data = self.hypertensor.get_subnet_epoch_data(slot)
                self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)

                # Start on fresh epoch
                if not started:
                    logger.info(
                        f"SubnetInfoTracker starting in {epoch_data.seconds_remaining} seconds"  # noqa: E501
                    )
                    started = True
                    await trio.sleep(epoch_data.seconds_remaining)
                    continue

                self.epoch_data = epoch_data
                self.previous_interval_timestamp = int(time.time())

                logger.info(f"SubnetInfoTracker total nodes={len(self.nodes)}")

                pct = self.epoch_data.percent_complete
                logger.info(f"Synced: epoch={self.epoch_data.epoch}, pct={pct:.2%}")

                # Always ensure we're on a new epoch for each interval
                if self.epoch_data.epoch == self.previous_interval_epoch[pct]:
                    logger.info(
                        f"SubnetInfoTracker waiting for next interval to match epoch {self.previous_interval_epoch[pct]}"  # noqa: E501
                    )
                    await trio.sleep(BLOCK_SECS)
                    continue

                self.previous_interval_epoch[pct] = self.epoch_data.epoch
                self.previous_interval = pct

                # Find next epoch update interval
                next_epoch_update_interval = None
                for d in self.epoch_update_intervals:
                    if pct < d:
                        next_epoch_update_interval = d
                        break

                # Calculate seconds to sleep
                if next_epoch_update_interval is not None:
                    pct_remaining = next_epoch_update_interval - pct
                    seconds_to_wait = pct_remaining * self.epoch_data.seconds_per_epoch
                else:
                    # No more epoch update intervals this epoch, wait until epoch ends
                    seconds_to_wait = self.epoch_data.seconds_remaining

                seconds_to_wait = max(BLOCK_SECS, seconds_to_wait)
                await trio.sleep(seconds_to_wait)

            except Exception:
                logger.exception("Error in sync loop")
                await trio.sleep(BLOCK_SECS)

    async def _get_subnet_slot(self, subnet_id: int) -> int | None:
        if self.slot is None or self.slot == "None":  # noqa: E711
            try:
                slot = self.hypertensor.get_subnet_slot(self.subnet_id)
                if slot == None or slot == "None":  # noqa: E711
                    return None
                self.slot = int(str(slot))
                logger.info(f"Subnet running in slot {self.slot}")
            except Exception as e:
                logger.warning(f"Consensus get_subnet_slot={e}", exc_info=True)
        return self.slot

    def get_nodes(self, classification: SubnetNodeClass) -> list[SubnetNodeInfo]:
        if self.nodes is None or self.epoch_data is None:
            return []

        return [
            node
            for node in self.nodes
            if subnet_node_class_to_enum(node.classification["node_class"]).value >= classification.value
            and node.classification["start_epoch"] <= self.epoch_data.epoch
        ]

    def is_node(self, peer_id: PeerID, force: bool = False) -> bool:
        """
        Returns True if the peer_id is a node in the subnet via self.nodes
        """
        all_peer_ids = self.get_all_peer_ids(force)
        return peer_id in all_peer_ids

    def get_peer_id_node_id(self, peer_id: PeerID) -> int:
        """
        Returns the node_id of the peer_id in the subnet via self.nodes
        """
        nodes = self.get_nodes(SubnetNodeClass.Registered)
        for node in nodes:
            if node.peer_id == peer_id:
                return node.subnet_node_id
        return -1

    def get_all_peer_ids(self, force: bool = False) -> list[PeerID]:
        """
        Returns a list of all peer_ids, client_peer_ids, and bootnode_peer_ids in the
        subnet via self.nodes
        """
        if force:
            nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)
        else:
            if self.nodes is None:
                return []
            nodes = self.nodes

        all_ids = []
        for node in nodes:
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
        return all_ids

    def get_seconds_since_previous_interval(self) -> int:
        if self.previous_interval_timestamp is None or self.epoch_data is None:
            return 0
        return int(time.time()) - self.previous_interval_timestamp

    def get_seconds_remaining_until_next_epoch(self) -> int:
        if self.epoch_data is None:
            return BLOCK_SECS

        true_seconds_remaining = max(
            1,
            self.epoch_data.seconds_remaining - self.get_seconds_since_previous_interval(),
        )
        print(f"SubnetInfoTracker epoch_data.epoch {self.epoch_data.epoch}")
        print(
            f"SubnetInfoTracker previous_interval_timestamp {self.previous_interval_timestamp}"  # noqa: E501
        )
        print(
            f"SubnetInfoTracker epoch_data.seconds_remaining {self.epoch_data.seconds_remaining}"  # noqa: E501
        )
        print(
            f"SubnetInfoTracker get_seconds_since_previous_interval() {self.get_seconds_since_previous_interval()}"  # noqa: E501
        )
        print(f"SubnetInfoTracker current time {time.time()}")
        print(f"SubnetInfoTracker previous_interval {self.previous_interval}")
        print(f"SubnetInfoTracker true_seconds_remaining {true_seconds_remaining}")

        return true_seconds_remaining
