from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.hypertensor.chain_functions import Hypertensor, EpochData
import logging
import trio
from typing import Optional
from subnet.hypertensor.chain_functions import SubnetNodeClass
from subnet.hypertensor.chain_data import SubnetNodeInfo
from subnet.hypertensor.chain_functions import subnet_node_class_to_enum

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

    Other updates can be integrated such as the subnet node info RPC query to always have the most
    updated subnet info.

    A central class for tracking the subnets epoch data across multiple components without
    having to call the Hypertensor RPC multiple times from multiple components
    """

    def __init__(
        self,
        termination_event: trio.Event,
        subnet_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        epoch_update_intervals: list[float],
    ):
        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.epoch_update_intervals = epoch_update_intervals
        self.termination_event = termination_event
        self.epoch_data: Optional[EpochData] = None
        self.slot: int | None = None
        self.nodes: Optional[list[SubnetNodeInfo]] = None

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
        while not self.termination_event.is_set():
            try:
                # Sync with blockchain
                slot = await self._get_subnet_slot(self.subnet_id)
                self.epoch_data = self.hypertensor.get_subnet_epoch_data(slot)
                self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(
                    self.subnet_id
                )

                pct = self.epoch_data.percent_complete
                logger.info(f"Synced: epoch={self.epoch_data.epoch}, pct={pct:.2%}")

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

                seconds_to_wait = max(1.0, seconds_to_wait + 0.5)
                await trio.sleep(seconds_to_wait)

            except Exception:
                logger.exception("Error in sync loop")
                await trio.sleep(5.0)

    async def _get_subnet_slot(self, subnet_id: int) -> int:
        if self.slot is None or self.slot == "None":  # noqa: E711
            try:
                slot = self.hypertensor.get_subnet_slot(self.subnet_id)
                if slot == None or slot == "None":  # noqa: E711
                    await trio.sleep(6.0)
                self.slot = int(str(slot))
                logger.info(f"Subnet running in slot {self.slot}")
            except Exception as e:
                logger.warning(f"Consensus get_subnet_slot={e}", exc_info=True)
        return self.slot

    def get_nodes(self, classification: SubnetNodeClass) -> list[SubnetNodeInfo]:
        if self.nodes is None:
            return []

        return [
            node
            for node in self.nodes
            if subnet_node_class_to_enum(node.classification["node_class"]).value
            >= classification.value
            and node.classification["start_epoch"] <= self.epoch_data.epoch
        ]
