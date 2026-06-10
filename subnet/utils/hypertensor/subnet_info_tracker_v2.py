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
from subnet.hypertensor.config import BLOCK_SECS, SECONDS_PER_EPOCH
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger("subnet-info-tracker-v2")


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
        updates_per_epoch: int = 5,
        start_fresh_epoch: bool = True,
    ):
        self.updates_per_epoch = updates_per_epoch

        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.start_fresh_epoch = start_fresh_epoch
        self.termination_event = termination_event
        self.epoch_data: Optional[EpochData] = None
        self.slot: int | None = None
        self.nodes: Optional[list[SubnetNodeInfo]] = None
        self.nodes_v2: Dict[int, list[SubnetNodeInfo]] = {}  # epoch -> nodes mapping
        self.bootnodes: Optional[AllSubnetBootnodes] = None
        self.overwatch_nodes: Optional[list[OverwatchNodeInfo]] = None
        self.previous_interval: Optional[float] = None
        self.previous_interval_epoch: int = 0
        self.interval_percentage: float = 1 / (self.updates_per_epoch + 1)
        epoch_length = self.hypertensor.get_epoch_length()
        self.interval_duration: float = float(BLOCK_SECS * int(str(epoch_length))) * self.interval_percentage
        self.previous_interval_timestamp: Optional[float] = None
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
        # logger.info("Starting subnet info tracker")
        self._async_stop_event = trio.Event()
        last_epoch = None

        while not self.termination_event.is_set() and not self._async_stop_event.is_set():
            try:
                slot = self.get_subnet_slot()
                if slot is None:
                    await trio.sleep(BLOCK_SECS)
                    continue

                subnet_epoch_data = self.hypertensor.get_subnet_epoch_data(slot)
                if subnet_epoch_data is None:
                    logger.debug("Waiting for subnet epoch data")
                    await trio.sleep(1.0)
                    continue

                current_epoch = subnet_epoch_data.epoch

                if current_epoch != last_epoch:
                    logger.info(f"🆕 Epoch Tracker {current_epoch}")
                    last_epoch = current_epoch

                    await self._update_data()

                    if self.interval_duration < self.get_seconds_remaining_until_next_epoch():
                        logger.info(f"Sleeping for {self.interval_duration} seconds for next update")
                        await trio.sleep(self.interval_duration)

                    while (
                        not self.termination_event.is_set()
                        and not self._async_stop_event.is_set()
                        and self.epoch_data.epoch == current_epoch
                        and self.interval_duration < self.get_seconds_remaining_until_next_epoch()
                    ):
                        logger.info(f"Updating subnet info for epoch {current_epoch}")
                        await self._update_data()

                        s_r = self.get_seconds_remaining_until_next_epoch()
                        logger.info(f"Seconds remaining until next update: {s_r}")

                        if self.interval_duration <= self.get_seconds_remaining_until_next_epoch():
                            logger.info(f"(nested) Sleeping for {self.interval_duration} seconds for next update")
                            await trio.sleep(self.interval_duration)

                try:
                    subnet_epoch_data = self.hypertensor.get_subnet_epoch_data(slot)
                    with trio.move_on_after(
                        max(
                            0.1,
                            subnet_epoch_data.seconds_remaining,
                        )
                    ):
                        await self._async_stop_event.wait()
                        break

                    if self._async_stop_event.is_set():
                        break

                    pass  # Timeout reached
                except Exception:
                    logger.exception("Exception in epoch loop")
                    pass
            except Exception as e:
                logger.warning(e, exc_info=True)
                await trio.sleep(1.0)

    async def _update_data(self) -> int:
        """
        Sync with blockchain and update epoch data.

        Returns:
            int: seconds to sleep

        """
        self.update_epoch_data()
        self.update_nodes()
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
            self.nodes = self.hypertensor.get_subnet_nodes_info_formatted(self.subnet_id)
            if self.nodes is not None:
                if len(self.nodes) > 0:
                    self.nodes_v2[self.epoch_data.epoch] = self.nodes

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
        if force:
            await self._update_data()
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
        if force:
            await self._update_data()

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
        if start_epoch is None:
            start_epoch = self.epoch_data.epoch

        while self.nodes_v2.get(on_epoch) is None:
            # logger.info(f"Waiting for epoch {on_epoch} to be synced")
            await trio.sleep(1.0)

        return [
            node
            for node in self.nodes_v2[on_epoch]
            if subnet_node_class_to_enum(node.classification["node_class"]).value >= classification.value
            and node.classification["start_epoch"] <= start_epoch
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
            if peer_id.__eq__(node.peer_info.peer_id):
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
            if peer_id.__eq__(node.peer_info.peer_id):
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
                    p_infos = [node.peer_info, node.bootnode_peer_info, node.client_peer_info]
                    for p_info in p_infos:
                        if p_info is not None and p_info.peer_id != "":
                            pid_raw = p_info.peer_id
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
