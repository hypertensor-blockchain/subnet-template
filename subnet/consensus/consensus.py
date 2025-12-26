# import asyncio
# import multiprocessing as mp
from dataclasses import asdict
from typing import List

from libp2p.kad_dht import KadDHT
from libp2p.peer.id import ID as PeerID
from subnet.consensus.utils import (
    compare_consensus_data,
    did_node_attest,
    is_validator_or_attestor,
)
from subnet.hypertensor.chain_data import SubnetNodeConsensusData
from subnet.hypertensor.chain_functions import Hypertensor, SubnetNodeClass
from subnet.hypertensor.config import BLOCK_SECS
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
import trio
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("consensus/1.0.0")


class Consensus:
    def __init__(
        self,
        dht: KadDHT,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: Hypertensor,
        skip_activate_subnet: bool = False,
        start: bool = True,
    ):
        super().__init__()
        self.dht = dht
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.is_subnet_active: bool = False
        self.skip_activate_subnet = skip_activate_subnet
        self.slot: int | None = None  # subnet epoch slot, set in `run_activate_subnet`
        self.stop = trio.Event()

    async def _main_loop(self):
        if not await self.run_activate_subnet():
            return
        if not await self.run_is_node_validator():
            return
        await self.run_forever()

    def get_validator(self, epoch: int):
        validator = self.hypertensor.get_rewards_validator(self.subnet_id, epoch)
        return validator

    async def get_scores(self, current_epoch: int) -> List[SubnetNodeConsensusData]:
        """
        Fill in a way to get scores on each node

        These scores must be deterministic - See docs
        """
        """
        It's impossible to get every Peer ID in the (a large) network so we test using
        the test IDs to get the heartbeat

        In production: We use the blockchain to get the list of all peer IDs
        """
        # Get each subnet node ID that is included onchain AND in the subnet
        included_nodes = self.hypertensor.get_min_class_subnet_nodes_formatted(
            self.subnet_id, current_epoch, SubnetNodeClass.Included
        )

        print("Included nodes: ", included_nodes)

        subnet_node_ids = []
        for node in included_nodes:
            peer_id = PeerID.from_base58(node.peer_id)
            key = f"/pk/{peer_id.to_bytes().hex()}"
            retrieved_value = await self.dht.get_value(key)
            if retrieved_value is not None:
                subnet_node_ids.append(node.subnet_node_id)
            else:
                print(f"Node not found: {node.subnet_node_id}, peer ID: {node.peer_id}")

        print("Subnet node IDs: ", subnet_node_ids)

        """
            {
                "subnet_node_id": int,
                "score": int
            }

            Is the expected format on-chain

            We use asdict() when submitting
        """
        consensus_score_list = [
            SubnetNodeConsensusData(subnet_node_id=node_id, score=int(1e18))
            for node_id in subnet_node_ids
        ]

        print("Consensus score list: ", consensus_score_list)

        return consensus_score_list

    # async def get_scores(self, current_epoch: int) -> List[SubnetNodeConsensusData]:
    #     """
    #     Fill in a way to get scores on each node

    #     These scores must be deterministic - See docs
    #     """
    #     """
    #     It's impossible to get every Peer ID in the (a large) network so we test using
    #     the test IDs to get the heartbeat

    #     In production: We use the blockchain to get the list of all peer IDs
    #     """
    #     peer_ids = [
    #         "12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf",  # Alith
    #         "12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF",  # Baltathar
    #         "12D3KooWBqJu85tnb3WciU3LcXhCmTdkvMi4k1Zq3BshUPhVfTui",  # Charleth
    #     ]

    #     retrieved_peer_ids = []
    #     for base58_peer_id in peer_ids:
    #         peer_id = PeerID.from_base58(base58_peer_id)
    #         key = f"/pk/{peer_id.to_bytes().hex()}"
    #         retrieved_value = await self.dht.get_value(key)
    #         if retrieved_value is not None:
    #             retrieved_peer_ids.append(base58_peer_id)

    #     print("Retrieved peer IDs: ", retrieved_peer_ids)

    #     # Get all included nodes
    #     if self.hypertensor is None:
    #         # self.logger.warning("Hypertensor RPC node required to get node scores")
    #         print("Hypertensor RPC node required to get node scores")
    #         return []

    #     # Get each subnet node ID that is included onchain AND in the subnet
    #     included_nodes = self.hypertensor.get_min_class_subnet_nodes_formatted(
    #         self.subnet_id, current_epoch, SubnetNodeClass.Included
    #     )
    #     subnet_node_ids = [
    #         n.subnet_node_id for n in included_nodes if n.peer_id in retrieved_peer_ids
    #     ]

    #     print("Included nodes: ", subnet_node_ids)

    #     """
    #         {
    #             "subnet_node_id": int,
    #             "score": int
    #         }

    #         Is the expected format on-chain

    #         We use asdict() when submitting
    #     """
    #     consensus_score_list = [
    #         SubnetNodeConsensusData(subnet_node_id=node_id, score=int(1e18))
    #         for node_id in subnet_node_ids
    #     ]

    #     print("Consensus score list: ", consensus_score_list)

    #     return consensus_score_list

    async def run_activate_subnet(self):
        """
        Verify subnet is active on-chain before starting consensus

        For initial coldkeys this will sleep until the enactment period, then proceed
        to check once per epoch after enactment starts if the owner activated the subnet
        """
        # Useful if subnet is already active and for testing
        if self.skip_activate_subnet:
            # self.logger.info(
            #     "Skipping subnet activation and attempting to start consensus"
            # )
            print("Skipping subnet activation and attempting to start consensus")
            return True

        last_epoch = None
        subnet_active = False
        max_errors = 3
        errors_count = 0
        while not self.stop.is_set():
            if self.slot is None or self.slot == "None":  # noqa: E711
                try:
                    slot = self.hypertensor.get_subnet_slot(self.subnet_id)
                    if slot == None or slot == "None":  # noqa: E711
                        await trio.sleep(BLOCK_SECS)
                        continue
                    self.slot = int(str(slot))
                    # self.logger.info(f"Subnet running in slot {self.slot}")
                    print(f"Subnet running in slot {self.slot}")
                except Exception as e:
                    # self.logger.warning(f"Consensus get_subnet_slot={e}", exc_info=True)
                    print(f"Consensus get_subnet_slot={e}")

            epoch_data = self.hypertensor.get_epoch_data()
            current_epoch = epoch_data.epoch
            # self.logger.info(
            #     f"Current epoch: {current_epoch}, checking subnet activation status"
            # )
            print(f"Current epoch: {current_epoch}, checking subnet activation status")

            if current_epoch != last_epoch:
                subnet_info = self.hypertensor.get_formatted_subnet_info(self.subnet_id)
                if subnet_info is None or subnet_info == None:  # noqa: E711
                    # None means the subnet is likely deactivated
                    if errors_count > max_errors:
                        # self.logger.warning(
                        #     "Cannot find subnet ID: %s, shutting down", self.subnet_id
                        # )
                        print(
                            "Cannot find subnet ID: %s, shutting down", self.subnet_id
                        )
                        self.shutdown()
                        subnet_active = False
                        break
                    else:
                        # self.logger.warning(
                        #     f"Cannot find subnet ID: {self.subnet_id}, trying {max_errors - errors_count} more times"
                        # )
                        print(
                            f"Cannot find subnet ID: {self.subnet_id}, trying {max_errors - errors_count} more times"
                        )
                        errors_count = errors_count + 1
                else:
                    if subnet_info.state == "Active":
                        # self.logger.info(
                        #     f"Subnet ID {self.subnet_id} is active, starting consensus"
                        # )
                        print(
                            f"Subnet ID {self.subnet_id} is active, starting consensus"
                        )
                        subnet_active = True
                        break
                    else:
                        # self.logger.info(
                        #     f"Subnet ID {self.subnet_id} is not active (state: {subnet_info.state}), waiting for activation"
                        # )
                        print(
                            f"Subnet ID {self.subnet_id} is not active (state: {subnet_info.state}), waiting for activation"
                        )

                last_epoch = current_epoch

            # self.logger.info(
            #     "Waiting for subnet to be activated. Sleeping until next epoch"
            # )
            print("Waiting for subnet to be activated. Sleeping until next epoch")
            await trio.sleep(max(0.0, epoch_data.seconds_remaining))

        return subnet_active

    async def run_is_node_validator(self):
        """
        Verify node is active on-chain before starting consensus

        Node must be classed as Idle on-chain to to start consensus

        Included nodes cannot be the elected validator or attest but must take part in consensus
        and be included in the consensus data to graduate to a Validator classed node
        """
        last_epoch = None
        while not self.stop.is_set():
            epoch_data = self.hypertensor.get_subnet_epoch_data(self.slot)
            current_epoch = epoch_data.epoch

            if current_epoch != last_epoch:
                nodes = self.hypertensor.get_min_class_subnet_nodes_formatted(
                    self.subnet_id, current_epoch, SubnetNodeClass.Idle
                )
                node_found = False
                for node in nodes:
                    if node.subnet_node_id == self.subnet_node_id:
                        node_found = True
                        break

                if not node_found:
                    # self.logger.info(
                    #     "Subnet Node ID %s is not active on epoch %s. Trying again next epoch",
                    #     self.subnet_node_id,
                    #     current_epoch,
                    # )
                    print(
                        "Subnet Node ID %s is not active on epoch %s. Trying again next epoch",
                        self.subnet_node_id,
                        current_epoch,
                    )
                else:
                    # self.logger.info(
                    #     "Subnet Node ID %s is classified as active on epoch %s. Starting consensus.",
                    #     self.subnet_node_id,
                    #     current_epoch,
                    # )
                    print(
                        "Subnet Node ID %s is classified as active on epoch %s. Starting consensus.",
                        self.subnet_node_id,
                        current_epoch,
                    )
                    break

                last_epoch = current_epoch

            await trio.sleep(max(0, epoch_data.seconds_remaining))

        return True

    async def run_forever(self):
        """
        Loop until a new epoch to found, then run consensus logic
        """
        self._async_stop_event = trio.Event()
        last_epoch = None
        started = False
        logged_started = False

        # self.logger.info("About to begin consensus")
        print("About to begin consensus")

        while not self.stop.is_set() and not self._async_stop_event.is_set():
            try:
                epoch_data = self.hypertensor.get_subnet_epoch_data(self.slot)

                # Start on fresh epoch
                if started is False:
                    started = True
                    try:
                        # self.logger.info(
                        #     f"Starting consensus on next epoch in {epoch_data.seconds_remaining}s"
                        # )
                        print(
                            f"Starting consensus on next epoch in {epoch_data.seconds_remaining}s"
                        )
                        with trio.move_on_after(epoch_data.seconds_remaining):
                            await self._async_stop_event.wait()
                            break  # Stop event was set (if wait returns, event is set)
                        # If we get here without break, it timed out or fell through, handled by checking event or loop
                        if self._async_stop_event.is_set():
                            break
                        continue  # Timeout reached
                    except Exception:  # trio.move_on_after doesn't raise TimeoutError
                        continue
                elif not logged_started:
                    # self.logger.info("✅ Starting consensus")
                    print("✅ Starting consensus")
                    logged_started = True

                current_epoch = epoch_data.epoch

                if current_epoch != last_epoch:
                    """
                    Add validation logic before and/or after `await run_consensus(current_epoch)`

                    The logic here should be for qualifying nodes (proving work), generating scores, etc.
                    """
                    logger.info(f"🆕 Epoch {current_epoch}")
                    last_epoch = current_epoch

                    # Attest/Validate
                    await self.run_consensus(current_epoch)

                try:
                    # Get fresh epoch data after processing for `seconds_remaining`
                    epoch_data = self.hypertensor.get_subnet_epoch_data(self.slot)
                    # self.logger.info(
                    #     f"Sleeping until next epoch for {max(0, epoch_data.seconds_remaining)}s"
                    # )
                    print(
                        f"Sleeping until next epoch for {max(0, epoch_data.seconds_remaining)}s"
                    )

                    with trio.move_on_after(max(0, epoch_data.seconds_remaining)):
                        await self._async_stop_event.wait()
                        break

                    if self._async_stop_event.is_set():
                        break

                    pass  # Timeout reached
                except Exception:
                    pass
            except Exception as e:
                # self.logger.warning(e, exc_info=True)
                print(e)
                await trio.sleep(BLOCK_SECS)

    async def run_consensus(self, current_epoch: int):
        """
        At the start of each epoch, we check if we are validator

        Scores are likely generated and rooted from the `run_forever` function, although, any use cases are possible

        We start by:
            - Getting scores
                - Can generate scores in real-time or get from the DHT database

        If elected on-chain validator:
            - Submit scores to Hypertensor

        If attestor (non-elected on-chain validator):
            - Retrieve validators score submission from Hypertensor
            - Compare to our own
            - Attest if 100% accuracy, else do nothing
        """
        # self.logger.info(f"[Consensus] epoch: {current_epoch}")
        print(f"[Consensus] epoch: {current_epoch}")

        scores = await self.get_scores(current_epoch)

        if scores is None:
            return

        validator = None
        # Wait until validator is chosen
        while not self.stop.is_set():
            validator = self.get_validator(current_epoch)
            epoch_data = self.hypertensor.get_subnet_epoch_data(self.slot)
            _current_epoch = epoch_data.epoch
            if _current_epoch != current_epoch:
                validator = None
                break

            if validator is not None or validator != "None":
                break

            # Wait until next block to try again
            await trio.sleep(BLOCK_SECS)

        if validator is None or validator == None:  # noqa: E711
            return

        # self.logger.info(
        #     f"Elected validator on epoch {current_epoch} is node ID {validator}"
        # )
        print(f"Elected validator on epoch {current_epoch} is node ID {validator}")

        if validator == self.subnet_node_id:
            # self.logger.info(
            #     f"🎖️ Acting as elected validator for epoch {current_epoch} and attempting to propose an attestation to the blockchain"
            # )
            print(
                f"🎖️ Acting as elected validator for epoch {current_epoch} and attempting to propose an attestation to the blockchain"
            )

            # See if attestation proposal submitted
            consensus_data = self.hypertensor.get_consensus_data_formatted(
                self.subnet_id, current_epoch
            )

            if consensus_data is not None:  # noqa: E711
                # self.logger.info("Already submitted data, moving to next epoch")
                print("Already submitted data, moving to next epoch")

                return

            # self.logger.info("Preparing to attempt to propose attestation")
            print("Preparing to attempt to propose attestation")

            if len(scores) == 0:
                """
                Add any logic here for when no scores are present.

                The blockchain allows the validator to submit an empty score. This can mean
                the subnet is in a broken state or not synced.

                If other peers also come up with the same "zero" scores, they can attest the validator
                and the validator will not accrue penalties or be slashed. The subnet itself will accrue
                penalties until it recovers (penalties decrease for every successful epoch).

                No scores are generated, likely subnet in broken state and all other nodes
                should be too, so we submit consensus with no scores.

                This will increase subnet penalties, but avoid validator penalties.

                Any successful epoch following will remove these penalties on the subnet
                """
                self.hypertensor.propose_attestation(
                    self.subnet_id, data=[asdict(s) for s in scores]
                )
            else:
                self.hypertensor.propose_attestation(
                    self.subnet_id, data=[asdict(s) for s in scores]
                )

        elif validator is not None:
            # self.logger.info(
            #     f"🗳️ Attempting to act as attestor/voter for epoch {current_epoch}"
            # )
            print(f"🗳️ Attempting to act as attestor/voter for epoch {current_epoch}")

            consensus_data = None  # Fetch one time once not None
            _is_validator_or_attestor = False  # Check only once
            while not self.stop.is_set():
                # Check consensus data exists in case attest fails
                if consensus_data is None or consensus_data == None:  # noqa: E711
                    consensus_data = self.hypertensor.get_consensus_data_formatted(
                        self.subnet_id, current_epoch
                    )

                epoch_data = self.hypertensor.get_subnet_epoch_data(self.slot)
                _current_epoch = epoch_data.epoch

                # If next epoch or validator took too long, move onto next steps
                if (
                    _current_epoch != current_epoch
                    or epoch_data.percent_complete > 0.25
                ):
                    logger.info(
                        "Skipping attestation, validator took too long to submit consensus data or next epoch"
                    )
                    break

                if consensus_data is None or consensus_data == None:  # noqa: E711
                    logger.info(
                        "Waiting for consensus data to be submitted, checking again in 1 block"
                    )
                    await trio.sleep(BLOCK_SECS)
                    continue

                """
                If this subnet doesn't utilize `prioritize_queue_node_id` or `remove_queue_node_id`, then always skip
                attestation. See https://docs.hypertensor.org/network/consensus for more information.
                """
                if (
                    consensus_data.prioritize_queue_node_id is not None
                    or consensus_data.remove_queue_node_id is not None
                ):
                    # self.logger.info(
                    #     "Skipping attestation, validator used prioritize_queue_node_id or remove_queue_node_id"
                    # )
                    print(
                        "Skipping attestation, validator used prioritize_queue_node_id or remove_queue_node_id"
                    )
                    break

                validator_data = consensus_data.data

                """
                Get all of the hosters inference outputs they stored to the DHT
                """
                if 1.0 == compare_consensus_data(
                    my_data=scores, validator_data=validator_data
                ):
                    # Check if we can attest
                    # This is important in case a node sets emergency validators
                    if not _is_validator_or_attestor:
                        _is_validator_or_attestor = is_validator_or_attestor(
                            self.hypertensor, self.subnet_id, self.subnet_node_id
                        )
                        # If False, break
                        # If True, check once
                        if not _is_validator_or_attestor:
                            # self.logger.info(
                            #     "Not attestor or validator, moving to next epoch"
                            # )
                            print("Not attestor or validator, moving to next epoch")
                            break

                    # Check if we already attested
                    if did_node_attest(self.subnet_node_id, consensus_data):
                        # self.logger.debug("Already attested, moving to next epoch")
                        print("Already attested, moving to next epoch")
                        break

                    # self.logger.info(
                    #     f"✅ Elected validator's data matches for epoch {current_epoch}, attesting their data"
                    # )
                    print(
                        f"✅ Elected validator's data matches for epoch {current_epoch}, attesting their data"
                    )

                    receipt = self.hypertensor.attest(self.subnet_id)

                    if isinstance(
                        self.hypertensor, LocalMockHypertensor
                    ):  # don't check receipt if using mock
                        break

                    if receipt.is_success:
                        break
                    else:
                        await trio.sleep(BLOCK_SECS)
                else:
                    # self.logger.info(
                    #     f"❌ Data doesn't match validator's for epoch {current_epoch}, moving forward with no attetation"
                    # )
                    print(
                        f"❌ Data doesn't match validator's for epoch {current_epoch}, moving forward with no attetation"
                    )

                    break

    async def shutdown(self):
        if not self.stop.is_set():
            self.stop.set()

        # In a trio task, we just set the event. The loops checking self.stop will exit.
        print("Consensus shutdown requested")
