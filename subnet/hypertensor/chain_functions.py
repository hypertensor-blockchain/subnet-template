from dataclasses import dataclass
from enum import Enum
import logging
from typing import Any, List, Optional

from scalecodec.base import RuntimeConfiguration
from scalecodec.types import CompactU32, Map
from substrateinterface import (
    ExtrinsicReceipt,
    Keypair,
    KeypairType,
    SubstrateInterface,
)
from substrateinterface.exceptions import SubstrateRequestException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from websocket import WebSocketConnectionClosedException, WebSocketProtocolException

from subnet.hypertensor.chain_data import (
    AllSubnetBootnodes,
    ConsensusData,
    DelegateStakeInfo,
    NodeDelegateStakeInfo,
    OverwatchNodeInfo,
    SubnetData,
    SubnetInfo,
    SubnetNode,
    SubnetNodeInfo,
    SubnetNodeStakeInfo,
)
from subnet.hypertensor.config import BLOCK_SECS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@dataclass
class EpochData:
    block: int
    epoch: int
    block_per_epoch: int
    seconds_per_epoch: int
    percent_complete: float
    blocks_elapsed: int
    blocks_remaining: int
    seconds_elapsed: int
    seconds_remaining: int

    @staticmethod
    def zero(current_block: int, epoch_length: int) -> "EpochData":
        return EpochData(
            block=current_block,
            epoch=0,
            block_per_epoch=epoch_length,
            seconds_per_epoch=epoch_length * BLOCK_SECS,
            percent_complete=0.0,
            blocks_elapsed=0,
            blocks_remaining=epoch_length,
            seconds_elapsed=0,
            seconds_remaining=epoch_length * BLOCK_SECS,
        )


@dataclass
class OverwatchEpochData:
    block: int
    epoch: int
    overwatch_epoch: int
    block_per_epoch: int
    seconds_per_epoch: int
    percent_complete: float
    blocks_elapsed: int
    blocks_remaining: int
    seconds_elapsed: int
    seconds_remaining: int
    seconds_remaining_until_reveal: int
    epoch_cutoff_block: int

    @staticmethod
    def zero(current_block: int, epoch_length: int) -> "OverwatchEpochData":
        return OverwatchEpochData(
            block=current_block,
            epoch=0,
            overwatch_epoch=0,
            block_per_epoch=epoch_length,
            seconds_per_epoch=epoch_length * BLOCK_SECS,
            percent_complete=0.0,
            blocks_elapsed=0,
            blocks_remaining=epoch_length,
            seconds_elapsed=0,
            seconds_remaining=epoch_length * BLOCK_SECS,
            seconds_remaining_until_reveal=0,
            epoch_cutoff_block=0,
        )


class KeypairFrom(Enum):
    MNEMONIC = 1
    PRIVATE_KEY = 2


class SubnetNodeClass(Enum):
    Registered = 0
    Idle = 1
    Included = 2
    Validator = 3


# lookup from string
def subnet_node_class_from_string(name: str) -> SubnetNodeClass:
    return SubnetNodeClass[name]


def subnet_node_class_to_enum(name: str) -> SubnetNodeClass:
    return SubnetNodeClass[name]


class Hypertensor:
    def __init__(
        self,
        url: str,
        phrase: str,
        keypair_from: Optional[KeypairFrom] = None,
        runtime_config: Optional[RuntimeConfiguration] = None,
    ):
        self.url = url
        self.interface: SubstrateInterface = SubstrateInterface(url=url, runtime_config=runtime_config)
        if keypair_from is None:
            self.keypair = Keypair.create_from_mnemonic(phrase, crypto_type=KeypairType.ECDSA)
            self.hotkey = self.keypair.ss58_address
        elif keypair_from is KeypairFrom.MNEMONIC:
            self.keypair = Keypair.create_from_mnemonic(phrase, crypto_type=KeypairType.ECDSA)
            self.hotkey = self.keypair.ss58_address
        elif keypair_from is KeypairFrom.PRIVATE_KEY:
            self.keypair = Keypair.create_from_private_key(phrase, crypto_type=KeypairType.ECDSA)
            self.hotkey = self.keypair.ss58_address

    def get_block_number(self):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    block_hash = _interface.get_block_hash()
                    block_number = _interface.get_block_number(block_hash)
                    return block_number
            except SubstrateRequestException as e:
                logger.error("Failed to get query request: {}".format(e))

        return make_query()

    def get_epoch(self):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    block_hash = _interface.get_block_hash()
                    current_block = _interface.get_block_number(block_hash)
                    epoch_length = _interface.get_constant("Network", "EpochLength")
                    epoch = int(str(current_block)) // int(str(epoch_length))
                    return epoch
            except SubstrateRequestException as e:
                logger.error("Failed to get query request: {}".format(e))

        return make_query()

    def propose_attestation(
        self,
        subnet_id: int,
        data,
        prioritize_queue_node_id: Optional[Any] = None,
        remove_queue_node_id: Optional[Any] = None,
        args: Optional[Any] = None,
        attest_data: Optional[Any] = None,
    ):
        """
        Submit consensus data on each epoch with no conditionals

        It is up to prior functions to decide whether to call this function

        :param subnet_id: self.keypair of extrinsic caller. Must be a subnet_node in the subnet
        :param data: an array of data containing all AccountIds, PeerIds, and scores per subnet hoster
        :param args: arbitrary data the validator can send in with consensus data

        Note: It's important before calling this to ensure the entrinsic will be successful.
              If the function reverts, the extrinsic is Pays::Yes
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="propose_attestation",
            call_params={
                "subnet_id": subnet_id,
                "data": data,
                "prioritize_queue_node_id": prioritize_queue_node_id,
                "remove_queue_node_id": remove_queue_node_id,
                "args": args,
                "attest_data": attest_data,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    if receipt.is_success:
                        print("✅ Extrinsic Success")
                    else:
                        logger.error(f"⚠️ Extrinsic Failed: {receipt.error_message}")

                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        try:
            return submit_extrinsic()
        except Exception as e:
            logger.warning(f"propose_attestation={e}", exc_info=True)

    def attest(self, subnet_id: int, attest_data: Optional[List[Any]] = None):
        """
        Attest validator submission on current epoch

        :param subnet_id: Subnet ID

        Note: It's important before calling this to ensure the entrinsic will be successful.
              If the function reverts, the extrinsic is Pays::Yes
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="attest",
            call_params={"subnet_id": subnet_id, "attest_data": attest_data},
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)

                    if receipt.is_success:
                        print("✅ Extrinsic Success")
                    else:
                        logger.error(f"⚠️ Extrinsic Failed: {receipt.error_message}")

                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def register_subnet(
        self,
        max_cost: int,
        name: str,
        repo: str,
        description: str,
        misc: str,
        min_stake: int,
        max_stake: int,
        delegate_stake_percentage: int,
        initial_coldkeys: Any,
        key_types: list,
        bootnodes: list,
    ) -> ExtrinsicReceipt:
        """
        Register subnet node and stake
        """
        _orig_process_encode = Map.process_encode

        # A temporary patch for using BTreeMap encoding for initial_coldkeys
        # Assumption is that there is a metadata version mismatch causing the issue
        # See: `scalecodec/types.py`
        def _patched_process_encode(self, value):
            if type(value) is not list:
                raise ValueError(
                    "value should be a list of tuples e.g.: [('1', 2), ('23', 24), ('28', 30), ('45', 80)]"
                )

            element_count_compact = CompactU32()
            element_count_compact.encode(len(value))

            data = element_count_compact.data

            # Force key/value map types
            if str(value[0][0]).startswith("0x"):
                # initial_coldkeys
                self.map_key = "[u8; 20]"
                self.map_value = "u32"
            elif str(value[0][1]).startswith("/"):
                # bootnodes
                self.map_key = "Vec<u8>"
                self.map_value = "Vec<u8>"

            for item_key, item_value in value:
                key_obj = self.runtime_config.create_scale_object(type_string=self.map_key, metadata=self.metadata)
                print("_patched_process_encode key_obj", key_obj)
                print("_patched_process_encode item_key", item_key)
                print("_patched_process_encode item_value", item_value)

                data += key_obj.encode(item_key)

                value_obj = self.runtime_config.create_scale_object(type_string=self.map_value, metadata=self.metadata)

                data += value_obj.encode(item_value)

            print("_patched_process_encode data", data)
            return data

        Map.process_encode = _patched_process_encode

        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="register_subnet",
            call_params={
                "max_cost": max_cost,
                "subnet_data": {
                    "name": name.encode(),
                    "repo": repo.encode(),
                    "description": description.encode(),
                    "misc": misc.encode(),
                    "min_stake": min_stake,
                    "max_stake": max_stake,
                    "delegate_stake_percentage": delegate_stake_percentage,
                    "initial_coldkeys": initial_coldkeys,
                    "key_types": sorted(set(key_types)),
                    "bootnodes": sorted(set(bootnodes)),
                },
            },
        )

        # create signed extrinsic
        extrinsic = self.interface.create_signed_extrinsic(call=call, keypair=self.keypair)

        Map.process_encode = _orig_process_encode

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def activate_subnet(
        self,
        subnet_id: str,
    ) -> ExtrinsicReceipt:
        """
        Activate a registered subnet node

        :param subnet_id: subnet ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="activate_subnet",
            call_params={
                "subnet_id": subnet_id,
            },
        )

        # @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(4))
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def remove_subnet(
        self,
        subnet_id: str,
    ) -> ExtrinsicReceipt:
        """
        Remove a subnet

        :param subnet_id: subnet ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="remove_subnet",
            call_params={
                "subnet_id": subnet_id,
            },
        )

        # create signed extrinsic
        extrinsic = self.interface.create_signed_extrinsic(call=call, keypair=self.keypair)

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def add_subnet_node(
        self,
        subnet_id: int,
        hotkey: str,
        peer_id: str,
        bootnode_peer_id: str,
        client_peer_id: str,
        delegate_reward_rate: int,
        stake_to_be_added: int,
        bootnode: Optional[str] = None,
        unique: Optional[str] = None,
        non_unique: Optional[str] = None,
    ) -> ExtrinsicReceipt:
        """
        Add subnet validator as subnet subnet_node and stake

        :param subnet_id: subnet ID
        :param hotkey: Hotkey of subnet node
        :param peer_id: peer Id of subnet node
        :param delegate_reward_rate: reward rate to delegate stakers (1e18)
        :param stake_to_be_added: amount to stake
        :param unique: unique optional parameter
        :param non_unique: optional parametr
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="add_subnet_node",
            call_params={
                "subnet_id": subnet_id,
                "hotkey": hotkey,
                "peer_id": peer_id,
                "bootnode_peer_id": bootnode_peer_id,
                "client_peer_id": client_peer_id,
                "bootnode": bootnode,
                "delegate_reward_rate": delegate_reward_rate,
                "stake_to_be_added": stake_to_be_added,
                "unique": unique,
                "non_unique": non_unique,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def register_subnet_node(
        self,
        subnet_id: int,
        hotkey: str,
        peer_id: str,
        bootnode_peer_id: str,
        client_peer_id: str,
        delegate_reward_rate: int,
        stake_to_be_added: int,
        max_burn_amount: int,
        bootnode: Optional[str] = None,
        unique: Optional[str] = None,
        non_unique: Optional[str] = None,
    ) -> ExtrinsicReceipt:
        """
        Register subnet node and stake

        :param subnet_id: subnet ID
        :param hotkey: Hotkey of subnet node
        :param peer_id: peer Id of subnet node
        :param delegate_reward_rate: reward rate to delegate stakers (1e18)
        :param stake_to_be_added: amount to stake
        :param a: unique optional parameter
        :param b: optional parametr
        :param c: optional parametr
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="register_subnet_node",
            call_params={
                "subnet_id": subnet_id,
                "hotkey": hotkey,
                "peer_id": peer_id,
                "bootnode_peer_id": bootnode_peer_id,
                "client_peer_id": client_peer_id,
                "bootnode": bootnode,
                "delegate_reward_rate": delegate_reward_rate,
                "stake_to_be_added": stake_to_be_added,
                "unique": unique,
                "non_unique": non_unique,
                "max_burn_amount": max_burn_amount,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def activate_subnet_node(
        self,
        subnet_id: int,
        subnet_node_id: int,
    ) -> ExtrinsicReceipt:
        """
        Activate registered subnet node

        :param subnet_id: subnet ID
        :param subnet_node_id: subnet node ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="activate_subnet_node",
            call_params={
                "subnet_id": subnet_id,
                "subnet_node_id": subnet_node_id,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def deactivate_subnet_node(
        self,
        subnet_id: int,
        subnet_node_id: int,
    ) -> ExtrinsicReceipt:
        """
        Temporarily deactivate subnet node

        :param subnet_id: subnet ID
        :param subnet_node_id: subnet node ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="deactivate_subnet_node",
            call_params={
                "subnet_id": subnet_id,
                "subnet_node_id": subnet_node_id,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def remove_subnet_node(
        self,
        subnet_id: int,
        subnet_node_id: int,
    ):
        """
        Remove subnet node

        :param subnet_id: subnet ID
        :param subnet_node_id: subnet node ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="remove_subnet_node",
            call_params={
                "subnet_id": subnet_id,
                "subnet_node_id": subnet_node_id,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def add_to_stake(
        self,
        subnet_id: int,
        subnet_node_id: int,
        stake_to_be_added: int,
    ):
        """
        Increase stake balance of a subnet node

        :param subnet_id: subnet ID
        :param subnet_node_id: subnet node ID
        :param stake_to_be_added: stake to be added towards subnet
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="add_to_stake",
            call_params={
                "subnet_id": subnet_id,
                "subnet_node_id": subnet_node_id,
                "hotkey": self.keypair.ss58_address,
                "stake_to_be_added": stake_to_be_added,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def remove_stake(
        self,
        subnet_id: int,
        stake_to_be_removed: int,
    ):
        """
        Remove stake balance towards specified subnet.

        Amount must be less than minimum required balance if an activate subnet node.

        :param subnet_id: Subnet ID
        :param stake_to_be_removed: stake to be removed from subnet
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="remove_stake",
            call_params={
                "subnet_id": subnet_id,
                "hotkey": self.keypair.ss58_address,
                "stake_to_be_removed": stake_to_be_removed,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def claim_stake_unbondings(self):
        """
        Remove balance from unbondings ledger
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="claim_unbondings",
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def add_to_delegate_stake(
        self,
        subnet_id: int,
        stake_to_be_added: int,
    ):
        """
        Add delegate stake balance to subnet

        :param subnet_id: subnet ID
        :param stake_to_be_added: stake to be added towards subnet
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="add_to_delegate_stake",
            call_params={
                "subnet_id": subnet_id,
                "stake_to_be_added": stake_to_be_added,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def transfer_delegate_stake(
        self,
        from_subnet_id: int,
        to_subnet_id: int,
        delegate_stake_shares_to_be_switched: int,
    ):
        """
        Transfer delegate stake from one subnet to another subnet

        :param from_subnet_id: from subnet ID
        :param to_subnet_id: to subnet ID
        :param stake_to_be_added: stake to be added towards subnet
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="transfer_delegate_stake",
            call_params={
                "from_subnet_id": from_subnet_id,
                "to_subnet_id": to_subnet_id,
                "delegate_stake_shares_to_be_switched": delegate_stake_shares_to_be_switched,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def remove_delegate_stake(
        self,
        subnet_id: int,
        shares_to_be_removed: int,
    ):
        """
        Remove delegate stake balance from subnet by shares

        :param subnet_id: to subnet ID
        :param shares_to_be_removed: sahares to be removed
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="add_to_delegate_stake",
            call_params={
                "subnet_id": subnet_id,
                "shares_to_be_removed": shares_to_be_removed,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def increase_delegate_stake(
        self,
        subnet_id: int,
        amount: int,
    ):
        """
        Increase delegate stake pool balance to subnet ID

        Note: This does ''NOT'' increase the balance of a user

        :param subnet_id: to subnet ID
        :param amount: TENSOR to be added
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="increase_delegate_stake",
            call_params={
                "subnet_id": subnet_id,
                "amount": amount,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def update_coldkey(
        self,
        hotkey: str,
        new_coldkey: str,
    ):
        """
        Update coldkey using current coldkey as self.keypair

        :param hotkey: Hotkey
        :param new_coldkey: New coldkey
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="update_coldkey",
            call_params={
                "hotkey": hotkey,
                "new_coldkey": new_coldkey,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def update_hotkey(
        self,
        old_hotkey: str,
        new_hotkey: str,
    ):
        """
        Updates hotkey using coldkey

        :param old_hotkey: Old hotkey
        :param new_hotkey: New hotkey
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="update_hotkey",
            call_params={
                "old_hotkey": old_hotkey,
                "new_hotkey": new_hotkey,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def register_overwatch_node(
        self,
        hotkey: str,
        stake_to_be_added: int,
    ):
        """
        Updates hotkey using coldkey

        :param hotkey: Hotkey
        :param stake_to_be_added: Stake to be added
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="register_overwatch_node",
            call_params={
                "hotkey": hotkey,
                "stake_to_be_added": stake_to_be_added,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                print("Failed to send: {}".format(e))

        return submit_extrinsic()

    def set_overwatch_node_peer_id(self, subnet_id: int, overwatch_node_id: int, peer_id: str):
        """
        Updates hotkey using coldkey

        :param subnet_id: Subnet ID
        :param overwatch_node_id: Overwatch node ID
        :param peer_id: Peer ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="set_overwatch_node_peer_id",
            call_params={
                "subnet_id": subnet_id,
                "overwatch_node_id": overwatch_node_id,
                "peer_id": peer_id,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                print("Failed to send: {}".format(e))

        return submit_extrinsic()

    def commit_overwatch_subnet_weights(
        self,
        overwatch_node_id: int,
        commit_weights: Any,
    ):
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="commit_overwatch_subnet_weights",
            call_params={
                "overwatch_node_id": overwatch_node_id,
                "commit_weights": commit_weights,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    if receipt.is_success:
                        print("✅ Extrinsic Success")
                    else:
                        logger.error(f"⚠️ Extrinsic Failed: {receipt.error_message}")

                    return receipt
            except SubstrateRequestException as e:
                print("Failed to send: {}".format(e))

        return submit_extrinsic()

    def reveal_overwatch_subnet_weights(
        self,
        overwatch_node_id: int,
        reveals: Any,
    ):
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="reveal_overwatch_subnet_weights",
            call_params={
                "overwatch_node_id": overwatch_node_id,
                "reveals": reveals,
            },
        )

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    # get none on retries
                    nonce = _interface.get_account_nonce(self.keypair.ss58_address)

                    # create signed extrinsic
                    extrinsic = _interface.create_signed_extrinsic(call=call, keypair=self.keypair, nonce=nonce)

                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    if receipt.is_success:
                        print("✅ Extrinsic Success")
                    else:
                        logger.error(f"⚠️ Extrinsic Failed: {receipt.error_message}")

                    return receipt
            except SubstrateRequestException as e:
                print("Failed to send: {}".format(e))

        return submit_extrinsic()

    def get_subnet_node_data(
        self,
        subnet_id: int,
        subnet_node_id: int,
    ) -> ExtrinsicReceipt:
        """
        Query a subnet node ID by its hotkey

        :param subnet_id: to subnet ID
        :param subnet_node_id: Subnet Node ID
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetNodesData", [subnet_id, subnet_node_id])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_hotkey_subnet_node_id(
        self,
        subnet_id: int,
        hotkey: str,
    ) -> ExtrinsicReceipt:
        """
        Query a subnet node ID by its hotkey

        :param subnet_id: to subnet ID
        :param hotkey: Hotkey of subnet node
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "HotkeySubnetNodeId", [subnet_id, hotkey])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_hotkey_owner(
        self,
        hotkey: str,
    ) -> ExtrinsicReceipt:
        """
        Get coldkey of hotkey

        :param hotkey: Hotkey of subnet node
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "HotkeyOwner", [hotkey])
                    return result.value["data"]["free"]
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_subnet_node_id_hotkey(
        self,
        subnet_id: int,
        hotkey: str,
    ) -> ExtrinsicReceipt:
        """
        Query hotkey by subnet node ID

        :param hotkey: Hotkey of subnet node
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetNodeIdHotkey", [subnet_id, hotkey])
                    return result.value["data"]["free"]
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_balance(self, address: str):
        """
        Function to return account balance

        :param address: address of account_id
        :returns: account balance
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("System", "Account", [address])
                    return result.value["data"]["free"]
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_subnet_stake_balance(self, subnet_id: int, address: str):
        """
        Function to return a subnet node stake balance

        :param subnet_id: Subnet ID
        :param address: address of account_id
        :returns: account stake balance towards subnet
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "AccountSubnetStake", [address, subnet_id])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_subnet_id_by_path(self, path: str):
        """
        Query subnet ID by path

        :param path: path of subnet
        :returns: subnet_id
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetPaths", [path])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_subnet_data(self, id: int):
        """
        Function to get data struct of the subnet

        :param id: id of subnet
        :returns: subnet_id
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetsData", [id])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_max_subnets(self):
        """
        Function to get the maximum number of subnets allowed on the blockchain

        :returns: max_subnets
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MaxSubnets")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_min_subnet_nodes(self):
        """
        Function to get the minimum number of subnet_nodes required to host a subnet

        :returns: min_subnet_nodes
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MinSubnetNodes")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_min_stake_balance(self):
        """
        Function to get the minimum stake balance required to host a subnet

        :returns: min_stake_balance
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MinStakeBalance")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_max_subnet_nodes(self):
        """
        Function to get the maximum number of subnet_nodes allowed to host a subnet

        :returns: max_subnet_nodes
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MaxSubnetNodes")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_tx_rate_limit(self):
        """
        Function to get the transaction rate limit

        :returns: tx_rate_limit
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "TxRateLimit")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_epoch_length(self):
        """
        Function to get the epoch length as blocks per epoch

        :returns: epoch_length
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.get_constant("Network", "EpochLength")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_rewards_validator(self, subnet_id: int, epoch: int):
        """
        Query an epochs chosen subnet validator

        :param subnet_id: subnet ID
        :param epoch: epoch to query SubnetElectedValidator
        :returns: epoch_length
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetElectedValidator", [subnet_id, epoch])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_overwatch_epoch_multiplier(self):
        """
        Function to get the transaction rate limit

        :returns: tx_rate_limit
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "OverwatchEpochLengthMultiplier")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_overwatch_commit_cutoff_percent(self):
        """
        Function to get the transaction rate limit

        :returns: tx_rate_limit
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "OverwatchCommitCutoffPercent")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_rewards_submission(self, subnet_id: int, epoch: int):
        """
        Query epochs validator rewards submission

        :param subnet_id: subnet ID
        :param epoch: epoch to query SubnetConsensusSubmission

        :returns: epoch_length
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetConsensusSubmission", [subnet_id, epoch])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_min_subnet_registration_blocks(self):
        """
        Query minimum subnet registration blocks

        :returns: epoch_length
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MinSubnetRegistrationBlocks")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_max_subnet_registration_blocks(self):
        """
        Query maximum subnet registration blocks

        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MaxSubnetRegistrationBlocks")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_max_subnet_entry_interval(self):
        """
        Query maximum subnet entry interval blocks
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "MaxSubnetEntryInterval")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_subnet_registration_epochs(self):
        """
        Query maximum subnet entry interval blocks
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetRegistrationEpochs")
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_subnet_slot(self, subnet_id: int):
        """
        Query maximum subnet entry interval blocks with retry + reconnect
        """

        @retry(
            wait=wait_fixed(BLOCK_SECS + 1),
            stop=stop_after_attempt(4),
            retry=retry_if_exception_type(
                (
                    SubstrateRequestException,
                    ConnectionError,
                    AttributeError,
                    WebSocketConnectionClosedException,
                    WebSocketProtocolException,
                )
            ),
        )
        def make_query():
            try:
                # Ensure interface is connected
                if not self.interface.websocket or not self.interface.websocket.connected:
                    self.interface.connect_websocket()

                # Ensure runtime metadata is loaded
                # if not self.interface.runtime_config:
                #     self.interface.init_runtime()

                # Query directly (avoid context manager which closes socket)
                with self.interface as interface:
                    result = interface.query("Network", "SubnetSlot", [subnet_id])
                    return result

            except Exception as e:  # noqa: F841
                # Force reconnect + metadata refresh so retry can succeed
                try:
                    self.interface.close()
                except Exception:
                    pass
                self.interface.connect_websocket()
                self.interface.init_runtime()
                raise

        try:
            return make_query()
        except Exception as e:
            logger.warning(f"get_subnet_slot={e}", exc_info=True)
            return None

    def get_consensus_data(self, subnet_id: int, epoch: int):
        """
        Query an epochs consesnus submission
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetConsensusSubmission", [subnet_id, epoch])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    """
  RPC
  """

    def get_subnet_info(
        self,
        subnet_id: int,
    ):
        """
        Query an epochs chosen subnet validator and return SubnetNode

        :param subnet_id: subnet ID
        :returns: Struct of subnet info
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getSubnetInfo",
                        params=[
                            subnet_id,
                        ],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_subnet_nodes(
        self,
        subnet_id: int,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getSubnetNodes", params=[subnet_id])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_all_subnet_info(
        self,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getAllSubnetsInfo", params=[])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_subnet_nodes_info(
        self,
        subnet_id: int,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getSubnetNodesInfo", params=[subnet_id])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_all_subnet_nodes_info(
        self,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getAllSubnetNodesInfo", params=[])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_bootnodes(
        self,
        subnet_id: int,
    ):
        """
        Function to return all bootnodes of a subnet

        :param subnet_id: subnet ID
        :returns: subnet_nodes_data
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    subnet_nodes_data = _interface.rpc_request(method="network_getBootnodes", params=[subnet_id])
                    return subnet_nodes_data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_coldkey_subnet_nodes_info(self, coldkey: str):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getColdkeySubnetNodesInfo", params=[coldkey])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_coldkey_stakes(self, coldkey: str):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getColdkeyStakes", params=[coldkey])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_delegate_stakes(self, account_id: str):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getDelegateStakes", params=[account_id])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_node_delegate_stakes(self, account_id: str):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getNodeDelegateStakes", params=[account_id])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_overwatch_commits(self, epoch: int, overwatch_node_id: int):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getOverwatchCommitsForEpochAndNode",
                        params=[epoch, overwatch_node_id],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_overwatch_reveals(self, epoch: int, overwatch_node_id: int):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getOverwatchRevealsForEpochAndNode",
                        params=[epoch, overwatch_node_id],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def proof_of_stake(self, subnet_id: int, peer_id: str, min_class: int):
        """
        Function to return all account_ids and subnet_node_ids from the substrate Hypertensor Blockchain by peer ID

        :param subnet_id: subnet ID
        :param peer_id: peer ID
        :param min_class: SubnetNodeClass enum

        Registered = 0
        Idle = 1
        Included = 2
        Validator = 3

        ```rust
        pub enum SubnetNodeClass {
          #[default] Registered,
          Idle,
          Included,
          Validator,
        }
        ```
        :returns: subnet_nodes_data
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    result = _interface.rpc_request(
                        method="network_proofOfStake",
                        params=[subnet_id, peer_id, min_class],
                    )
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_minimum_delegate_stake(
        self,
        subnet_id: int,
    ):
        """
            Query required minimum stake balance based on memory

        =    :param subnet_id: Subnet ID

            :returns: subnet_nodes_data
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(method="network_getMinimumDelegateStake", params=[subnet_id])
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_subnet_node_info(self, subnet_id: int, subnet_node_id: int):
        """
        Function to return all subnet nodes in the SubnetNodeInfo struct format

        :param subnet_id: subnet ID

        :returns: subnet_nodes_data
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getSubnetNodeInfo",
                        params=[subnet_id, subnet_node_id],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_elected_validator_info(self, subnet_id: int, subnet_epoch: int):
        """
        Query an epochs chosen subnet validator and return SubnetNode

        :param subnet_id: subnet ID
        :returns: Struct of subnet info
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getElectedValidatorInfo",
                        params=[subnet_id, subnet_epoch],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_validators_and_attestors(
        self,
        subnet_id: int,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getValidatorsAndAttestors",
                        params=[
                            subnet_id,
                        ],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_overwatch_node_info(
        self,
        overwatch_node_id: int,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getOverwatchNodeInfo",
                        params=[
                            overwatch_node_id,
                        ],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    def get_all_overwatch_nodes_info(
        self,
    ):
        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_rpc_request():
            try:
                with self.interface as _interface:
                    data = _interface.rpc_request(
                        method="network_getAllOverwatchNodesInfo",
                        params=[],
                    )
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_rpc_request()

    """
  Events
  """

    def get_reward_result_event(self, target_subnet_id: int, epoch: int):
        """
        Query the event of an epochs rewards submission

        :param target_subnet_id: subnet ID

        :returns: subnet_nodes_data
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_event_query():
            try:
                epoch_length = self.get_epoch_length()
                epoch_length = int(str(epoch_length))
                block_number = epoch_length * epoch
                block_hash = self.interface.get_block_hash(block_number=block_number)
                with self.interface as _interface:
                    data = None
                    events = _interface.get_events(block_hash=block_hash)
                    for event in events:
                        if event["event"]["module_id"] == "Network" and event["event"]["event_id"] == "RewardResult":
                            subnet_id, attestation_percentage = event["event"]["attributes"]
                            if subnet_id == target_subnet_id:
                                data = subnet_id, attestation_percentage
                                break
                    return data
            except SubstrateRequestException as e:
                logger.error("Failed to get event request: {}".format(e))

        return make_event_query()

    """
  Helpers
  """

    def get_epoch_data(self) -> EpochData:
        current_block = self.get_block_number()
        epoch_length = self.get_epoch_length()
        current_block = int(str(current_block))
        epoch_length = int(str(epoch_length))
        epoch = current_block // epoch_length
        blocks_elapsed = current_block % epoch_length
        percent_complete = blocks_elapsed / epoch_length
        blocks_remaining = epoch_length - blocks_elapsed
        seconds_elapsed = blocks_elapsed * BLOCK_SECS
        seconds_remaining = blocks_remaining * BLOCK_SECS

        return EpochData(
            block=current_block,
            epoch=epoch,
            block_per_epoch=epoch_length,
            seconds_per_epoch=epoch_length * BLOCK_SECS,
            percent_complete=percent_complete,
            blocks_elapsed=blocks_elapsed,
            blocks_remaining=blocks_remaining,
            seconds_elapsed=seconds_elapsed,
            seconds_remaining=seconds_remaining,
        )

    def get_subnet_epoch_data(self, slot: int) -> EpochData:
        current_block = int(str(self.get_block_number()))
        epoch_length = int(str(self.get_epoch_length()))

        if current_block < slot:
            return EpochData.zero(current_block=current_block, epoch_length=epoch_length)

        blocks_since_start = current_block - slot
        epoch = blocks_since_start // epoch_length
        blocks_elapsed = blocks_since_start % epoch_length
        percent_complete = blocks_elapsed / epoch_length
        blocks_remaining = epoch_length - blocks_elapsed
        seconds_elapsed = blocks_elapsed * BLOCK_SECS
        seconds_remaining = blocks_remaining * BLOCK_SECS

        return EpochData(
            block=current_block,
            epoch=epoch,
            block_per_epoch=epoch_length,
            seconds_per_epoch=epoch_length * BLOCK_SECS,
            percent_complete=percent_complete,
            blocks_elapsed=blocks_elapsed,
            blocks_remaining=blocks_remaining,
            seconds_elapsed=seconds_elapsed,
            seconds_remaining=seconds_remaining,
        )

    def get_overwatch_epoch_data(self) -> OverwatchEpochData:
        current_block = self.get_block_number()
        epoch_length = self.get_epoch_length()
        current_block = int(str(current_block))
        epoch_length = int(str(epoch_length))
        epoch = current_block // epoch_length
        blocks_elapsed = current_block % epoch_length
        percent_complete = blocks_elapsed / epoch_length
        blocks_remaining = epoch_length - blocks_elapsed
        seconds_elapsed = blocks_elapsed * BLOCK_SECS
        seconds_remaining = blocks_remaining * BLOCK_SECS

        multiplier = self.get_overwatch_epoch_multiplier()
        multiplier = int(str(multiplier))
        overwatch_epoch_length = epoch_length * multiplier
        overwatch_epoch = current_block // overwatch_epoch_length
        cutoff_percentage = float(int(str(self.get_overwatch_commit_cutoff_percent())) / 1e18)
        block_increase_cutoff = overwatch_epoch_length * cutoff_percentage
        epoch_cutoff_block = overwatch_epoch_length * overwatch_epoch + block_increase_cutoff

        if current_block > epoch_cutoff_block:
            seconds_remaining_until_reveal = 0
        else:
            seconds_remaining_until_reveal = (epoch_cutoff_block - current_block) * BLOCK_SECS

        return OverwatchEpochData(
            block=current_block,
            epoch=epoch,
            overwatch_epoch=overwatch_epoch,
            block_per_epoch=epoch_length,
            seconds_per_epoch=epoch_length * BLOCK_SECS,
            percent_complete=percent_complete,
            blocks_elapsed=blocks_elapsed,
            blocks_remaining=blocks_remaining,
            seconds_elapsed=seconds_elapsed,
            seconds_remaining=seconds_remaining,
            seconds_remaining_until_reveal=seconds_remaining_until_reveal,
            epoch_cutoff_block=epoch_cutoff_block,
        )

    def in_overwatch_commit_period(self) -> bool:
        epoch_data = self.get_epoch_data()
        epoch_length = epoch_data.block_per_epoch
        multiplier = self.get_overwatch_epoch_multiplier()
        multiplier = int(str(multiplier))
        overwatch_epoch_length = epoch_length * multiplier
        current_epoch = epoch_data.epoch
        cutoff_percentage = float(int(str(self.get_overwatch_commit_cutoff_percent())) / 1e18)
        block_increase_cutoff = overwatch_epoch_length * cutoff_percentage
        epoch_cutoff_block = overwatch_epoch_length * current_epoch + block_increase_cutoff
        return epoch_data.block < epoch_cutoff_block

    """
  Formatted
  """

    def get_elected_validator_node_formatted(self, subnet_id: int, subnet_epoch: int) -> Optional["SubnetNode"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_elected_validator_info(subnet_id, subnet_epoch)

            subnet_node = SubnetNodeInfo.from_vec_u8(result["result"])

            return subnet_node
        except Exception:
            return None

    def get_validators_and_attestors_formatted(self, subnet_id: int) -> Optional[List["SubnetNodeInfo"]]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_validators_and_attestors(subnet_id)

            subnet_nodes = SubnetNodeInfo.list_from_vec_u8(result["result"])

            return subnet_nodes
        except Exception:
            return None

    def get_formatted_subnet_data(self, subnet_id: int) -> Optional["SubnetData"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_subnet_data(
                subnet_id,
            )

            subnet = SubnetData.from_vec_u8(result["result"])

            return subnet
        except Exception:
            return None

    def get_formatted_subnet_info(self, subnet_id: int) -> Optional["SubnetInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_subnet_info(subnet_id)

            subnet = SubnetInfo.from_vec_u8(result["result"])

            return subnet
        except Exception:
            return None

    def get_formatted_all_subnet_info(self) -> List["SubnetInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_all_subnet_info()

            subnets_info = SubnetInfo.list_from_vec_u8(result["result"])

            return subnets_info
        except Exception:
            return None

    def get_formatted_get_subnet_node_info(self, subnet_id: int, subnet_node_id: int) -> Optional["SubnetNodeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_subnet_node_info(subnet_id, subnet_node_id)

            subnet_node_info = SubnetNodeInfo.from_vec_u8(result["result"])

            return subnet_node_info
        except Exception:
            return None

    def get_subnet_nodes_info_formatted(self, subnet_id: int) -> List["SubnetNodeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_subnet_nodes_info(subnet_id)

            subnet_nodes_info = SubnetNodeInfo.list_from_vec_u8(result["result"])

            return subnet_nodes_info
        except Exception:
            return None

    def get_all_subnet_nodes_info_formatted(self) -> List["SubnetNodeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_all_subnet_nodes_info()

            subnet_nodes_info = SubnetNodeInfo.list_from_vec_u8(result["result"])

            return subnet_nodes_info
        except Exception:
            return None

    def get_bootnodes_formatted(self, subnet_id: int) -> Optional["AllSubnetBootnodes"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_bootnodes(subnet_id)

            all_subnet_bootnodes = AllSubnetBootnodes.from_vec_u8(result["result"])

            return all_subnet_bootnodes
        except Exception:
            return None

    def get_coldkey_subnet_nodes_info_formatted(self, coldkey: str) -> List["SubnetNodeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_coldkey_subnet_nodes_info(coldkey)

            subnet_nodes_info = SubnetNodeInfo.list_from_vec_u8(result["result"])

            return subnet_nodes_info
        except Exception:
            return None

    def get_coldkey_stakes_formatted(self, coldkey: str) -> List["SubnetNodeStakeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_coldkey_stakes(coldkey)

            coldkey_stakes = SubnetNodeStakeInfo.list_from_vec_u8(result["result"])

            return coldkey_stakes
        except Exception:
            return None

    def get_delegate_stakes_formatted(self, account_id: str) -> List["DelegateStakeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_delegate_stakes(account_id)

            delegate_stakes = DelegateStakeInfo.list_from_vec_u8(result["result"])

            return delegate_stakes
        except Exception:
            return None

    def get_node_delegate_stakes_formatted(self, account_id: str) -> List["NodeDelegateStakeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_node_delegate_stakes(account_id)

            node_delegate_stakes = NodeDelegateStakeInfo.list_from_vec_u8(result["result"])

            return node_delegate_stakes
        except Exception:
            return None

    def get_consensus_data_formatted(self, subnet_id: int, epoch: int) -> Optional[ConsensusData]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_consensus_data(subnet_id, epoch)

            if result is None or result == "None" or result == None:  # noqa: E711
                return None

            consensus_data = ConsensusData.fix_decoded_values(result)

            return consensus_data
        except Exception as e:
            logger.error(f"Error get_consensus_data_formatted={e}", exc_info=True)
            return None

    def get_min_class_subnet_nodes_formatted(
        self, subnet_id: int, subnet_epoch: int, min_class: SubnetNodeClass
    ) -> List["SubnetNodeInfo"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            result = self.get_subnet_nodes_info(subnet_id)

            subnet_nodes = SubnetNodeInfo.list_from_vec_u8(result["result"])

            return [
                node
                for node in subnet_nodes
                if subnet_node_class_to_enum(node.classification["node_class"]).value >= min_class.value
                and node.classification["start_epoch"] <= subnet_epoch
            ]
        except Exception as e:
            logger.error(f"Error get_min_class_subnet_nodes_formatted={e}", exc_info=True)
            return []

    def update_bootnodes(
        self,
        subnet_id: int,
        add: list,
        remove: list,
    ) -> ExtrinsicReceipt:
        """
        Remove a subnet

        :param self.keypair: self.keypair of extrinsic caller. Must be a subnet_node in the subnet
        :param subnet_id: subnet ID
        """
        # compose call
        call = self.interface.compose_call(
            call_module="Network",
            call_function="update_bootnodes",
            call_params={
                "subnet_id": subnet_id,
                "add": sorted(set(add)),
                "remove": sorted(set(remove)),
            },
        )

        # create signed extrinsic
        extrinsic = self.interface.create_signed_extrinsic(call=call, keypair=self.keypair)

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def submit_extrinsic():
            try:
                with self.interface as _interface:
                    receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
                    return receipt
            except SubstrateRequestException as e:
                logger.error("Failed to send: {}".format(e))

        return submit_extrinsic()

    def get_subnet_key_types(
        self,
        subnet_id: int,
    ) -> ExtrinsicReceipt:
        """
        Query hotkey by subnet node ID

        :param hotkey: Hotkey of subnet node
        """

        @retry(wait=wait_fixed(BLOCK_SECS + 1), stop=stop_after_attempt(4))
        def make_query():
            try:
                with self.interface as _interface:
                    result = _interface.query("Network", "SubnetKeyTypes", [subnet_id])
                    return result
            except SubstrateRequestException as e:
                logger.error("Failed to get rpc request: {}".format(e))

        return make_query()

    def get_overwatch_node_info_formatted(self, overwatch_node_id: int) -> Optional["OverwatchNodeInfo"]:
        """
        Get formatted list of Overwatch nodes

        :param overwatch_node_id: Overwatch node ID

        :returns: Overwatch node info
        """
        try:
            result = self.get_overwatch_node_info(overwatch_node_id)

            overwatch_node_info = OverwatchNodeInfo.from_vec_u8(result["result"])

            return overwatch_node_info
        except Exception:
            return None

    def get_all_overwatch_nodes_info_formatted(self) -> Optional[List["OverwatchNodeInfo"]]:
        try:
            result = self.get_all_overwatch_nodes_info()

            overwatch_nodes_info = OverwatchNodeInfo.list_from_vec_u8(result["result"])

            return overwatch_nodes_info
        except Exception:
            return None
