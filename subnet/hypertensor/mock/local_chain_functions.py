import json
import logging
import random
import time
from typing import Any, List, Optional

from libp2p.peer.id import ID as PeerID

from subnet.hypertensor.chain_data import (
    AllSubnetBootnodes,
    Attest,
    AttestEntry,
    ConsensusData,
    OverwatchCommit,
    OverwatchNodeInfo,
    OverwatchReveals,
    SubnetInfo,
    SubnetNode,
    SubnetNodeConsensusData,
    SubnetNodeInfo,
)
from subnet.hypertensor.chain_functions import (
    EpochData,
    OverwatchEpochData,
    SubnetNodeClass,
    subnet_node_class_to_enum,
)
from subnet.hypertensor.config import BLOCK_SECS, EPOCH_LENGTH
from subnet.hypertensor.mock.mock_db import MockDatabase  # assume separate file
from subnet.utils.crypto.store_key import get_peer_id

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("local-hypertensor")


class LocalMockHypertensor:
    def __init__(
        self,
        subnet_id: int,
        peer_id: PeerID,
        subnet_node_id: int,
        coldkey: str,
        hotkey: str,
        bootnode_peer_id: str,
        client_peer_id: str,
        reset_db: bool = False,
        insert_mock_overwatch_node: bool = False,
        insert_mock_subnet_nodes: tuple[bool, int] = (False, 0),
    ):
        # Initialize database
        self.db = MockDatabase()
        if reset_db:
            logger.info("Resetting database")
            self.db.reset_database()

        if insert_mock_overwatch_node:
            # Insert mock overwatch node for testing (see overwatch node repo)
            self.insert_overwatch_node(
                overwatch_node_id=1,
                coldkey="",
                hotkey="",
                peer_id=PeerID.from_base58("12D3KooWHNjWMaBA4eW4KyrzPfduh6e7CQ91iqXfZ69ZLSNW1m6Q").to_base58(),
            )

        if insert_mock_subnet_nodes[0]:
            # Insert mock bootnodes
            try:
                bootnode_peer_id = get_peer_id("bootnode.key")
                self.db.insert_bootnode(
                    subnet_id=subnet_id,
                    peer_id=bootnode_peer_id.to_base58(),
                    bootnode=f"/ip4/127.0.0.1/tcp/38960/p2p/{bootnode_peer_id.to_base58()}",
                )
            except Exception as e:
                logger.error(f"Error inserting bootnode: {e}")
                pass

            # Insert mock subnet nodes for testing
            # Get all .key files from root directory in alphabetical order
            import glob
            import os

            root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            key_files = sorted(glob.glob(os.path.join(root_dir, "*.key")))

            # Insert subnet nodes up to the specified count
            id = 1
            for i in range(min(len(key_files), insert_mock_subnet_nodes[1])):
                key_path = key_files[i]
                if key_path is None:
                    break
                if "bootnode.key" in key_path:
                    continue
                if "overwatch.key" in key_path:
                    continue
                subnet_node_peer_id = get_peer_id(key_path)
                self.insert_subnet_node(
                    subnet_id=subnet_id,
                    subnet_node_id=id,
                    peer_id=subnet_node_peer_id.to_base58(),
                    bootnode_peer_id="",
                    client_peer_id="",
                )
                id += 1

        self.subnet_id = subnet_id
        self.peer_id = peer_id
        self.subnet_node_id = subnet_node_id
        self.coldkey = coldkey
        self.hotkey = hotkey
        self.bootnode_peer_id = bootnode_peer_id
        self.client_peer_id = client_peer_id

        # Only store if not bootnode, use `subnet_node_id=0` if bootnode
        if subnet_node_id != 0:
            # Register this node
            self.insert_subnet_node(
                subnet_id=1,
                subnet_node_id=self.subnet_node_id,
                peer_id=self.peer_id.to_base58(),
                bootnode_peer_id=self.bootnode_peer_id,
                client_peer_id=self.client_peer_id,
            )
        else:
            self.db.insert_bootnode(
                subnet_id=self.subnet_id,
                peer_id=self.peer_id.to_base58(),
                bootnode="",
            )

    def insert_subnet_node(
        self,
        subnet_id: int,
        subnet_node_id: int,
        peer_id: str,
        bootnode_peer_id: str = "",
        client_peer_id: str = "",
    ):
        self.db.insert_subnet_node(
            subnet_id=subnet_id,
            node_info=dict(
                subnet_node_id=subnet_node_id,
                coldkey="",
                hotkey="",
                peer_id=peer_id,
                bootnode_peer_id=bootnode_peer_id,
                client_peer_id=client_peer_id,
                bootnode="",
                identity="",
                classification={
                    "node_class": "Validator",
                    "start_epoch": self.get_epoch(),
                },
                delegate_reward_rate=0,
                last_delegate_reward_rate_update=0,
                unique="",
                non_unique="",
                stake_balance=int(1e18),
                total_node_delegate_stake_shares=int(1e18),
                node_delegate_stake_balance=int(1e18),
                coldkey_reputation={
                    "start_epoch": self.get_epoch(),
                    "score": int(1e18),
                    "lifetime_node_count": int(1e18),
                    "total_active_nodes": int(1e18),
                    "total_increases": int(1e18),
                    "total_decreases": int(1e18),
                    "average_attestation": int(1e18),
                    "last_validator_epoch": 0,
                    "ow_score": int(1e18),
                },
                subnet_node_reputation=int(1e18),
                node_slot_index=subnet_node_id,
                consecutive_idle_epochs=0,
                consecutive_included_epochs=0,
            ),
        )

    def insert_overwatch_node(
        self,
        overwatch_node_id: int,
        coldkey: str,
        hotkey: str,
        peer_id: str,
    ):
        self.db.insert_overwatch_node(
            overwatch_node_id=overwatch_node_id,
            node_info=dict(
                overwatch_node_id=overwatch_node_id,
                coldkey=coldkey,
                hotkey=hotkey,
                peer_ids=[(i, peer_id) for i in range(1, 16)],
                reputation={
                    "start_epoch": self.get_epoch(),
                    "score": int(1e18),
                    "lifetime_node_count": int(1e18),
                    "total_active_nodes": int(1e18),
                    "total_increases": int(1e18),
                    "total_decreases": int(1e18),
                    "average_attestation": int(1e18),
                    "last_validator_epoch": 0,
                    "ow_score": int(1e18),
                },
                account_overwatch_stake=int(1e18),
            ),
        )

    def propose_attestation(
        self,
        subnet_id: int,
        data,
        args: Optional[Any] = None,
        attest_data: Optional[Any] = None,
    ):
        epoch = self.get_epoch()
        subnet_nodes = self.db.get_all_subnet_nodes(subnet_id)
        validator_node_id = self.get_rewards_validator(subnet_id, epoch)
        proposal = {
            "validator_id": validator_node_id,
            "validator_epoch_progress": 0,
            "attests": [
                {
                    node["subnet_node_id"]: {
                        "block": 0,
                        "attestor_progress": 0,
                        "reward_factor": int(1e18),
                        "data": attest_data,
                    }
                }
                for node in subnet_nodes
                if node["subnet_node_id"] == validator_node_id
            ],
            "subnet_nodes": subnet_nodes,
            "prioritize_queue_node_id": None,
            "remove_queue_node_id": None,
            "data": data,
            "args": args,
        }

        self.db.insert_consensus_data(subnet_id, epoch, proposal)

        print("✅ Extrinsic Success")

        return proposal

    def attest(self, subnet_id: int, data: Optional[List[Any]] = None):
        """
        Append this peer's attestation data to the existing consensus record
        for the current epoch.
        """
        epoch = self.get_epoch()

        # Load existing consensus data for this subnet and epoch
        consensus = self.db.get_consensus_data(subnet_id, epoch)
        if consensus is None:
            raise ValueError(f"No consensus proposal found for subnet {subnet_id} epoch {epoch}")

        # Build this peer's attestation record
        attestation_entry = Attest(
            attestor_id=self.subnet_node_id,
            entry=AttestEntry(
                block=self.get_block_number(),
                attestor_progress=0,
                reward_factor=int(1e18),
                data=data or "",
            ),
        )

        # Append or update attestation
        updated_attests = consensus.get("attests", [])
        # Remove any existing entry for this same peer
        updated_attests = [a for a in updated_attests if str(self.subnet_node_id) not in map(str, a.keys())]
        updated_attests.append(attestation_entry)

        # Save updated record back to database
        consensus["attests"] = updated_attests

        print("✅ Extrinsic Success")

        self.db.insert_consensus_data(subnet_id, epoch, consensus)

    def get_consensus_data_formatted(self, subnet_id: int, epoch: int) -> Optional["ConsensusData"]:
        record = self.db.get_consensus_data(subnet_id, epoch)
        if record is None:
            return None

        # Convert subnet_nodes into SubnetNode dataclasses if available
        subnet_nodes_data = record.get("subnet_nodes", [])
        subnet_nodes: List[SubnetNode] = []

        # Handle if stored as JSON string
        if isinstance(subnet_nodes_data, str):
            import json

            try:
                subnet_nodes_data = json.loads(subnet_nodes_data)
            except Exception:
                return []

        # Map to dataclasses
        for node_dict in subnet_nodes_data:
            try:
                classification_data = node_dict.get("classification", {})

                if isinstance(classification_data, str):
                    try:
                        classification = json.loads(classification_data)
                    except json.JSONDecodeError:
                        classification = {}
                else:
                    classification = classification_data

                subnet_nodes.append(
                    SubnetNode(
                        id=node_dict.get("subnet_node_id"),
                        hotkey=node_dict.get("hotkey", ""),
                        peer_id=node_dict.get("peer_id", ""),
                        bootnode_peer_id=node_dict.get("bootnode_peer_id", ""),
                        bootnode=node_dict.get("bootnode", ""),
                        client_peer_id=node_dict.get("client_peer_id", ""),
                        classification=classification,
                        delegate_reward_rate=node_dict.get("delegate_reward_rate", 0),
                        last_delegate_reward_rate_update=node_dict.get("last_delegate_reward_rate_update", 0),
                        unique=node_dict.get("unique", ""),
                        non_unique=node_dict.get("non_unique", ""),
                    )
                )
            except Exception as e:
                print(f"[WARN] Failed to parse subnet node: {e}")

        raw_data = record.get("data", [])
        consensus_scores: List[SubnetNodeConsensusData] = [
            SubnetNodeConsensusData(subnet_node_id=item["subnet_node_id"], score=item["score"]) for item in raw_data
        ]

        raw_attests = record.get("attests", [])

        attests: list[Attest] = []

        for item in raw_attests:
            # Each item is like {"6": {...}}
            attestor_id_str, entry_dict = next(iter(item.items()))
            attestor_id = int(attestor_id_str)

            attest = Attest.fix_decoded_values((attestor_id, entry_dict))
            attests.append(attest)

        # Return final ConsensusData object
        return ConsensusData(
            validator_id=record["validator_id"],
            block=self.get_block_number(),
            validator_epoch_progress=record["validator_epoch_progress"],
            validator_reward_factor=int(1e18),
            attests=attests,
            subnet_nodes=subnet_nodes,
            prioritize_queue_node_id=record.get("prioritize_queue_node_id"),
            remove_queue_node_id=record.get("remove_queue_node_id"),
            data=consensus_scores,
            args=record.get("args"),
        )

    def get_block_number(self) -> int:
        now = time.time()
        return int(now // BLOCK_SECS)

    def get_epoch_length(self):
        return EPOCH_LENGTH

    def get_epoch(self):
        current_block = self.get_block_number()
        epoch_length = self.get_epoch_length()
        return current_block // epoch_length

    def proof_of_stake(self, subnet_id: int, peer_id: str, min_class: int):
        return {"result": True}

    def get_subnet_slot(self, subnet_id: int):
        return 3

    def get_epoch_data(self) -> EpochData:
        current_block = self.get_block_number()
        epoch_length = self.get_epoch_length()
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
        current_block = self.get_block_number()
        epoch_length = self.get_epoch_length()

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

    def get_overwatch_epoch_multiplier(self):
        return 6

    def get_overwatch_commit_cutoff_percent(self):
        return 0.9e18

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
        overwatch_epoch_length = epoch_length * multiplier
        overwatch_epoch = current_block // overwatch_epoch_length
        cutoff_percentage = float(self.get_overwatch_commit_cutoff_percent() / 1e18)
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

    def get_rewards_validator(self, subnet_id: int, epoch: int):
        subnet_nodes = self.get_min_class_subnet_nodes_formatted(subnet_id, epoch, SubnetNodeClass.Validator)

        # TODO: Random selection, save current epochs chosen node in db
        # random_subnet_node = random.choice(subnet_nodes)
        # return random_subnet_node.subnet_node_id

        return subnet_nodes[0].subnet_node_id

    def get_min_class_subnet_nodes_formatted(
        self, subnet_id: int, subnet_epoch: int, min_class: SubnetNodeClass
    ) -> List["SubnetNodeInfo"]:
        """
        Return all subnet nodes that meet or exceed the minimum classification
        requirement and have started on or before the given subnet_epoch.
        """
        try:
            subnet_nodes = self.db.get_all_subnet_nodes(subnet_id)
            qualified_nodes = []

            for node_dict in subnet_nodes:
                classification_data = node_dict.get("classification", {})

                if isinstance(classification_data, str):
                    try:
                        classification = json.loads(classification_data)
                    except json.JSONDecodeError:
                        classification = {}
                else:
                    classification = classification_data

                coldkey_reputation_data = node_dict.get("coldkey_reputation", {})

                if isinstance(coldkey_reputation_data, str):
                    try:
                        coldkey_reputation = json.loads(coldkey_reputation_data)
                    except json.JSONDecodeError:
                        coldkey_reputation = {}
                else:
                    coldkey_reputation = coldkey_reputation_data

                node_class_name = classification.get("node_class", "Validator")
                start_epoch = classification.get("start_epoch", 0)

                node_class_enum = subnet_node_class_to_enum(node_class_name)

                if node_class_enum.value >= min_class.value and start_epoch <= subnet_epoch:
                    qualified_nodes.append(
                        SubnetNodeInfo(
                            subnet_id=self.subnet_id,
                            subnet_node_id=node_dict["subnet_node_id"],
                            coldkey=node_dict["coldkey"],
                            hotkey=node_dict["hotkey"],
                            peer_id=node_dict["peer_id"],
                            bootnode_peer_id=node_dict["bootnode_peer_id"],
                            client_peer_id=node_dict["client_peer_id"],
                            bootnode=node_dict["bootnode"],
                            identity=node_dict["identity"],
                            classification=classification,
                            delegate_reward_rate=0,
                            last_delegate_reward_rate_update=0,
                            unique=node_dict["unique"],
                            non_unique=node_dict["non_unique"],
                            stake_balance=int(node_dict.get("stake_balance", 0)),
                            total_node_delegate_stake_shares=int(node_dict.get("total_node_delegate_stake_shares", 0)),
                            node_delegate_stake_balance=int(node_dict.get("node_delegate_stake_balance", 0)),
                            coldkey_reputation=coldkey_reputation,
                            subnet_node_reputation=int(node_dict.get("subnet_node_reputation", 0)),
                            node_slot_index=int(node_dict.get("node_slot_index", 0)),
                            consecutive_idle_epochs=int(node_dict.get("consecutive_idle_epochs", 0)),
                            consecutive_included_epochs=int(node_dict.get("consecutive_included_epochs", 0)),
                        )
                    )

            return qualified_nodes
        except Exception as e:
            logger.warning(f"[WARN] get_min_class_subnet_nodes_formatted error: {e}", exc_info=True)
            return []

    def get_subnet_nodes_info_formatted(self, subnet_id: int) -> List["SubnetNodeInfo"]:
        """
        Return all subnet nodes formatted.
        """
        try:
            subnet_nodes = self.db.get_all_subnet_nodes(subnet_id)
            qualified_nodes = []

            for node_dict in subnet_nodes:
                classification_data = node_dict.get("classification", {})

                if isinstance(classification_data, str):
                    try:
                        classification = json.loads(classification_data)
                    except json.JSONDecodeError:
                        classification = {}
                else:
                    classification = classification_data

                coldkey_reputation_data = node_dict.get("coldkey_reputation", {})

                if isinstance(coldkey_reputation_data, str):
                    try:
                        coldkey_reputation = json.loads(coldkey_reputation_data)
                    except json.JSONDecodeError:
                        coldkey_reputation = {}
                else:
                    coldkey_reputation = coldkey_reputation_data

                qualified_nodes.append(
                    SubnetNodeInfo(
                        subnet_id=self.subnet_id,
                        subnet_node_id=node_dict["subnet_node_id"],
                        coldkey=node_dict["coldkey"],
                        hotkey=node_dict["hotkey"],
                        peer_id=node_dict["peer_id"],
                        bootnode_peer_id=node_dict["bootnode_peer_id"],
                        client_peer_id=node_dict["client_peer_id"],
                        bootnode=node_dict["bootnode"],
                        identity=node_dict["identity"],
                        classification=classification,
                        delegate_reward_rate=0,
                        last_delegate_reward_rate_update=0,
                        unique=node_dict["unique"],
                        non_unique=node_dict["non_unique"],
                        stake_balance=int(node_dict.get("stake_balance", 0)),
                        total_node_delegate_stake_shares=int(node_dict.get("total_node_delegate_stake_shares", 0)),
                        node_delegate_stake_balance=int(node_dict.get("node_delegate_stake_balance", 0)),
                        coldkey_reputation=coldkey_reputation,
                        subnet_node_reputation=int(node_dict.get("subnet_node_reputation", 0)),
                        node_slot_index=int(node_dict.get("node_slot_index", 0)),
                        consecutive_idle_epochs=int(node_dict.get("consecutive_idle_epochs", 0)),
                        consecutive_included_epochs=int(node_dict.get("consecutive_included_epochs", 0)),
                    )
                )

            return qualified_nodes
        except Exception as e:
            logger.warning(f"[WARN] get_subnet_nodes_info_formatted error: {e}", exc_info=True)
            return []

    def get_overwatch_node_info_formatted(self, overwatch_node_id: int) -> Optional["OverwatchNodeInfo"]:
        try:
            overwatch_nodes = self.db.get_all_overwatch_nodes()

            if not overwatch_nodes:
                return None

            for overwatch_node in overwatch_nodes:
                if overwatch_node["overwatch_node_id"] == overwatch_node_id:
                    break

            if not overwatch_node:
                return None

            reputation_data = overwatch_node.get("reputation", {})

            if isinstance(reputation_data, str):
                try:
                    reputation = json.loads(reputation_data)
                except json.JSONDecodeError:
                    reputation = {}
            else:
                reputation = reputation_data

            peer_ids_data = overwatch_node.get("peer_ids", [])

            if isinstance(peer_ids_data, str):
                try:
                    peer_ids_raw = json.loads(peer_ids_data)
                except json.JSONDecodeError:
                    peer_ids_raw = []
            else:
                peer_ids_raw = peer_ids_data

            if isinstance(peer_ids_raw, dict):
                peer_ids = [(int(k), v) for k, v in peer_ids_raw.items()]
            else:
                peer_ids = [tuple(item) for item in peer_ids_raw]

            return OverwatchNodeInfo(
                overwatch_node_id=overwatch_node["overwatch_node_id"],
                coldkey=overwatch_node["coldkey"],
                hotkey=overwatch_node["hotkey"],
                peer_ids=peer_ids,
                reputation=reputation,
                account_overwatch_stake=int(overwatch_node.get("account_overwatch_stake", 0)),
            )
        except Exception as e:
            logger.warning(f"[WARN] get_overwatch_node_info_formatted error: {e}", exc_info=True)
            return None

    def get_all_overwatch_nodes_info_formatted(self) -> Optional[List["OverwatchNodeInfo"]]:
        try:
            overwatch_nodes = self.db.get_all_overwatch_nodes()
            all_overwatch_nodes = []

            for node_dict in overwatch_nodes:
                reputation_data = node_dict.get("reputation", {})

                if isinstance(reputation_data, str):
                    try:
                        reputation = json.loads(reputation_data)
                    except json.JSONDecodeError:
                        reputation = {}
                else:
                    reputation = reputation_data

                peer_ids_data = node_dict.get("peer_ids", [])

                if isinstance(peer_ids_data, str):
                    try:
                        peer_ids_raw = json.loads(peer_ids_data)
                    except json.JSONDecodeError:
                        peer_ids_raw = []
                else:
                    peer_ids_raw = peer_ids_data

                if isinstance(peer_ids_raw, dict):
                    peer_ids = [(int(k), v) for k, v in peer_ids_raw.items()]
                else:
                    peer_ids = [tuple(item) for item in peer_ids_raw]

                all_overwatch_nodes.append(
                    OverwatchNodeInfo(
                        overwatch_node_id=int(node_dict["overwatch_node_id"]),
                        coldkey=node_dict["coldkey"],
                        hotkey=node_dict["hotkey"],
                        peer_ids=peer_ids,
                        reputation=reputation,
                        account_overwatch_stake=int(node_dict.get("account_overwatch_stake", 0)),
                    )
                )

            return all_overwatch_nodes
        except Exception as e:
            logger.warning(f"[WARN] get_all_overwatch_nodes_info_formatted error: {e}", exc_info=True)
            return []

    def get_validators_and_attestors(
        self,
        subnet_id: int,
    ):
        try:
            subnet_nodes = self.db.get_all_subnet_nodes(subnet_id)
            qualified_nodes = []

            for node_dict in subnet_nodes:
                classification_data = node_dict.get("classification", {})

                if isinstance(classification_data, str):
                    try:
                        classification = json.loads(classification_data)
                    except json.JSONDecodeError:
                        classification = {}
                else:
                    classification = classification_data

                coldkey_reputation_data = node_dict.get("coldkey_reputation", {})

                if isinstance(coldkey_reputation_data, str):
                    try:
                        coldkey_reputation = json.loads(coldkey_reputation_data)
                    except json.JSONDecodeError:
                        coldkey_reputation = {}
                else:
                    coldkey_reputation = coldkey_reputation_data

                node_class_name = classification.get("node_class", "Validator")

                node_class_enum = subnet_node_class_to_enum(node_class_name)

                if node_class_enum.value >= 3:
                    qualified_nodes.append(
                        SubnetNodeInfo(
                            subnet_id=self.subnet_id,
                            subnet_node_id=node_dict["subnet_node_id"],
                            coldkey=node_dict["coldkey"],
                            hotkey=node_dict["hotkey"],
                            peer_id=node_dict["peer_id"],
                            bootnode_peer_id=node_dict["bootnode_peer_id"],
                            client_peer_id=node_dict["client_peer_id"],
                            bootnode=node_dict["bootnode"],
                            identity=node_dict["identity"],
                            classification=classification,
                            delegate_reward_rate=0,
                            last_delegate_reward_rate_update=0,
                            unique=node_dict["unique"],
                            non_unique=node_dict["non_unique"],
                            stake_balance=int(node_dict.get("stake_balance", 0)),
                            total_node_delegate_stake_shares=int(node_dict.get("total_node_delegate_stake_shares", 0)),
                            node_delegate_stake_balance=0,
                            coldkey_reputation=coldkey_reputation,
                            subnet_node_reputation=int(node_dict.get("subnet_node_reputation", 0)),
                            node_slot_index=0,
                            consecutive_idle_epochs=0,
                            consecutive_included_epochs=0,
                        )
                    )
            return qualified_nodes
        except Exception as e:
            logger.warning(f"[WARN] get_min_class_subnet_nodes_formatted error: {e}", exc_info=True)
            return []

    def get_formatted_subnet_info(self, subnet_id: int) -> Optional["SubnetInfo"]:
        return SubnetInfo(
            id=self.subnet_id,
            friendly_id=self.subnet_id,
            name="subnet-name",
            repo="subnet-repo",
            description="subnet-description",
            misc="subnet-misc",
            state="Active",
            start_epoch=0,
            churn_limit=10,
            churn_limit_multiplier=1,
            min_stake=0,
            max_stake=0,
            queue_immunity_epochs=0,
            target_node_registrations_per_epoch=0,
            node_registrations_this_epoch=0,
            subnet_node_queue_epochs=0,
            idle_classification_epochs=0,
            included_classification_epochs=0,
            delegate_stake_percentage=0,
            last_delegate_stake_rewards_update=0,
            node_burn_rate_alpha=0,
            current_node_burn_rate=0,
            initial_coldkeys=[],
            initial_coldkey_data=[],
            max_registered_nodes=0,
            owner="000000000000000000000000000000000000000000000000",
            pending_owner="000000000000000000000000000000000000000000000000",
            registration_epoch=0,
            prev_pause_epoch=0,
            key_types=0,
            slot_index=3,
            slot_assignment=0,
            subnet_node_min_weight_decrease_reputation_threshold=0,
            reputation=0,
            min_subnet_node_reputation=0,
            absent_decrease_reputation_factor=0,
            included_increase_reputation_factor=0,
            below_min_weight_decrease_reputation_factor=0,
            non_attestor_decrease_reputation_factor=0,
            non_consensus_attestor_decrease_reputation_factor=0,
            validator_absent_subnet_node_reputation_factor=0,
            validator_non_consensus_subnet_node_reputation_factor=0,
            bootnode_access=[],
            bootnodes=[],
            total_nodes=0,
            total_active_nodes=0,
            total_electable_nodes=0,
            current_min_delegate_stake=0,
            total_subnet_stake=0,
            total_subnet_delegate_stake_shares=0,
            total_subnet_delegate_stake_balance=0,
        )

    def get_bootnodes(
        self,
        subnet_id: int,
    ):
        try:
            subnet_bootnodes = []
            bootnodes = self.db.get_all_bootnodes(subnet_id)

            for bootnode_dict in bootnodes:
                subnet_bootnodes.append((bootnode_dict["peer_id"], bootnode_dict["bootnode"]))

            subnet_nodes = self.db.get_all_subnet_nodes(subnet_id)
            node_bootnodes = []
            registered_bootnodes = []

            for node_dict in subnet_nodes:
                classification_data = node_dict.get("classification", {})

                if isinstance(classification_data, str):
                    try:
                        classification = json.loads(classification_data)
                    except json.JSONDecodeError:
                        classification = {}
                else:
                    classification = classification_data

                node_class_name = classification.get("node_class", "Validator")
                start_epoch = classification.get("start_epoch", 0)

                node_class_enum = subnet_node_class_to_enum(node_class_name)

                if node_class_enum.value > SubnetNodeClass.Registered.value:
                    node_bootnodes.append((node_dict["peer_id"], node_dict["bootnode"]))
                else:
                    registered_bootnodes.append((node_dict["peer_id"], node_dict["bootnode"]))

            return (
                subnet_bootnodes,
                node_bootnodes,
                registered_bootnodes,
            )
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

            return result
        except Exception:
            return None

    def get_bootnodes_formatted(self, subnet_id: int) -> Optional["AllSubnetBootnodes"]:
        """
        Get formatted list of subnet nodes classified as Validator

        :param subnet_id: subnet ID

        :returns: List of subnet node IDs
        """
        try:
            (
                subnet_bootnodes,
                node_bootnodes,
                registered_bootnodes,
            ) = self.get_bootnodes(subnet_id)

            all_subnet_bootnodes = AllSubnetBootnodes(
                subnet_bootnodes=subnet_bootnodes,
                node_bootnodes=node_bootnodes,
                registered_bootnodes=registered_bootnodes,
            )

            return all_subnet_bootnodes
        except Exception as e:
            logger.error(f"Error getting bootnodes: {e}")
            return None

    def register_overwatch_node(
        self,
        hotkey: str,
        stake_to_be_added: int,
    ):
        pass

    def commit_overwatch_subnet_weights(
        self,
        overwatch_node_id: int,
        commits: List["OverwatchCommit"],
    ):
        pass

    def reveal_overwatch_subnet_weights(
        self,
        overwatch_node_id: int,
        reveals: List["OverwatchReveals"],
    ):
        pass
