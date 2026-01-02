import logging
import time
from typing import Dict

from libp2p.peer.id import ID as PeerID
from subnet.hypertensor.chain_functions import Hypertensor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("proof-of-stake")


class ProofOfStake:
    def __init__(
        self,
        subnet_id: int,
        hypertensor: Hypertensor,
        min_class: int,
    ):
        super().__init__()

        self.subnet_id = subnet_id
        self.hypertensor = hypertensor
        self.peer_id_to_last_successful_pos: Dict[PeerID, float] = {}
        self.pos_success_cooldown = 300
        self.peer_id_to_last_failed_pos: Dict[PeerID, float] = {}
        self.pos_fail_cooldown: float = 300
        self.min_class = min_class

    def get_peer_id_last_success(self, peer_id: PeerID) -> float:
        return self.peer_id_to_last_successful_pos.get(peer_id, 0)

    def update_peer_id_success(self, peer_id: PeerID):
        self.peer_id_to_last_successful_pos[peer_id] = time.time()
        self.peer_id_to_last_failed_pos.pop(peer_id, None)

    def get_peer_id_last_fail(self, peer_id: PeerID) -> float:
        return self.peer_id_to_last_failed_pos.get(peer_id, 0)

    def update_peer_id_fail(self, peer_id: PeerID):
        self.peer_id_to_last_failed_pos[peer_id] = time.time()
        self.peer_id_to_last_successful_pos.pop(peer_id, None)

    def proof_of_stake(self, peer_id: PeerID) -> bool:
        now = time.time()

        # Recently failed — reject immediately
        last_fail = self.get_peer_id_last_fail(peer_id)
        if last_fail and now - last_fail < self.pos_fail_cooldown:
            logger.debug("Peer recently failed, rejecting")
            return False

        # Recent success — no need to check again
        last_success = self.get_peer_id_last_success(peer_id)
        if last_success and now - last_success < self.pos_success_cooldown:
            logger.debug("Peer recently succeeded, initiating cooldown")
            return True

        # On-chain proof of stake check
        peer_id_vec = self.to_vec_u8(peer_id.to_base58())
        _proof_of_stake = self.is_staked(peer_id_vec)

        if _proof_of_stake:
            self.update_peer_id_success(peer_id)
            return True
        else:
            self.update_peer_id_fail(peer_id)
            return False

    def to_vec_u8(self, string):
        """Get peer_id in Vec<u8> for blockchain"""
        return [ord(char) for char in string]

    def is_staked(self, peer_id_vector) -> bool:
        """
        Each subnet node must be staked
        """
        is_staked = self.is_peer_staked(peer_id_vector)

        return is_staked

    def is_peer_staked(self, peer_id_vector) -> bool:
        """
        Uses the Hypertensor `proof_of_stake` RPC method that checks
        for a subnet nodes peer_id and bootstrap_peer_id being staked
        """
        result = self.hypertensor.proof_of_stake(
            self.subnet_id, peer_id_vector, self.min_class
        )

        if "result" not in result:
            return False

        # must be True or False
        if result["result"] is not True and result["result"] is not False:
            return False

        return result["result"]
