import logging
from typing import Callable

from libp2p.pubsub.pb import (
    rpc_pb2,
)
from libp2p.pubsub.pubsub import ID
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.heartbeat import HeartbeatData
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.subnet_info_tracker import SubnetInfoTracker

logger = logging.getLogger("util.pubsub_validation")


class HeartbeatValidator:
    def __init__(self, fn: Callable[[ID, rpc_pb2.Message], bool]):
        self.fn = fn

    @classmethod
    def from_predicate_class(cls, predicate_cls: type, *args, **kwargs) -> "HeartbeatValidator":
        """
        Example:
            HeartbeatValidator.from_predicate_class(
                host.get_id(),
                HeartbeatPredicate,
                subnet_info_tracker,
                hypertensor,
                subnet_id,
                proof_of_stake,
            )

        """
        predicate = predicate_cls(*args, **kwargs)
        return cls(predicate)

    def validate(self, peer_id: ID, msg: rpc_pb2.Message) -> bool:
        logger.info(f"HeartbeatValidator validate {peer_id}")
        return self.fn(peer_id, msg)


class HeartbeatPredicate:
    def __init__(
        self,
        my_peer_id: ID,
        subnet_info_tracker: SubnetInfoTracker,
        hypertensor: LocalMockHypertensor | Hypertensor,
        subnet_id: int,
        proof_of_stake: ProofOfStake,
    ):
        self.my_peer_id = my_peer_id
        self.subnet_info_tracker = subnet_info_tracker
        self.hypertensor = hypertensor
        self.subnet_id = subnet_id
        self.proof_of_stake = proof_of_stake

    def __call__(self, peer_id: ID, msg: rpc_pb2.Message) -> bool:
        try:
            # Ignore validating our own messages
            if peer_id == self.my_peer_id:
                return True

            heartbeat_data = HeartbeatData.from_json(msg.data.decode("utf-8"))

            # Verify subnet ID
            if heartbeat_data.subnet_id != self.subnet_id:
                return False

            # Verify subnet node ID
            peer_node_id = self.subnet_info_tracker.get_peer_id_node_id(peer_id)
            if heartbeat_data.subnet_node_id != peer_node_id:
                return False

            # Verify epoch
            current_epoch = self.hypertensor.get_subnet_epoch_data(self.subnet_info_tracker.slot).epoch
            same_epoch = heartbeat_data.epoch == current_epoch
            if not same_epoch:
                logger.debug(
                    f"Heartbeat validation, peer_id {peer_id}, heartbeat epoch {heartbeat_data.epoch}, current_epoch {current_epoch}"  # noqa: E501
                )
                return False

            # Verify proof of stake
            pos = self.proof_of_stake.proof_of_stake(peer_id)
            if not pos:
                logger.debug(f"Heartbeat validation, peer_id {peer_id}, proof of stake: {pos}")
                return False

            return True
        except Exception as e:
            logger.warning(f"Heartbeat validation failed: {e}", exc_info=True)
            return False
