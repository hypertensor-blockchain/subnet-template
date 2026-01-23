import logging
from typing import Awaitable, Callable

from libp2p.pubsub.pb import (
    rpc_pb2,
)
from libp2p.pubsub.pubsub import ID
from pydantic import ValidationError

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.hypertensor.subnet_info_tracker import SubnetInfoTracker
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.pubsub.heartbeat import HeartbeatData

logger = logging.getLogger("util.pubsub_validation")


class AsyncPubsubTopicValidator:
    """
    Validator for Puspub heartbeat messages.

    Create a class that implements __call__ to validate messages.

    Example:
        pubsub = Pubsub(...)
        pubsub.set_topic_validator(
            HEARTBEAT_TOPIC,
            AsyncPubsubTopicValidator.from_predicate_class(
                AsyncHeartbeatMsgValidator,
                host.get_id(),
                subnet_info_tracker,
                self.hypertensor,
                self.subnet_id,
                proof_of_stake,
            ).validate,
            is_async_validator=True,
        )

    """

    def __init__(self, fn: Callable[[ID, rpc_pb2.Message], Awaitable[bool]]):
        self.fn = fn

    @classmethod
    def from_predicate_class(cls, predicate_cls: type, *args, **kwargs) -> "AsyncPubsubTopicValidator":
        """
        Example:
            AsyncPubsubTopicValidator.from_predicate_class(
                host.get_id(),
                AsyncHeartbeatMsgValidator,
                subnet_info_tracker,
                hypertensor,
                subnet_id,
                proof_of_stake,
            )

        """
        predicate = predicate_cls(*args, **kwargs)
        return cls(predicate)

    async def validate(self, peer_id: ID, msg: rpc_pb2.Message) -> bool:
        return await self.fn(peer_id, msg)


class SyncPubsubTopicValidator:
    """
    Validator for Puspub heartbeat messages.

    Create a class that implements __call__ to validate messages.

    Example:
        pubsub = Pubsub(...)
        pubsub.set_topic_validator(
            HEARTBEAT_TOPIC,
            SyncPubsubTopicValidator.from_predicate_class(
                AsyncHeartbeatMsgValidator,
                host.get_id(),
                subnet_info_tracker,
                self.hypertensor,
                self.subnet_id,
                proof_of_stake,
            ).validate,
            is_async_validator=True,
        )

    """

    def __init__(self, fn: Callable[[ID, rpc_pb2.Message], bool]):
        self.fn = fn

    @classmethod
    def from_predicate_class(cls, predicate_cls: type, *args, **kwargs) -> "SyncPubsubTopicValidator":
        """
        Example:
            AsyncPubsubTopicValidator.from_predicate_class(
                host.get_id(),
                AsyncHeartbeatMsgValidator,
                subnet_info_tracker,
                hypertensor,
                subnet_id,
                proof_of_stake,
            )

        """
        predicate = predicate_cls(*args, **kwargs)
        return cls(predicate)

    def validate(self, peer_id: ID, msg: rpc_pb2.Message) -> bool:
        return self.fn(peer_id, msg)


class AsyncHeartbeatMsgValidator:
    """
    Predicate for Pushsub heartbeat messages.

    Pushsub validator calls __call__ to validate messages.
    """

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

    async def __call__(self, forwarder_peer_id: ID, msg: rpc_pb2.Message) -> bool:
        try:
            # TODO: Ensure forwarder peer ID is in subnet (main, bootnode, or client)

            # Get originator peer ID
            from_peer_id = ID(msg.from_id)

            # Ignore validating our own messages
            if from_peer_id.__eq__(self.my_peer_id):
                return True

            try:
                heartbeat_data = HeartbeatData.from_json(msg.data.decode("utf-8"))
            except (ValidationError, Exception) as e:
                logger.warning(f"HeartbeatData validation failed: {e}", exc_info=True)
                return False

            logger.debug(f"AsyncHeartbeatMsgValidator validate {from_peer_id}, HeartbeatData {heartbeat_data}")

            # Verify subnet ID
            if heartbeat_data.subnet_id != self.subnet_id:
                return False

            # Verify subnet node ID
            peer_node_id = await self.subnet_info_tracker.get_peer_id_node_id(from_peer_id, force=True)
            if heartbeat_data.subnet_node_id != peer_node_id:
                # logger.info(
                #     f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat subnet node ID {heartbeat_data.subnet_node_id}, expected subnet node ID {peer_node_id}"  # noqa: E501
                # )
                return False

            # Verify epoch
            current_epoch = self.hypertensor.get_subnet_epoch_data(self.subnet_info_tracker.slot).epoch
            same_epoch = heartbeat_data.epoch == current_epoch
            if not same_epoch:
                # logger.info(
                #     f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat epoch {heartbeat_data.epoch}, current_epoch {current_epoch}"  # noqa: E501
                # )
                return False

            # Verify proof of stake
            pos = self.proof_of_stake.proof_of_stake(from_peer_id)
            if not pos:
                # logger.info(f"Heartbeat validation, from_peer_id {from_peer_id}, proof of stake: {pos}")
                return False

            return True
        except Exception as e:
            logger.warning(f"Heartbeat validation failed: {e}", exc_info=True)
            return False


class SyncHeartbeatMsgValidator:
    """
    Predicate for Pushsub heartbeat messages.

    Pushsub validator calls __call__ to validate messages.
    """

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
        self.last_epoch = None
        self._seen_heartbeats: set[str] = set()  # e.g.: "epoch:peer_id"

    def __call__(self, forwarder_peer_id: ID, msg: rpc_pb2.Message) -> bool:
        try:
            # TODO: Ensure forwarder peer ID is in subnet (main, bootnode, or client)

            # Get originator peer ID
            from_peer_id = ID(msg.from_id)

            # Ignore validating our own messages
            if from_peer_id.__eq__(self.my_peer_id):
                return True

            try:
                heartbeat_data = HeartbeatData.from_json(msg.data.decode("utf-8"))
            except (ValidationError, Exception) as e:
                logger.warning(f"HeartbeatData validation failed: {e}", exc_info=True)
                return False

            logger.debug(f"SyncHeartbeatMsgValidator validate {from_peer_id}, HeartbeatData {heartbeat_data}")

            # Verify subnet ID
            if heartbeat_data.subnet_id != self.subnet_id:
                return False

            # Verify from peer ID subnet node ID
            peer_node_id = self.subnet_info_tracker.get_peer_id_node_id_sync(from_peer_id, force=True)
            if heartbeat_data.subnet_node_id != peer_node_id:
                logger.debug(
                    f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat subnet node ID {heartbeat_data.subnet_node_id}, expected subnet node ID {peer_node_id}"  # noqa: E501
                )
                return False

            # Verify epoch
            current_epoch = self.hypertensor.get_subnet_epoch_data(self.subnet_info_tracker.slot).epoch
            if self.last_epoch is not None and current_epoch != self.last_epoch:
                self._seen_heartbeats.clear()
                self.last_epoch = current_epoch

            same_epoch = heartbeat_data.epoch == current_epoch
            if not same_epoch:
                logger.debug(
                    f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat epoch {heartbeat_data.epoch}, current_epoch {current_epoch}"  # noqa: E501
                )
                return False

            # Verify in-memory heartbeat
            key = f"{current_epoch}:{from_peer_id}"
            if key in self._seen_heartbeats:
                logger.debug(
                    f"Heartbeat validation, from_peer_id {from_peer_id}, epoch {current_epoch}, duplicate heartbeat"
                )
                return False

            self._seen_heartbeats.add(key)

            # Verify proof of stake
            pos = self.proof_of_stake.proof_of_stake(from_peer_id)
            if not pos:
                logger.debug(f"Heartbeat validation, from_peer_id {from_peer_id}, proof of stake: {pos}")
                return False

            return True
        except Exception as e:
            logger.warning(f"Heartbeat validation failed: {e}", exc_info=True)
            return False
