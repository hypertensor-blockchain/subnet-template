from enum import Enum
import logging
from typing import Awaitable, Callable

from libp2p.pubsub.pb import (
    rpc_pb2,
)
from libp2p.pubsub.pubsub import ID
from pydantic import ValidationError

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker
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
            SyncPubsubTopicValidator.from_predicate_class(
                host.get_id(),
                SyncHeartbeatMsgValidator,
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


class ValidationFailReason(Enum):
    WRONG_SUBNET_ID = "wrong_subnet_id"
    WRONG_SUBNET_NODE_ID = "wrong_subnet_node_id"
    WRONG_EPOCH = "wrong_epoch"
    PROOF_OF_STAKE_FAILURE = "proof_of_stake_failure"
    SEEN_BEFORE = "seen_before"
    INVALID_DATA = "invalid_data"


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
        proof_of_stake: ProofOfStake | None = None,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ):
        self.my_peer_id = my_peer_id
        self.subnet_info_tracker = subnet_info_tracker
        self.hypertensor = hypertensor
        self.subnet_id = subnet_id
        self.proof_of_stake = proof_of_stake
        self.telemetry = telemetry
        self.log_level = log_level

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
                await _async_validation_fail(
                    forwarder_peer_id,
                    from_peer_id,
                    None,
                    ValidationFailReason.INVALID_DATA,
                    self.telemetry,
                )
                return False

            # Verify subnet ID
            if heartbeat_data.subnet_id != self.subnet_id:
                logger.debug(
                    f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat {heartbeat_data}, expected subnet ID {self.subnet_id}"  # noqa: E501
                )
                await _async_validation_fail(
                    forwarder_peer_id,
                    from_peer_id,
                    heartbeat_data,
                    ValidationFailReason.WRONG_SUBNET_ID,
                    self.telemetry,
                )
                return False

            # Verify subnet node ID
            peer_node_id = await self.subnet_info_tracker.get_peer_id_node_id(from_peer_id, force=True)
            if heartbeat_data.subnet_node_id != peer_node_id:
                logger.debug(
                    f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat {heartbeat_data}, expected subnet node ID {peer_node_id}"  # noqa: E501
                )
                await _async_validation_fail(
                    forwarder_peer_id,
                    from_peer_id,
                    heartbeat_data,
                    ValidationFailReason.WRONG_SUBNET_NODE_ID,
                    self.telemetry,
                )
                return False

            # Verify epoch
            current_epoch = self.hypertensor.get_subnet_epoch_data(
                self.hypertensor.get_subnet_slot(self.subnet_id)
            ).epoch
            same_epoch = heartbeat_data.epoch == current_epoch
            if not same_epoch:
                logger.debug(
                    f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat {heartbeat_data}, current_epoch {current_epoch}"  # noqa: E501
                )
                await _async_validation_fail(
                    forwarder_peer_id,
                    from_peer_id,
                    heartbeat_data,
                    ValidationFailReason.WRONG_EPOCH,
                    self.telemetry,
                )
                return False

            # Verify proof of stake
            if self.proof_of_stake is not None:
                pos = self.proof_of_stake.proof_of_stake(from_peer_id)
                if not pos:
                    logger.debug(
                        f"Heartbeat validation, from_peer_id {from_peer_id}, heartbeat {heartbeat_data}, proof of stake: {pos}"  # noqa: E501
                    )
                    await _async_validation_fail(
                        forwarder_peer_id,
                        from_peer_id,
                        heartbeat_data,
                        ValidationFailReason.PROOF_OF_STAKE_FAILURE,
                        self.telemetry,
                    )
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
        proof_of_stake: ProofOfStake | None = None,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ):
        self.my_peer_id = my_peer_id
        self.subnet_info_tracker = subnet_info_tracker
        self.hypertensor = hypertensor
        self.subnet_id = subnet_id
        self.proof_of_stake = proof_of_stake
        self.last_epoch = None
        self.telemetry = telemetry
        self.log_level = log_level

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
                _validation_fail(
                    forwarder_peer_id, from_peer_id, None, ValidationFailReason.INVALID_DATA, self.telemetry
                )
                return False

            logger.log(
                self.log_level, f"SyncHeartbeatMsgValidator validate {from_peer_id}, HeartbeatData {heartbeat_data}"
            )

            # Verify subnet ID
            if heartbeat_data.subnet_id != self.subnet_id:
                _validation_fail(forwarder_peer_id, from_peer_id, heartbeat_data, ValidationFailReason.WRONG_SUBNET_ID)
                return False

            # Verify from peer ID subnet node ID
            peer_node_id = self.subnet_info_tracker.get_peer_id_node_id_sync(from_peer_id, force=True)
            if heartbeat_data.subnet_node_id != peer_node_id:
                _validation_fail(
                    forwarder_peer_id,
                    from_peer_id,
                    heartbeat_data,
                    ValidationFailReason.WRONG_SUBNET_NODE_ID,
                    self.telemetry,
                )
                return False

            # Verify epoch
            current_epoch = self.hypertensor.get_subnet_epoch_data(self.subnet_info_tracker.get_subnet_slot()).epoch
            if self.last_epoch is not None and current_epoch != self.last_epoch:
                self.last_epoch = current_epoch

            same_epoch = heartbeat_data.epoch == current_epoch
            if not same_epoch:
                _validation_fail(
                    forwarder_peer_id, from_peer_id, heartbeat_data, ValidationFailReason.WRONG_EPOCH, self.telemetry
                )
                return False

            # Verify proof of stake
            if self.proof_of_stake is not None:
                pos = self.proof_of_stake.proof_of_stake(from_peer_id)
                if not pos:
                    _validation_fail(
                        forwarder_peer_id,
                        from_peer_id,
                        heartbeat_data,
                        ValidationFailReason.PROOF_OF_STAKE_FAILURE,
                        self.telemetry,
                    )
                    return False

            return True
        except Exception as e:
            logger.exception(f"Heartbeat validation failed: {e}")
            return False


def _validation_fail(
    forwarder_peer_id: ID,
    from_peer_id: ID,
    heartbeat_data: HeartbeatData | None,
    reason: str,
    telemetry: Telemetry | None = None,
) -> bool:
    logger.warning(
        f"Heartbeat validation failed, forwarder_peer_id {forwarder_peer_id}, from_peer_id {from_peer_id}, heartbeat {heartbeat_data}, reason: {reason}"
    )

    if heartbeat_data is not None:
        # Store failure reason in db
        # Ensure we have a created_at parameter in the db
        ...
    else:
        # Store failure reason in db
        # Ensure we have a created_at parameter in the db
        ...

    if telemetry:
        telemetry.emit(
            "heartbeat_validation_failed", from_peer_id=from_peer_id.to_string(), reason=reason, data=heartbeat_data
        )

    return False


async def _async_validation_fail(
    forwarder_peer_id: ID,
    from_peer_id: ID,
    heartbeat_data: HeartbeatData | None,
    reason: str,
    telemetry: Telemetry | None = None,
) -> bool:
    logger.warning(
        f"Heartbeat validation failed, forwarder_peer_id {forwarder_peer_id}, from_peer_id {from_peer_id}, heartbeat {heartbeat_data}, reason: {reason}"
    )

    if heartbeat_data is not None:
        # Store failure reason in db
        # Ensure we have a created_at parameter in the db
        ...
    else:
        # Store failure reason in db
        # Ensure we have a created_at parameter in the db
        ...

    # Optionally emit telemetry event
    if telemetry:
        await telemetry.emit_async(
            "heartbeat_validation_failed", peer_id=from_peer_id.to_string(), reason=reason, data=heartbeat_data
        )

    return False
