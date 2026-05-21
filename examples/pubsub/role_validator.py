from libp2p.peer.id import ID
from libp2p.pubsub.pb import (
    rpc_pb2,
)
import trio

from subnet.utils.pubsub.pubsub_validation import SyncPubsubTopicValidator
from subnet.utils.pubsub.templates.gossip_receiver_template import GossipReceiverTemplate, GossipTopicConfig


async def handle_score_message(from_peer_id: ID, message: rpc_pb2.Message) -> None:
    payload = message.data.decode("utf-8")


def validate_score(from_peer_id: ID, message: rpc_pb2.Message) -> bool:
    return bool(message.data)

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
                return False

            # Verify from peer ID subnet node ID
            peer_node_id = self.subnet_info_tracker.get_peer_id_node_id_sync(from_peer_id, force=True)
            if heartbeat_data.subnet_node_id != peer_node_id:
                return False

            # Verify epoch
            current_epoch = self.hypertensor.get_subnet_epoch_data(self.subnet_info_tracker.get_subnet_slot()).epoch
            if self.last_epoch is not None and current_epoch != self.last_epoch:
                self.last_epoch = current_epoch

            same_epoch = heartbeat_data.epoch == current_epoch
            if not same_epoch:
                return False

            # Verify proof of stake
            if self.proof_of_stake is not None:
                pos = self.proof_of_stake.proof_of_stake(from_peer_id)
                if not pos:
                    return False

            return True
        except Exception as e:
            logger.exception(f"Heartbeat validation failed: {e}")
            return False

termination_event = trio.Event()
receiver = GossipReceiverTemplate(
    pubsub=pubsub,
    termination_event=termination_event,
    topics_config=[
        GossipTopicConfig(
            topic="scores",
            topic_handler=handle_score_message,
            topic_validator=SyncPubsubTopicValidator.from_predicate_class(
                SyncPubsubTopicValidator,
                host.get_id(),
                subnet_info_tracker,
                self.hypertensor,
                self.subnet_id,
                proof_of_stake,
            ).validate,
        ),
    ],
)

async with trio.open_nursery() as nursery:
    nursery.start_soon(receiver.run)
    termination_event.set()
