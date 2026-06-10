import logging
import secrets
from typing import Any

from libp2p.crypto.keys import KeyPair
from libp2p.custom_types import TProtocol
from libp2p.peer.id import ID
from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel
import trio

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.config import BLOCK_SECS
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.logging_config import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

HEARTBEATS_PER_EPOCH = 1


class HeartbeatData(BaseModel):
    uid: str  # UID required for any topics that may be duplicate within the TTL of the message
    epoch: int
    subnet_id: int
    subnet_node_id: int

    def model_post_init(self, __context: Any) -> None:
        assert self.subnet_id > 0, "Subnet ID must be greater than 0"
        assert self.subnet_node_id > 0, "Subnet node ID must be greater than 0"

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return self.model_dump_json()

    def to_bytes(self) -> bytes:
        """Serialize to bytes for pubsub."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: str) -> "HeartbeatData":
        """Deserialize from JSON string."""
        return cls.model_validate_json(data)


async def publish_heartbeat_loop(
    pubsub: Pubsub,
    topic: TProtocol,
    termination_event: trio.Event,
    subnet_id: int,
    subnet_node_id: int,
    key_pair: KeyPair,
    hypertensor: LocalMockHypertensor | Hypertensor,
    telemetry: Telemetry | None = None,
    log_level: int = logging.DEBUG,
):
    """Continuously publish heartbeats at regular intervals within each epoch."""
    logger.log(log_level, "Starting publish heartbeat loop...")

    last_epoch = None
    heartbeat_count_in_epoch = 0

    # Small initial sleep to let things initialize
    await trio.sleep(1)

    while not termination_event.is_set():
        try:
            epoch_length = hypertensor.get_epoch_length()
            if epoch_length is None:
                epoch_length = 20

            current_epoch = hypertensor.get_subnet_epoch_data(hypertensor.get_subnet_slot(subnet_id)).epoch

            # Detect epoch change
            if current_epoch != last_epoch:
                logger.log(log_level, f"Publishing heartbeats for epoch {current_epoch}")
                last_epoch = current_epoch
                heartbeat_count_in_epoch = 0

            # Only send if we haven't exceeded heartbeats for this epoch
            if heartbeat_count_in_epoch < HEARTBEATS_PER_EPOCH:
                message = HeartbeatData(
                    uid=secrets.token_hex(16), epoch=current_epoch, subnet_id=subnet_id, subnet_node_id=subnet_node_id
                )

                message_bytes = message.to_bytes()

                logger.log(
                    log_level,
                    f"Publishing heartbeat {heartbeat_count_in_epoch + 1}/{HEARTBEATS_PER_EPOCH} for epoch {current_epoch}",
                )
                await pubsub.publish(topic, message_bytes)
                if telemetry:
                    await telemetry.emit_async(
                        "heartbeat_sent",
                        message=message.to_json(),
                        message_size=len(message_bytes),
                    )
                logger.log(log_level, f"Published: {message}")

                heartbeat_count_in_epoch += 1

            # Sleep for the interval between heartbeats
            # Divide epoch duration by number of heartbeats to get interval
            sleep_duration = (epoch_length * BLOCK_SECS) / HEARTBEATS_PER_EPOCH
            await trio.sleep(sleep_duration)

        except Exception as e:
            logger.exception(f"Error in publish loop, error={e}")
            await trio.sleep(1)  # Avoid tight loop on error


class HeartbeatPublisher:
    def __init__(
        self,
        pubsub: Pubsub,
        topic: TProtocol,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: LocalMockHypertensor | Hypertensor,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ):
        self.pubsub = pubsub
        self.topic = topic
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.telemetry = telemetry
        self.log_level = log_level

    async def run(self):
        epoch_length = self.hypertensor.get_epoch_length()
        sleep_duration = (epoch_length * BLOCK_SECS) / HEARTBEATS_PER_EPOCH
        while True:
            await self.publish()
            await trio.sleep(sleep_duration)

    async def publish(self) -> None:
        try:
            current_epoch = self.hypertensor.get_subnet_epoch_data(
                self.hypertensor.get_subnet_slot(self.subnet_id)
            ).epoch

            message = HeartbeatData(
                uid=secrets.token_hex(16),
                epoch=current_epoch,
                subnet_id=self.subnet_id,
                subnet_node_id=self.subnet_node_id,
            )

            message_bytes = message.to_bytes()

            logger.log(
                self.log_level,
                f"Publishing heartbeat for epoch {current_epoch}",
            )
            await self.pubsub.publish(self.topic, message_bytes)
            await self._after_publish(message, message_bytes)
        except Exception as e:
            logger.exception(f"Error in publish heartbeat, error={e}")

    async def _after_publish(self, message: HeartbeatData, message_bytes: bytes) -> None:
        if self.telemetry:
            await self.telemetry.emit_async(
                "heartbeat_sent",
                message=message.to_json(),
                message_size=len(message_bytes),
            )
        logger.log(self.log_level, f"Published: {message}")
