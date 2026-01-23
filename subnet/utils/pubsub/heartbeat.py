import logging
from typing import Any

from libp2p.custom_types import TProtocol
from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel
import trio

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.config import SECONDS_PER_EPOCH
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.hypertensor.subnet_info_tracker import SubnetInfoTracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")

HEARTBEAT_TOPIC = "heartbeat"


class HeartbeatData(BaseModel):
    epoch: int
    subnet_id: int
    subnet_node_id: int

    def model_post_init(self, __context: Any) -> None:
        assert self.epoch > 0, "Epoch must be greater than 0"
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


async def publish_loop(
    pubsub: Pubsub,
    topic: TProtocol,
    termination_event: trio.Event,
    subnet_id: int,
    subnet_node_id: int,
    subnet_info_tracker: SubnetInfoTracker,
    hypertensor: LocalMockHypertensor | Hypertensor,
):
    """Continuously read input from user and publish to the topic."""
    logger.info("Starting publish loop...")

    await trio.sleep(1)

    last_epoch = None

    while not termination_event.is_set():
        try:
            # current_epoch = subnet_info_tracker.epoch_data.epoch
            current_epoch = hypertensor.get_subnet_epoch_data(subnet_info_tracker.slot).epoch

            # One heartbeat per epoch
            # if current_epoch != last_epoch:
            last_epoch = current_epoch

            message = HeartbeatData(epoch=current_epoch, subnet_id=subnet_id, subnet_node_id=subnet_node_id).to_bytes()

            logger.debug(f"Publishing message: {message}")
            await pubsub.publish(topic, message)
            logger.debug(f"Published: {message}")

            # await trio.sleep(
            #     subnet_info_tracker.get_seconds_remaining_until_next_epoch() + 0.5
            # )

            # Run heartbeat 4 times per epoch
            # await trio.sleep(SECONDS_PER_EPOCH / 4)
            await trio.sleep(3)
        except Exception as e:
            logger.exception(f"Error in publish loop, error={e}")
            await trio.sleep(1)  # Avoid tight loop on error

    # last_epoch = None
    # heartbeats_per_epoch = 4
    # total_epoch_heartbeats = 0

    # while not termination_event.is_set():
    #     try:
    #         # current_epoch = subnet_info_tracker.epoch_data.epoch
    #         current_epoch = hypertensor.get_subnet_epoch_data(
    #             subnet_info_tracker.slot
    #         ).epoch

    #         if total_epoch_heartbeats < heartbeats_per_epoch:
    #             total_epoch_heartbeats += 1
    #             if current_epoch != last_epoch:
    #                 total_epoch_heartbeats = 0
    #                 last_epoch = current_epoch

    #             message = HeartbeatData(
    #                 current_epoch, subnet_id, subnet_node_id
    #             ).to_bytes()

    #             await pubsub.publish(topic, message)
    #             logger.info(f"Published: {message}")

    #         await trio.sleep(SECONDS_PER_EPOCH / heartbeats_per_epoch)
    #     except Exception:
    #         logger.exception("Error in publish loop")
    #         await trio.sleep(1)  # Avoid tight loop on error
