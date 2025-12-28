from dataclasses import dataclass, asdict
import json
import logging
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.db.database import RocksDB
import base58
import trio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")

HEARTBEAT_TOPIC = "heartbeat"


@dataclass
class HeartbeatData:
    epoch: int
    subnet_id: int
    subnet_node_id: int

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(asdict(self))

    def to_bytes(self) -> bytes:
        """Serialize to bytes for pubsub."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: str) -> "HeartbeatData":
        """Deserialize from JSON string."""
        return cls(**json.loads(data))


async def receive_loop(
    subscription,
    termination_event,
    hypertensor: Hypertensor | LocalMockHypertensor,
    db: RocksDB,
):
    logger.info("Starting receive loop")
    while not termination_event.is_set():
        try:
            message = await subscription.get()
            logger.info(f"Received full message: {message}")
            logger.info(f"Received topicIDs: {message.topicIDs}")

            from_peer = base58.b58encode(message.from_id).decode()
            logger.info(f"From peer: {from_peer}")
            logger.info(f"Received message: {message.data.decode('utf-8')}")

            if message.topicIDs[0] == HEARTBEAT_TOPIC:
                logger.info("Message topic is heartbeat")
                epoch_data = hypertensor.get_epoch_data()
                epoch = epoch_data.epoch
                heartbeat_data = HeartbeatData.from_json(message.data.decode("utf-8"))
                logger.info(f"Heartbeat data: {heartbeat_data}")

                exists = (
                    db.nmap_get(HEARTBEAT_TOPIC, f"{epoch}:{from_peer}") is not None
                )

                if exists:
                    logger.info(
                        f"Heartbeat already exists for epoch: {epoch}, peer: {from_peer}"
                    )
                    continue

                db.nmap_set(
                    HEARTBEAT_TOPIC,
                    f"{epoch}:{from_peer}",
                    message.data.decode("utf-8"),
                )
                logger.info(
                    f"Heartbeat stored under nmap key {HEARTBEAT_TOPIC}:{epoch}:{from_peer}"
                )

        except Exception:
            logger.exception("Error in receive loop")
            await trio.sleep(1)


async def publish_loop(
    pubsub,
    topic,
    termination_event,
    subnet_id,
    subnet_node_id,
    hypertensor: Hypertensor | LocalMockHypertensor,
):
    """Continuously read input from user and publish to the topic."""
    logger.info("Starting publish loop...")

    await trio.sleep(1)

    while not termination_event.is_set():
        try:
            epoch_data = hypertensor.get_epoch_data()
            epoch = epoch_data.epoch
            seconds_remaining = epoch_data.seconds_remaining
            logger.info(f"Epoch: {epoch}, Seconds remaining: {seconds_remaining}")
            # Use trio's run_sync_in_worker_thread to avoid blocking the event loop
            # message = f"{MESSAGE} {i}"
            message = HeartbeatData(epoch, subnet_id, subnet_node_id).to_bytes()
            logger.info(f"Publishing message: {message}")
            await pubsub.publish(topic, message)
            logger.info(f"Published: {message}")
            await trio.sleep(10)
        except Exception:
            logger.exception("Error in publish loop")
            await trio.sleep(1)  # Avoid tight loop on error
