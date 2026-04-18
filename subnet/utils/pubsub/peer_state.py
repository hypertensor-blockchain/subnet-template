from enum import Enum
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")


class ServerState(Enum):
    OFFLINE = 0
    JOINING = 1
    ONLINE = 2


class PeerRole(Enum):
    """
    Add custom roles, e.g. miner, validator, producer, etc.

    This logic can be stored and used for role permission based logic.
    """

    VALIDATOR = 0


class PeerStateData(BaseModel):
    uid: str  # UID required for any topics that may be duplicate within the TTL of the message
    epoch: int
    subnet_id: int
    subnet_node_id: int
    state: ServerState
    role: PeerRole

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
    def from_json(cls, data: str) -> "PeerStateData":
        """Deserialize from JSON string."""
        return cls.model_validate_json(data)


async def publish_peer_state(
    pubsub: Pubsub,
    topic: TProtocol,
    state: ServerState,
    role: PeerRole,
    subnet_id: int,
    subnet_node_id: int,
    key_pair: KeyPair,
    hypertensor: LocalMockHypertensor | Hypertensor,
    telemetry: Telemetry | None = None,
    log_level: int = logging.INFO,
):
    """Continuously publish peer state at regular intervals within each epoch."""
    try:
        while True:
            current_epoch = hypertensor.get_subnet_epoch_data(hypertensor.get_subnet_slot(subnet_id)).epoch

            message = PeerStateData(
                uid=secrets.token_hex(16),
                epoch=current_epoch,
                subnet_id=subnet_id,
                subnet_node_id=subnet_node_id,
                state=state,
                role=role,
            )

            message_bytes = message.to_bytes()

            logger.log(
                log_level,
                f"Publishing peer state {state} for epoch {current_epoch}",
            )
            await pubsub.publish(topic, message_bytes)
            if telemetry:
                await telemetry.emit_async(
                    "peer_state_sent",
                    message=message.to_json(),
                    message_size=len(message_bytes),
                )
                logger.log(log_level, f"Published: {message}")
            await trio.sleep(20)
    except Exception as e:
        logger.exception(f"Error publishing peer state, error={e}")


class PeerStatePublisherV1:
    def __init__(
        self,
        pubsub: Pubsub,
        topic: TProtocol,
        start_state: ServerState,
        start_role: PeerRole,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: LocalMockHypertensor | Hypertensor,
        telemetry: Telemetry | None = None,
        log_level: int = logging.INFO,
    ):
        self.pubsub = pubsub
        self.topic = topic
        self.state = start_state
        self.role = start_role
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.telemetry = telemetry
        self.log_level = log_level

    async def run(self):
        epoch_length = self.hypertensor.get_epoch_length()
        sleep_duration = (epoch_length * BLOCK_SECS) / 4
        while True:
            await self.publish()
            await trio.sleep(2)

    async def publish(self) -> None:
        try:
            current_epoch = self.hypertensor.get_subnet_epoch_data(
                self.hypertensor.get_subnet_slot(self.subnet_id)
            ).epoch

            message = PeerStateData(
                uid=secrets.token_hex(16),
                epoch=current_epoch,
                subnet_id=self.subnet_id,
                subnet_node_id=self.subnet_node_id,
                state=self.state,
                role=self.role,
            )

            message_bytes = message.to_bytes()

            logger.log(
                self.log_level,
                f"Publishing peer state {self.state} for epoch {current_epoch}",
            )
            await self.pubsub.publish(self.topic, message_bytes)
            await self._after_publish(message, message_bytes)
        except Exception as e:
            logger.exception(f"Error in publish peer state, error={e}")

    async def _after_publish(self, message: PeerStateData, message_bytes: bytes) -> None:
        if self.telemetry:
            await self.telemetry.emit_async(
                "peer_state_sent",
                message=message.to_json(),
                message_size=len(message_bytes),
            )
        logger.log(self.log_level, f"Published: {message}")
