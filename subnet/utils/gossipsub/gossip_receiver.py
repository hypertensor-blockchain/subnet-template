import logging

from libp2p.abc import ISubscriptionAPI
from libp2p.peer.id import ID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pb import rpc_pb2
from libp2p.pubsub.pubsub import Pubsub
import trio

from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB
from subnet.utils.pubsub.heartbeat import HeartbeatData
from subnet.utils.pubsub.peer_state import PeerStateData
from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC, PEER_STATE_TOPIC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("gossip_topics")


class GossipReceiver:
    """
    Manages receiving gossip messages and logic for storing in the base path db

    Params:
        gossipsub: GossipSub instance
        pubsub: Pubsub instance
        termination_event: trio.Event to signal termination
        db: RocksDB instance
        topics: list of topic strings

    Usage:
        gossip = GossipReceiver(
            gossipsub,
            pubsub,
            termination_event,
            db,
            [HEARTBEAT_TOPIC, "commit", "reveal", "custom"],
        )
        nursery.start_soon(gossip.run)  # Starts sync loop + receive loops

    Validating database entries:
        Use topic validators for validating a pubsub message.

        The use of this class is to handle what happens after the peer receives a message.

        Capture messages from topics in `_handle_message`

        See `_handle_*` functions for examples

    """

    def __init__(
        self,
        gossipsub: GossipSub,
        pubsub: Pubsub,
        termination_event: trio.Event,
        db: RocksDB,
        topics: list[str],
        telemetry: Telemetry | None = None,
        log_level: int = logging.INFO,
    ):
        self.gossipsub = gossipsub
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.db = db
        self.topics = topics
        self.telemetry = telemetry
        self._last_epoch: int | None = None
        self.log_level = log_level
        self._seen_heartbeats: set[str] = set()  # e.g.: "epoch:peer_id"
        self._cleanup_interval = 300

        """
        self._seen_commits: set[str] = set()  # e.g.: "epoch:peer_id"
        self._seen_reveals: set[str] = set()  # e.g.: "epoch:peer_id"
        self._seen_customs: set[str] = set()  # e.g.: "epoch:peer_id"
        """

    async def run(self) -> None:
        """
        Main entry point - starts sync loop and receive loops for all topics.

        Call this with: nursery.start_soon(gossip.run)
        """
        async with trio.open_nursery() as nursery:
            for topic in self.topics:
                subscription = await self.pubsub.subscribe(topic)
                logger.log(self.log_level, f"Subscribed to topic: {topic}")
                nursery.start_soon(self._receive_loop, subscription)

    async def _receive_loop(self, subscription: ISubscriptionAPI) -> None:
        """Receive loop for a single topic subscription."""
        logger.log(self.log_level, "Starting gossip receive loop")
        while not self.termination_event.is_set():
            try:
                message = await subscription.get()
                await self._handle_message(message)

            except Exception:
                logger.exception("Error in gossip receive loop")
                await trio.sleep(1)

    async def _cleanup_loop(self) -> None:
        """Periodically clear seen message caches."""
        while not self.termination_event.is_set():
            try:
                await trio.sleep(self._cleanup_interval)
                self._seen_heartbeats.clear()
                logger.log(self.log_level, "Cleared gossip seen caches")
            except Exception:
                logger.exception("Error in gossip cleanup loop")
                await trio.sleep(1)

    async def _handle_message(self, message: rpc_pb2.Message) -> None:
        """Handle incoming message based on topic."""
        from_peer = base58.b58encode(message.from_id).decode()
        topic = message.topicIDs[0] if message.topicIDs else None
        logger.log(self.log_level, f"From peer: {from_peer}, topic: {topic}")

        if topic == HEARTBEAT_TOPIC:
            await self._handle_heartbeat(message, from_peer)
        elif topic == PEER_STATE_TOPIC:
            await self._handle_peer_state(message, from_peer)

        # Add custom topics and handlers here
        """
        elif topic == COMMIT_TOPIC:
            await self._handle_commit(message, from_peer)
        elif topic == REVEAL_TOPIC:
            await self._handle_reveal(message, from_peer)
        elif topic == CUSTOM_TOPIC_1:
            await self._handle_custom_1(message, from_peer)
        elif topic == CUSTOM_TOPIC_2:
            await self._handle_custom_2(message, from_peer)
        """

    """Handle Heartbeat message"""

    async def _handle_heartbeat(self, message: rpc_pb2.Message, from_peer: str) -> None:
        """Store heartbeat message if not already stored for this epoch."""
        try:
            heartbeat_data = HeartbeatData.from_json(message.data.decode("utf-8"))
        except Exception as e:
            logger.warning(f"HeartbeatData validation failed: {e}")
            return

        key = f"{heartbeat_data.epoch}:{from_peer}"

        # Fast in-memory check
        if key in self._seen_heartbeats:
            logger.log(self.log_level, f"Heartbeat already seen (cached): {key}")
            return

        # Check if already exists
        if self.db.nmap_get(HEARTBEAT_TOPIC, key) is not None:
            logger.log(self.log_level, f"Heartbeat already exists: {key}")
            return

        # Store it
        self.db.nmap_set(HEARTBEAT_TOPIC, key, message.data.decode("utf-8"))
        logger.log(
            self.log_level,
            f"Heartbeat stored: {HEARTBEAT_TOPIC}:{key} for node ID {heartbeat_data.subnet_node_id}",
        )

        if self.telemetry:
            await self.telemetry.emit_async(
                "heartbeat_received",
                peer_id=from_peer,
                data=message.data.decode("utf-8"),
                message_size=len(message.data),
            )

        # Add to in-memory set
        self._seen_heartbeats.add(key)

    async def _handle_peer_state(self, message: rpc_pb2.Message, from_peer: str) -> None:
        """Store heartbeat message if not already stored for this epoch."""
        try:
            peer_state_data = PeerStateData.from_json(message.data.decode("utf-8"))
        except Exception as e:
            logger.warning(f"PeerStateData validation failed: {e}")
            return

        logger.log(
            self.log_level,
            f"Peer state received: {PEER_STATE_TOPIC} for node ID {peer_state_data.subnet_node_id}",
        )

        # -------------------------------------------
        # Do something here with the peer state data
        # ------------------------------------------

        if self.telemetry:
            await self.telemetry.emit_async(
                "peer_state_received",
                peer_id=from_peer,
                data=message.data.decode("utf-8"),
                message_size=len(message.data),
            )

    """Handle commit message (example)"""

    async def _handle_commit(self, message: rpc_pb2.Message, from_peer: str) -> None: ...

    """Handle reveal message (example)"""

    async def _handle_reveal(self, message: rpc_pb2.Message, from_peer: str) -> None: ...

    """
    Handle custom message
    (create your own validation functions for storing messages in the database)
    """

    async def _handle_custom(self, message: rpc_pb2.Message, from_peer: str) -> None: ...
