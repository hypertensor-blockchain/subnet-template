import logging

import base58
from libp2p.abc import ISubscriptionAPI
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pb import rpc_pb2
from libp2p.pubsub.pubsub import Pubsub
import trio

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.db.database import RocksDB
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker
from subnet.utils.pubsub.heartbeat import HEARTBEAT_TOPIC, HeartbeatData

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
        subnet_info_tracker: SubnetInfoTracker instance
        hypertensor: LocalMockHypertensor | Hypertensor instance

    Usage:
        gossip = GossipReceiver(
            gossipsub,
            pubsub,
            termination_event,
            db,
            [HEARTBEAT_TOPIC, "commit", "reveal", "custom"],
            subnet_info_tracker,
            hypertensor,
        )
        nursery.start_soon(gossip.run)  # Starts sync loop + receive loops

    Validating database entries:
        Create a function or class specific for validating pubsub messages based on the
        topic.

        See `_handle_*` functions for examples

    """

    def __init__(
        self,
        gossipsub: GossipSub,
        pubsub: Pubsub,
        termination_event: trio.Event,
        db: RocksDB,
        topics: list[str],
        subnet_info_tracker: SubnetInfoTracker,
        hypertensor: LocalMockHypertensor | Hypertensor,
    ):
        self.gossipsub = gossipsub
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.db = db
        self.topics = topics
        self.subnet_info_tracker = subnet_info_tracker
        self.hypertensor = hypertensor
        self._last_epoch: int | None = None
        self._seen_heartbeats: set[str] = set()  # e.g.: "epoch:peer_id"

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
                logger.info(f"Subscribed to topic: {topic}")
                nursery.start_soon(self._receive_loop, subscription)

    async def _receive_loop(self, subscription: ISubscriptionAPI) -> None:
        """Receive loop for a single topic subscription."""
        logger.debug("Starting receive loop")
        while not self.termination_event.is_set():
            try:
                message = await subscription.get()
                await self._handle_message(message)

            except Exception:
                logger.exception("Error in receive loop")
                await trio.sleep(1)

    async def _handle_message(self, message: rpc_pb2.Message) -> None:
        """Handle incoming message based on topic."""
        from_peer = base58.b58encode(message.from_id).decode()
        topic = message.topicIDs[0] if message.topicIDs else None
        logger.debug(f"From peer: {from_peer}, topic: {topic}")

        if topic == HEARTBEAT_TOPIC:
            await self._handle_heartbeat(message, from_peer)

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
        # epoch = self.hypertensor.get_subnet_epoch_data(self.subnet_info_tracker.get_subnet_slot()).epoch

        # # Clear cache on epoch change
        # if self._last_epoch != epoch:
        #     logger.debug(
        #         f"Clearing heartbeat cache for epoch change: {self._last_epoch} -> {epoch}"  # noqa: E501
        #     )
        #     self._seen_heartbeats.clear()
        #     self._last_epoch = epoch

        try:
            heartbeat_data = HeartbeatData.from_json(message.data.decode("utf-8"))
        except Exception as e:
            logger.warning(f"HeartbeatData validation failed: {e}")
            return

        key = f"{heartbeat_data.epoch}:{from_peer}"

        # Fast in-memory check
        if key in self._seen_heartbeats:
            logger.debug(f"Heartbeat already seen (cached): {key}")
            return

        # Check if already exists
        if self.db.nmap_get(HEARTBEAT_TOPIC, key) is not None:
            logger.debug(f"Heartbeat already exists: {key}")
            return

        # Store it
        self.db.nmap_set(HEARTBEAT_TOPIC, key, message.data.decode("utf-8"))
        logger.debug(
            f"Heartbeat stored: {HEARTBEAT_TOPIC}:{key} for node ID {heartbeat_data.subnet_node_id}"  # noqa: E501
        )

        # Add to in-memory set
        self._seen_heartbeats.add(key)

    """Handle commit message (example)"""

    async def _handle_commit(self, message: rpc_pb2.Message, from_peer: str) -> None:
        # if self.subnet_info_tracker.epoch_data is None:
        #     logger.warning("epoch_data not yet synced, skipping commit")
        #     return

        # Add commit logic here
        """
        if self.subnet_info_tracker.epoch_data.percent_complete > 0.5:
            logger.warning("Reveal attempted too late")
            return
        """
        ...

    """Handle reveal message (example)"""

    async def _handle_reveal(self, message: rpc_pb2.Message, from_peer: str) -> None:
        # if self.subnet_info_tracker.epoch_data is None:
        #     logger.warning("epoch_data not yet synced, skipping reveal")
        #     return

        # Add reveal logic here
        """
        if (
            self.subnet_info_tracker.epoch_data.percent_complete < 0.5
            or self.subnet_info_tracker.epoch_data.percent_complete > 0.6
        ):
            logger.warning("Reveal attempted outside of window")
            return
        """
        ...

    """
    Handle custom message
    (create your own validation functions for storing messages in the database)
    """

    async def _handle_custom(self, message: rpc_pb2.Message, from_peer: str) -> None:
        ...
        # if self.subnet_info_tracker.epoch_data is None:
        #     logger.warning("epoch_data not yet synced, skipping custom")
        #     return

        # Add custom logic here
