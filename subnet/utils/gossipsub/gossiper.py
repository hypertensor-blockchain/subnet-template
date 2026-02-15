from enum import Enum, unique
from typing import AsyncIterable

from eth2.beacon.types.blocks import SignedBeaconBlock
from libp2p.abc import IHost, ISubscriptionAPI
from libp2p.pubsub.gossipsub import (
    PROTOCOL_ID as GOSSIPSUB_PROTOCOL_ID,
    GossipSub,
)
from libp2p.pubsub.pubsub import Pubsub, ValidatorFn

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.pubsub.heartbeat import HeartbeatData
from subnet.utils.pubsub.pubsub_validation import (
    SyncHeartbeatMsgValidator,
    SyncPubsubTopicValidator,
)


@unique
class PubSubTopic(Enum):
    heartbeat: str = "heartbeat"


class Gossiper:
    """
    Manage pubsub topics and messages.
    """

    # `D` (topic stable mesh target count)
    DEGREE: int = 6
    # `D_low` (topic stable mesh low watermark)
    DEGREE_LOW: int = 4
    # `D_high` (topic stable mesh high watermark)
    DEGREE_HIGH: int = 12
    # `D_lazy` (gossip target)
    DEGREE_LAZY: int = 6
    # `fanout_ttl` (ttl for fanout maps for topics we are not subscribed to
    #   but have published to seconds).
    FANOUT_TTL: int = 60
    # `gossip_advertise` (number of windows to gossip about).
    GOSSIP_WINDOW: int = 3
    # `gossip_history` (number of heartbeat intervals to retain message IDs).
    GOSSIP_HISTORY: int = 5
    # `heartbeat_interval` (frequency of heartbeat, seconds).
    HEARTBEAT_INTERVAL: int = 1  # seconds

    ATTESTATION_SUBNET_COUNT = 64
    ATTESTATION_PROPAGATION_SLOT_RANGE = 32

    def __init__(
        self,
        subnet_id: int,
        subnet_node_id: int,
        host: IHost,
        subnet_info_tracker: SubnetInfoTracker,
        hypertensor: Hypertensor,
        proof_of_stake: ProofOfStake,
    ) -> None:
        self._subnet_id = subnet_id
        self._subnet_node_id = subnet_node_id
        self._host = host
        self._subnet_info_tracker = subnet_info_tracker
        self._hypertensor = hypertensor
        self._proof_of_stake = proof_of_stake
        gossipsub_router = GossipSub(
            protocols=[GOSSIPSUB_PROTOCOL_ID],
            degree=self.DEGREE,
            degree_low=self.DEGREE_LOW,
            degree_high=self.DEGREE_HIGH,
            time_to_live=self.FANOUT_TTL,
            gossip_window=self.GOSSIP_WINDOW,
            gossip_history=self.GOSSIP_HISTORY,
            heartbeat_interval=self.HEARTBEAT_INTERVAL,
        )
        self.gossipsub = gossipsub_router
        self.pubsub = Pubsub(host=self._host, router=gossipsub_router)

    async def _subscribe_to_gossip_topic(
        self, topic: str, validator: ValidatorFn, is_async_validator: bool = False
    ) -> ISubscriptionAPI:
        self.pubsub.set_topic_validator(topic, validator, is_async_validator)
        return await self.pubsub.subscribe(topic)

    def _gossip_topic_id_for(self, topic: PubSubTopic) -> str:
        """
        Set topic id for a given topic. This can be based on epochs, time, etc.

        Example:
            `return f"/topic/{get_epoch()}/{topic.value}"`

        """
        return f"/subnet/{self.subnet_id}/{topic.value}"

    async def subscribe_gossip_channels(self) -> None:
        # TODO handle concurrently when multiple channels
        self._heartbeat_gossip = await self._subscribe_to_gossip_topic(
            self._gossip_topic_id_for(PubSubTopic.heartbeat),
            validator=(
                SyncPubsubTopicValidator.from_predicate_class(
                    SyncHeartbeatMsgValidator,
                    self._host.get_id(),
                    self._subnet_info_tracker,
                    self._hypertensor,
                    self._subnet_id,
                    self._proof_of_stake,
                ).validate,
            ),
            is_async_validator=False,
        )

    async def unsubscribe_gossip_channels(self) -> None:
        # TODO handle concurrently when multiple channels
        await self.pubsub.unsubscribe(self._gossip_topic_id_for(PubSubTopic.heartbeat))

    async def stream_heartbeat_gossip(self) -> AsyncIterable[HeartbeatData]:
        async with self._heartbeat_gossip:
            async for heartbeat_message in self._heartbeat_gossip:
                if heartbeat_message.from_id == self.pubsub.my_id:
                    # FIXME: this check should happen inside `py-libp2p`
                    continue
                heartbeat_data = heartbeat_message.data
                yield HeartbeatData.from_bytes(heartbeat_data)

    async def broadcast_heartbeat(self, current_epoch: int) -> None:
        message = HeartbeatData(
            epoch=current_epoch, subnet_id=self._subnet_id, subnet_node_id=self._subnet_node_id
        ).to_bytes()

        await self.pubsub.publish(self._gossip_topic_id_for(PubSubTopic.heartbeat), message)
