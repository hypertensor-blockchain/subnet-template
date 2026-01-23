from collections.abc import Mapping
import logging
from typing import TYPE_CHECKING, List, cast

from libp2p import (
    new_host,
)
from libp2p.abc import (
    IHost,
    IMuxedStream,
    INetStream,
    IPubsub,
    IPubsubRouter,
    ISubscriptionAPI,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.custom_types import TProtocol
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p.kad_dht.peer_routing import PeerRouting
from libp2p.network.connection.exceptions import RawConnError
from libp2p.network.stream.exceptions import StreamClosed, StreamReset
from libp2p.network.swarm import Swarm
from libp2p.peer.id import ID
from libp2p.pubsub.exceptions import NoPubsubAttached
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.records.validator import NamespacedValidator
from libp2p.security.noise.transport import (
    PROTOCOL_ID as NOISE_PROTOCOL_ID,
    Transport as NoiseTransport,
)
from libp2p.stream_muxer.exceptions import MuxedStreamError, MuxedStreamReset
from libp2p.stream_muxer.mplex.datastructures import StreamID
from libp2p.stream_muxer.mplex.exceptions import MplexUnavailable
from libp2p.stream_muxer.mplex.mplex import MPLEX_PROTOCOL_ID, Mplex
from libp2p.stream_muxer.mplex.mplex_stream import MplexStream
from libp2p.tools.async_service import background_trio_service
from libp2p.utils.varint import encode_uvarint
import trio

from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.consensus.consensus import Consensus
from subnet.db.database import RocksDB
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.connection import (
    demonstrate_random_walk_discovery,
    maintain_connections,
)
from subnet.utils.connections.bootstrap import connect_to_bootstrap_nodes
from subnet.utils.gossipsub.gossip_receiver import GossipReceiver
from subnet.utils.hypertensor.subnet_info_tracker import SubnetInfoTracker
from subnet.utils.pos.pos_noise_transport import (
    PROTOCOL_ID as POS_PROTOCOL_ID,
    POSNoiseTransport,
)
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.pubsub.custom_score_params import custom_score_params
from subnet.utils.pubsub.heartbeat import (
    HEARTBEAT_TOPIC,
    publish_loop,
)
from subnet.utils.pubsub.pubsub_validation import AsyncHeartbeatMsgValidator, AsyncPubsubTopicValidator

if TYPE_CHECKING:
    from libp2p.network.swarm import Swarm

# --- Monkey-patching for stability ---

# Global lock storage for dial synchronization
# _DIAL_LOCKS: dict[ID, trio.Lock] = {}
# _DIAL_LOCKS_LOCK = trio.Lock()


# async def get_dial_lock(peer_id: ID) -> trio.Lock:
#     async with _DIAL_LOCKS_LOCK:
#         if peer_id not in _DIAL_LOCKS:
#             _DIAL_LOCKS[peer_id] = trio.Lock()
#         return _DIAL_LOCKS[peer_id]


# # Monkey-patch Swarm.dial_peer to prevent parallel duplicate dials
# _orig_dial_peer = Swarm.dial_peer


# async def synchronized_dial_peer(self: Swarm, peer_id: ID):
#     lock = await get_dial_lock(peer_id)
#     async with lock:
#         # Check if we established a connection while waiting for the lock
#         conns = self.get_connections(peer_id)
#         if conns:
#             return conns
#         return await _orig_dial_peer(self, peer_id)


# Swarm.dial_peer = synchronized_dial_peer


# Monkey-patch PeerRouting._query_peer_for_closest to skip peers with no addresses
# _orig_query_peer = PeerRouting._query_peer_for_closest


# async def filtered_query_peer_for_closest(
#     self: PeerRouting, peer: ID, target_key: bytes
# ):
#     # Skip if we don't have addresses to avoid redundant dial failures
#     if not self.host.get_peerstore().addrs(peer):
#         return []
#     return await _orig_query_peer(self, peer, target_key)


# PeerRouting._query_peer_for_closest = filtered_query_peer_for_closest


# Monkey-patch Pubsub.write_msg to prevent crashes on StreamReset
_orig_write_msg = Pubsub.write_msg


async def safe_write_msg(self: Pubsub, stream: INetStream, rpc_msg) -> bool:
    try:
        return await _orig_write_msg(self, stream, rpc_msg)
    except (StreamReset, MuxedStreamReset, MuxedStreamError) as e:
        logger.info(f"safe_write_msg: error: {e}", exc_info=True)
        try:
            peer_id = stream.muxed_conn.peer_id
        except Exception:
            logger.info("safe_write_msg: Exception Fail to write message to %s: stream reset", peer_id)
            # If we can't even get the peer_id, just return False
            return False

        # safe_write_msg: Fail to write message to 12D3KooWMGKEpzz3EWGU2ayhwFriRh23QnQ479Ctfj8xSmDRirde: stream reset
        logger.info("safe_write_msg: Fail to write message to %s: stream reset", peer_id)
        self._handle_dead_peer(peer_id)
        return False


Pubsub.write_msg = safe_write_msg


# Monkey-patch GossipSub._get_in_topic_gossipsub_peers_from_minus to prevent KeyError
# This happens when a peer is in peer_topics but not in peer_protocol due to a race condition.
_orig_get_peers = GossipSub._get_in_topic_gossipsub_peers_from_minus


def safe_get_in_topic_gossipsub_peers_from_minus(
    self: GossipSub,
    topic: str,
    num_to_select: int,
    minus: List[ID],
    backoff_check: bool = False,
) -> List[ID]:
    if self.pubsub is None:
        raise NoPubsubAttached

    # Use .get() to safely check protocol participation
    from libp2p.pubsub.gossipsub import PROTOCOL_ID, PROTOCOL_ID_V11, PROTOCOL_ID_V12

    gossipsub_peers_in_topic = {
        peer_id
        for peer_id in self.pubsub.peer_topics[topic]
        if self.peer_protocol.get(peer_id) in (PROTOCOL_ID, PROTOCOL_ID_V11, PROTOCOL_ID_V12)
    }
    if backoff_check:
        # filter out peers that are in back off for this topic
        gossipsub_peers_in_topic = {
            peer_id for peer_id in gossipsub_peers_in_topic if self._check_back_off(peer_id, topic) is False
        }
    return self.select_from_minus(num_to_select, list(gossipsub_peers_in_topic), minus)


GossipSub._get_in_topic_gossipsub_peers_from_minus = safe_get_in_topic_gossipsub_peers_from_minus

# -------------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")


class SerializedMplex(Mplex):
    """
    Subclass of Mplex that injects negotiation semaphores upon initialization.
    This guarantees that the semaphores are present before any protocols (DHT, Pubsub,
    Identify) can attempt to open a stream.

    We use separate semaphores for outbound (client) and inbound (server) handshakes
     to prevent deadlocks when two nodes dial each other simultaneously.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Capacity of 1 forces strict serialization of handshakes per connection
        self._negotiation_semaphore = trio.Semaphore(1)
        self._server_negotiation_semaphore = trio.Semaphore(1)
        # Add a write lock to prevent interleaving of bytes from concurrent writes
        # on the underlying secured connection.
        self._write_lock = trio.Lock()
        # Override the default 0-capacity channel to prevent blocking the reader loop
        # when many streams arrive at once.
        self.new_stream_send_channel, self.new_stream_receive_channel = trio.open_memory_channel[IMuxedStream](512)
        # self._handshake_completed = True

    # def is_established(self) -> bool:
    #     return not self.event_closed.is_set()

    # async def write_to_stream(self, _bytes: bytes) -> None:
    #     """
    #     Write a byte array with a lock and robust error reporting.
    #     """
    #     async with self._write_lock:
    #         try:
    #             await self.secured_conn.write(_bytes)
    #         except RawConnError as e:
    #             raise MplexUnavailable("Underlying connection write failed") from e

    # async def _initialize_stream(self, stream_id: StreamID, name: str) -> MplexStream:
    #     """
    #     Increase the per-stream message buffer size.
    #     Default MPLEX_MESSAGE_CHANNEL_SIZE is 8, which is too small for GossipSub.
    #     """
    #     send_channel, receive_channel = trio.open_memory_channel[bytes](1024)
    #     mplex_stream = MplexStream(name, stream_id, self, receive_channel)
    #     async with self.streams_lock:
    #         self.streams[stream_id] = mplex_stream
    #         self.streams_msg_channels[stream_id] = send_channel
    #     return mplex_stream

    # async def _handle_message(self, stream_id: StreamID, message: bytes) -> None:
    #     """
    #     Robust message handler that doesn't kill the whole connection if a stream
    #     channel is full or closed.
    #     """
    #     async with self.streams_lock:
    #         if stream_id not in self.streams:
    #             return
    #         send_channel = self.streams_msg_channels[stream_id]

    #     try:
    #         send_channel.send_nowait(message)
    #     except (trio.BrokenResourceError, trio.ClosedResourceError):
    #         # Safe ignore: local side closed the stream but remote is still sending
    #         pass
    #     except trio.WouldBlock:
    #         logger.warning(
    #             f"Mplex stream {stream_id} buffer full (1024); dropping message"
    #         )

    # async def handle_incoming(self) -> None:
    #     """
    #     Override handle_incoming to provide better visibility into why a connection died.
    #     """
    #     self.event_started.set()
    #     while True:
    #         try:
    #             await self._handle_incoming_message()
    #         except MplexUnavailable as e:
    #             logger.info(f"Mplex closed for peer {self.peer_id}: {e}")
    #             break
    #         except Exception as e:
    #             logger.error(
    #                 f"Unexpected error in Mplex loop for peer {self.peer_id}: {e}"
    #             )
    #             break
    #     await self._cleanup()

    # async def _cleanup(self) -> None:
    #     logger.info(f"SerializedMplex cleaning up for peer {self.peer_id}")
    #     await super()._cleanup()


# Monkey-patch KadDHT PeerRouting to silence "No known addresses" warnings
# for peers that were removed from the network but still linger in DHT caches.
# original_query_peer = PeerRouting._query_peer_for_closest


# async def patched_query_peer_for_closest(self, peer, target_key):
#     # Check if we have addresses for this peer before trying to open a stream.
#     # This prevents its internal:
#     # logger.warning("Failed to open stream to ...: No known addresses")
#     if not self.host.get_peerstore().addrs(peer):
#         return []
#     return await original_query_peer(self, peer, target_key)


# PeerRouting._query_peer_for_closest = patched_query_peer_for_closest


class Server:
    def __init__(
        self,
        *,
        port: int,
        bootstrap_addrs: List[str] | None = None,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int = 0,
        subnet_node_id: int = 0,
        hypertensor: Hypertensor | LocalMockHypertensor,
        **kwargs,
    ):
        self.port = port
        self.bootstrap_addrs = bootstrap_addrs
        self.key_pair = key_pair
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.db = db

    async def run(self):
        print("running server gossip")
        from libp2p.utils.address_validation import (
            get_available_interfaces,
            get_optimal_binding_address,
        )

        listen_addrs = get_available_interfaces(self.port)

        proof_of_stake = ProofOfStake(
            subnet_id=self.subnet_id,
            hypertensor=self.hypertensor,
            min_class=0,
        )

        # Create a new libp2p host
        host = new_host(
            key_pair=self.key_pair,
            # muxer_opt={MPLEX_PROTOCOL_ID: Mplex},
            # muxer_opt={MPLEX_PROTOCOL_ID: SerializedMplex},
        )
        # Increase connection limits to prevent aggressive pruning (EOF/0-byte reads)
        # This is done manually because new_host() only exposes this via QUIC config.
        # We cast to Swarm so the IDE/type checker recognizes the connection_config.
        cast("Swarm", host.get_network()).connection_config.max_connections_per_peer = 10
        # Log available protocols
        logger.info(f"Host ID: {host.get_id()}")
        logger.info(
            f"Host multiselect protocols: {host.get_mux().get_protocols() if hasattr(host, 'get_mux') else 'N/A'}"
        )

        termination_event = trio.Event()  # Event to signal termination
        async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
            # Start the peer-store cleanup task, TTL
            nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)

            dht = KadDHT(
                host,
                DHTMode.SERVER,
                enable_random_walk=True,
                validator=NamespacedValidator({"pk": PublicKeyValidator()}),
            )

            gossipsub = GossipSub(
                protocols=[GOSSIPSUB_PROTOCOL_ID],
                degree=3,  # Number of peers to maintain in mesh
                degree_low=2,  # Lower bound for mesh peers
                degree_high=4,  # Upper bound for mesh peers
                direct_peers=None,  # Direct peers
                time_to_live=60,  # TTL for message cache in seconds
                gossip_window=2,  # Smaller window for faster gossip
                gossip_history=5,  # Keep more history
                heartbeat_initial_delay=2.0,  # Start heartbeats sooner
                heartbeat_interval=5,  # More frequent heartbeats for testing
                # score_params=custom_score_params(),
            )
            pubsub = Pubsub(host, gossipsub)

            # Start the background services
            async with background_trio_service(dht):
                subnet_info_tracker = SubnetInfoTracker(
                    termination_event,
                    self.subnet_id,
                    self.hypertensor,
                    epoch_update_intervals=[
                        0.0,
                        0.5,
                    ],  # Update at the start and middle of each subnet epoch
                )
                nursery.start_soon(subnet_info_tracker.run)

                # Display the random walk
                nursery.start_soon(demonstrate_random_walk_discovery, dht, 30)

                async with background_trio_service(pubsub):
                    async with background_trio_service(gossipsub):
                        logger.info("Pubsub and GossipSub services started.")
                        await pubsub.wait_until_ready()
                        logger.info("Pubsub ready.")

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

                        # Connect to bootstrap nodes AFTER starting services
                        # This avoids AttributeError on incoming streams
                        if self.bootstrap_addrs is not None:
                            await connect_to_bootstrap_nodes(host, self.bootstrap_addrs)

                        optimal_addr = get_optimal_binding_address(self.port)
                        optimal_addr_with_peer = f"{optimal_addr}/p2p/{host.get_id().to_string()}"
                        logger.info(f"\nRunning peer on {optimal_addr_with_peer}\n")

                        for peer_id in host.get_peerstore().peer_ids():
                            await dht.routing_table.add_peer(peer_id)

                        # Start gossip receiver
                        gossip_receiver = GossipReceiver(
                            gossipsub=gossipsub,
                            pubsub=pubsub,
                            termination_event=termination_event,
                            db=self.db,
                            topics=[HEARTBEAT_TOPIC],
                            subnet_info_tracker=subnet_info_tracker,
                            hypertensor=self.hypertensor,
                        )
                        nursery.start_soon(gossip_receiver.run)

                        # Keep nodes connected to each other
                        # NOTE: Start this after host, gossipsub, and pubsub are initialized
                        nursery.start_soon(
                            maintain_connections,
                            host,
                            subnet_info_tracker,
                            gossipsub,
                            pubsub,
                            dht,
                        )

                        # Start heartbeat publisher
                        nursery.start_soon(
                            publish_loop,
                            pubsub,
                            HEARTBEAT_TOPIC,
                            termination_event,
                            self.subnet_id,
                            self.subnet_node_id,
                            subnet_info_tracker,
                            self.hypertensor,
                        )

                        # Start consensus
                        consensus = Consensus(
                            db=self.db,
                            subnet_id=self.subnet_id,
                            subnet_node_id=self.subnet_node_id,
                            subnet_info_tracker=subnet_info_tracker,
                            hypertensor=self.hypertensor,
                            skip_activate_subnet=False,
                            start=True,
                        )
                        nursery.start_soon(consensus._main_loop)

                        await termination_event.wait()

            nursery.cancel_scope.cancel()

        print("Application shutdown complete")  # Print shutdown message
