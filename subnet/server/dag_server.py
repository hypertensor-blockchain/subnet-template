from collections.abc import Mapping
from functools import partial
import logging
from typing import TYPE_CHECKING, List, cast

from libp2p import (
    new_host,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.custom_types import ISecureTransport, TProtocol
from libp2p.discovery.rendezvous import (
    RendezvousDiscovery,
    RendezvousService,
    config,
)
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p.network.swarm import Swarm
from libp2p.peer.persistent import create_async_peerstore, create_sync_peerstore, create_sync_rocksdb_peerstore
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.rcmgr.manager import ResourceManager
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.records.validator import NamespacedValidator
from libp2p.security.noise.transport import (
    PROTOCOL_ID as NOISE_PROTOCOL_ID,
    Transport as NoiseTransport,
)
import libp2p.security.secio.transport as secio
from libp2p.security.secio.transport import Transport as SecioTransport
from libp2p.tools.async_service import background_trio_service
from libp2p.transport.quic.config import QUICTransportConfig
import trio

from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.consensus.consensus import Consensus
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.merkle_dag.sync_scheduler import SyncScheduler
from subnet.merkle_dag.sync_service import MerkleDagSyncService
from subnet.protocols.sync_protocol import (
    MerkleDagSyncProtocol,
    PeerStateDagPeerSetProvider,
    SyncProtocolPeerRequestClient,
)
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.addresses import get_public_ip_interfaces
from subnet.utils.connection import (
    basic_maintain_connections,
    demonstrate_random_walk_discovery,
    maintain_connections,
)
from subnet.utils.connections.bootstrap import connect_to_bootstrap_nodes
from subnet.utils.db.database import RocksDB
from subnet.utils.gossipsub.gossip_receiver import GossipReceiver
from subnet.utils.gossipsub.peer_status_gossip_receiver import PeerStatusGossipReceiver
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker
from subnet.utils.patches import apply_all_patches
from subnet.utils.pos.pos_transport import (
    PROTOCOL_ID as POS_PROTOCOL_ID,
    POSTransport,
)
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.protocols.ping import handle_ping
from subnet.utils.pubsub.custom_score_params import custom_score_params
from subnet.utils.pubsub.heartbeat import (
    HeartbeatPublisher,
)
from subnet.utils.pubsub.peer_state_publisher import PeerRole, PeerStateDagSchema, PeerStatePublisher, ServerState
from subnet.utils.pubsub.pubsub_validation import (
    SyncHeartbeatMsgValidator,
    SyncPubsubTopicValidator,
)
from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC, PEER_STATE_TOPIC

if TYPE_CHECKING:
    from libp2p.network.swarm import Swarm

# Apply patches for stability
apply_all_patches()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")

PING_PROTOCOL_ID = TProtocol("/ipfs/ping/1.0.0")


class Server:
    def __init__(
        self,
        *,
        ip: str | None = None,
        port: int,
        peerstore_db_path: str | None = None,
        bootstrap_addrs: List[str] | None = None,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int = 0,
        subnet_slot: int = 3,
        subnet_node_id: int = 0,
        hypertensor: Hypertensor | LocalMockHypertensor,
        is_bootstrap: bool = False,
        enable_pubsub_validator: bool = True,
        enable_consensus: bool = True,
        enable_proof_of_stake: bool = True,
        strict_maintain_connections: bool = True,
        # Host specific arguments
        enable_mDNS: bool = False,
        enable_upnp: bool = False,
        enable_autotls: bool = False,
        resource_manager: ResourceManager | None = None,
        psk: str | None = None,
        telemetry: Telemetry | None = None,
        heartbeat_validator_log_level: int = logging.DEBUG,
        gossip_receiver_log_level: int = logging.DEBUG,
        publish_heartbeat_log_level: int = logging.DEBUG,
        maintain_connections_log_level: int = logging.DEBUG,
        **kwargs,
    ):
        logger.info(f"Server starting subnet_id={subnet_id}")
        self.ip = ip
        self.port = port
        self.bootstrap_addrs = bootstrap_addrs
        self.key_pair = key_pair
        self.subnet_id = subnet_id
        self.subnet_slot = subnet_slot
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.db = db
        self.is_bootstrap = is_bootstrap
        self.enable_pubsub_validator = enable_pubsub_validator
        self.enable_consensus = enable_consensus
        self.enable_proof_of_stake = enable_proof_of_stake
        self.strict_maintain_connections = strict_maintain_connections
        # Host specific arguments
        self.enable_mDNS = enable_mDNS
        self.enable_upnp = enable_upnp
        self.enable_autotls = enable_autotls
        self.resource_manager = resource_manager
        self.psk = psk
        self.telemetry = telemetry
        self.peerstore_db_path = peerstore_db_path
        self.heartbeat_validator_log_level = heartbeat_validator_log_level
        self.gossip_receiver_log_level = gossip_receiver_log_level
        self.publish_heartbeat_log_level = publish_heartbeat_log_level
        self.maintain_connections_log_level = maintain_connections_log_level

    async def run(self):
        try:
            logger.info(f"Server running subnet_id={self.subnet_id}")
            from libp2p.utils.address_validation import (
                get_available_interfaces,
                get_optimal_binding_address,
            )

            listen_addrs = get_available_interfaces(self.port)

            logger.info(f"Initial listen addrs: {listen_addrs}")

            # ------------------------------------------------------------------------
            # Initialize the Proof of Stake mechanism
            #
            # This is used to validate the proof of stake of other peers.
            #
            # This ensures only peers that are registered to the subnet on-chain
            # are allowed to connect and communicate with the peer.
            #
            # This mechanism is used at the host level alongside the secure transports
            # for secure connections (i.e., handshakes, negotiating secure channels),
            # and gossip validators for pubsub messages.
            # ------------------------------------------------------------------------
            proof_of_stake = None
            if self.enable_proof_of_stake:
                proof_of_stake = ProofOfStake(
                    subnet_id=self.subnet_id,
                    hypertensor=self.hypertensor,
                    min_class=0,
                )

                pos_noise_transport = POSTransport(
                    transport=NoiseTransport(
                        self.key_pair,
                        noise_privkey=create_new_x25519_key_pair().private_key,
                    ),
                    pos=proof_of_stake,
                    log_level=logging.INFO if self.is_bootstrap else logging.DEBUG,
                )

                pos_secio_transport = POSTransport(
                    transport=SecioTransport(
                        self.key_pair,
                    ),
                    pos=proof_of_stake,
                    log_level=logging.INFO if self.is_bootstrap else logging.DEBUG,
                )

                secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
                    POS_PROTOCOL_ID: pos_noise_transport,
                    TProtocol(secio.ID): pos_secio_transport,
                }
            else:
                secure_transports_by_protocol = None  # Use default transports

            if self.peerstore_db_path is not None:
                # peerstore = create_async_peerstore(
                #     db_path=self.peerstore_db_path,
                #     backend="leveldb",
                # )
                # peerstore = create_sync_peerstore(
                #     db_path=self.peerstore_db_path,
                #     backend="leveldb",
                # )
                raise NotImplementedError("Persistent peerstore not implemented.")
            else:
                peerstore = None

            # ----------------------------------------------------------------
            # Create a new libp2p host
            #
            # This is the base layer of your peer.
            # ----------------------------------------------------------------
            host = new_host(
                key_pair=self.key_pair,
                listen_addrs=listen_addrs,
                sec_opt=secure_transports_by_protocol,
                peerstore_opt=peerstore,
                enable_upnp=self.enable_upnp,
                enable_mDNS=self.enable_mDNS,
                enable_autotls=self.enable_autotls,
                # enable_quic=True,
                # quic_transport_opt=QUICTransportConfig(),
                resource_manager=self.resource_manager,
                psk=self.psk,
            )

            # Increase connection limits to prevent aggressive pruning (EOF/0-byte reads)
            # This is done manually because new_host() only exposes this via QUIC config.
            # We cast to Swarm so the IDE/type checker recognizes the connection_config.
            cast("Swarm", host.get_network()).connection_config.max_connections_per_peer = 6

            # Log available protocols
            logger.info(f"Host ID: {host.get_id()}")

            termination_event = trio.Event()  # Event to signal termination
            async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
                logger.info(f"Listening address: {listen_addrs}")

                # Start the peer-store cleanup task, TTL
                nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)

                # Set stream handler for ping protocol (used by overwatch nodes)
                host.set_stream_handler(PING_PROTOCOL_ID, handle_ping)

                # -----------------------------------------------
                # Create a new DHT
                #
                # The DHT is used for peer discovery and routing.
                # -----------------------------------------------
                dht = KadDHT(
                    host,
                    DHTMode.SERVER,
                    enable_random_walk=True,
                    validator=NamespacedValidator({"pk": PublicKeyValidator()}),
                )

                gossipsub = GossipSub(
                    protocols=[GOSSIPSUB_PROTOCOL_ID],
                    degree=7,  # Number of peers to maintain in mesh
                    degree_low=5,  # Lower bound for mesh peers
                    degree_high=10,  # Upper bound for mesh peers
                    direct_peers=None,  # Direct peers
                    time_to_live=60,  # TTL for message cache in seconds
                    gossip_window=2,  # Smaller window for faster gossip
                    gossip_history=5,  # Keep more history
                    heartbeat_initial_delay=0.5,  # Start heartbeats sooner
                    heartbeat_interval=2,  # More frequent heartbeats for testing
                    # score_params=custom_score_params(),
                )
                pubsub = Pubsub(host, gossipsub)

                # Start the background services
                async with background_trio_service(dht):
                    subnet_info_tracker = SubnetInfoTracker(
                        termination_event,
                        self.subnet_id,
                        self.subnet_slot,
                        self.hypertensor,
                    )

                    # ----------------------------------------------------------------
                    # Display the random walk
                    #
                    # This is only for logging purposes.
                    # ----------------------------------------------------------------
                    nursery.start_soon(demonstrate_random_walk_discovery, dht, 30)

                    async with background_trio_service(pubsub):
                        async with background_trio_service(gossipsub):
                            logger.info("Pubsub and GossipSub services started.")
                            await pubsub.wait_until_ready()
                            logger.info("Pubsub ready.")

                            # ----------------------------------------------------------------
                            # Start telemetry service to send metrics to the telemetry server.
                            # ----------------------------------------------------------------
                            if self.telemetry:
                                nursery.start_soon(self.telemetry.run)

                            # if self.enable_pubsub_validator:
                            #     # ---------------------------------------------------------------
                            #     # Set pubsub top validator for heartbeat topic
                            #     #
                            #     # This is responsible for validating incoming heartbeat messages.
                            #     #
                            #     # More validators can be added for each topic.
                            #     # ---------------------------------------------------------------
                            #     pubsub.set_topic_validator(
                            #         HEARTBEAT_TOPIC,
                            #         SyncPubsubTopicValidator.from_predicate_class(
                            #             SyncHeartbeatMsgValidator,
                            #             host.get_id(),
                            #             subnet_info_tracker,
                            #             self.hypertensor,
                            #             self.subnet_id,
                            #             proof_of_stake,
                            #             telemetry=self.telemetry,
                            #             log_level=self.heartbeat_validator_log_level,
                            #         ).validate,
                            #         is_async_validator=False,
                            #     )

                            # Connect to bootstrap nodes AFTER starting services
                            # This avoids AttributeError on incoming streams
                            if self.bootstrap_addrs is not None:
                                await connect_to_bootstrap_nodes(host, self.bootstrap_addrs)

                            optimal_addr = get_optimal_binding_address(self.port)
                            optimal_addr_with_peer = f"{optimal_addr}/p2p/{host.get_id().to_string()}"
                            logger.info(f"\nRunning peer on {optimal_addr_with_peer}\n")

                            for peer_id in host.get_peerstore().peer_ids():
                                await dht.routing_table.add_peer(peer_id)

                            # ---------------------------------------------------------------------------
                            # Start gossip receiver
                            #
                            # This is responsible for handling incoming messages from the gossip network.
                            # ---------------------------------------------------------------------------
                            # gossip_receiver = GossipReceiver(
                            #     gossipsub=gossipsub,
                            #     pubsub=pubsub,
                            #     termination_event=termination_event,
                            #     db=self.db,
                            #     topics=[HEARTBEAT_TOPIC, PEER_STATE_TOPIC],
                            #     telemetry=self.telemetry,
                            #     log_level=self.gossip_receiver_log_level,
                            # )
                            # nursery.start_soon(gossip_receiver.run)

                            # Keep nodes connected to each other
                            # NOTE: Start this after host, gossipsub, and pubsub are initialized
                            if self.strict_maintain_connections:
                                nursery.start_soon(
                                    partial(
                                        maintain_connections,
                                        host,
                                        subnet_info_tracker,
                                        gossipsub=gossipsub,
                                        pubsub=pubsub,
                                        dht=dht,
                                        telemetry=self.telemetry,
                                        log_level=self.maintain_connections_log_level,
                                    )
                                )
                            else:
                                # Start basic connection maintenance
                                nursery.start_soon(
                                    partial(
                                        basic_maintain_connections,
                                        host,
                                        telemetry=self.telemetry,
                                        log_level=self.maintain_connections_log_level,
                                    )
                                )

                            # -----------------------------
                            # Start Merkle DAG sync service
                            # -----------------------------
                            sync_protocol = MerkleDagSyncProtocol(
                                host=host,
                                db=self.db,
                                dht=dht,
                                telemetry=self.telemetry,
                            )
                            request_client = SyncProtocolPeerRequestClient(sync_protocol)
                            peer_provider = PeerStateDagPeerSetProvider(sync_protocol)

                            runtime = MerkleDagRuntime(
                                db=self.db,
                                payload_schemas=[PeerStateDagSchema()],
                                local_peer_id=host.get_id(),
                                namespace="peer-state",
                                dag_topic=PEER_STATE_TOPIC,
                                request_client=request_client,
                            )

                            sync_scheduler = SyncScheduler(
                                runtime=runtime,
                                termination_event=termination_event,
                                peer_provider=peer_provider,
                                telemetry=self.telemetry,
                            )

                            peer_status_receiver = PeerStatusGossipReceiver(
                                pubsub=pubsub,
                                termination_event=termination_event,
                                runtime=runtime,
                                dag_topic=PEER_STATE_TOPIC,
                                telemetry=self.telemetry,
                                db=self.db,
                                sync_scheduler=sync_scheduler,
                            )
                            nursery.start_soon(sync_scheduler.run)
                            nursery.start_soon(peer_status_receiver.run)

                            sync_service = MerkleDagSyncService(
                                runtime=runtime,
                                termination_event=termination_event,
                                peer_provider=peer_provider,
                                telemetry=self.telemetry,
                            )
                            sync_protocol.set_request_handler(sync_service.handle_sync_request_bytes)

                            if not self.is_bootstrap:
                                if self.enable_consensus:
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

                                # ------------------------------------------------------------------
                                # Start gossip publishers
                                #
                                # The following are gossip examples to get you started and display how
                                # to use multiple topics.
                                #
                                # See topic validator above at `pubsub.set_topic_validator`
                                # ------------------------------------------------------------------

                                # # Start heartbeat publisher
                                # heartbeat_publisher = HeartbeatPublisher(
                                #     pubsub=pubsub,
                                #     topic=HEARTBEAT_TOPIC,
                                #     subnet_id=self.subnet_id,
                                #     subnet_node_id=self.subnet_node_id,
                                #     hypertensor=self.hypertensor,
                                #     telemetry=self.telemetry,
                                #     log_level=self.publish_heartbeat_log_level,
                                # )
                                # nursery.start_soon(heartbeat_publisher.run)

                                # # Start peer state publisher
                                # # Publish peer state JOINING, and later update it
                                # peer_state_publisher = PeerStatePublisher(
                                #     pubsub=pubsub,
                                #     topic=PEER_STATE_TOPIC,
                                #     start_state=ServerState.JOINING,
                                #     start_role=PeerRole.VALIDATOR,
                                #     subnet_id=self.subnet_id,
                                #     subnet_node_id=self.subnet_node_id,
                                #     hypertensor=self.hypertensor,
                                #     telemetry=self.telemetry,
                                #     log_level=self.publish_heartbeat_log_level,
                                # )
                                # nursery.start_soon(peer_state_publisher.run)

                                # Add any other required custom logic here
                                # await trio.sleep(5)

                                # Update the publisher state to ONLINE for the next publish once peer is ready.
                                # See `ServerState` for more information and to add more states.
                                # peer_state_publisher.state = ServerState.ONLINE

                                # RocksDBDagStorage

                                peer_state_publisher = PeerStatePublisher(
                                    pubsub=pubsub,
                                    topic=PEER_STATE_TOPIC,
                                    start_state=ServerState.JOINING,
                                    start_role=PeerRole.VALIDATOR,
                                    subnet_id=self.subnet_id,
                                    subnet_node_id=self.subnet_node_id,
                                    hypertensor=self.hypertensor,
                                    db=self.db,
                                    key_pair=self.key_pair,
                                    telemetry=self.telemetry,
                                    local_peer_id=host.get_id(),
                                    multiaddr=optimal_addr,
                                    runtime=runtime,
                                    termination_event=termination_event,
                                    log_level=self.publish_heartbeat_log_level,
                                )

                                nursery.start_soon(peer_state_publisher.run)

                            else:
                                # TODO: Start Rendezvous
                                # service = RendezvousService(host)
                                pass

                            await termination_event.wait()

        finally:
            # TODO: Publish peer state OFFLINE
            # peer_state_publisher.state = ServerState.OFFLINE

            logger.info("Server shutting down")
            nursery.cancel_scope.cancel()
