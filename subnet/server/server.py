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
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p.network.swarm import Swarm
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.rcmgr.manager import ResourceManager
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.records.validator import NamespacedValidator
from libp2p.security.noise.transport import (
    Transport as NoiseTransport,
)
import libp2p.security.secio.transport as secio
from libp2p.security.secio.transport import Transport as SecioTransport
from libp2p.tools.async_service import background_trio_service
import trio

from examples.dag.peer_state_dag_publisher import (
    PeerRole,
    PeerStateDagPublisher,
    PeerStateData,
    ServerState,
)
from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.consensus.consensus import Consensus
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag import Libp2pKeyPairSigner
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig
from subnet.merkle_dag.bases.dag_publisher_template import DagPublisherTemplateSchema
from subnet.protocols.dag_sync_protocol import (
    DagPeerSetProvider,
    MerkleDagSyncProtocol,
    SyncProtocolPeerRequestClient,
)
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.connections.bootstrap import connect_to_bootstrap_nodes
from subnet.utils.connections.connection import (
    basic_maintain_connections,
    demonstrate_random_walk_discovery,
    maintain_connections,
)
from subnet.utils.dag.heartbeat_dag_publisher import HeartbeatDagPublisher, HeartbeatDagSchema  # noqa: F401
from subnet.utils.db.database import RocksDB
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker
from subnet.utils.logging_config import configure_logging
from subnet.utils.patches import apply_all_patches
from subnet.utils.pos.pos_transport import (
    PROTOCOL_ID as POS_PROTOCOL_ID,
    POSTransport,
)
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.protocols.ping import handle_ping
from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC, PEER_STATE_TOPIC

if TYPE_CHECKING:
    from libp2p.network.swarm import Swarm

# Apply patches for stability
apply_all_patches()

# Configure logging
configure_logging()
logger = logging.getLogger("server/1.0.0")

PING_PROTOCOL_ID = TProtocol("/ipfs/ping/1.0.0")
DAG_NAMESPACE = "general-dag"
PEER_STATE_SCHEMA_ID = "peer-state"
HEARTBEAT_SCHEMA_ID = "heartbeat"


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
        dag_startup_sync_min_connected_peers: int = 2,
        dag_startup_sync_timeout: float = 180.0,
        dag_startup_sync_settle_time: float = 10.0,
        dag_startup_sync_poll_interval: float = 1.0,
        dag_missing_sync_retry_delay: float = 5.0,
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
        self.dag_startup_sync_min_connected_peers = dag_startup_sync_min_connected_peers
        self.dag_startup_sync_timeout = dag_startup_sync_timeout
        self.dag_startup_sync_settle_time = dag_startup_sync_settle_time
        self.dag_startup_sync_poll_interval = dag_startup_sync_poll_interval
        self.dag_missing_sync_retry_delay = dag_missing_sync_retry_delay

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
                                        gossipsub=gossipsub,
                                        pubsub=pubsub,
                                        dht=dht,
                                    )
                                )

                            # ---------------------------------------
                            # Start Merkle DAG gossip + sync template
                            # ---------------------------------------
                            sync_protocol = MerkleDagSyncProtocol(
                                host=host,
                                db=self.db,
                                dht=dht,
                                pubsub=pubsub,
                                gossipsub=gossipsub,
                                telemetry=self.telemetry,
                            )
                            request_client = SyncProtocolPeerRequestClient(sync_protocol)
                            peer_provider = DagPeerSetProvider(sync_protocol)

                            dag_system = DagGossipSystem(
                                pubsub=pubsub,
                                termination_event=termination_event,
                                db=self.db,
                                local_peer_id=host.get_id(),
                                topics=[
                                    DagGossipTopicConfig(
                                        topic=PEER_STATE_TOPIC,
                                        namespace=DAG_NAMESPACE,
                                        payload_schemas=[
                                            DagPublisherTemplateSchema(PEER_STATE_SCHEMA_ID, PeerStateData)
                                        ],
                                        schema_id=PEER_STATE_SCHEMA_ID,
                                        author=host.get_id().to_string(),
                                        parent_schema_id=PEER_STATE_SCHEMA_ID,
                                        signer=Libp2pKeyPairSigner(self.key_pair),
                                        skip_if_orphans=True,
                                        request_client=request_client,
                                        peer_provider=peer_provider,
                                        sync_retry_delay=self.dag_missing_sync_retry_delay,
                                        latest_node_snapshot_db_key=PEER_STATE_TOPIC,
                                    ),
                                    # DagGossipTopicConfig(
                                    #     topic=HEARTBEAT_TOPIC,
                                    #     namespace=DAG_NAMESPACE,
                                    #     payload_schemas=[HeartbeatDagSchema(HEARTBEAT_SCHEMA_ID)],
                                    #     schema_id=HEARTBEAT_SCHEMA_ID,
                                    #     author=host.get_id().to_string(),
                                    #     parent_schema_id=HEARTBEAT_SCHEMA_ID,
                                    #     signer=Libp2pKeyPairSigner(self.key_pair),
                                    #     skip_if_orphans=True,
                                    #     request_client=request_client,
                                    #     peer_provider=peer_provider,
                                    #     sync_retry_delay=self.dag_missing_sync_retry_delay,
                                    #     latest_node_snapshot_db_key=HEARTBEAT_TOPIC,
                                    # ),
                                ],
                                telemetry=self.telemetry,
                                log_level=self.gossip_receiver_log_level,
                            )
                            sync_protocol.set_request_handler(dag_system.handle_sync_request_bytes)
                            nursery.start_soon(dag_system.run)

                            if not self.is_bootstrap:
                                sync_peers = await dag_system.sync_dag(
                                    DAG_NAMESPACE,
                                    min_peer_count=self.dag_startup_sync_min_connected_peers,
                                    wait_timeout=self.dag_startup_sync_timeout,
                                    poll_interval=self.dag_startup_sync_poll_interval,
                                    settle_time=self.dag_startup_sync_settle_time,
                                )
                                logger.info("Initial DAG startup sync attempted with peers: %s", sync_peers)

                                peer_state_publisher = PeerStateDagPublisher(
                                    dag_system=dag_system,
                                    start_state=ServerState.JOINING,
                                    start_role=PeerRole.VALIDATOR,
                                    subnet_id=self.subnet_id,
                                    subnet_node_id=self.subnet_node_id,
                                    hypertensor=self.hypertensor,
                                    schema_id=PEER_STATE_SCHEMA_ID,
                                    namespace=DAG_NAMESPACE,
                                    multiaddr=str(optimal_addr),
                                    dag_topic=PEER_STATE_TOPIC,
                                    telemetry=self.telemetry,
                                    termination_event=termination_event,
                                    publish_interval_seconds=20.0,
                                    log_level=self.publish_heartbeat_log_level,
                                )
                                nursery.start_soon(peer_state_publisher.run)

                                # peer_state_publisher = PeerStateDagPublisher(
                                #     dag_system=dag_system,
                                #     start_state=ServerState.JOINING,
                                #     start_role=PeerRole.VALIDATOR,
                                #     subnet_id=self.subnet_id,
                                #     subnet_node_id=self.subnet_node_id,
                                #     hypertensor=self.hypertensor,
                                #     schema_id=PEER_STATE_SCHEMA_ID,
                                #     multiaddr=optimal_addr,
                                #     telemetry=self.telemetry,
                                #     termination_event=termination_event,
                                #     namespace=DAG_NAMESPACE,
                                #     publish_interval_seconds=20.0,
                                #     log_level=self.publish_heartbeat_log_level,
                                # )
                                # nursery.start_soon(peer_state_publisher.run)

                                # heartbeat_publisher = HeartbeatDagPublisher(
                                #     dag_system=dag_system,
                                #     subnet_id=self.subnet_id,
                                #     subnet_node_id=self.subnet_node_id,
                                #     hypertensor=self.hypertensor,
                                #     schema_id=HEARTBEAT_SCHEMA_ID,
                                #     multiaddr=optimal_addr,
                                #     telemetry=self.telemetry,
                                #     termination_event=termination_event,
                                #     namespace=DAG_NAMESPACE,
                                #     publish_interval_seconds=10.0,
                                #     log_level=self.publish_heartbeat_log_level,
                                # )
                                # nursery.start_soon(heartbeat_publisher.run)

                                if self.enable_consensus:
                                    Consensus(
                                        db=self.db,
                                        subnet_id=self.subnet_id,
                                        subnet_node_id=self.subnet_node_id,
                                        subnet_info_tracker=subnet_info_tracker,
                                        hypertensor=self.hypertensor,
                                    )

                            await termination_event.wait()

        finally:
            # TODO: Publish peer state OFFLINE
            # Peer-state publishing now lives outside DagGossipSystem.

            logger.info("Server shutting down")
            nursery.cancel_scope.cancel()
