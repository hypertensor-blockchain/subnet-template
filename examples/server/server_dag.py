from __future__ import annotations

from collections.abc import Sequence
import logging
import random

from libp2p.crypto.keys import KeyPair
from libp2p.rcmgr.manager import ResourceManager
import trio

from examples.dag.peer_state_dag_publisher import (
    PeerRole,
    PeerStateDagPublisher,
    PeerStateData,
    ServerState,
)
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
from subnet.server.server_template import ApplicationBase, P2PNetworkContext, ServerBase
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB
from subnet.utils.logging_config import configure_logging
from subnet.utils.pubsub.topics import PEER_STATE_TOPIC

configure_logging()
logger = logging.getLogger("server_example/1.0.0")

DAG_NAMESPACE = "general-dag"
PEER_STATE_SCHEMA_ID = "peer-state"
OFFLINE_GOSSIP_SETTLE_SECONDS = 3.0


class SubnetApplication(ApplicationBase):
    """Subnet-specific application logic that runs on top of ``ServerBase``."""

    def __init__(
        self,
        *,
        port: int,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        is_bootstrap: bool,
        enable_pubsub_validator: bool,
        telemetry: Telemetry | None,
        heartbeat_validator_log_level: int,
        gossip_receiver_log_level: int,
        publish_heartbeat_log_level: int,
        dag_startup_sync_min_connected_peers: int,
        dag_startup_sync_timeout: float,
        dag_startup_sync_settle_time: float,
        dag_startup_sync_poll_interval: float,
        dag_missing_sync_retry_delay: float,
        offline_gossip_settle_seconds: float,
    ) -> None:
        super().__init__()
        self.port = port
        self.key_pair = key_pair
        self.db = db
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.is_bootstrap = is_bootstrap
        self.enable_pubsub_validator = enable_pubsub_validator
        self.telemetry = telemetry
        self.heartbeat_validator_log_level = heartbeat_validator_log_level
        self.gossip_receiver_log_level = gossip_receiver_log_level
        self.publish_heartbeat_log_level = publish_heartbeat_log_level
        self.dag_startup_sync_min_connected_peers = dag_startup_sync_min_connected_peers
        self.dag_startup_sync_timeout = dag_startup_sync_timeout
        self.dag_startup_sync_settle_time = dag_startup_sync_settle_time
        self.dag_startup_sync_poll_interval = dag_startup_sync_poll_interval
        self.dag_missing_sync_retry_delay = dag_missing_sync_retry_delay
        self.offline_gossip_settle_seconds = max(0.0, offline_gossip_settle_seconds)
        self.dag_system: DagGossipSystem | None = None
        self.peer_state_publisher: PeerStateDagPublisher | None = None

    async def setup(self, context: P2PNetworkContext) -> None:
        """Wire the old server behavior into the running P2P context."""
        if context.pubsub is None or context.gossipsub is None:
            raise RuntimeError("SubnetApplication requires pubsub and gossipsub to be enabled")

        if self.telemetry:
            context.nursery.start_soon(self.telemetry.run)

    async def start_application(self, context: P2PNetworkContext) -> None:
        """Start DAG sync/publishing after the template starts connection maintenance."""
        if context.pubsub is None or context.gossipsub is None:
            raise RuntimeError("SubnetApplication requires pubsub and gossipsub to be enabled")

        logger.info("Starting application")
        host = context.host
        dht = context.dht
        pubsub = context.pubsub
        gossipsub = context.gossipsub
        multiaddr = str(context.optimal_addr) if context.optimal_addr is not None else ""

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

        self.dag_system = DagGossipSystem(
            pubsub=pubsub,
            termination_event=context.termination_event,
            db=self.db,
            local_peer_id=host.get_id(),
            topics=[
                DagGossipTopicConfig(
                    topic=PEER_STATE_TOPIC,
                    namespace=DAG_NAMESPACE,
                    payload_schemas=[DagPublisherTemplateSchema(PEER_STATE_SCHEMA_ID, PeerStateData)],
                    schema_id=PEER_STATE_SCHEMA_ID,
                    author=host.get_id().to_string(),
                    parent_schema_id=PEER_STATE_SCHEMA_ID,
                    gossip_handler=None,
                    topic_validator=None,
                    is_async_topic_validator=False,
                    signer=Libp2pKeyPairSigner(self.key_pair),
                    skip_if_orphans=True,
                    request_client=request_client,
                    peer_provider=peer_provider,
                    sync_retry_delay=self.dag_missing_sync_retry_delay,
                    latest_node_snapshot_db_key=PEER_STATE_TOPIC,
                ),
            ],
            telemetry=self.telemetry,
            log_level=self.gossip_receiver_log_level,
        )
        sync_protocol.set_request_handler(self.dag_system.handle_sync_request_bytes)
        context.nursery.start_soon(self.dag_system.run)

        if self.is_bootstrap:
            return

        logger.info("Syncing DAG")
        sync_peers = await self.dag_system.sync_dag(
            DAG_NAMESPACE,
            min_peer_count=self.dag_startup_sync_min_connected_peers,
            wait_timeout=self.dag_startup_sync_timeout,
            poll_interval=self.dag_startup_sync_poll_interval,
            settle_time=self.dag_startup_sync_settle_time,
        )
        logger.info("Initial DAG startup sync attempted with peers: %s", sync_peers)

        self.peer_state_publisher = PeerStateDagPublisher(
            dag_system=self.dag_system,
            start_state=ServerState.JOINING,
            start_role=PeerRole.VALIDATOR,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            hypertensor=self.hypertensor,
            schema_id=PEER_STATE_SCHEMA_ID,
            namespace=DAG_NAMESPACE,
            multiaddr=multiaddr,
            dag_topic=PEER_STATE_TOPIC,
            telemetry=self.telemetry,
            termination_event=context.termination_event,
            publish_interval_seconds=random.randint(5, 15),
            log_level=self.publish_heartbeat_log_level,
        )
        context.nursery.start_soon(self.peer_state_publisher.run)
        logger.info("Starting application complete")

    async def cleanup(self, context: P2PNetworkContext) -> None:
        """Hook for publishing final state before shutdown."""
        if self.peer_state_publisher is None:
            logger.info("Server application shutting down")
            return

        logger.info("Announcing that we're going offline before shutting down")
        result = await self.peer_state_publisher.publish_offline_state()
        if result is None:
            logger.warning("Offline peer-state DAG announcement was not published before shutdown")
            logger.info("Server application shutting down")
            return

        logger.info(
            "Offline peer-state DAG node %s gossiped; waiting %.1f seconds before shutdown",
            result.node_id,
            self.offline_gossip_settle_seconds,
        )
        if self.offline_gossip_settle_seconds > 0:
            await trio.sleep(self.offline_gossip_settle_seconds)
        logger.info("Server application shutting down")


class Server(ServerBase):
    """Version of the subnet server implemented on top of ``ServerBase``."""

    def __init__(
        self,
        *,
        ip: str | None = None,
        port: int,
        peerstore_db_path: str | None = None,
        bootstrap_addrs: Sequence[str] | None = None,
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
        offline_gossip_settle_seconds: float = OFFLINE_GOSSIP_SETTLE_SECONDS,
        **kwargs: object,
    ) -> None:
        logger.info("Server starting subnet_id=%s", subnet_id)

        application = SubnetApplication(
            port=port,
            key_pair=key_pair,
            db=db,
            subnet_id=subnet_id,
            subnet_node_id=subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=is_bootstrap,
            enable_pubsub_validator=enable_pubsub_validator,
            telemetry=telemetry,
            heartbeat_validator_log_level=heartbeat_validator_log_level,
            gossip_receiver_log_level=gossip_receiver_log_level,
            publish_heartbeat_log_level=publish_heartbeat_log_level,
            dag_startup_sync_min_connected_peers=dag_startup_sync_min_connected_peers,
            dag_startup_sync_timeout=dag_startup_sync_timeout,
            dag_startup_sync_settle_time=dag_startup_sync_settle_time,
            dag_startup_sync_poll_interval=dag_startup_sync_poll_interval,
            dag_missing_sync_retry_delay=dag_missing_sync_retry_delay,
            offline_gossip_settle_seconds=offline_gossip_settle_seconds,
        )

        super().__init__(
            ip=ip or "0.0.0.0",
            port=port,
            application=application,
            key_pair=key_pair,
            bootstrap_addrs=bootstrap_addrs,
            use_available_interfaces=True,
            enable_pubsub=True,
            enable_random_walk=True,
            enable_mDNS=enable_mDNS,
            enable_upnp=enable_upnp,
            enable_autotls=enable_autotls,
            resource_manager=resource_manager,
            psk=psk,
            peerstore_db_path=peerstore_db_path,
            max_connections_per_peer=6,
            enable_ping=True,
            enable_proof_of_stake=enable_proof_of_stake,
            db=db,
            subnet_id=subnet_id,
            subnet_slot=subnet_slot,
            subnet_node_id=subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=is_bootstrap,
            enable_subnet_info_tracker=True,
            enable_consensus=enable_consensus,
            log_random_walk=True,
            random_walk_log_interval=30,
            enable_connection_maintenance=True,
            strict_maintain_connections=strict_maintain_connections,
            telemetry=telemetry,
            maintain_connections_log_level=maintain_connections_log_level,
        )

        self.is_bootstrap = is_bootstrap
        self.subnet_id = subnet_id
        self.subnet_slot = subnet_slot
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.db = db
        self.telemetry = telemetry

    async def run(self) -> None:
        """Run the server through the shared template lifecycle."""
        try:
            logger.info("Server running subnet_id=%s", self.subnet_id)
            await super().run()
        finally:
            logger.info("Server shutting down")


__all__ = ["Server", "SubnetApplication"]
