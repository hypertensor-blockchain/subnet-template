from __future__ import annotations

from collections.abc import Sequence
import logging

from libp2p.crypto.keys import KeyPair
from libp2p.rcmgr.manager import ResourceManager

from examples.commit_reveal.consensus.consenus import Consensus
from examples.commit_reveal.dag import (
    COMMIT_REVEAL_CUTOFF_PERCENT,
    COMMIT_REVEAL_DAG_NAMESPACE,
    COMMIT_SCHEMA_ID,
    COMMIT_TOPIC,
    REVEAL_SCHEMA_ID,
    REVEAL_TOPIC,
    CommitDagSchema,
    CommitRevealDagPublisher,
    RevealDagSchema,
    build_epoch_phase_topic_validator,
)
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag import Libp2pKeyPairSigner
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig
from subnet.protocols.dag_sync_protocol import (
    DagPeerSetProvider,
    MerkleDagSyncProtocol,
    SyncProtocolPeerRequestClient,
)
from subnet.server.server_template import ApplicationBase, P2PNetworkContext, ServerBase
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB
from subnet.utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger("commit-reveal-example")


class CommitRevealApplication(ApplicationBase):
    def __init__(
        self,
        *,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        is_bootstrap: bool,
        enable_consensus: bool,
        telemetry: Telemetry | None,
        dag_startup_sync_min_connected_peers: int,
        dag_startup_sync_timeout: float,
        dag_startup_sync_settle_time: float,
        dag_startup_sync_poll_interval: float,
        dag_missing_sync_retry_delay: float,
        publish_interval_seconds: float,
        cutoff_percent: float,
        gossip_receiver_log_level: int,
        publish_log_level: int,
        skip_activate_subnet: bool,
    ) -> None:
        super().__init__()
        self.key_pair = key_pair
        self.db = db
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.is_bootstrap = is_bootstrap
        self.enable_consensus = enable_consensus
        self.telemetry = telemetry
        self.dag_startup_sync_min_connected_peers = dag_startup_sync_min_connected_peers
        self.dag_startup_sync_timeout = dag_startup_sync_timeout
        self.dag_startup_sync_settle_time = dag_startup_sync_settle_time
        self.dag_startup_sync_poll_interval = dag_startup_sync_poll_interval
        self.dag_missing_sync_retry_delay = dag_missing_sync_retry_delay
        self.publish_interval_seconds = publish_interval_seconds
        self.cutoff_percent = cutoff_percent
        self.gossip_receiver_log_level = gossip_receiver_log_level
        self.publish_log_level = publish_log_level
        self.skip_activate_subnet = skip_activate_subnet
        self.dag_system: DagGossipSystem | None = None
        self.publisher: CommitRevealDagPublisher | None = None
        self.consensus: Consensus | None = None

    async def setup(self, context: P2PNetworkContext) -> None:
        if context.pubsub is None or context.gossipsub is None:
            raise RuntimeError("CommitRevealApplication requires pubsub and gossipsub")
        if self.telemetry:
            context.nursery.start_soon(self.telemetry.run)

    async def start_application(self, context: P2PNetworkContext) -> None:
        if context.pubsub is None or context.gossipsub is None:
            raise RuntimeError("CommitRevealApplication requires pubsub and gossipsub")

        sync_protocol = MerkleDagSyncProtocol(
            host=context.host,
            db=self.db,
            dht=context.dht,
            pubsub=context.pubsub,
            gossipsub=context.gossipsub,
            telemetry=self.telemetry,
        )
        request_client = SyncProtocolPeerRequestClient(sync_protocol)
        peer_provider = DagPeerSetProvider(sync_protocol)
        signer = Libp2pKeyPairSigner(self.key_pair)
        local_peer_id = context.host.get_id().to_string()

        self.dag_system = DagGossipSystem(
            pubsub=context.pubsub,
            termination_event=context.termination_event,
            db=self.db,
            local_peer_id=context.host.get_id(),
            topics=[
                DagGossipTopicConfig(
                    topic=COMMIT_TOPIC,
                    namespace=COMMIT_REVEAL_DAG_NAMESPACE,
                    payload_schemas=[CommitDagSchema(COMMIT_SCHEMA_ID)],
                    schema_id=COMMIT_SCHEMA_ID,
                    signer=signer,
                    author=local_peer_id,
                    gossip_handler=None,
                    topic_validator=build_epoch_phase_topic_validator(
                        self.hypertensor,
                        self.subnet_id,
                        phase="commit",
                        schema_id=COMMIT_SCHEMA_ID,
                        cutoff_percent=self.cutoff_percent,
                    ),
                    is_async_topic_validator=False,
                    request_client=request_client,
                    peer_provider=peer_provider,
                    sync_retry_delay=self.dag_missing_sync_retry_delay,
                    latest_node_snapshot_db_key=COMMIT_TOPIC,
                ),
                DagGossipTopicConfig(
                    topic=REVEAL_TOPIC,
                    namespace=COMMIT_REVEAL_DAG_NAMESPACE,
                    payload_schemas=[RevealDagSchema(REVEAL_SCHEMA_ID, commit_schema_id=COMMIT_SCHEMA_ID)],
                    schema_id=REVEAL_SCHEMA_ID,
                    signer=signer,
                    author=local_peer_id,
                    parent_schema_id=COMMIT_SCHEMA_ID,
                    gossip_handler=None,
                    topic_validator=build_epoch_phase_topic_validator(
                        self.hypertensor,
                        self.subnet_id,
                        phase="reveal",
                        schema_id=REVEAL_SCHEMA_ID,
                        cutoff_percent=self.cutoff_percent,
                    ),
                    is_async_topic_validator=False,
                    request_client=request_client,
                    peer_provider=peer_provider,
                    sync_retry_delay=self.dag_missing_sync_retry_delay,
                    latest_node_snapshot_db_key=REVEAL_TOPIC,
                ),
            ],
            telemetry=self.telemetry,
            log_level=self.gossip_receiver_log_level,
        )
        sync_protocol.set_request_handler(self.dag_system.handle_sync_request_bytes)
        context.nursery.start_soon(self.dag_system.run)

        if self.is_bootstrap:
            return

        sync_peers = await self.dag_system.sync_dag(
            COMMIT_REVEAL_DAG_NAMESPACE,
            min_peer_count=self.dag_startup_sync_min_connected_peers,
            wait_timeout=self.dag_startup_sync_timeout,
            poll_interval=self.dag_startup_sync_poll_interval,
            settle_time=self.dag_startup_sync_settle_time,
        )
        logger.info("Initial commit-reveal DAG sync attempted with peers: %s", sync_peers)

        self.publisher = CommitRevealDagPublisher(
            dag_system=self.dag_system,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            hypertensor=self.hypertensor,
            telemetry=self.telemetry,
            termination_event=context.termination_event,
            publish_interval_seconds=self.publish_interval_seconds,
            cutoff_percent=self.cutoff_percent,
            log_level=self.publish_log_level,
        )
        context.nursery.start_soon(self.publisher.run)

        if self.enable_consensus:
            if context.subnet_info_tracker is None:
                raise RuntimeError("Commit-reveal consensus requires SubnetInfoTracker")
            self.consensus = Consensus(
                db=self.db,
                subnet_id=self.subnet_id,
                subnet_node_id=self.subnet_node_id,
                subnet_info_tracker=context.subnet_info_tracker,
                hypertensor=self.hypertensor,
                skip_activate_subnet=self.skip_activate_subnet,
            )
            context.nursery.start_soon(self.consensus._main_loop)

    async def cleanup(self, context: P2PNetworkContext) -> None:
        if self.consensus is not None:
            await self.consensus.shutdown()
        logger.info("Commit-reveal application shutting down")


class Server(ServerBase):
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
        enable_consensus: bool = True,
        skip_activate_subnet: bool = False,
        enable_proof_of_stake: bool = True,
        strict_maintain_connections: bool = True,
        enable_mDNS: bool = False,
        enable_upnp: bool = False,
        enable_autotls: bool = False,
        resource_manager: ResourceManager | None = None,
        psk: str | None = None,
        telemetry: Telemetry | None = None,
        gossip_receiver_log_level: int = logging.DEBUG,
        publish_log_level: int = logging.DEBUG,
        maintain_connections_log_level: int = logging.DEBUG,
        dag_startup_sync_min_connected_peers: int = 2,
        dag_startup_sync_timeout: float = 180.0,
        dag_startup_sync_settle_time: float = 10.0,
        dag_startup_sync_poll_interval: float = 1.0,
        dag_missing_sync_retry_delay: float = 5.0,
        publish_interval_seconds: float = 5.0,
        cutoff_percent: float = COMMIT_REVEAL_CUTOFF_PERCENT,
        **kwargs: object,
    ) -> None:
        application = CommitRevealApplication(
            key_pair=key_pair,
            db=db,
            subnet_id=subnet_id,
            subnet_node_id=subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=is_bootstrap,
            enable_consensus=enable_consensus,
            telemetry=telemetry,
            dag_startup_sync_min_connected_peers=dag_startup_sync_min_connected_peers,
            dag_startup_sync_timeout=dag_startup_sync_timeout,
            dag_startup_sync_settle_time=dag_startup_sync_settle_time,
            dag_startup_sync_poll_interval=dag_startup_sync_poll_interval,
            dag_missing_sync_retry_delay=dag_missing_sync_retry_delay,
            publish_interval_seconds=publish_interval_seconds,
            cutoff_percent=cutoff_percent,
            gossip_receiver_log_level=gossip_receiver_log_level,
            publish_log_level=publish_log_level,
            skip_activate_subnet=skip_activate_subnet,
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
            enable_proof_of_stake=enable_proof_of_stake,
            db=db,
            subnet_id=subnet_id,
            subnet_slot=subnet_slot,
            subnet_node_id=subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=is_bootstrap,
            enable_subnet_info_tracker=True,
            enable_consensus=False,
            log_random_walk=True,
            random_walk_log_interval=30,
            enable_connection_maintenance=True,
            strict_maintain_connections=strict_maintain_connections,
            telemetry=telemetry,
            maintain_connections_log_level=maintain_connections_log_level,
            **kwargs,
        )

        self.is_bootstrap = is_bootstrap
        self.subnet_id = subnet_id
        self.subnet_slot = subnet_slot
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.db = db
        self.telemetry = telemetry

    async def run(self) -> None:
        try:
            logger.info("Commit-reveal server running subnet_id=%s", self.subnet_id)
            await super().run()
        finally:
            logger.info("Commit-reveal server shutting down")


__all__ = ["CommitRevealApplication", "Server"]
