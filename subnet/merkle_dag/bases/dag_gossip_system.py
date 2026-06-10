"""
One-class template for running DAG-backed GossipSub topics.

This module is the beginner-facing entrypoint for decentralized app builders:
configure one or more topics, start one system, and use the returned contexts or
helpers to publish/query each topic's DAG.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
import inspect
import logging
from typing import Any, TypeAlias, cast

from libp2p.peer.id import ID
from libp2p.pubsub.pubsub import Pubsub, ValidatorFn
import trio

from subnet.merkle_dag.bases.dag_publisher_base import (
    DagNodePublishRequirements,
    DagPublisher,
    DagPublisherConfig,
    DagPublishResult,
)
from subnet.merkle_dag.bases.gossip_dag_receiver import (
    DagGossipHandlerResult,
    DagGossipMessage,
    DagGossipSubReceiver,
    GossipDagTopicConfig,
    GossipDagTopicContext,
)
from subnet.merkle_dag.interfaces import DagStorage, PayloadSchema, PeerRequestClient, PeerSetProvider, Signer
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.merkle_dag.sync_scheduler import SyncScheduler
from subnet.merkle_dag.sync_service import MerkleDagSyncService
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB

logger = logging.getLogger(__name__)

_MISSING = object()

DagTopicGossipHandler: TypeAlias = Callable[
    ["DagGossipTopicContext", ID, DagGossipMessage, DagGossipHandlerResult],
    Awaitable[None] | None,
]


@dataclass(frozen=True, slots=True)
class DagGossipTopicConfig:
    """
    Configure one GossipSub topic, a DAG namespace, and publish defaults.

    The default namespace is the topic name. That keeps the common deployment
    model simple: one topic is one DAG. Reuse the same ``namespace`` across
    multiple topic configs when separate wire topics should feed one DAG graph.

    Minimal publish-ready topic::

        DagGossipTopicConfig(
            topic="orders",
            payload_schemas=[OrderSchema()],
            schema_id="order",
            signer=Libp2pKeyPairSigner(key_pair),
        )

    Receiving-only topics are valid too. Omit ``schema_id`` and ``signer``, and
    publish later with explicit ``DagNodePublishRequirements`` if needed.

    Args:

        topic: GossipSub topic name. This is the network topic peers subscribe
            to and publish DAG gossip messages on.
        payload_schemas: Payload schemas accepted by this topic's DAG. These
            validate node payloads before local storage or gossip accepts them.
        schema_id: Default payload schema id for template-style publishing.
            Required with ``signer`` when calling ``publish`` with only a
            payload.
        signer: Signer used to sign local DAG nodes for this topic. Required
            with ``schema_id`` for template-style publishing.
        namespace: DAG namespace for this topic. Defaults to ``topic``. Use a
            custom value only when the wire topic and DAG namespace should
            intentionally differ.
        author: Optional DAG node header author. Defaults to the local peer id.
        parent_schema_id: Optional schema id filter for default parent
            selection. Useful when a DAG stores multiple schemas but this
            publisher should link only one chain/frontier.
        skip_if_orphans: If true, template publishing pauses while unresolved
            orphan nodes exist locally.
        default_metadata: Static metadata used for template publishes when
            callers do not pass explicit metadata.
        gossip_handler: Optional hook called after an incoming DAG gossip
            message is processed.
        topic_validator: Optional py-libp2p topic validator override. If
            omitted, the system registers a DAG-aware validator.
        is_async_topic_validator: Set true when ``topic_validator`` is async.
        storage: Optional DAG storage for this topic. If omitted, the runtime
            creates namespace-isolated default storage.
        request_client: Optional direct DAG sync client used to fetch missing
            nodes from peers.
        peer_provider: Optional provider for peer ids used by missing-parent
            sync, startup sync, and periodic reconciliation.
        enable_missing_parent_sync: If true, starts the event-driven sync
            scheduler that repairs orphan nodes.
        sync_on_startup: If true, the missing-parent scheduler checks existing
            orphan state when the system starts.
        sync_batch_window: Short delay used to coalesce multiple orphan sync
            signals into one fetch pass.
        enable_periodic_reconciliation: If true, starts periodic anti-entropy
            reconciliation against peers from ``peer_provider``.
        reconciliation_interval: Delay between periodic reconciliation passes.
        latest_node_snapshot_db_key: Optional plain-DB key used to cache the
            latest node metadata by peer id for this topic.
        max_queue_size: Maximum internal queue size for lower-level publisher
            compatibility.
        max_fetch_batch: Maximum number of DAG nodes fetched per sync batch.

    """

    topic: str
    payload_schemas: Sequence[PayloadSchema]

    schema_id: str | None = None
    signer: Signer | None = None
    namespace: str | None = None
    author: str | None = None
    parent_schema_id: str | None = None
    skip_if_orphans: bool = True
    default_metadata: dict[str, Any] = field(default_factory=dict)

    gossip_handler: DagTopicGossipHandler | None = None
    topic_validator: ValidatorFn | None = None
    is_async_topic_validator: bool = False
    storage: DagStorage | None = None
    request_client: PeerRequestClient | None = None
    peer_provider: PeerSetProvider | None = None
    enable_missing_parent_sync: bool = True
    sync_on_startup: bool = True
    sync_batch_window: float = 0.05
    sync_retry_delay: float = 5.0
    enable_periodic_reconciliation: bool = False
    reconciliation_interval: float = 30.0
    latest_node_snapshot_db_key: str | None = None
    max_queue_size: int = 1024
    max_fetch_batch: int = 32

    @property
    def dag_namespace(self) -> str:
        """Return the concrete DAG namespace for this topic."""
        return self.namespace or self.topic

    @property
    def has_template_publisher(self) -> bool:
        """Return whether this topic has enough config for template-style publishing."""
        return self.schema_id is not None and self.signer is not None


@dataclass(frozen=True, slots=True)
class DagGossipTopicContext:
    """Runtime objects assembled for one configured DAG gossip topic."""

    config: DagGossipTopicConfig
    receiver_context: GossipDagTopicContext
    publisher: DagPublisher

    @property
    def topic(self) -> str:
        """Return the GossipSub topic."""
        return self.config.topic

    @property
    def namespace(self) -> str:
        """Return the DAG namespace."""
        return self.config.dag_namespace

    @property
    def runtime(self) -> MerkleDagRuntime:
        """Return the topic's DAG runtime."""
        return self.receiver_context.runtime

    @property
    def sync_service(self) -> MerkleDagSyncService:
        """Return the direct DAG sync service."""
        return self.receiver_context.sync_service

    @property
    def sync_scheduler(self) -> SyncScheduler | None:
        """Return the orphan-driven sync scheduler, if enabled."""
        return self.receiver_context.sync_scheduler

    @property
    def dag(self):
        """Return the underlying Merkle DAG."""
        return self.runtime.dag


class DagGossipSystem:
    """
    Run DAG receive/sync logic and expose namespace-scoped publishing.

    Use this class when building an app on top of py-libp2p GossipSub and the
    Merkle DAG runtime. It handles:

    - one or more GossipSub topics per DAG namespace,
    - topic validators for DAG gossip messages,
    - GossipSub subscriptions and DAG node ingestion,
    - missing-parent sync and optional periodic reconciliation,
    - direct sync request routing across multiple namespaces,
    - direct local DAG publishes by namespace.

    Typical deployment::

        sync_protocol = MerkleDagSyncProtocol(host=host, db=db, dht=dht, pubsub=pubsub, gossipsub=gossipsub)
        request_client = SyncProtocolPeerRequestClient(sync_protocol)
        peer_provider = DagPeerSetProvider(sync_protocol)

        dag_system = DagGossipSystem(
            pubsub=pubsub,
            termination_event=termination_event,
            db=db,
            local_peer_id=host.get_id(),
            topics=[
                DagGossipTopicConfig(
                    topic="orders",
                    payload_schemas=[OrderSchema()],
                    schema_id="order",
                    signer=Libp2pKeyPairSigner(key_pair),
                    request_client=request_client,
                    peer_provider=peer_provider,
                ),
                DagGossipTopicConfig(
                    topic="invoices",
                    payload_schemas=[InvoiceSchema()],
                    schema_id="invoice",
                    signer=Libp2pKeyPairSigner(key_pair),
                    request_client=request_client,
                    peer_provider=peer_provider,
                ),
            ],
            telemetry=telemetry,
        )

        # Register the system-level router when one process owns multiple DAGs.
        sync_protocol.set_request_handler(dag_system.handle_sync_request_bytes)
        nursery.start_soon(dag_system.run)

        await dag_system.publish("orders", {"kind": "order", "order_id": "o-1"})

    Application code owns when and what to publish. Call ``publish(...)`` with
    the configured DAG namespace; the system stores the node locally and gossips
    it on the namespace's configured GossipSub topic.
    """

    def __init__(
        self,
        pubsub: Pubsub,
        termination_event: trio.Event,
        db: RocksDB,
        local_peer_id: ID,
        topics: Sequence[DagGossipTopicConfig],
        *,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.db = db
        self.local_peer_id = local_peer_id
        self.topics = tuple(topics)
        self.telemetry = telemetry
        self.log_level = log_level

        self._validate_topics()
        self.receiver = DagGossipSubReceiver(
            pubsub=pubsub,
            termination_event=termination_event,
            db=db,
            local_peer_id=local_peer_id,
            topics_config=[self._receiver_config_for(config) for config in self.topics],
            telemetry=telemetry,
            log_level=log_level,
        )
        self._contexts_by_topic = self._build_contexts_by_topic()
        self._contexts_by_namespace = self._primary_contexts_by_namespace()

    @property
    def contexts_by_topic(self) -> Mapping[str, DagGossipTopicContext]:
        """Return configured topic contexts keyed by GossipSub topic."""
        return self._contexts_by_topic

    @property
    def contexts_by_namespace(self) -> Mapping[str, DagGossipTopicContext]:
        """Return primary topic contexts keyed by DAG namespace."""
        return self._contexts_by_namespace

    def context_for_topic(self, topic: str) -> DagGossipTopicContext:
        """Return the assembled context for a GossipSub topic."""
        try:
            return self._contexts_by_topic[topic]
        except KeyError as exc:
            raise ValueError(f"No DAG gossip topic configured for {topic!r}") from exc

    def context_for_namespace(self, namespace: str) -> DagGossipTopicContext:
        """Return the assembled context for a DAG namespace."""
        try:
            return self._contexts_by_namespace[namespace]
        except KeyError as exc:
            raise ValueError(f"No DAG namespace configured for {namespace!r}") from exc

    async def run(self) -> None:
        """Run all configured receivers and sync helpers."""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.receiver.run)

            await self.termination_event.wait()
            nursery.cancel_scope.cancel()

    async def publish(
        self,
        namespace: str,
        payload_or_requirements: DagNodePublishRequirements | Any = _MISSING,
        *,
        metadata: dict[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        """Publish a DAG node immediately on one configured namespace."""
        publish_schema_id = self._publish_schema_id(payload_or_requirements, schema_id)
        publisher = self._context_for_publish(namespace, publish_schema_id).publisher
        if isinstance(payload_or_requirements, DagNodePublishRequirements):
            return await publisher.publish_now(payload_or_requirements)
        if payload_or_requirements is _MISSING:
            return await publisher.publish(
                metadata=metadata,
                parent_ids=parent_ids,
                schema_id=schema_id,
                signer=signer,
                author=author,
                created_at_ms=created_at_ms,
            )
        return await publisher.publish(
            payload_or_requirements,
            metadata=metadata,
            parent_ids=parent_ids,
            schema_id=schema_id,
            signer=signer,
            author=author,
            created_at_ms=created_at_ms,
        )

    async def sync_dag(
        self,
        namespace: str,
        *,
        min_peer_count: int = 2,
        wait_timeout: float = 30.0,
        poll_interval: float = 1.0,
        settle_time: float = 3.0,
    ) -> tuple[str, ...]:
        """Run one startup reconciliation pass for a configured namespace."""
        return await self.context_for_namespace(namespace).sync_service.sync_dag(
            min_peer_count=min_peer_count,
            wait_timeout=wait_timeout,
            poll_interval=poll_interval,
            settle_time=settle_time,
        )

    async def handle_sync_request_bytes(self, from_peer: str, payload: bytes) -> bytes:
        """Route a direct DAG sync request to the correct namespace service."""
        return await self.receiver.handle_sync_request_bytes(from_peer, payload)

    def _receiver_config_for(self, config: DagGossipTopicConfig) -> GossipDagTopicConfig:
        return GossipDagTopicConfig(
            topic=config.topic,
            payload_schemas=config.payload_schemas,
            topic_handler=self._receiver_handler_for(config),
            topic_validator=config.topic_validator,
            is_async_topic_validator=config.is_async_topic_validator,
            namespace=config.namespace,
            storage=config.storage,
            request_client=config.request_client,
            peer_provider=config.peer_provider,
            enable_missing_parent_sync=config.enable_missing_parent_sync,
            sync_on_startup=config.sync_on_startup,
            sync_batch_window=config.sync_batch_window,
            sync_retry_delay=config.sync_retry_delay,
            enable_periodic_reconciliation=config.enable_periodic_reconciliation,
            reconciliation_interval=config.reconciliation_interval,
            start_publisher=False,
            latest_node_snapshot_db_key=config.latest_node_snapshot_db_key,
            max_queue_size=config.max_queue_size,
            max_fetch_batch=config.max_fetch_batch,
        )

    def _receiver_handler_for(self, config: DagGossipTopicConfig):
        if config.gossip_handler is None:
            return None

        async def handle(
            _receiver_context: GossipDagTopicContext,
            from_peer_id: ID,
            message: DagGossipMessage,
            result: DagGossipHandlerResult,
        ) -> None:
            context = self.context_for_topic(config.topic)
            handler_result = config.gossip_handler(context, from_peer_id, message, result)
            if inspect.isawaitable(handler_result):
                await cast(Awaitable[None], handler_result)

        return handle

    def _build_contexts_by_topic(self) -> dict[str, DagGossipTopicContext]:
        contexts: dict[str, DagGossipTopicContext] = {}
        for config in self.topics:
            receiver_context = self.receiver.context_for_topic(config.topic)
            publisher = DagPublisher.from_runtime(
                pubsub=self.pubsub,
                termination_event=self.termination_event,
                runtime=receiver_context.runtime,
                config=self._publisher_config_for(config),
                dag_topic=config.topic,
                telemetry=self.telemetry,
                max_queue_size=config.max_queue_size,
                log_level=self.log_level,
            )
            contexts[config.topic] = DagGossipTopicContext(
                config=config,
                receiver_context=receiver_context,
                publisher=publisher,
            )
        return contexts

    def _primary_contexts_by_namespace(self) -> dict[str, DagGossipTopicContext]:
        contexts: dict[str, DagGossipTopicContext] = {}
        for context in self._contexts_by_topic.values():
            contexts.setdefault(context.namespace, context)
        return contexts

    def _publish_schema_id(
        self,
        requirements_or_payload: DagNodePublishRequirements | Any,
        schema_id: str | None,
    ) -> str | None:
        if isinstance(requirements_or_payload, DagNodePublishRequirements):
            return requirements_or_payload.schema_id
        return schema_id

    def _context_for_publish(self, namespace: str, schema_id: str | None) -> DagGossipTopicContext:
        namespace_contexts = [context for context in self._contexts_by_topic.values() if context.namespace == namespace]
        if not namespace_contexts:
            raise ValueError(f"No DAG namespace configured for {namespace!r}")
        if schema_id is None:
            return namespace_contexts[0]

        for context in namespace_contexts:
            if schema_id in self._schema_ids_for_config(context.config):
                return context

        raise ValueError(f"No DAG topic in namespace {namespace!r} is configured for schema_id={schema_id!r}")

    @staticmethod
    def _schema_ids_for_config(config: DagGossipTopicConfig) -> set[str]:
        return {str(schema.schema_id) for schema in config.payload_schemas}

    def _publisher_config_for(self, config: DagGossipTopicConfig) -> DagPublisherConfig | None:
        if not config.has_template_publisher:
            return None

        assert config.schema_id is not None
        assert config.signer is not None
        return DagPublisherConfig(
            topic=config.topic,
            namespace=config.dag_namespace,
            schema_id=config.schema_id,
            signer=config.signer,
            payload_schemas=config.payload_schemas,
            author=config.author,
            parent_schema_id=config.parent_schema_id,
            skip_if_orphans=config.skip_if_orphans,
            latest_node_snapshot_db_key=config.latest_node_snapshot_db_key,
            storage=config.storage,
            max_queue_size=config.max_queue_size,
            max_fetch_batch=config.max_fetch_batch,
            default_metadata=config.default_metadata,
        )

    def _validate_topics(self) -> None:
        if not self.topics:
            raise ValueError("DagGossipSystem requires at least one DagGossipTopicConfig")

        seen_topics: set[str] = set()
        duplicate_topics: set[str] = set()
        namespace_storage: dict[str, DagStorage] = {}
        namespace_request_client: dict[str, PeerRequestClient] = {}
        namespace_peer_provider: dict[str, PeerSetProvider] = {}

        for config in self.topics:
            if not config.topic:
                raise ValueError("DagGossipTopicConfig.topic must be a non-empty string")
            if not config.dag_namespace:
                raise ValueError("DagGossipTopicConfig.namespace must be a non-empty string")
            if not config.payload_schemas:
                raise ValueError("DagGossipTopicConfig.payload_schemas must contain at least one schema")

            if config.topic in seen_topics:
                duplicate_topics.add(config.topic)
            seen_topics.add(config.topic)

            existing_storage = namespace_storage.get(config.dag_namespace)
            if config.storage is not None and existing_storage is not None and config.storage is not existing_storage:
                raise ValueError(f"Shared DAG namespace {config.dag_namespace!r} cannot use multiple storage objects")
            if config.storage is not None:
                namespace_storage[config.dag_namespace] = config.storage

            existing_request_client = namespace_request_client.get(config.dag_namespace)
            if (
                config.request_client is not None
                and existing_request_client is not None
                and config.request_client is not existing_request_client
            ):
                raise ValueError(f"Shared DAG namespace {config.dag_namespace!r} cannot use multiple request clients")
            if config.request_client is not None:
                namespace_request_client[config.dag_namespace] = config.request_client

            existing_peer_provider = namespace_peer_provider.get(config.dag_namespace)
            if (
                config.peer_provider is not None
                and existing_peer_provider is not None
                and config.peer_provider is not existing_peer_provider
            ):
                raise ValueError(f"Shared DAG namespace {config.dag_namespace!r} cannot use multiple peer providers")
            if config.peer_provider is not None:
                namespace_peer_provider[config.dag_namespace] = config.peer_provider

            self._validate_publisher_options(config)

        if duplicate_topics:
            duplicates = ", ".join(sorted(duplicate_topics))
            raise ValueError(f"Duplicate DAG gossip topic configuration: {duplicates}")

    def _validate_publisher_options(self, config: DagGossipTopicConfig) -> None:
        if config.schema_id is None and config.signer is None:
            publisher_options = (
                config.author,
                config.parent_schema_id,
            )
            if any(option is not None for option in publisher_options) or config.default_metadata:
                raise ValueError(
                    "DagGossipTopicConfig publisher options require both schema_id and signer "
                    f"for topic {config.topic!r}"
                )
            return

        if config.schema_id is None or config.signer is None:
            raise ValueError(f"DagGossipTopicConfig requires schema_id and signer together for topic {config.topic!r}")


DagSystem = DagGossipSystem
