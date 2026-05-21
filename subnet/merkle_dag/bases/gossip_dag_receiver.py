"""
Template for wiring one or more Merkle DAG namespaces onto py-libp2p GossipSub.

Topic configs with the same DAG namespace share one runtime, storage graph, and
sync service while keeping separate GossipSub subscriptions.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
import inspect
import logging
from typing import TypeAlias, cast

from libp2p.abc import ISubscriptionAPI
from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2
from libp2p.pubsub.pubsub import Pubsub, ValidatorFn
import trio

from subnet.merkle_dag import DagAnnouncement, DagNodeGossip, NodeIngestResult, NodeIngestStatus
from subnet.merkle_dag.bases.dag_config import GossipDagTopicConfig, GossipDagTopicContext
from subnet.merkle_dag.bases.dag_publisher_base import (
    DagNodePublishRequirements,
    DagPublisher,
    PubsubDagAnnouncementPublisher,
)
from subnet.merkle_dag.exceptions import MerkleDagError
from subnet.merkle_dag.interfaces import DagStorage, PayloadSchema, PeerRequestClient, PeerSetProvider
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.merkle_dag.sync_scheduler import SyncScheduler
from subnet.merkle_dag.sync_service import MerkleDagSyncService
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB

logger = logging.getLogger(__name__)

DagGossipMessage: TypeAlias = DagNodeGossip | DagAnnouncement
DagGossipHandlerResult: TypeAlias = NodeIngestResult | bool

class DagGossipSubReceiver:
    """
    Set up and run DAG-backed GossipSub topics from simple configs.

    Typical deployment with a py-libp2p ``Pubsub`` instance and a direct DAG
    sync protocol::

        sync_protocol = MerkleDagSyncProtocol(host=host, db=db, dht=dht)
        request_client = SyncProtocolPeerRequestClient(sync_protocol)
        peer_provider = DagPeerSetProvider(sync_protocol)

        gossip_dag_receiver = DagGossipSubReceiver(
            pubsub=pubsub,
            termination_event=termination_event,
            db=db,
            local_peer_id=host.get_id(),
            topics_config=[
                GossipDagTopicConfig(
                    topic=PEER_STATE_TOPIC,
                    namespace="general-dag",
                    payload_schemas=[PeerStateDagSchema("general-dag")],
                    request_client=request_client,
                    peer_provider=peer_provider,
                    latest_node_snapshot_db_key=PEER_STATE_TOPIC,
                )
            ],
            telemetry=telemetry,
        )

        # Yes: register the receiver-level router, not an individual
        # sync_service, when this class owns one or more DAG namespaces.
        sync_protocol.set_request_handler(gossip_dag_receiver.handle_sync_request_bytes)
        nursery.start_soon(gossip_dag_receiver.run)

        # For app-facing periodic peer-state publishing, prefer
        # DagGossipSystem plus PeerStateDagPublisher so publishes route through
        # DagGossipSystem.publish(namespace, ...).

    For multiple topics, add one ``GossipDagTopicConfig`` per topic. Configs
    with distinct namespaces get distinct ``MerkleDagRuntime`` instances.
    Configs with the same namespace share one runtime and graph. Direct sync
    requests are routed by the request message namespace through
    ``handle_sync_request_bytes``.

    Builders can publish through the included helpers::

        await gossip_dag_receiver.publish_now("orders-topic", requirements)
        await gossip_dag_receiver.trigger_publish("orders-topic", requirements)
        await gossip_dag_receiver.publish_heads("orders-topic")

    """

    def __init__(
        self,
        pubsub: Pubsub,
        termination_event: trio.Event,
        db: RocksDB,
        local_peer_id: ID,
        topics_config: Sequence[GossipDagTopicConfig],
        *,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.db = db
        self.local_peer_id = local_peer_id
        self.topics_config = tuple(topics_config)
        self.telemetry = telemetry
        self.log_level = log_level

        self._validate_topics_config()
        self._contexts_by_topic = self._build_contexts_by_topic()
        self._contexts_by_namespace = self._primary_contexts_by_namespace()
        self._subscribed_topics: set[str] = set()  # Dag specific subscriptions

    @property
    def contexts_by_topic(self) -> Mapping[str, GossipDagTopicContext]:
        """Return configured topic contexts keyed by GossipSub topic."""
        return self._contexts_by_topic

    @property
    def contexts_by_namespace(self) -> Mapping[str, GossipDagTopicContext]:
        """Return configured topic contexts keyed by DAG namespace."""
        return self._contexts_by_namespace

    def context_for_topic(self, topic: str) -> GossipDagTopicContext:
        """Return the assembled context for a topic."""
        return self._contexts_by_topic[topic]

    def context_for_namespace(self, namespace: str) -> GossipDagTopicContext:
        """Return the assembled context for a namespace."""
        return self._contexts_by_namespace[namespace]

    async def run(self) -> None:
        """Run subscriptions, optional publisher loops, and sync helpers."""
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self._cancel_on_termination, nursery.cancel_scope)
                started_sync_namespaces: set[str] = set()
                started_reconciliation_namespaces: set[str] = set()

                for context in self._contexts_by_topic.values():
                    self.pubsub.set_topic_validator(
                        context.topic,
                        self._topic_validator_for(context),
                        is_async_validator=context.config.is_async_topic_validator
                        if context.config.topic_validator is not None
                        else False,
                    )

                    subscription = await self.pubsub.subscribe(context.topic)
                    self._subscribed_topics.add(context.topic)
                    logger.log(
                        self.log_level,
                        "Subscribed to DAG gossip topic '%s' for namespace '%s'",
                        context.topic,
                        context.namespace,
                    )
                    nursery.start_soon(self._receive_loop, context.topic, subscription)

                    if context.config.start_publisher:
                        nursery.start_soon(context.publisher.run)
                    if context.sync_scheduler is not None and context.namespace not in started_sync_namespaces:
                        nursery.start_soon(context.sync_scheduler.run)
                        started_sync_namespaces.add(context.namespace)
                    if (
                        context.config.enable_periodic_reconciliation
                        and context.namespace not in started_reconciliation_namespaces
                    ):
                        nursery.start_soon(context.sync_service.run)
                        started_reconciliation_namespaces.add(context.namespace)
        finally:
            await self._unsubscribe_all()

    async def publish_now(self, topic: str, requirements: DagNodePublishRequirements):
        """Build, store, and gossip a DAG node on one configured topic."""
        return await self.context_for_topic(topic).publisher.publish_now(requirements)

    async def trigger_publish(self, topic: str, requirements: DagNodePublishRequirements):
        """Queue a background DAG publish on one configured topic."""
        return await self.context_for_topic(topic).publisher.trigger_publish(requirements)

    async def publish_heads(self, topic: str) -> DagAnnouncement | None:
        """Publish the current DAG heads for one configured topic."""
        return await self.context_for_topic(topic).publisher.publish_heads()

    async def handle_sync_request_bytes(self, from_peer: str, payload: bytes) -> bytes:
        """Route a direct DAG sync request to the service for its namespace."""
        message = next(iter(self._contexts_by_topic.values())).runtime.codec.decode(payload)
        namespace = getattr(message, "namespace", None)
        if not isinstance(namespace, str) or namespace not in self._contexts_by_namespace:
            raise ValueError(f"No DAG gossip context configured for namespace {namespace!r}")

        return await self._contexts_by_namespace[namespace].sync_service.handle_sync_request_bytes(from_peer, payload)

    async def _receive_loop(self, topic: str, subscription: ISubscriptionAPI) -> None:
        """Receive messages for one DAG gossip topic."""
        logger.log(self.log_level, "Starting DAG gossip receive loop for topic '%s'", topic)
        while not self.termination_event.is_set():
            try:
                message = await subscription.get()
                await self._handle_message(message)
            except Exception:
                logger.exception("Error in DAG gossip receive loop for topic '%s'", topic)
                await trio.sleep(1)

    async def _handle_message(self, message: rpc_pb2.Message) -> None:
        """Dispatch a raw py-libp2p message to each matching DAG topic."""
        matching_topics = [topic for topic in message.topicIDs if topic in self._contexts_by_topic]
        if not matching_topics:
            logger.warning("Ignoring DAG gossip message for unconfigured topics: %s", list(message.topicIDs))
            return

        for topic in matching_topics:
            await self._handle_topic_message(self._contexts_by_topic[topic], message)

    async def _handle_topic_message(self, context: GossipDagTopicContext, message: rpc_pb2.Message) -> None:
        from_peer_id = ID(message.from_id)
        from_peer = from_peer_id.to_string()
        if from_peer == context.runtime.local_peer_id:
            return

        decoded = context.runtime.codec.decode(message.data)
        if isinstance(decoded, DagNodeGossip):
            result = await self._handle_node_gossip(context, from_peer_id, decoded)
            if result is not None:
                await self._call_topic_handler(context, from_peer_id, decoded, result)
            return

        if isinstance(decoded, DagAnnouncement):
            processed = await self._handle_announcement(context, from_peer_id, decoded)
            if processed:
                await self._call_topic_handler(context, from_peer_id, decoded, processed)
            return

        logger.warning(
            "Ignoring non-gossip DAG sync message %s on topic '%s'",
            type(decoded).__name__,
            context.topic,
        )

    async def _handle_node_gossip(
        self,
        context: GossipDagTopicContext,
        from_peer_id: ID,
        message: DagNodeGossip,
    ) -> NodeIngestResult | None:
        from_peer = from_peer_id.to_string()
        if not self._node_gossip_matches_context(context, from_peer, message):
            return None

        try:
            context.runtime.validator.validate_header_source_peer(message.node.header, from_peer)
            ingest_result = await context.runtime.dag.add_node(
                message.node,
                from_peer=from_peer,
                validate_remote_timestamp=True,
            )
        except MerkleDagError as exc:
            logger.warning("Rejected DAG node from %s on topic '%s': %s", from_peer, context.topic, exc)
            if self.telemetry:
                await self.telemetry.emit_async(
                    "dag_node_rejected",
                    peer_id=from_peer,
                    topic=context.topic,
                    namespace=context.namespace,
                    reason=type(exc).__name__,
                )
            return None

        await self._schedule_missing_parent_sync(context, from_peer, ingest_result)
        if self.telemetry:
            await self.telemetry.emit_async(
                "dag_node_received",
                peer_id=from_peer,
                topic=context.topic,
                namespace=context.namespace,
                header=message.node.header.to_primitive(),
                node_id=message.node.header.node_id,
                status=ingest_result.status.value,
                missing_parents=list(ingest_result.missing_parents),
            )

        logger.log(
            self.log_level,
            "Processed DAG node %s from %s on topic '%s' with status %s",
            message.node.header.node_id,
            from_peer,
            context.topic,
            ingest_result.status.value,
        )
        if ingest_result.status != NodeIngestStatus.REJECTED:
            self._store_latest_node_snapshot(context, from_peer, message)
        return ingest_result

    async def _handle_announcement(
        self,
        context: GossipDagTopicContext,
        from_peer_id: ID,
        announcement: DagAnnouncement,
    ) -> bool:
        from_peer = from_peer_id.to_string()
        if announcement.namespace != context.namespace:
            logger.warning(
                "Ignoring DAG announcement for namespace '%s' on topic '%s'",
                announcement.namespace,
                context.topic,
            )
            return False
        if announcement.peer_id != from_peer:
            logger.warning(
                "Ignoring DAG announcement from %s that claimed peer_id %s",
                from_peer,
                announcement.peer_id,
            )
            return False

        processed = await context.sync_service.handle_announcement(announcement, source_peer=from_peer)
        if processed and self.telemetry:
            await self.telemetry.emit_async(
                "dag_announcement_received",
                peer_id=from_peer,
                topic=context.topic,
                namespace=context.namespace,
                head_ids=list(announcement.head_ids),
                node_count=announcement.node_count,
            )
        return processed

    async def _call_topic_handler(
        self,
        context: GossipDagTopicContext,
        from_peer_id: ID,
        message: DagGossipMessage,
        result: DagGossipHandlerResult,
    ) -> None:
        if context.config.topic_handler is None:
            return

        handler_result = context.config.topic_handler(context, from_peer_id, message, result)
        if inspect.isawaitable(handler_result):
            await cast(Awaitable[None], handler_result)

    async def _schedule_missing_parent_sync(
        self,
        context: GossipDagTopicContext,
        peer_id: str,
        ingest_result: NodeIngestResult,
    ) -> None:
        if ingest_result.status != NodeIngestStatus.ORPHAN or not ingest_result.missing_parents:
            return

        try:
            if context.sync_scheduler is not None:
                await context.sync_scheduler.schedule(peer_id)
                return

            await context.runtime.coordinator.fetch_missing(peer_id, ingest_result.missing_parents)
        except Exception:
            logger.exception(
                "Failed to schedule missing DAG parents %s from peer %s on topic '%s'",
                ingest_result.missing_parents,
                peer_id,
                context.topic,
            )

    def _build_contexts_by_topic(self) -> dict[str, GossipDagTopicContext]:
        contexts: dict[str, GossipDagTopicContext] = {}
        runtimes_by_namespace: dict[str, MerkleDagRuntime] = {}
        sync_services_by_namespace: dict[str, MerkleDagSyncService] = {}
        sync_schedulers_by_namespace: dict[str, SyncScheduler | None] = {}
        for config in self.topics_config:
            namespace = config.dag_namespace
            runtime = runtimes_by_namespace.get(namespace)
            if runtime is None:
                namespace_config = self._namespace_config(namespace)
                namespace_configs = self._configs_for_namespace(namespace)
                runtime = MerkleDagRuntime(
                    db=self.db,
                    payload_schemas=self._payload_schemas_for_namespace(namespace),
                    local_peer_id=self.local_peer_id,
                    namespace=namespace,
                    dag_topic=namespace_config.topic,
                    storage=self._storage_for_namespace(namespace),
                    request_client=self._request_client_for_namespace(namespace),
                    gossip_publisher=PubsubDagAnnouncementPublisher(self.pubsub),
                    max_fetch_batch=max(topic_config.max_fetch_batch for topic_config in namespace_configs),
                )
                runtimes_by_namespace[namespace] = runtime
                sync_services_by_namespace[namespace] = MerkleDagSyncService(
                    runtime=runtime,
                    termination_event=self.termination_event,
                    peer_provider=self._peer_provider_for_namespace(namespace),
                    enable_periodic_reconciliation=any(
                        topic_config.enable_periodic_reconciliation for topic_config in namespace_configs
                    ),
                    reconciliation_interval=min(
                        topic_config.reconciliation_interval for topic_config in namespace_configs
                    ),
                )
                sync_schedulers_by_namespace[namespace] = (
                    SyncScheduler(
                        runtime=runtime,
                        termination_event=self.termination_event,
                        peer_provider=self._peer_provider_for_namespace(namespace),
                        telemetry=self.telemetry,
                        batch_window=min(topic_config.sync_batch_window for topic_config in namespace_configs),
                        retry_delay=min(topic_config.sync_retry_delay for topic_config in namespace_configs),
                        sync_on_startup=any(topic_config.sync_on_startup for topic_config in namespace_configs),
                        log_level=self.log_level,
                    )
                    if any(topic_config.enable_missing_parent_sync for topic_config in namespace_configs)
                    else None
                )

            publisher = DagPublisher.from_runtime(
                pubsub=self.pubsub,
                termination_event=self.termination_event,
                runtime=runtime,
                dag_topic=config.topic,
                telemetry=self.telemetry,
                max_queue_size=config.max_queue_size,
                log_level=self.log_level,
            )
            contexts[config.topic] = GossipDagTopicContext(
                config=config,
                runtime=runtime,
                publisher=publisher,
                sync_service=sync_services_by_namespace[namespace],
                sync_scheduler=sync_schedulers_by_namespace[namespace],
            )
        return contexts

    def _configs_for_namespace(self, namespace: str) -> tuple[GossipDagTopicConfig, ...]:
        return tuple(config for config in self.topics_config if config.dag_namespace == namespace)

    def _namespace_config(self, namespace: str) -> GossipDagTopicConfig:
        return self._configs_for_namespace(namespace)[0]

    def _storage_for_namespace(self, namespace: str) -> DagStorage | None:
        return next(
            (config.storage for config in self._configs_for_namespace(namespace) if config.storage is not None),
            None,
        )

    def _request_client_for_namespace(self, namespace: str) -> PeerRequestClient | None:
        return next(
            (
                config.request_client
                for config in self._configs_for_namespace(namespace)
                if config.request_client is not None
            ),
            None,
        )

    def _peer_provider_for_namespace(self, namespace: str) -> PeerSetProvider | None:
        return next(
            (
                config.peer_provider
                for config in self._configs_for_namespace(namespace)
                if config.peer_provider is not None
            ),
            None,
        )

    def _payload_schemas_for_namespace(self, namespace: str) -> tuple[PayloadSchema, ...]:
        schemas: list[PayloadSchema] = []
        seen_schema_ids: set[str] = set()
        for config in self._configs_for_namespace(namespace):
            for schema in config.payload_schemas:
                schema_id = str(schema.schema_id)
                if schema_id in seen_schema_ids:
                    continue
                seen_schema_ids.add(schema_id)
                schemas.append(schema)
        return tuple(schemas)

    def _primary_contexts_by_namespace(self) -> dict[str, GossipDagTopicContext]:
        contexts: dict[str, GossipDagTopicContext] = {}
        for context in self._contexts_by_topic.values():
            contexts.setdefault(context.namespace, context)
        return contexts

    def _store_latest_node_snapshot(
        self,
        context: GossipDagTopicContext,
        peer_id: str,
        message: DagNodeGossip,
    ) -> None:
        db_key = context.config.latest_node_snapshot_db_key
        if db_key is None:
            return

        node_id = message.node.header.node_id
        created_at_ms = message.node.header.created_at_ms
        current = {
            "peer_id": peer_id,
            "node_id": node_id,
            "created_at_ms": created_at_ms,
            **message.node.header.metadata,
        }

        previous = self.db.get_nested(db_key, peer_id)
        if isinstance(previous, dict):
            previous_created_at_ms = int(previous.get("created_at_ms", -1))
            previous_node_id = str(previous.get("node_id", ""))
            if (previous_created_at_ms, previous_node_id) >= (created_at_ms, node_id):
                return

        self.db.set_nested(db_key, peer_id, current)

    def _topic_validator_for(self, context: GossipDagTopicContext) -> ValidatorFn:
        if context.config.topic_validator is not None:
            return context.config.topic_validator
        return self._default_topic_validator_for(context)

    def _default_topic_validator_for(self, context: GossipDagTopicContext) -> ValidatorFn:
        def validate(_forwarder_peer_id: ID, message: rpc_pb2.Message) -> bool:
            try:
                return self._validate_dag_pubsub_message(context, message)
            except Exception:
                logger.debug(
                    "DAG pubsub validation failed for topic '%s'",
                    context.topic,
                    exc_info=True,
                )
                return False

        return validate

    def _validate_dag_pubsub_message(self, context: GossipDagTopicContext, message: rpc_pb2.Message) -> bool:
        from_peer = ID(message.from_id).to_string()
        decoded = context.runtime.codec.decode(message.data)

        if isinstance(decoded, DagNodeGossip):
            if not self._node_gossip_matches_context(context, from_peer, decoded):
                return False
            context.runtime.validator.validate_header_source_peer(decoded.node.header, from_peer)
            context.runtime.validator.validate_node(decoded.node)
            context.runtime.validator.validate_remote_header(decoded.node.header)
            return True

        if isinstance(decoded, DagAnnouncement):
            return decoded.namespace == context.namespace and decoded.peer_id == from_peer

        return False

    def _node_gossip_matches_context(
        self,
        context: GossipDagTopicContext,
        from_peer: str,
        message: DagNodeGossip,
    ) -> bool:
        if message.namespace != context.namespace or message.node.header.namespace != context.namespace:
            logger.warning(
                "Ignoring DAG node for namespace '%s' on topic '%s'",
                message.namespace,
                context.topic,
            )
            return False
        if message.peer_id != from_peer:
            logger.warning(
                "Ignoring DAG node from %s that claimed peer_id %s",
                from_peer,
                message.peer_id,
            )
            return False
        topic_schema_ids = {str(schema.schema_id) for schema in context.config.payload_schemas}
        if message.node.header.schema_id not in topic_schema_ids:
            logger.warning(
                "Ignoring DAG node with schema_id '%s' on topic '%s'",
                message.node.header.schema_id,
                context.topic,
            )
            return False
        return True

    def _validate_topics_config(self) -> None:
        if not self.topics_config:
            raise ValueError("DagGossipSubReceiver requires at least one GossipDagTopicConfig")

        seen_topics: set[str] = set()
        duplicate_topics: set[str] = set()
        namespace_storage: dict[str, DagStorage] = {}
        namespace_request_client: dict[str, PeerRequestClient] = {}
        namespace_peer_provider: dict[str, PeerSetProvider] = {}

        for config in self.topics_config:
            if not config.topic:
                raise ValueError("GossipDagTopicConfig.topic must be a non-empty string")
            if not config.dag_namespace:
                raise ValueError("GossipDagTopicConfig.namespace must be a non-empty string")
            if not config.payload_schemas:
                raise ValueError("GossipDagTopicConfig.payload_schemas must contain at least one schema")

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

        if duplicate_topics:
            duplicates = ", ".join(sorted(duplicate_topics))
            raise ValueError(f"Duplicate DAG gossip topic configuration: {duplicates}")

    async def _cancel_on_termination(self, cancel_scope: trio.CancelScope) -> None:
        await self.termination_event.wait()
        cancel_scope.cancel()

    async def _unsubscribe_all(self) -> None:
        for topic in tuple(self._subscribed_topics):
            try:
                await self.pubsub.unsubscribe(topic)
            except Exception:
                logger.exception("Failed to unsubscribe from DAG gossip topic '%s'", topic)
            finally:
                self._subscribed_topics.discard(topic)
