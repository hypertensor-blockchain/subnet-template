"""Template publisher for writing local data into a Merkle DAG and GossipSub."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
import inspect
import logging
import time
from typing import Any, TypeAlias, cast

from libp2p.peer.id import ID
from libp2p.pubsub.pubsub import Pubsub
import trio

from subnet.merkle_dag import DagAnnouncement, DagNodeGossip
from subnet.merkle_dag.interfaces import DagStorage, PayloadSchema, Signer
from subnet.merkle_dag.models import NodeIngestStatus
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB

logger = logging.getLogger(__name__)

DEFAULT_DAG_TOPIC = "merkle_dag_sync"
_MISSING = object()

DagPayloadFactory: TypeAlias = Callable[["DagPublisher"], Awaitable[Any] | Any]
DagMetadataFactory: TypeAlias = Callable[["DagPublisher"], Awaitable[dict[str, Any]] | dict[str, Any]]
DagParentSelector: TypeAlias = Callable[["DagPublisher", str], Awaitable[Sequence[str]] | Sequence[str]]
DagPublishHandler: TypeAlias = Callable[["DagPublisher", "DagPublishResult"], Awaitable[None] | None]


class DagPublishError(Exception):
    """Base exception for DAG publishing failures."""


class DagPublishPreconditionError(DagPublishError):
    """Raised when local DAG state does not satisfy publish requirements."""


class PubsubDagAnnouncementPublisher:
    """Publishes DAG sync announcements over the existing `Pubsub` service."""

    def __init__(self, pubsub: Pubsub):
        self._pubsub = pubsub

    async def publish(self, topic: str, payload: bytes) -> None:
        """Publish a raw DAG sync message to GossipSub."""
        await self._pubsub.publish(topic, payload)


@dataclass(frozen=True)
class DagNodePublishRequirements:
    """Requirements needed to construct a new immutable DAG node."""

    schema_id: str
    payload: Any
    parent_ids: tuple[str, ...]
    author: str
    signer: Signer
    created_at_ms: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DagPublishEvent:
    """Trigger payload consumed by the publisher event loop."""

    requirements: DagNodePublishRequirements
    event_id: str
    triggered_at_ms: int


@dataclass(frozen=True)
class DagPublishResult:
    """Outcome of a local DAG publish operation."""

    event: DagPublishEvent
    node_id: str
    announcement: DagAnnouncement | None
    gossip_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class DagPublisherConfig:
    """
    Configuration for publishing one DAG namespace to one GossipSub topic.

    Args:
        topic: GossipSub topic to publish DAG node gossip onto.
        schema_id: Payload schema id used by template-style publish calls.
        signer: Signer used by template-style publish calls.
        payload_schemas: Schemas used when this publisher builds its own
            ``MerkleDagRuntime``. Leave empty when passing an existing runtime.
        namespace: DAG namespace. Defaults to ``topic`` for simple one-topic,
            one-namespace setups.
        author: Header author. Defaults to the local peer id.
        payload_factory: Optional factory used by ``run()`` or by ``publish()``
            when no payload is supplied.
        metadata_factory: Optional metadata factory.
        parent_selector: Optional custom parent selector. Defaults to current
            complete DAG heads, optionally filtered by ``parent_schema_id``.
        after_publish: Optional hook called after a successful immediate,
            queued, or periodic publish.
        parent_schema_id: If set, default parent selection only uses heads with
            this schema id.
        skip_if_orphans: Avoid template-style publishing while unresolved
            orphan nodes exist.
        latest_node_snapshot_db_key: Optional plain-DB key used to cache the
            latest local node metadata by author.

    """

    topic: str
    schema_id: str
    signer: Signer
    payload_schemas: Sequence[PayloadSchema] = ()
    namespace: str | None = None
    author: str | None = None
    payload_factory: DagPayloadFactory | None = None
    metadata_factory: DagMetadataFactory | None = None
    parent_selector: DagParentSelector | None = None
    after_publish: DagPublishHandler | None = None
    parent_schema_id: str | None = None
    skip_if_orphans: bool = True
    latest_node_snapshot_db_key: str | None = None
    publish_interval_seconds: float = 20.0
    storage: DagStorage | None = None
    max_queue_size: int = 1024
    max_fetch_batch: int = 32
    default_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def dag_namespace(self) -> str:
        """Return the concrete DAG namespace for this publisher."""
        return self.namespace or self.topic


class DagPublisher:
    """
    Build local DAG nodes and gossip them on one topic/namespace.

    Existing low-level usage remains supported::

        result = await publisher.publish_now(
            DagNodePublishRequirements(
                schema_id="counter",
                payload={"value": 1},
                parent_ids=(),
                author=peer_id,
                signer=signer,
            )
        )

    Template-style usage supplies ``DagPublisherConfig`` once and then publishes
    payloads directly::

        publisher = DagPublisher(
            pubsub=pubsub,
            termination_event=termination_event,
            db=db,
            local_peer_id=host.get_id(),
            config=DagPublisherConfig(
                topic="orders",
                schema_id="order",
                signer=Libp2pKeyPairSigner(key_pair),
                payload_schemas=[OrderSchema()],
            ),
        )

        result = await publisher.publish(
            {"kind": "order", "order_id": "order-1"},
            metadata={"status": "created"},
        )

    For periodic publishing, provide a ``payload_factory`` and start
    ``publisher.run``. If no payload factory is configured, ``run`` processes the
    lower-level queued publish events created by ``trigger_publish``.

    When paired with ``DagGossipSubReceiver``, pass
    ``runtime=gossip_dag_receiver.context_for_topic(topic).runtime`` so both
    sides share the same namespace, storage, codec, and sync coordinator.

    """

    def __init__(
        self,
        pubsub: Pubsub,
        termination_event: trio.Event,
        db: RocksDB,
        payload_schemas: Sequence[PayloadSchema] | None = None,
        *,
        local_peer_id: ID,
        config: DagPublisherConfig | None = None,
        namespace: str = "default",
        dag_topic: str = DEFAULT_DAG_TOPIC,
        storage: DagStorage | None = None,
        telemetry: Telemetry | None = None,
        max_queue_size: int = 1024,
        max_fetch_batch: int = 32,
        log_level: int = logging.DEBUG,
    ):
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.db = db
        self.telemetry = telemetry
        self.config = config
        self.local_peer_id = local_peer_id.to_string()
        self.log_level = log_level

        resolved_topic = config.topic if config is not None else dag_topic
        resolved_namespace = config.dag_namespace if config is not None else namespace
        resolved_storage = config.storage if config is not None and config.storage is not None else storage
        resolved_payload_schemas = (
            config.payload_schemas if config is not None and config.payload_schemas else payload_schemas
        )
        resolved_max_queue_size = config.max_queue_size if config is not None else max_queue_size
        resolved_max_fetch_batch = config.max_fetch_batch if config is not None else max_fetch_batch

        self._validate_init_config(
            config=config,
            payload_schemas=resolved_payload_schemas,
            topic=resolved_topic,
            namespace=resolved_namespace,
        )

        self.dag_topic = resolved_topic
        self.runtime = MerkleDagRuntime(
            db=db,
            payload_schemas=resolved_payload_schemas or (),
            local_peer_id=local_peer_id,
            namespace=resolved_namespace,
            dag_topic=resolved_topic,
            storage=resolved_storage,
            gossip_publisher=PubsubDagAnnouncementPublisher(pubsub),
            max_fetch_batch=resolved_max_fetch_batch,
        )
        self.serializer = self.runtime.serializer
        self.hasher = self.runtime.hasher
        self.codec = self.runtime.codec
        self.schema_registry = self.runtime.schema_registry
        self.signature_verifier = self.runtime.signature_verifier
        self.storage = self.runtime.storage
        self.validator = self.runtime.validator
        self.dag = self.runtime.dag
        self.coordinator = self.runtime.coordinator
        self._send_channel, self._receive_channel = trio.open_memory_channel[DagPublishEvent](resolved_max_queue_size)

    @classmethod
    def from_runtime(
        cls,
        pubsub: Pubsub,
        termination_event: trio.Event,
        runtime: MerkleDagRuntime,
        *,
        config: DagPublisherConfig | None = None,
        dag_topic: str | None = None,
        telemetry: Telemetry | None = None,
        max_queue_size: int = 1024,
        log_level: int = logging.DEBUG,
    ) -> "DagPublisher":
        """Construct a publisher around a prebuilt DAG runtime."""
        if config is not None:
            cls._validate_runtime_config(config, runtime)
            max_queue_size = config.max_queue_size

        resolved_dag_topic = dag_topic or (config.topic if config is not None else runtime.dag_topic)
        if not resolved_dag_topic:
            raise ValueError("DagPublisher topic must be a non-empty string")

        instance = cls.__new__(cls)
        instance.pubsub = pubsub
        instance.termination_event = termination_event
        instance.db = runtime.db
        instance.telemetry = telemetry
        instance.config = config
        instance.local_peer_id = runtime.local_peer_id
        instance.dag_topic = resolved_dag_topic
        instance.log_level = log_level
        instance.runtime = runtime
        instance.serializer = runtime.serializer
        instance.hasher = runtime.hasher
        instance.codec = runtime.codec
        instance.schema_registry = runtime.schema_registry
        instance.signature_verifier = runtime.signature_verifier
        instance.storage = runtime.storage
        instance.validator = runtime.validator
        instance.dag = runtime.dag
        instance.coordinator = runtime.coordinator
        instance._send_channel, instance._receive_channel = trio.open_memory_channel[DagPublishEvent](max_queue_size)
        return instance

    async def run(self) -> None:
        """Run periodic template publishing or the queued publish-event loop."""
        if self.config is not None and self.config.payload_factory is not None:
            await self._run_periodic_template_publish()
            return

        await self._run_event_queue()

    async def publish(
        self,
        payload: Any = _MISSING,
        *,
        metadata: dict[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
        after_publish: DagPublishHandler | None = None,
    ) -> DagPublishResult | None:
        """Template-style publish: build requirements from config and payload."""
        requirements = await self._build_requirements(
            payload,
            metadata=metadata,
            parent_ids=parent_ids,
            schema_id=schema_id,
            signer=signer,
            author=author,
            created_at_ms=created_at_ms,
        )
        if requirements is None:
            return None

        result = await self._publish_requirements_now(requirements)
        await self._after_publish(result, after_publish)
        return result

    async def trigger_publish(
        self,
        requirements_or_payload: DagNodePublishRequirements | Any = _MISSING,
        *,
        metadata: dict[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
    ) -> DagPublishEvent | None:
        """Queue a publish event for background processing."""
        requirements = await self._requirements_from_public_args(
            requirements_or_payload,
            metadata=metadata,
            parent_ids=parent_ids,
            schema_id=schema_id,
            signer=signer,
            author=author,
            created_at_ms=created_at_ms,
        )
        if requirements is None:
            return None
        return await self._trigger_requirements(requirements)

    async def publish_now(
        self,
        requirements_or_payload: DagNodePublishRequirements | Any = _MISSING,
        *,
        metadata: dict[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
        after_publish: DagPublishHandler | None = None,
    ) -> DagPublishResult | None:
        """Publish requirements directly, or publish a payload using config."""
        if isinstance(requirements_or_payload, DagNodePublishRequirements):
            result = await self._publish_requirements_now(requirements_or_payload)
            await self._after_publish(result, after_publish)
            return result

        return await self.publish(
            requirements_or_payload,
            metadata=metadata,
            parent_ids=parent_ids,
            schema_id=schema_id,
            signer=signer,
            author=author,
            created_at_ms=created_at_ms,
            after_publish=after_publish,
        )

    async def publish_heads(self) -> DagAnnouncement | None:
        """Publish a lightweight announcement of this DAG's current heads."""
        summary = await self.dag.summary()
        created_at_ms = self._now_ms()
        announcement = DagAnnouncement(
            message_id=f"announcement:{self.local_peer_id}:{created_at_ms}",
            namespace=summary.namespace,
            peer_id=self.local_peer_id,
            head_ids=summary.head_ids,
            node_count=summary.node_count,
            created_at_ms=created_at_ms,
        )
        await self.pubsub.publish(self.dag_topic, self.codec.encode(announcement))
        return announcement

    async def current_parent_ids(self, schema_id: str | None = None) -> tuple[str, ...]:
        """Return the default parent ids for the next template-style publish."""
        config = self._require_template_config()
        resolved_schema_id = schema_id or config.schema_id
        if config.parent_selector is not None:
            selected = config.parent_selector(self, resolved_schema_id)
            if inspect.isawaitable(selected):
                selected = await selected
            return tuple(sorted(set(str(parent_id) for parent_id in selected)))

        parent_ids: list[str] = []
        for head_id in await self.dag.get_heads():
            node = await self.dag.get_node(head_id)
            if node is None:
                continue
            if config.parent_schema_id is not None and node.header.schema_id != config.parent_schema_id:
                continue
            parent_ids.append(head_id)
        return tuple(sorted(set(parent_ids)))

    async def _run_event_queue(self) -> None:
        while not self.termination_event.is_set():
            try:
                event = await self._receive_channel.receive()
            except trio.EndOfChannel:
                return

            try:
                result = await self._publish_event(event)
                await self._after_publish(result, None)
            except Exception:
                logger.exception("Failed to publish DAG event %s", event.event_id)
                if self.telemetry:
                    await self.telemetry.emit_async("dag_publish_failed", event_id=event.event_id)

    async def _run_periodic_template_publish(self) -> None:
        config = self._require_template_config()
        while not self.termination_event.is_set():
            try:
                await self.publish()
            except Exception:
                logger.exception("Error publishing DAG payload on topic '%s'", config.topic)
                if self.telemetry:
                    await self.telemetry.emit_async("dag_template_publish_failed", topic=config.topic)

            await trio.sleep(config.publish_interval_seconds)

    async def _requirements_from_public_args(
        self,
        requirements_or_payload: DagNodePublishRequirements | Any,
        *,
        metadata: dict[str, Any] | None,
        parent_ids: Sequence[str] | None,
        schema_id: str | None,
        signer: Signer | None,
        author: str | None,
        created_at_ms: int | None,
    ) -> DagNodePublishRequirements | None:
        if isinstance(requirements_or_payload, DagNodePublishRequirements):
            return requirements_or_payload

        return await self._build_requirements(
            requirements_or_payload,
            metadata=metadata,
            parent_ids=parent_ids,
            schema_id=schema_id,
            signer=signer,
            author=author,
            created_at_ms=created_at_ms,
        )

    async def _build_requirements(
        self,
        payload: Any,
        *,
        metadata: dict[str, Any] | None,
        parent_ids: Sequence[str] | None,
        schema_id: str | None,
        signer: Signer | None,
        author: str | None,
        created_at_ms: int | None,
    ) -> DagNodePublishRequirements | None:
        config = self._require_template_config()
        if config.skip_if_orphans and await self.dag.storage.count_orphans() > 0:
            logger.log(
                self.log_level,
                "Skipping DAG publish on topic '%s' while DAG has unresolved orphan nodes",
                config.topic,
            )
            return None

        resolved_schema_id = schema_id or config.schema_id
        resolved_payload = await self._resolve_payload(payload)
        resolved_metadata = await self._resolve_metadata(metadata)
        if parent_ids is None:
            resolved_parent_ids = await self.current_parent_ids(resolved_schema_id)
        else:
            resolved_parent_ids = tuple(parent_ids)

        return DagNodePublishRequirements(
            schema_id=resolved_schema_id,
            payload=resolved_payload,
            parent_ids=tuple(sorted(set(resolved_parent_ids))),
            author=author or config.author or self.local_peer_id,
            signer=signer or config.signer,
            created_at_ms=created_at_ms,
            metadata=resolved_metadata,
        )

    async def _resolve_payload(self, payload: Any) -> Any:
        config = self._require_template_config()
        if payload is not _MISSING:
            return payload
        if config.payload_factory is None:
            raise ValueError("publish requires a payload or DagPublisherConfig.payload_factory")

        resolved_payload = config.payload_factory(self)
        if inspect.isawaitable(resolved_payload):
            resolved_payload = await resolved_payload
        return resolved_payload

    async def _resolve_metadata(self, metadata: dict[str, Any] | None) -> dict[str, Any]:
        config = self._require_template_config()
        if metadata is not None:
            return metadata

        resolved_metadata = dict(config.default_metadata)
        if config.metadata_factory is None:
            return resolved_metadata

        factory_metadata = config.metadata_factory(self)
        if inspect.isawaitable(factory_metadata):
            factory_metadata = await factory_metadata
        resolved_metadata.update(cast(dict[str, Any], factory_metadata))
        return resolved_metadata

    async def _trigger_requirements(self, requirements: DagNodePublishRequirements) -> DagPublishEvent:
        event = DagPublishEvent(
            requirements=requirements,
            event_id=self._event_id(),
            triggered_at_ms=self._now_ms(),
        )
        await self._send_channel.send(event)
        if self.telemetry:
            await self.telemetry.emit_async(
                "dag_publish_triggered",
                event_id=event.event_id,
                parent_ids=list(requirements.parent_ids),
                schema_id=requirements.schema_id,
            )
        return event

    async def _publish_requirements_now(self, requirements: DagNodePublishRequirements) -> DagPublishResult:
        event = DagPublishEvent(
            requirements=requirements,
            event_id=self._event_id(),
            triggered_at_ms=self._now_ms(),
        )
        return await self._publish_event(event)

    async def _publish_event(self, event: DagPublishEvent) -> DagPublishResult:
        await self._ensure_requirements(event.requirements)
        node = await self.dag.create_node(
            schema_id=event.requirements.schema_id,
            payload=event.requirements.payload,
            parent_ids=event.requirements.parent_ids,
            signer=event.requirements.signer,
            author=event.requirements.author,
            created_at_ms=event.requirements.created_at_ms,
            metadata=event.requirements.metadata,
        )
        ingest_result = await self.dag.add_node(node, from_peer=self.coordinator.local_peer_id)

        if ingest_result.status == NodeIngestStatus.DUPLICATE:
            return DagPublishResult(event=event, node_id=node.header.node_id, announcement=None)

        if ingest_result.status != NodeIngestStatus.ACCEPTED:
            raise DagPublishError(
                f"Local DAG node {node.header.node_id} was not publishable: {ingest_result.status.value}"
            )

        gossip_message = DagNodeGossip(
            message_id=self._node_message_id(node.header.node_id),
            namespace=self.dag.namespace,
            peer_id=self.coordinator.local_peer_id,
            node=node,
            created_at_ms=self._now_ms(),
        )
        await self.pubsub.publish(self.dag_topic, self.codec.encode(gossip_message))
        logger.log(
            self.log_level,
            "Published DAG node %s on topic '%s'",
            node.header.node_id,
            self.dag_topic,
        )
        if self.telemetry:
            await self.telemetry.emit_async(
                "dag_node_published",
                event_id=event.event_id,
                gossip_message_id=gossip_message.message_id,
                node_id=node.header.node_id,
                parent_ids=list(node.header.parent_ids),
                schema_id=node.header.schema_id,
            )
        return DagPublishResult(
            event=event,
            node_id=node.header.node_id,
            announcement=None,
            gossip_message_id=gossip_message.message_id,
        )

    async def _after_publish(self, result: DagPublishResult, after_publish: DagPublishHandler | None) -> None:
        await self._store_latest_local_snapshot(result.node_id)
        config_handler = None if self.config is None else self.config.after_publish
        handler = after_publish or config_handler
        if handler is not None:
            handler_result = handler(self, result)
            if inspect.isawaitable(handler_result):
                await cast(Awaitable[None], handler_result)

        if self.config is not None and self.telemetry:
            await self.telemetry.emit_async(
                "dag_template_node_published",
                topic=self.config.topic,
                namespace=self.config.dag_namespace,
                node_id=result.node_id,
                gossip_message_id=result.gossip_message_id,
            )

    async def _store_latest_local_snapshot(self, node_id: str) -> None:
        if self.config is None or self.config.latest_node_snapshot_db_key is None:
            return

        db_key = self.config.latest_node_snapshot_db_key
        header = await self.dag.get_header(node_id)
        created_at_ms = -1 if header is None else header.created_at_ms
        metadata = {} if header is None else dict(header.metadata)
        current = {
            "peer_id": self.local_peer_id,
            "node_id": node_id,
            "created_at_ms": created_at_ms,
            **metadata,
        }

        previous = self.db.get_nested(db_key, self.local_peer_id)
        if isinstance(previous, dict):
            previous_created_at_ms = int(previous.get("created_at_ms", -1))
            previous_node_id = str(previous.get("node_id", ""))
            if (previous_created_at_ms, previous_node_id) >= (created_at_ms, node_id):
                return

        self.db.set_nested(db_key, self.local_peer_id, current)

    async def _ensure_requirements(self, requirements: DagNodePublishRequirements) -> None:
        missing_parents = [
            parent_id
            for parent_id in requirements.parent_ids
            if not (await self.dag.has_header(parent_id) and await self.dag.has_body(parent_id))
        ]
        if missing_parents:
            raise DagPublishPreconditionError(
                f"Cannot publish node before required parents exist locally: {sorted(missing_parents)}"
            )

    def _require_template_config(self) -> DagPublisherConfig:
        if self.config is None:
            raise ValueError(
                "Template-style publishing requires DagPublisherConfig. "
                "Pass DagNodePublishRequirements to publish_now/trigger_publish for low-level publishing."
            )
        return self.config

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _event_id(self) -> str:
        return f"dag-publish:{self.coordinator.local_peer_id}:{self._now_ms()}"

    def _node_message_id(self, node_id: str) -> str:
        return f"node-gossip:{self.coordinator.local_peer_id}:{node_id}:{self._now_ms()}"

    @staticmethod
    def _validate_init_config(
        *,
        config: DagPublisherConfig | None,
        payload_schemas: Sequence[PayloadSchema] | None,
        topic: str,
        namespace: str,
    ) -> None:
        if not topic:
            raise ValueError("DagPublisher topic must be a non-empty string")
        if not namespace:
            raise ValueError("DagPublisher namespace must be a non-empty string")
        if not payload_schemas:
            raise ValueError("DagPublisher requires at least one payload schema")
        if config is None:
            return
        if not config.schema_id:
            raise ValueError("DagPublisherConfig.schema_id must be a non-empty string")
        if config.publish_interval_seconds <= 0:
            raise ValueError("DagPublisherConfig.publish_interval_seconds must be greater than zero")

    @staticmethod
    def _validate_runtime_config(config: DagPublisherConfig, runtime: MerkleDagRuntime) -> None:
        if not config.topic:
            raise ValueError("DagPublisherConfig.topic must be a non-empty string")
        if not config.dag_namespace:
            raise ValueError("DagPublisherConfig.namespace must be a non-empty string")
        if not config.schema_id:
            raise ValueError("DagPublisherConfig.schema_id must be a non-empty string")
        if config.publish_interval_seconds <= 0:
            raise ValueError("DagPublisherConfig.publish_interval_seconds must be greater than zero")
        if runtime.namespace != config.dag_namespace:
            raise ValueError(
                f"Runtime namespace {runtime.namespace!r} does not match publisher namespace {config.dag_namespace!r}"
            )
