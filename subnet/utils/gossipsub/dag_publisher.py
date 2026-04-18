"""Event-driven Merkle DAG node publisher for the existing GossipSub stack."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
import logging
import time
from typing import Any

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


class DagPublisher:
    """Builds new local DAG nodes and gossips complete node messages."""

    def __init__(
        self,
        pubsub: Pubsub,
        termination_event: trio.Event,
        db: RocksDB,
        payload_schemas: Sequence[PayloadSchema],
        *,
        local_peer_id: ID,
        namespace: str = "default",
        dag_topic: str = DEFAULT_DAG_TOPIC,
        storage: DagStorage | None = None,
        telemetry: Telemetry | None = None,
        max_queue_size: int = 1024,
        max_fetch_batch: int = 32,
        max_ancestor_depth: int = 64,
        log_level: int = logging.INFO,
    ):
        self.pubsub = pubsub
        self.termination_event = termination_event
        self.db = db
        self.telemetry = telemetry
        self.dag_topic = dag_topic
        self.log_level = log_level

        self.runtime = MerkleDagRuntime(
            db=db,
            payload_schemas=payload_schemas,
            local_peer_id=local_peer_id,
            namespace=namespace,
            dag_topic=dag_topic,
            storage=storage,
            gossip_publisher=PubsubDagAnnouncementPublisher(pubsub),
            max_fetch_batch=max_fetch_batch,
            max_ancestor_depth=max_ancestor_depth,
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
        self._send_channel, self._receive_channel = trio.open_memory_channel[DagPublishEvent](max_queue_size)

    @classmethod
    def from_runtime(
        cls,
        pubsub: Pubsub,
        termination_event: trio.Event,
        runtime: MerkleDagRuntime,
        *,
        telemetry: Telemetry | None = None,
        max_queue_size: int = 1024,
        log_level: int = logging.INFO,
    ) -> "DagPublisher":
        """Construct a publisher around a prebuilt DAG runtime."""
        instance = cls.__new__(cls)
        instance.pubsub = pubsub
        instance.termination_event = termination_event
        instance.db = runtime.db
        instance.telemetry = telemetry
        instance.dag_topic = runtime.dag_topic
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
        """Process publish events until shutdown is requested."""
        while not self.termination_event.is_set():
            try:
                event = await self._receive_channel.receive()
            except trio.EndOfChannel:
                return

            try:
                await self._publish_event(event)
            except Exception:
                logger.exception("Failed to publish DAG event %s", event.event_id)
                if self.telemetry:
                    await self.telemetry.emit_async("dag_publish_failed", event_id=event.event_id)

    async def trigger_publish(self, requirements: DagNodePublishRequirements) -> DagPublishEvent:
        """Queue a publish event for background processing."""
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

    async def publish_now(self, requirements: DagNodePublishRequirements) -> DagPublishResult:
        """Build, store, and gossip a new node immediately."""
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

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _event_id(self) -> str:
        return f"dag-publish:{self.coordinator.local_peer_id}:{self._now_ms()}"

    def _node_message_id(self, node_id: str) -> str:
        return f"node-gossip:{self.coordinator.local_peer_id}:{node_id}:{self._now_ms()}"
