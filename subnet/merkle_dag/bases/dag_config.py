"""
Template for wiring one or more Merkle DAG namespaces onto py-libp2p GossipSub.

Topic configs with the same DAG namespace share one runtime, storage graph, and
sync service while keeping separate GossipSub subscriptions.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import logging
from typing import TypeAlias

from libp2p.peer.id import ID
from libp2p.pubsub.pubsub import ValidatorFn

from subnet.merkle_dag import DagAnnouncement, DagNodeGossip, NodeIngestResult
from subnet.merkle_dag.bases.dag_publisher_base import (
    DagPublisher,
)
from subnet.merkle_dag.interfaces import DagStorage, PayloadSchema, PeerRequestClient, PeerSetProvider
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.merkle_dag.sync_scheduler import SyncScheduler
from subnet.merkle_dag.sync_service import MerkleDagSyncService

logger = logging.getLogger(__name__)

DagGossipMessage: TypeAlias = DagNodeGossip | DagAnnouncement
DagGossipHandlerResult: TypeAlias = NodeIngestResult | bool
DagGossipHandler: TypeAlias = Callable[
    ["GossipDagTopicContext", ID, DagGossipMessage, DagGossipHandlerResult],
    Awaitable[None] | None,
]


@dataclass(frozen=True, slots=True)
class GossipDagTopicConfig:
    """
    Configuration for one GossipSub topic backed by a Merkle DAG namespace.

    Args:
        topic: GossipSub topic string.
        payload_schemas: Payload schemas supported by this topic's DAG.
        topic_handler: Optional hook called after a DAG gossip message is
            processed. It receives ``(context, from_peer_id, decoded_message,
            result)`` where result is a ``NodeIngestResult`` for node gossip or
            ``bool`` for announcements.
        topic_validator: Optional py-libp2p topic validator. If omitted, a
            DAG-aware validator is registered automatically.
        is_async_topic_validator: Set to ``True`` when ``topic_validator`` is
            async.
        namespace: DAG namespace for this topic. Defaults to ``topic``.
        storage: Optional DAG storage. Defaults to namespace-isolated RocksDB
            storage through ``MerkleDagRuntime``.
        request_client: Optional direct request client used to fetch missing
            DAG content from peers.
        peer_provider: Optional peer provider used by missing-parent and
            periodic sync.
        latest_node_snapshot_db_key: Optional DB namespace used to cache the
            latest valid gossiped node metadata by peer id. This is application
            metadata only; DAG sync resolves peer addresses from libp2p host,
            DHT, and gossip state.

    """

    topic: str
    payload_schemas: Sequence[PayloadSchema]
    topic_handler: DagGossipHandler | None = None
    topic_validator: ValidatorFn | None = None
    is_async_topic_validator: bool = False
    namespace: str | None = None
    storage: DagStorage | None = None
    request_client: PeerRequestClient | None = None
    peer_provider: PeerSetProvider | None = None
    enable_missing_parent_sync: bool = True
    sync_on_startup: bool = True
    sync_batch_window: float = 0.05
    sync_retry_delay: float = 5.0
    enable_periodic_reconciliation: bool = False
    reconciliation_interval: float = 30.0
    start_publisher: bool = True
    latest_node_snapshot_db_key: str | None = None
    max_queue_size: int = 1024
    max_fetch_batch: int = 32

    @property
    def dag_namespace(self) -> str:
        """Return the concrete DAG namespace for this config."""
        return self.namespace or self.topic


@dataclass(frozen=True, slots=True)
class GossipDagTopicContext:
    """Runtime objects assembled for one configured DAG gossip topic."""

    config: GossipDagTopicConfig
    runtime: MerkleDagRuntime
    publisher: DagPublisher
    sync_service: MerkleDagSyncService
    sync_scheduler: SyncScheduler | None

    @property
    def topic(self) -> str:
        """Return the GossipSub topic for this context."""
        return self.config.topic

    @property
    def namespace(self) -> str:
        """Return the DAG namespace for this context."""
        return self.config.dag_namespace

    @property
    def dag(self):
        """Return the underlying Merkle DAG."""
        return self.runtime.dag
