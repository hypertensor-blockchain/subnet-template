"""Transport-agnostic sync coordinator and wire codec for Merkle DAG replication."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
import hashlib
import logging
import time

from subnet.merkle_dag.dag import MerkleDag
from subnet.merkle_dag.exceptions import MessageDecodingError
from subnet.merkle_dag.interfaces import GossipPublisher, PeerRequestClient, SyncMessage, SyncRequest, SyncResponse
from subnet.merkle_dag.models import (
    DagAnnouncement,
    DagFetchRequest,
    DagFetchResponse,
    DagInventoryRequest,
    DagInventoryResponse,
    DagNodeGossip,
    DagNodeSnapshot,
    DagSummary,
    NodeIngestResult,
    NodeIngestStatus,
    PeerSyncState,
    SyncMessageKind,
)
from subnet.merkle_dag.serialization import CanonicalJSONSerializer

logger = logging.getLogger(__name__)


class DagSyncMessageCodec:
    """Encodes and decodes DAG sync messages over gossip and request-response transports."""

    _DECODERS = {
        SyncMessageKind.NODE_GOSSIP.value: DagNodeGossip.from_primitive,
        SyncMessageKind.ANNOUNCEMENT.value: DagAnnouncement.from_primitive,
        SyncMessageKind.INVENTORY_REQUEST.value: DagInventoryRequest.from_primitive,
        SyncMessageKind.INVENTORY_RESPONSE.value: DagInventoryResponse.from_primitive,
        SyncMessageKind.FETCH_REQUEST.value: DagFetchRequest.from_primitive,
        SyncMessageKind.FETCH_RESPONSE.value: DagFetchResponse.from_primitive,
    }

    def __init__(self, serializer: CanonicalJSONSerializer | None = None):
        self._serializer = serializer or CanonicalJSONSerializer()

    def encode(self, message: SyncMessage) -> bytes:
        """Encode a sync message into canonical JSON."""
        return self._serializer.serialize(message.to_primitive())

    def decode(self, payload: bytes) -> SyncMessage:
        """Decode a sync message from canonical JSON."""
        value = self._serializer.deserialize(payload)
        if not isinstance(value, dict):
            raise MessageDecodingError("Sync message must decode to an object")
        kind = value.get("kind")
        decoder = self._DECODERS.get(kind)
        if decoder is not None:
            return decoder(value)
        raise MessageDecodingError(f"Unsupported sync message kind: {kind!r}")


class MerkleDagSyncCoordinator:
    """Coordinates live announcements, direct fetches, and periodic reconciliation."""

    def __init__(
        self,
        dag: MerkleDag,
        local_peer_id: str,
        topic: str,
        codec: DagSyncMessageCodec,
        *,
        gossip_publisher: GossipPublisher | None = None,
        request_client: PeerRequestClient | None = None,
        max_fetch_batch: int = 32,
    ):
        self._dag = dag
        self._local_peer_id = local_peer_id
        self._topic = topic
        self._codec = codec
        self._gossip_publisher = gossip_publisher
        self._request_client = request_client
        self._max_fetch_batch = max_fetch_batch

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    @property
    def local_peer_id(self) -> str:
        """Return the local peer id used on outbound sync messages."""
        return self._local_peer_id

    async def publish_heads(self) -> DagAnnouncement | None:
        """Publish the current head set as a lightweight gossip announcement."""
        if self._gossip_publisher is None:
            return None
        summary = await self._dag.summary()
        created_at_ms = self._now_ms()
        announcement = DagAnnouncement(
            message_id=self._announcement_id(summary, created_at_ms),
            namespace=summary.namespace,
            peer_id=self._local_peer_id,
            head_ids=summary.head_ids,
            node_count=summary.node_count,
            created_at_ms=created_at_ms,
        )
        await self._gossip_publisher.publish(self._topic, self._codec.encode(announcement))
        return announcement

    async def ingest_local_node(self, node) -> None:
        """Store a locally created node and announce the updated head set."""
        result = await self._dag.add_node(node, from_peer=self._local_peer_id)
        if result.status == NodeIngestStatus.ACCEPTED:
            await self.publish_heads()

    async def handle_announcement(self, announcement: DagAnnouncement, *, source_peer: str) -> bool:
        """Process a gossip announcement and trigger reconciliation if needed."""
        if announcement.namespace != self._dag.namespace:
            return False
        if announcement.peer_id == self._local_peer_id or source_peer == self._local_peer_id:
            return False
        if not await self._dag.storage.mark_seen_announcement(announcement.message_id):
            return False

        await self._dag.storage.set_peer_state(
            PeerSyncState(
                peer_id=source_peer,
                summary=DagSummary(
                    namespace=announcement.namespace,
                    head_ids=announcement.head_ids,
                    node_count=announcement.node_count,
                    orphan_count=0,
                    generated_at_ms=announcement.created_at_ms,
                ),
                updated_at_ms=self._now_ms(),
            )
        )

        if self._request_client is None:
            return True

        local_count = await self._dag.storage.count_complete_nodes()
        unknown_heads = [
            head_id
            for head_id in announcement.head_ids
            if not (await self._dag.storage.has_header(head_id) and await self._dag.storage.has_body(head_id))
        ]
        if unknown_heads or announcement.node_count > local_count:
            await self.reconcile_with_peer(source_peer, remote_head_hint=announcement.head_ids)
        return True

    async def reconcile_with_peer(self, peer_id: str, *, remote_head_hint: Sequence[str] = ()) -> None:
        """Perform inventory comparison and fetch missing content from a peer."""
        if self._request_client is None:
            return

        summary = await self._dag.summary()
        response = await self._request_client.request(
            peer_id,
            DagInventoryRequest(
                message_id=self._message_id("inventory", peer_id),
                namespace=summary.namespace,
                peer_id=self._local_peer_id,
                known_heads=summary.head_ids,
                node_count=summary.node_count,
                created_at_ms=self._now_ms(),
            ),
        )
        if not isinstance(response, DagInventoryResponse):
            raise TypeError(f"Expected DagInventoryResponse, got {type(response)!r}")

        await self._dag.storage.set_peer_state(
            PeerSyncState(peer_id=peer_id, summary=response.summary, updated_at_ms=self._now_ms())
        )

        candidate_heads = tuple(sorted(set(response.summary.head_ids).union(remote_head_hint)))
        missing = [
            head_id
            for head_id in candidate_heads
            if not (await self._dag.storage.has_header(head_id) and await self._dag.storage.has_body(head_id))
        ]
        if missing:
            await self.fetch_missing(peer_id, missing)

    async def fetch_missing(self, peer_id: str, node_ids: Sequence[str]) -> None:
        """
        Fetch missing nodes until the requested frontier is satisfied.

        The requester asks for the exact missing frontier only. If a fetched
        node still depends on unknown parents, those parent ids are queued and
        fetched in follow-up requests. This keeps sync walking backward from the
        newest known gap instead of asking the responder to eagerly send a deep
        ancestor window for every request.
        """
        if self._request_client is None:
            return

        queue: deque[str] = deque(sorted(set(node_ids)))
        requested: set[str] = set()

        while queue:
            batch: list[str] = []
            while queue and len(batch) < self._max_fetch_batch:
                node_id = queue.popleft()
                if node_id in requested:
                    continue
                if await self._dag.storage.has_header(node_id) and await self._dag.storage.has_body(node_id):
                    requested.add(node_id)
                    continue
                requested.add(node_id)
                batch.append(node_id)

            if not batch:
                continue

            response = await self._request_client.request(
                peer_id,
                DagFetchRequest(
                    message_id=self._message_id("fetch", peer_id),
                    namespace=self._dag.namespace,
                    peer_id=self._local_peer_id,
                    node_ids=tuple(batch),
                    include_bodies=True,
                    max_ancestor_depth=0,
                    created_at_ms=self._now_ms(),
                ),
            )
            if not isinstance(response, DagFetchResponse):
                raise TypeError(f"Expected DagFetchResponse, got {type(response)!r}")

            accepted_any = False
            for snapshot in response.nodes:
                if snapshot.body is None:
                    result = await self._dag.add_snapshot(
                        snapshot,
                        from_peer=peer_id,
                        validate_remote_timestamp=True,
                    )
                else:
                    result = await self._dag.add_node(
                        snapshot.to_node(),
                        from_peer=peer_id,
                        validate_remote_timestamp=True,
                    )
                self._log_peer_sync_store(snapshot, result, peer_id)
                if result.status == NodeIngestStatus.ACCEPTED:
                    accepted_any = True
                for missing_parent in result.missing_parents:
                    if missing_parent not in requested:
                        queue.append(missing_parent)

            if accepted_any and self._gossip_publisher is not None:
                await self.publish_heads()

    def _log_peer_sync_store(
        self,
        snapshot: DagNodeSnapshot,
        result: NodeIngestResult,
        peer_id: str,
    ) -> None:
        if result.status not in {
            NodeIngestStatus.ACCEPTED,
            NodeIngestStatus.ORPHAN,
            NodeIngestStatus.PENDING_BODY,
        }:
            return

        logger.info(
            "Peer sync stored DAG node %s topic=%s namespace=%s schema_id=%s status=%s from_peer=%s",
            snapshot.header.node_id,
            self._topic,
            snapshot.header.namespace,
            snapshot.header.schema_id,
            result.status.value,
            peer_id,
            extra={
                "event": "peer_sync_dag_node_stored",
                "node_id": snapshot.header.node_id,
                "namespace": snapshot.header.namespace,
                "schema_id": snapshot.header.schema_id,
                "topic": self._topic,
                "from_peer": peer_id,
                "ingest_status": result.status.value,
                "missing_parents": result.missing_parents,
                "resolved_nodes": result.resolved_nodes,
            },
        )

    async def handle_request(self, peer_id: str, request: SyncRequest) -> SyncResponse:
        """Serve a typed inventory or fetch request."""
        if isinstance(request, DagInventoryRequest):
            if request.namespace != self._dag.namespace:
                raise ValueError(f"Unexpected namespace '{request.namespace}'")
            summary = await self._dag.summary()
            return DagInventoryResponse(
                message_id=request.message_id,
                namespace=request.namespace,
                peer_id=self._local_peer_id,
                summary=summary,
                created_at_ms=self._now_ms(),
            )

        if isinstance(request, DagFetchRequest):
            if request.namespace != self._dag.namespace:
                raise ValueError(f"Unexpected namespace '{request.namespace}'")
            snapshots = await self._dag.snapshots_for_fetch(
                request.node_ids,
                include_bodies=request.include_bodies,
                max_ancestor_depth=request.max_ancestor_depth,
            )
            available_ids = {snapshot.header.node_id for snapshot in snapshots}
            not_found = tuple(sorted(node_id for node_id in request.node_ids if node_id not in available_ids))
            return DagFetchResponse(
                message_id=request.message_id,
                namespace=request.namespace,
                peer_id=self._local_peer_id,
                nodes=snapshots,
                not_found=not_found,
                created_at_ms=self._now_ms(),
            )

        raise TypeError(f"Unsupported sync request type: {type(request)!r}")

    def _announcement_id(self, summary: DagSummary, created_at_ms: int) -> str:
        seed = self._codec.encode(
            DagAnnouncement(
                message_id="",
                namespace=summary.namespace,
                peer_id=self._local_peer_id,
                head_ids=summary.head_ids,
                node_count=summary.node_count,
                created_at_ms=created_at_ms,
            )
        )
        return f"announcement:{hashlib.sha256(seed).hexdigest()}"

    def _message_id(self, prefix: str, peer_id: str) -> str:
        return f"{prefix}:{self._local_peer_id}:{peer_id}:{self._now_ms()}"
