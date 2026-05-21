# """Receive-only DAG node gossip adapter for the existing libp2p GossipSub stack."""

# from __future__ import annotations

# import logging
# from typing import TYPE_CHECKING

# from libp2p.abc import ISubscriptionAPI
# from libp2p.peer.id import ID
# from libp2p.pubsub.pb import rpc_pb2
# from libp2p.pubsub.pubsub import Pubsub
# import trio

# from subnet.merkle_dag import DagNodeGossip
# from subnet.merkle_dag.exceptions import MerkleDagError
# from subnet.merkle_dag.models import NodeIngestStatus
# from subnet.merkle_dag.runtime import MerkleDagRuntime
# from subnet.telemetry.telemetry import Telemetry
# from subnet.utils.db.database import RocksDB
# from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC

# if TYPE_CHECKING:
#     from subnet.merkle_dag.sync_scheduler import SyncScheduler

# logger = logging.getLogger(__name__)


# class HeartbeatDagGossipReceiver:
#     """Subscribe to a DAG topic, validate gossiped DAG nodes, and store them locally."""

#     def __init__(
#         self,
#         pubsub: Pubsub,
#         termination_event: trio.Event,
#         runtime: MerkleDagRuntime,
#         *,
#         dag_topic: str = HEARTBEAT_TOPIC,
#         telemetry: Telemetry | None = None,
#         db: RocksDB | None = None,
#         sync_scheduler: SyncScheduler | None = None,
#         log_level: int = logging.DEBUG,
#     ):
#         self.pubsub = pubsub
#         self.termination_event = termination_event
#         self.runtime = runtime
#         self.dag_topic = dag_topic
#         self.telemetry = telemetry
#         self.db = db
#         self.sync_scheduler = sync_scheduler
#         self.log_level = log_level

#     async def run(self) -> None:
#         """Subscribe to the DAG topic and process gossip messages until shutdown."""
#         subscription = await self.pubsub.subscribe(self.dag_topic)
#         logger.log(self.log_level, "Subscribed to DAG topic '%s'", self.dag_topic)
#         await self._receive_loop(subscription)

#     async def _receive_loop(self, subscription: ISubscriptionAPI) -> None:
#         while not self.termination_event.is_set():
#             try:
#                 message = await subscription.get()
#                 await self._handle_message(message)
#             except Exception:
#                 logger.exception("Error in DAG gossip receive loop")
#                 await trio.sleep(1)

#     async def _handle_message(self, message: rpc_pb2.Message) -> None:
#         topic = message.topicIDs[0] if message.topicIDs else None
#         if topic != self.dag_topic:
#             return

#         from_peer = ID(message.from_id).to_string()

#         if from_peer == self.runtime.local_peer_id:
#             return

#         decoded = self.runtime.codec.decode(message.data)

#         logger.info("Decoded DAG gossip message from %s: %s", from_peer, decoded)
#         if not isinstance(decoded, DagNodeGossip):
#             logger.warning("Ignoring non-node gossip message on DAG topic from %s", from_peer)
#             return

#         if from_peer != decoded.peer_id:
#             logger.warning(
#                 "Rejecting DAG node gossip from %s that claimed peer_id %s",
#                 from_peer,
#                 decoded.peer_id,
#             )
#             if self.telemetry:
#                 await self.telemetry.emit_async(
#                     "dag_node_rejected",
#                     peer_id=from_peer,
#                     claimed_peer_id=decoded.peer_id,
#                     reason="peer_id_mismatch",
#                 )
#             return

#         if decoded.namespace != self.runtime.namespace or decoded.node.header.namespace != self.runtime.namespace:
#             logger.warning(
#                 "Rejecting DAG node gossip from %s for unexpected namespace %s",
#                 from_peer,
#                 decoded.namespace,
#             )
#             return

#         try:
#             self.runtime.validator.validate_header_source_peer(decoded.node.header, from_peer)
#             ingest_result = await self.runtime.dag.add_node(decoded.node, validate_remote_timestamp=True)
#             logger.info(
#                 "Processed gossiped DAG node %s from %s with ingest result %s",
#                 decoded.node.header.node_id,
#                 from_peer,
#                 ingest_result.status.value,
#             )
#         except MerkleDagError as exc:
#             logger.warning("Rejected DAG node from %s: %s", from_peer, exc)
#             if self.telemetry:
#                 await self.telemetry.emit_async(
#                     "dag_node_rejected",
#                     peer_id=from_peer,
#                     node_id=decoded.node.header.node_id,
#                     reason=type(exc).__name__,
#                 )
#             return

#         self._store_heartbeat_snapshot(from_peer, decoded)
#         await self._schedule_missing_parent_sync(from_peer, ingest_result)

#         if self.telemetry:
#             await self.telemetry.emit_async(
#                 "dag_node_received",
#                 peer_id=from_peer,
#                 header=decoded.node.header.to_primitive(),
#                 node_id=decoded.node.header.node_id,
#                 status=ingest_result.status.value,
#                 missing_parents=list(ingest_result.missing_parents),
#             )
#         if ingest_result.status == NodeIngestStatus.ACCEPTED:
#             logger.log(self.log_level, "Accepted gossiped DAG node %s", decoded.node.header.node_id)
#         elif ingest_result.status == NodeIngestStatus.ORPHAN:
#             logger.log(
#                 self.log_level,
#                 "Stored orphan DAG node %s with missing parents %s",
#                 decoded.node.header.node_id,
#                 ingest_result.missing_parents,
#             )

#     def _store_heartbeat_snapshot(self, peer_id: str, message: DagNodeGossip) -> None:
#         """Store the latest valid heartbeat snapshot in the plain DB for fast lookup."""
#         if self.db is None:
#             return

#         current = {
#             "peer_id": peer_id,
#             "node_id": message.node.header.node_id,
#             "created_at_ms": message.node.header.created_at_ms,
#             **message.node.header.metadata,
#         }

#         previous = self.db.get_nested(HEARTBEAT_TOPIC, peer_id)
#         if isinstance(previous, dict):
#             previous_created_at_ms = int(previous.get("created_at_ms", -1))
#             previous_node_id = str(previous.get("node_id", ""))
#             if (previous_created_at_ms, previous_node_id) >= (current["created_at_ms"], current["node_id"]):
#                 return

#         self.db.set_nested(HEARTBEAT_TOPIC, peer_id, current)

#     async def _schedule_missing_parent_sync(self, peer_id: str, ingest_result) -> None:
#         """Queue missing-parent sync work only when local ingest found a gap."""
#         if ingest_result.status != NodeIngestStatus.ORPHAN or not ingest_result.missing_parents:
#             return

#         try:
#             if self.sync_scheduler is not None:
#                 await self.sync_scheduler.schedule(peer_id)
#                 return

#             await self.runtime.coordinator.fetch_missing(peer_id, ingest_result.missing_parents)
#         except Exception:
#             logger.exception(
#                 "Failed to schedule missing parents %s from peer %s",
#                 ingest_result.missing_parents,
#                 peer_id,
#             )
#             if self.telemetry:
#                 await self.telemetry.emit_async(
#                     "dag_missing_parent_fetch_failed",
#                     peer_id=peer_id,
#                     missing_parents=list(ingest_result.missing_parents),
#                 )
