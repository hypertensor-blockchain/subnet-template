# from __future__ import annotations

# from dataclasses import dataclass
# import logging
# import secrets
# from typing import Any

# from multiaddr import Multiaddr
# from pydantic import BaseModel
# import trio

# from subnet.hypertensor.chain_functions import Hypertensor
# from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
# from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem
# from subnet.merkle_dag.bases.dag_publisher_base import DagPublishResult
# from subnet.merkle_dag.exceptions import PayloadValidationError, SchemaNotFoundError
# from subnet.merkle_dag.payloads import MappingPayloadSchema
# from subnet.telemetry.telemetry import Telemetry
# from subnet.utils.pubsub.topics import HEARTBEAT_TOPIC

# logger = logging.getLogger(__name__)


# class HeartbeatData(BaseModel):
#     uid: str  # UID required for any topics that may be duplicate within the TTL of the message
#     epoch: int
#     subnet_id: int
#     subnet_node_id: int

#     def model_post_init(self, __context: Any) -> None:
#         assert self.subnet_id > 0, "Subnet ID must be greater than 0"
#         assert self.subnet_node_id > 0, "Subnet node ID must be greater than 0"

#     def to_json(self) -> str:
#         """Serialize to JSON string."""
#         return self.model_dump_json()

#     def to_bytes(self) -> bytes:
#         """Serialize to bytes for pubsub."""
#         return self.to_json().encode("utf-8")

#     def to_metadata(self) -> dict[str, Any]:
#         """Serialize to a JSON-compatible mapping for DAG header metadata."""
#         return self.model_dump(mode="json")

#     @classmethod
#     def from_json(cls, data: str) -> "HeartbeatData":
#         """Deserialize from JSON string."""
#         return cls.model_validate_json(data)

#     @classmethod
#     def from_metadata(cls, data: dict[str, Any]) -> "HeartbeatData":
#         """Deserialize from DAG header metadata."""
#         return cls.model_validate(data)


# @dataclass(frozen=True)
# class HeartbeatDagRecord:
#     """Convenience value returned when reading heartbeat DAG nodes back out."""

#     node_id: str
#     peer_id: str
#     heartbeat: HeartbeatData


# class HeartbeatDagSchema(MappingPayloadSchema):
#     """Schema for peer-owned status updates stored in a shared Merkle DAG."""

#     def __init__(self, schema_id: str) -> None:
#         if not schema_id:
#             raise ValueError("HeartbeatDagSchema schema_id must be a non-empty string")
#         super().__init__(schema_id)
#         self.schema_id = schema_id

#     def validate_payload(self, payload) -> None:
#         super().validate_payload(payload)
#         kind = payload.get("kind")
#         if kind != self.schema_id:
#             raise PayloadValidationError(f"heartbeat payload requires kind={self.schema_id}")

#     def validate_signer_peer(self, _node, _signer_peer_id: str) -> None:
#         """
#         Header validation already binds the stored author to the signed peer.
#         """
#         return None

#     def validate_parent_links(self, node, parents) -> None:
#         for parent in parents:
#             if parent.header.schema_id != self.schema_id:
#                 raise PayloadValidationError(
#                     f"heartbeat nodes can only reference parents with schema_id={self.schema_id!r}"
#                 )

#     def materialize(self, node, parent_states):
#         return HeartbeatDagRecord(
#             node_id=node.header.node_id,
#             peer_id=node.header.author,
#             heartbeat=HeartbeatData.from_metadata(node.header.metadata),
#         )


# class HeartbeatDagPublisher:
#     """
#     Periodically publishes the local peer's state as a signed Merkle DAG node.

#     The dynamic heartbeat fields are stored in the DAG node header metadata while
#     the body payload carries the payload kind. The signed DAG header stores the
#     publisher peer identity. Each new local status node references the current
#     heartbeat frontier known locally so the example demonstrates one shared,
#     converging DAG rather than isolated per-peer subgraphs.

#     Publish calls go through ``DagGossipSystem.publish(namespace, ...)`` so the
#     DAG system owns the final local write and GossipSub publish.
#     """

#     def __init__(
#         self,
#         dag_system: DagGossipSystem,
#         subnet_id: int,
#         subnet_node_id: int,
#         hypertensor: LocalMockHypertensor | Hypertensor,
#         schema_id: str,
#         *,
#         multiaddr: Multiaddr | None = None,
#         telemetry: Telemetry | None = None,
#         termination_event: trio.Event | None = None,
#         namespace: str,
#         publish_interval_seconds: float = 20.0,
#         log_level: int = logging.DEBUG,
#     ):
#         if not schema_id:
#             raise ValueError("HeartbeatDagPublisher schema_id must be a non-empty string")
#         if not namespace:
#             raise ValueError("HeartbeatDagPublisher namespace must be a non-empty string")
#         self.dag_system = dag_system
#         self.namespace = namespace
#         self.subnet_id = subnet_id
#         self.subnet_node_id = subnet_node_id
#         self.hypertensor = hypertensor
#         self.schema_id = schema_id
#         self.telemetry = telemetry
#         self.publish_interval_seconds = publish_interval_seconds
#         self.log_level = log_level
#         self.multiaddr = multiaddr
#         self.termination_event = termination_event if termination_event is not None else trio.Event()

#         context = dag_system.context_for_namespace(namespace)
#         publisher_config = context.publisher.config
#         if publisher_config is None or publisher_config.signer is None:
#             raise ValueError(f"Heartbeat publisher requires namespace {namespace!r} to be configured with a signer")
#         try:
#             context.runtime.schema_registry.require(self.schema_id)
#         except SchemaNotFoundError as exc:
#             raise ValueError(
#                 f"Heartbeat publisher requires namespace {namespace!r} to register schema_id={self.schema_id!r}"
#             ) from exc

#         self.dag = context.dag
#         self.db = dag_system.db
#         self.local_peer_id = dag_system.local_peer_id.to_string()

#     async def run(self) -> None:
#         """Continuously publish the current peer state until shutdown or cancellation."""
#         while not self.termination_event.is_set():
#             await self.publish()
#             await trio.sleep(self.publish_interval_seconds)

#     async def publish(self) -> DagPublishResult | None:
#         """Publish the current local peer state as a new DAG node."""
#         try:
#             if await self.dag.storage.count_orphans() > 0:
#                 logger.log(
#                     self.log_level,
#                     "Skipping peer state publish while DAG has unresolved orphan nodes",
#                 )
#                 return None

#             message = self._build_peer_state_data()
#             result = await self.dag_system.publish(
#                 self.namespace,
#                 {
#                     "kind": self.schema_id,
#                 },
#                 metadata=message.to_metadata(),
#                 parent_ids=await self._schema_head_ids(),
#                 schema_id=self.schema_id,
#                 author=self.local_peer_id,
#             )
#             if result is None:
#                 raise RuntimeError("Heartbeat DAG publish unexpectedly returned no result")
#             await self._after_publish(message, result)
#             return result
#         except Exception as exc:
#             logger.exception("Error in publish peer state DAG node, error=%s", exc)
#             return None

#     async def latest_local_peer_state(self) -> HeartbeatDagRecord | None:
#         """Return the latest local heartbeat snapshot stored for this peer."""
#         snapshot = self.db.get_nested(HEARTBEAT_TOPIC, self.local_peer_id)
#         if isinstance(snapshot, dict):
#             node_id = snapshot.get("node_id")
#             if isinstance(node_id, str) and node_id:
#                 return HeartbeatDagRecord(
#                     node_id=node_id,
#                     peer_id=self.local_peer_id,
#                     heartbeat=HeartbeatData.from_metadata(snapshot),
#                 )

#         latest_header = None
#         for head_id in await self._schema_head_ids():
#             header = await self.dag.get_header(head_id)
#             if header is None:
#                 continue
#             if header.author != self.local_peer_id:
#                 continue
#             if latest_header is None or (header.created_at_ms, header.node_id) > (
#                 latest_header.created_at_ms,
#                 latest_header.node_id,
#             ):
#                 latest_header = header

#         if latest_header is None:
#             return None

#         node = await self.dag.get_node(latest_header.node_id)
#         if node is None:
#             return None

#         return HeartbeatDagRecord(
#             node_id=node.header.node_id,
#             peer_id=self.local_peer_id,
#             heartbeat=HeartbeatData.from_metadata(node.header.metadata),
#         )

#     def _build_peer_state_data(self) -> HeartbeatData:
#         current_epoch = self.hypertensor.get_subnet_epoch_data(self.hypertensor.get_subnet_slot(self.subnet_id)).epoch
#         return HeartbeatData(
#             uid=secrets.token_hex(16),
#             epoch=current_epoch,
#             subnet_id=self.subnet_id,
#             subnet_node_id=self.subnet_node_id,
#         )

#     async def _schema_head_ids(self) -> tuple[str, ...]:
#         schema_head_ids: list[str] = []
#         for head_id in await self.dag.get_heads():
#             node = await self.dag.get_node(head_id)
#             if node is None:
#                 continue
#             if node.header.schema_id != self.schema_id:
#                 continue
#             schema_head_ids.append(head_id)
#         return tuple(sorted(schema_head_ids))

#     async def _after_publish(self, message: HeartbeatData, result: DagPublishResult) -> None:
#         await self._store_local_peer_state_snapshot(message, result.node_id)
#         if self.telemetry:
#             await self.telemetry.emit_async(
#                 "heartbeat_dag_sent",
#                 announcement_id=None if result.announcement is None else result.announcement.message_id,
#                 message=message.to_json(),
#                 node_id=result.node_id,
#             )
#         logger.log(
#             self.log_level,
#             "Published heartbeat DAG node %s for epoch=%s",
#             result.node_id,
#             message.epoch,
#         )

#     async def _store_local_peer_state_snapshot(self, message: HeartbeatData, node_id: str) -> None:
#         header = await self.dag.get_header(node_id)
#         created_at_ms = -1 if header is None else header.created_at_ms
#         current = {
#             "peer_id": self.local_peer_id,
#             "node_id": node_id,
#             "created_at_ms": created_at_ms,
#             **message.to_metadata(),
#         }

#         previous = self.db.get_nested(HEARTBEAT_TOPIC, self.local_peer_id)
#         if isinstance(previous, dict):
#             previous_created_at_ms = int(previous.get("created_at_ms", -1))
#             previous_node_id = str(previous.get("node_id", ""))
#             if (previous_created_at_ms, previous_node_id) >= (created_at_ms, node_id):
#                 return

#         self.db.set_nested(HEARTBEAT_TOPIC, self.local_peer_id, current)
