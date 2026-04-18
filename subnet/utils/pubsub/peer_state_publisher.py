from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
import secrets
from typing import Any

from libp2p.crypto.keys import KeyPair
from libp2p.custom_types import TProtocol
from libp2p.peer.id import ID
from libp2p.pubsub.pubsub import Pubsub
from multiaddr import Multiaddr
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator
import trio

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag.crypto import Libp2pKeyPairSigner
from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.interfaces import DagStorage
from subnet.merkle_dag.payloads import MappingPayloadSchema
from subnet.merkle_dag.runtime import MerkleDagRuntime
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB
from subnet.utils.gossipsub.dag_publisher import DagNodePublishRequirements, DagPublisher, DagPublishResult
from subnet.utils.pubsub.topics import PEER_STATE_TOPIC

logger = logging.getLogger("server/1.0.0")

PEER_STATE_SCHEMA_ID = "peer-state"


class ServerState(Enum):
    OFFLINE = 0
    JOINING = 1
    ONLINE = 2


class PeerRole(Enum):
    """
    Add custom roles, e.g. miner, validator, producer, etc.

    This logic can be stored and used for role permission based logic.
    """

    VALIDATOR = 0


class PeerStateData(BaseModel):
    """Application-level peer status stored in DAG node metadata."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    uid: str
    epoch: int
    subnet_id: int
    subnet_node_id: int
    state: ServerState
    role: PeerRole
    multiaddr: Multiaddr | None = None

    @field_validator("multiaddr", mode="before")
    @classmethod
    def _parse_multiaddr(cls, value: Multiaddr | str | None) -> Multiaddr | None:
        if value in (None, ""):
            return None
        if isinstance(value, Multiaddr):
            return value
        return Multiaddr(str(value))

    @field_serializer("multiaddr", when_used="json")
    def _serialize_multiaddr(self, value: Multiaddr | None) -> str | None:
        if value is None:
            return None
        return str(value)

    def model_post_init(self, __context: Any) -> None:
        assert self.subnet_id > 0, "Subnet ID must be greater than 0"
        assert self.subnet_node_id > 0, "Subnet node ID must be greater than 0"

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return self.model_dump_json()

    def to_bytes(self) -> bytes:
        """Serialize to bytes for raw pubsub compatibility."""
        return self.to_json().encode("utf-8")

    def to_metadata(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible mapping for DAG header metadata."""
        return self.model_dump(mode="json")

    @classmethod
    def from_json(cls, data: str) -> "PeerStateData":
        """Deserialize from JSON string."""
        return cls.model_validate_json(data)

    @classmethod
    def from_metadata(cls, data: dict[str, Any]) -> "PeerStateData":
        """Deserialize from DAG header metadata."""
        return cls.model_validate(data)


@dataclass(frozen=True)
class PeerStateDagRecord:
    """Convenience value returned when reading peer-state DAG nodes back out."""

    node_id: str
    peer_id: str
    state: PeerStateData


class PeerStateDagSchema(MappingPayloadSchema):
    """Schema for peer-owned status updates stored in a shared Merkle DAG."""

    def __init__(self) -> None:
        super().__init__(PEER_STATE_SCHEMA_ID)

    def validate_payload(self, payload) -> None:
        super().validate_payload(payload)
        peer_id = payload.get("peer_id")
        if not isinstance(peer_id, str) or not peer_id:
            raise PayloadValidationError("peer-state payload requires a non-empty 'peer_id'")
        kind = payload.get("kind")
        if kind != PEER_STATE_SCHEMA_ID:
            raise PayloadValidationError(f"peer-state payload requires kind={PEER_STATE_SCHEMA_ID!r}")

    def validate_signer_peer(self, node, signer_peer_id: str) -> None:
        if node.body.payload.get("peer_id") != signer_peer_id:
            raise PayloadValidationError(
                f"peer-state payload peer_id {node.body.payload.get('peer_id')!r} "
                f"does not match signer peer {signer_peer_id!r}"
            )

    def validate_parent_links(self, node, parents) -> None:
        for parent in parents:
            if parent.header.schema_id != self.schema_id:
                raise PayloadValidationError("peer-state nodes can only reference peer-state parents")

    def materialize(self, node, parent_states):
        return PeerStateDagRecord(
            node_id=node.header.node_id,
            peer_id=str(node.body.payload["peer_id"]),
            state=PeerStateData.from_metadata(node.header.metadata),
        )


class PeerStatePublisher:
    """
    Periodically publishes the local peer's state as a signed Merkle DAG node.

    The dynamic peer-state fields are stored in the DAG node header metadata while
    the body payload carries only the publisher identity binding required for
    schema validation. Each new local status node references the current
    peer-state frontier known locally so the example demonstrates one shared,
    converging DAG rather than isolated per-peer subgraphs.
    """

    def __init__(
        self,
        pubsub: Pubsub,
        topic: TProtocol,
        start_state: ServerState,
        start_role: PeerRole,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: LocalMockHypertensor | Hypertensor,
        *,
        db: RocksDB,
        key_pair: KeyPair,
        local_peer_id: ID,
        multiaddr: Multiaddr | None = None,
        telemetry: Telemetry | None = None,
        storage: DagStorage | None = None,
        runtime: MerkleDagRuntime | None = None,
        termination_event: trio.Event | None = None,
        namespace: str = "peer-state",
        publish_interval_seconds: float = 20.0,
        log_level: int = logging.INFO,
    ):
        self.pubsub = pubsub
        self.topic = topic
        self.state = start_state
        self.role = start_role
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.telemetry = telemetry
        self.publish_interval_seconds = publish_interval_seconds
        self.log_level = log_level
        self.local_peer_id = local_peer_id.to_string()
        self.multiaddr = multiaddr
        self.termination_event = termination_event if termination_event is not None else trio.Event()

        if runtime is None:
            self.dag_publisher = DagPublisher(
                pubsub=pubsub,
                termination_event=self.termination_event,
                db=db,
                payload_schemas=[PeerStateDagSchema()],
                local_peer_id=local_peer_id,
                namespace=namespace,
                dag_topic=topic,
                storage=storage,
                telemetry=telemetry,
                log_level=log_level,
            )
        else:
            self.dag_publisher = DagPublisher.from_runtime(
                pubsub=pubsub,
                termination_event=self.termination_event,
                runtime=runtime,
                telemetry=telemetry,
                log_level=log_level,
            )
        self.dag = self.dag_publisher.dag
        self.db = self.dag_publisher.db
        self._signer = Libp2pKeyPairSigner(key_pair)

    async def run(self) -> None:
        """Continuously publish the current peer state until shutdown or cancellation."""
        while not self.termination_event.is_set():
            await self.publish()
            await trio.sleep(self.publish_interval_seconds)

    async def publish(self) -> DagPublishResult | None:
        """Publish the current local peer state as a new DAG node."""
        try:
            if await self.dag.storage.count_orphans() > 0:
                logger.log(
                    self.log_level,
                    "Skipping peer state publish while DAG has unresolved orphan nodes",
                )
                return None

            message = self._build_peer_state_data()
            requirements = DagNodePublishRequirements(
                schema_id=PEER_STATE_SCHEMA_ID,
                payload={
                    "kind": PEER_STATE_SCHEMA_ID,
                    "peer_id": self.local_peer_id,
                },
                parent_ids=await self._peer_state_parent_ids(),
                author=self.local_peer_id,
                signer=self._signer,
                metadata=message.to_metadata(),
            )
            result = await self.dag_publisher.publish_now(requirements)
            await self._after_publish(message, result)
            return result
        except Exception as exc:
            logger.exception("Error in publish peer state DAG node, error=%s", exc)
            return None

    async def latest_local_peer_state(self) -> PeerStateDagRecord | None:
        """Return the latest local peer-state snapshot stored for this peer."""
        snapshot = self.db.get_nested(PEER_STATE_TOPIC, self.local_peer_id)
        if isinstance(snapshot, dict):
            node_id = snapshot.get("node_id")
            if isinstance(node_id, str) and node_id:
                return PeerStateDagRecord(
                    node_id=node_id,
                    peer_id=self.local_peer_id,
                    state=PeerStateData.from_metadata(snapshot),
                )

        latest_header = None
        for head_id in await self._peer_state_parent_ids():
            header = await self.dag.get_header(head_id)
            if header is None:
                continue
            if header.author != self.local_peer_id:
                continue
            if latest_header is None or (header.created_at_ms, header.node_id) > (
                latest_header.created_at_ms,
                latest_header.node_id,
            ):
                latest_header = header

        if latest_header is None:
            return None

        node = await self.dag.get_node(latest_header.node_id)
        if node is None:
            return None

        return PeerStateDagRecord(
            node_id=node.header.node_id,
            peer_id=self.local_peer_id,
            state=PeerStateData.from_metadata(node.header.metadata),
        )

    def _build_peer_state_data(self) -> PeerStateData:
        current_epoch = self.hypertensor.get_subnet_epoch_data(
            self.hypertensor.get_subnet_slot(self.subnet_id)
        ).epoch
        return PeerStateData(
            uid=secrets.token_hex(16),
            epoch=current_epoch,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            state=self.state,
            role=self.role,
            multiaddr=self.multiaddr,
        )

    async def _peer_state_parent_ids(self) -> tuple[str, ...]:
        peer_state_head_ids: list[str] = []
        for head_id in await self.dag.get_heads():
            node = await self.dag.get_node(head_id)
            if node is None:
                continue
            if node.header.schema_id != PEER_STATE_SCHEMA_ID:
                continue
            peer_state_head_ids.append(head_id)
        return tuple(sorted(peer_state_head_ids))

    async def _after_publish(self, message: PeerStateData, result: DagPublishResult) -> None:
        await self._store_local_peer_state_snapshot(message, result.node_id)
        if self.telemetry:
            await self.telemetry.emit_async(
                "peer_state_dag_sent",
                announcement_id=None if result.announcement is None else result.announcement.message_id,
                message=message.to_json(),
                node_id=result.node_id,
            )
        logger.log(
            self.log_level,
            "Published peer state DAG node %s for state=%s epoch=%s",
            result.node_id,
            self.state,
            message.epoch,
        )

    async def _store_local_peer_state_snapshot(self, message: PeerStateData, node_id: str) -> None:
        header = await self.dag.get_header(node_id)
        created_at_ms = -1 if header is None else header.created_at_ms
        current = {
            "peer_id": self.local_peer_id,
            "node_id": node_id,
            "created_at_ms": created_at_ms,
            **message.to_metadata(),
        }

        previous = self.db.get_nested(PEER_STATE_TOPIC, self.local_peer_id)
        if isinstance(previous, dict):
            previous_created_at_ms = int(previous.get("created_at_ms", -1))
            previous_node_id = str(previous.get("node_id", ""))
            if (previous_created_at_ms, previous_node_id) >= (created_at_ms, node_id):
                return

        self.db.set_nested(PEER_STATE_TOPIC, self.local_peer_id, current)
