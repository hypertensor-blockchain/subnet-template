from __future__ import annotations

from enum import Enum
import logging
import secrets
from typing import Any

from pydantic import ConfigDict
import trio

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem
from subnet.merkle_dag.bases.dag_publisher_base import DagPublishResult
from subnet.merkle_dag.bases.dag_publisher_template import (
    DagPayloadTemplate,
    DagPublisherTemplate,
)
from subnet.telemetry.telemetry import Telemetry

logger = logging.getLogger(__name__)


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


class PeerStateData(DagPayloadTemplate):
    """Application-level peer status."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    uid: str
    epoch: int
    subnet_id: int
    subnet_node_id: int
    state: ServerState
    role: PeerRole
    multiaddr: str

    def model_post_init(self, __context: Any) -> None:
        assert self.subnet_id > 0, "Subnet ID must be greater than 0"
        assert self.subnet_node_id > 0, "Subnet node ID must be greater than 0"


class PeerStateDagPublisher(DagPublisherTemplate[PeerStateData]):
    """
    Template-based peer-state DAG publisher example.

    The full ``PeerStateData`` object is stored as the DAG body payload. The
    inherited ``latest_local_peer_state`` returns ``DagRecord[PeerStateData]``
    with ``node_id``, ``peer_id``, and ``data``.

    Call ``publish_peer_state(...)`` when application logic wants to publish a
    specific payload immediately. Start ``run()`` in a nursery to publish the
    current local peer state every ``publish_interval_seconds``.
    """

    def __init__(
        self,
        dag_system: DagGossipSystem,
        start_state: ServerState,
        start_role: PeerRole,
        subnet_id: int,
        subnet_node_id: int,
        hypertensor: LocalMockHypertensor | Hypertensor,
        schema_id: str,
        *,
        namespace: str,
        multiaddr: str,
        dag_topic: str | None = None,
        telemetry: Telemetry | None = None,
        termination_event: trio.Event | None = None,
        publish_interval_seconds: float = 20.0,
        log_level: int = logging.DEBUG,
    ) -> None:
        super().__init__(
            dag_system=dag_system,
            namespace=namespace,
            schema_id=schema_id,
            snapshot_db_key=dag_topic,
            telemetry=telemetry,
            log_level=log_level,
        )
        if publish_interval_seconds <= 0:
            raise ValueError("PeerStateDagPublisher publish_interval_seconds must be greater than zero")
        self.state = start_state
        self.role = start_role
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.multiaddr = multiaddr
        self.termination_event = termination_event if termination_event is not None else dag_system.termination_event
        self.publish_interval_seconds = publish_interval_seconds

    def update_state(self, new_state: ServerState) -> None:
        if not isinstance(new_state, ServerState):
            raise TypeError("Peer state must be a ServerState")

        previous_state = self.state
        self.state = new_state
        if previous_state is not new_state:
            logger.log(
                self.log_level,
                "Updated peer state from %s to %s",
                previous_state,
                new_state,
            )

    async def run(self) -> None:
        """Continuously publish the current peer state until shutdown or cancellation."""
        while not self.termination_event.is_set():
            await self.publish()
            await trio.sleep(self.publish_interval_seconds)

    def build_payload(self) -> PeerStateData:
        """Build the current local peer state for periodic/template publishing."""
        current_epoch = self.hypertensor.get_subnet_epoch_data(self.hypertensor.get_subnet_slot(self.subnet_id)).epoch
        return PeerStateData(
            uid=secrets.token_hex(16),
            epoch=current_epoch,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            state=self.state,
            role=self.role,
            multiaddr=self.multiaddr,
        )

    async def after_publish(self, payload: PeerStateData, result: DagPublishResult) -> None:
        """Emit peer-state telemetry after the template stores the latest-node cache."""
        await super().after_publish(payload, result)
        if self.telemetry:
            await self.telemetry.emit_async(
                "peer_state_dag_sent",
                announcement_id=None if result.announcement is None else result.announcement.message_id,
                message=payload.to_json(),
                node_id=result.node_id,
            )
        logger.log(
            self.log_level,
            "Published peer state DAG node %s for state=%s epoch=%s",
            result.node_id,
            payload.state,
            payload.epoch,
        )

    async def publish_peer_state(
        self,
        data: PeerStateData,
        *,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        """Publish caller-supplied peer state from external application logic."""
        return await self.publish_payload(
            data,
            created_at_ms=created_at_ms,
        )

    async def publish_offline_state(
        self,
        *,
        created_at_ms: int | None = None,
        force: bool = True,
    ) -> DagPublishResult | None:
        """
        Publish one final OFFLINE peer-state DAG node before shutdown.

        ``force`` lets shutdown publish even if normal interval publishing is
        paused by unresolved orphans. The final offline status should still be
        announced to connected peers whenever the local DAG can accept it.
        """
        logger.info(
            "Announcing that this peer is going offline with one final peer-state DAG gossip"
        )
        self.update_state(ServerState.OFFLINE)

        previous_skip_if_orphans = self.skip_if_orphans
        if force:
            self.skip_if_orphans = False
        try:
            return await self.publish(created_at_ms=created_at_ms)
        finally:
            self.skip_if_orphans = previous_skip_if_orphans

    async def trigger_publish_payload(
        self,
        data: PeerStateData,
        *,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        """Backward-compatible alias for publishing caller-supplied peer state."""
        return await self.publish_peer_state(data, created_at_ms=created_at_ms)
