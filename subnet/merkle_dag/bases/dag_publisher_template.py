"""
Developer-facing base classes for custom DAG publishers.

Use ``DagPublisherTemplate`` when an app has domain data but wants the DAG
publishing boilerplate handled in one place. The template validates that the
``DagGossipSystem`` namespace has the requested schema and signer, chooses
schema-local DAG heads as parents, skips publishes while unresolved orphans
exist, and exposes:

- ``publish(...)`` to build a payload and publish immediately.
- ``publish_payload(...)`` for external application logic that decides exactly
  when to publish a caller-supplied payload.
- ``latest_local_peer_state()`` for a generic ``DagRecord`` with
  ``node_id``, ``peer_id``, and arbitrary ``data``.

By default, ``data`` is the DAG body payload. Pydantic payload objects can
inherit ``DagPayloadTemplate`` for ``to_metadata``, ``to_json``,
``from_metadata``, and ``from_json``. If your application object is not already
a JSON-compatible DAG payload, override ``dag_payload_from_data`` and
``data_from_dag_payload``. Pair template publishers with
``DagPublisherTemplateSchema`` to validate and materialize app payloads while
the DAG header stores the signed peer identity.

Peer-state style example
------------------------

The built-in ``PeerStateDagPublisher`` can be replaced with a small template
subclass. This version stores the full peer-state object in the DAG body, so
``latest_local_peer_state()`` is inherited from the template and automatically
returns ``DagRecord[PeerStateData]``.

::

    from enum import Enum
    from typing import Any

    from multiaddr import Multiaddr
    from pydantic import ConfigDict, field_serializer, field_validator

    from subnet.utils.dag.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig
    from subnet.utils.dag.dag_publisher import DagPublishResult
    from subnet.utils.dag.dag_publisher_template import (
        DagPayloadTemplate,
        DagPublisherTemplate,
        DagPublisherTemplateSchema,
        DagRecord,
    )


    class ServerState(Enum):
        OFFLINE = 0
        JOINING = 1
        ONLINE = 2


    class PeerRole(Enum):
        VALIDATOR = 0


    class PeerStateData(DagPayloadTemplate):
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore", frozen=True)

        uid: str
        epoch: int
        subnet_id: int
        subnet_node_id: int
        state: ServerState
        role: PeerRole
        multiaddr: Multiaddr

        @field_validator("multiaddr", mode="before")
        @classmethod
        def _parse_multiaddr(cls, value: Multiaddr | str | None) -> Multiaddr:
            if value in (None, ""):
                raise ValueError("multiaddr is required")
            if isinstance(value, Multiaddr):
                return value
            return Multiaddr(str(value))

        @field_serializer("multiaddr", when_used="json")
        def _serialize_multiaddr(self, value: Multiaddr) -> str:
            return str(value)

        def model_post_init(self, __context: Any) -> None:
            assert self.subnet_id > 0
            assert self.subnet_node_id > 0


    class PeerStateTemplatePublisher(DagPublisherTemplate[PeerStateData]):
        def __init__(
            self,
            dag_system: DagGossipSystem,
            *,
            namespace: str,
            schema_id: str,
            snapshot_db_key: str | None,
            multiaddr: Multiaddr,
            subnet_id: int,
            subnet_node_id: int,
            start_state: ServerState,
            start_role: PeerRole,
        ) -> None:
            super().__init__(
                dag_system=dag_system,
                namespace=namespace,
                schema_id=schema_id,
                snapshot_db_key=snapshot_db_key,
            )
            self.subnet_id = subnet_id
            self.subnet_node_id = subnet_node_id
            self.state = start_state
            self.role = start_role
            self.multiaddr = multiaddr
            self.epoch = 0

        def build_payload(self) -> PeerStateData:
            return PeerStateData(
                uid=f"{self.local_peer_id}:{self.epoch}",
                epoch=self.epoch,
                subnet_id=self.subnet_id,
                subnet_node_id=self.subnet_node_id,
                state=self.state,
                role=self.role,
                multiaddr=self.multiaddr,
            )

        async def after_publish(self, data: PeerStateData, result: DagPublishResult) -> None:
            await super().after_publish(data, result)
            if self.telemetry:
                await self.telemetry.emit_async("peer_state_dag_sent", message=data.to_json(), node_id=result.node_id)

        async def publish_peer_state(self, data: PeerStateData) -> DagPublishResult | None:
            return await self.publish_payload(data)


    peer_state_schema_id = app_config.peer_state_schema_id
    peer_state_topic = app_config.peer_state_topic
    dag_namespace = app_config.dag_namespace
    peer_state_snapshot_key = app_config.peer_state_snapshot_key
    peer_state_multiaddr = Multiaddr(app_config.peer_state_multiaddr)

    dag_system = DagGossipSystem(
        pubsub=pubsub,
        termination_event=termination_event,
        db=db,
        local_peer_id=host.get_id(),
            topics=[
                DagGossipTopicConfig(
                    topic=peer_state_topic,
                    namespace=dag_namespace,
                    payload_schemas=[DagPublisherTemplateSchema(peer_state_schema_id, PeerStateData)],
                    schema_id=peer_state_schema_id,
                    signer=signer,
                author=host.get_id().to_string(),
                parent_schema_id=peer_state_schema_id,
                latest_node_snapshot_db_key=peer_state_snapshot_key,
            )
        ],
    )

    peer_state_publisher = PeerStateTemplatePublisher(
        dag_system,
        namespace=dag_namespace,
        schema_id=peer_state_schema_id,
        snapshot_db_key=peer_state_snapshot_key,
        multiaddr=peer_state_multiaddr,
        subnet_id=1,
        subnet_node_id=2,
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
    )

    await peer_state_publisher.publish()

    await peer_state_publisher.publish_peer_state(
        PeerStateData(
            uid="manual-update-1",
            epoch=42,
            subnet_id=1,
            subnet_node_id=2,
            state=ServerState.ONLINE,
            role=PeerRole.VALIDATOR,
            multiaddr=peer_state_multiaddr,
        )
    )

    latest: DagRecord[PeerStateData] | None = await peer_state_publisher.latest_local_peer_state()
    if latest is not None:
        print(latest.node_id, latest.peer_id, latest.data.state)

For simpler cases, use ``CallableDagPublisherTemplate`` and pass
``payload_factory``, ``metadata_factory``, and ``after_publish_hook`` callables
instead of writing a subclass. If an application needs interval publishing,
implement that loop in the application-specific publisher, not in the template.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
import inspect
import logging
from typing import Any, Generic, TypeAlias, TypeVar, cast

from pydantic import BaseModel

from subnet.merkle_dag.bases.dag_gossip_system import DagGossipSystem, DagGossipTopicContext
from subnet.merkle_dag.bases.dag_publisher_base import DagNodePublishRequirements, DagPublishResult
from subnet.merkle_dag.exceptions import PayloadValidationError, SchemaNotFoundError
from subnet.merkle_dag.interfaces import Signer
from subnet.merkle_dag.payloads import MappingPayloadSchema
from subnet.telemetry.telemetry import Telemetry

logger = logging.getLogger(__name__)

DataT = TypeVar("DataT")
DagPayloadT = TypeVar("DagPayloadT", bound="DagPayloadTemplate")
MaybeAwaitable: TypeAlias = Awaitable[DataT] | DataT
MetadataFactory: TypeAlias = Callable[[DataT], Awaitable[Mapping[str, Any] | None] | Mapping[str, Any] | None]
PayloadFactory: TypeAlias = Callable[[], MaybeAwaitable[DataT]]
AfterPublishHook: TypeAlias = Callable[[DataT, DagPublishResult], Awaitable[None] | None]

_MISSING = object()


class DagPayloadTemplate(BaseModel):
    """Pydantic payload base with the standard DAG metadata/JSON helpers."""

    def to_metadata(self) -> dict[str, Any]:
        """Serialize this payload to JSON-compatible DAG metadata."""
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Serialize this payload to a JSON string."""
        return self.model_dump_json()

    @classmethod
    def from_metadata(cls: type[DagPayloadT], metadata: Any) -> DagPayloadT:
        """Deserialize this payload from DAG metadata or a DAG body payload."""
        return cls.model_validate(metadata)

    @classmethod
    def from_json(cls: type[DagPayloadT], payload: str | bytes | bytearray) -> DagPayloadT:
        """Deserialize this payload from a JSON string or bytes."""
        return cls.model_validate_json(payload)


@dataclass(frozen=True, slots=True)
class DagRecord(Generic[DataT]):
    """Latest local DAG node record returned by the default template reader."""

    node_id: str
    peer_id: str
    data: DataT


class DagPublisherTemplateSchema(MappingPayloadSchema, Generic[DataT]):
    """Schema base for template-managed payloads owned by one publishing peer."""

    def __init__(self, schema_id: str, payload_type: type[DagPayloadTemplate] | None = None) -> None:
        """Store the schema id and optional payload model used by ``DagGossipSystem``."""
        if not schema_id:
            raise ValueError("DagPublisherTemplateSchema schema_id must be a non-empty string")
        if payload_type is not None and (
            not isinstance(payload_type, type) or not issubclass(payload_type, DagPayloadTemplate)
        ):
            raise TypeError("DagPublisherTemplateSchema payload_type must be a DagPayloadTemplate subclass")
        super().__init__(schema_id)
        self.schema_id = schema_id
        self.payload_type = payload_type

    def validate_payload(self, payload) -> None:
        """
        Validate the app data conversion.

        Used by DAG validation before template-managed nodes are accepted or
        published. If ``payload_type`` was provided, the default conversion uses
        ``payload_type.from_metadata(payload)``. Override ``data_from_payload``
        for app-specific typed checks or non-``DagPayloadTemplate`` data.
        """
        super().validate_payload(payload)
        try:
            self.data_from_payload(payload)
        except PayloadValidationError:
            raise
        except Exception as exc:
            raise PayloadValidationError(
                f"invalid DAG template payload for schema_id={self.schema_id!r}: {exc}"
            ) from exc

    def validate_signer_peer(self, _node, _signer_peer_id: str) -> None:
        """
        Accept any signer peer allowed by the generic DAG header validation.

        Override this only when a payload contains additional domain-specific
        peer ownership that must be checked against the signed peer identity.
        """
        return None

    def validate_parent_links(self, node, parents) -> None:
        """
        Require template nodes to build on parents from the same schema by default.

        Used during DAG validation. Override only when an app intentionally allows
        cross-schema parent links.
        """
        for parent in parents:
            if parent.header.schema_id != self.schema_id:
                raise PayloadValidationError(
                    f"DAG template nodes can only reference parents with schema_id={self.schema_id!r}"
                )

    def data_from_payload(self, payload: Any) -> DataT:
        """
        Return app-level data from a canonical DAG body payload.

        Used by payload validation and materialization. Subclasses override this
        when the stored DAG payload should become a typed application object and
        cannot be rebuilt by the optional ``payload_type``.
        """
        if self.payload_type is not None:
            return cast(DataT, self.payload_type.from_metadata(payload))
        return cast(DataT, payload)

    def materialize(self, node, _parent_states) -> DagRecord[DataT]:
        """
        Materialize a generic DAG record from a template-managed node.

        Used by DAG materialization to return ``DagRecord(node_id, peer_id, data)``
        using the node header author and ``data_from_payload``.
        """
        return DagRecord(
            node_id=node.header.node_id,
            peer_id=node.header.author,
            data=self.data_from_payload(node.body.payload),
        )


class DagPublisherTemplate(ABC, Generic[DataT]):
    """
    Abstract base class for app-specific DAG publishers.

    Subclasses provide ``build_payload`` for template-built publishes.
    ``latest_local_peer_state`` is built automatically as
    ``DagRecord(node_id, peer_id, data)`` where ``data`` defaults to the DAG body
    payload. ``DagPayloadTemplate`` app objects are serialized with
    ``to_metadata()`` by default. Override ``dag_payload_from_data`` or
    ``data_from_dag_payload`` if the in-memory app object differs from the
    canonical DAG payload.
    """

    def __init__(
        self,
        dag_system: DagGossipSystem,
        *,
        namespace: str,
        schema_id: str,
        snapshot_db_key: str | None = None,
        skip_if_orphans: bool = True,
        default_metadata: Mapping[str, Any] | None = None,
        author: str | None = None,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        """
        Wire this publisher to a configured DAG namespace and schema.

        Used by concrete publishers to reuse the shared DAG publish boilerplate:
        context lookup, local DAG/db handles, signer checks, and schema checks.
        """
        if not namespace:
            raise ValueError("DagPublisherTemplate namespace must be a non-empty string")
        if not schema_id:
            raise ValueError("DagPublisherTemplate schema_id must be a non-empty string")

        self.dag_system = dag_system
        self.namespace = namespace
        self.schema_id = schema_id
        self.context = self._context_for_schema(dag_system, namespace, schema_id)
        self.publisher = self.context.publisher
        self.dag = self.context.dag
        self.payload_schema = self.context.runtime.schema_registry.require(self.schema_id)
        self.db = dag_system.db
        self.local_peer_id = dag_system.local_peer_id.to_string()
        self.snapshot_db_key = snapshot_db_key or self.context.config.latest_node_snapshot_db_key
        self.skip_if_orphans = skip_if_orphans
        self.default_metadata = dict(default_metadata or {})
        self.author = author
        self.telemetry = telemetry if telemetry is not None else dag_system.telemetry
        self.log_level = log_level

        self._validate_publisher_ready()

    async def publish(
        self,
        payload: DataT | object = _MISSING,
        *,
        metadata: Mapping[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        """
        Build or accept a payload, write a DAG node, and gossip it immediately.

        This is the main high-level publish path. It builds requirements, calls
        the underlying ``DagPublisher.publish_now``, then runs ``after_publish``.
        """
        try:
            requirements = await self.build_publish_requirements(
                payload=payload,
                metadata=metadata,
                parent_ids=parent_ids,
                schema_id=schema_id,
                signer=signer,
                author=author,
                created_at_ms=created_at_ms,
            )
            if requirements is None:
                return None

            data = self.data_from_dag_payload(requirements.payload)
            result = await self.publisher.publish_now(requirements)
            if result is None:
                raise RuntimeError("DAG template publish unexpectedly returned no result")
            await self.after_publish(data, result)
            return result
        except Exception as exc:
            logger.exception("Error publishing DAG template node for schema_id=%s, error=%s", self.schema_id, exc)
            return None

    async def publish_payload(
        self,
        payload: DataT,
        *,
        metadata: Mapping[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
    ) -> DagPublishResult | None:
        """
        Publish a caller-supplied payload from external application logic.

        Used when the app already has data ready to publish, often through a
        domain-specific wrapper such as ``publish_peer_state``.
        """
        return await self.publish(
            payload,
            metadata=metadata,
            parent_ids=parent_ids,
            schema_id=schema_id,
            signer=signer,
            author=author,
            created_at_ms=created_at_ms,
        )

    async def build_publish_requirements(
        self,
        *,
        payload: DataT | object = _MISSING,
        metadata: Mapping[str, Any] | None = None,
        parent_ids: Sequence[str] | None = None,
        schema_id: str | None = None,
        signer: Signer | None = None,
        author: str | None = None,
        created_at_ms: int | None = None,
    ) -> DagNodePublishRequirements | None:
        """
        Return the complete low-level publish requirements for this payload.

        Used by ``publish`` before gossiping. Call directly only when application
        code needs to inspect or adjust requirements before ``publish_now``.
        """
        if self.skip_if_orphans and await self.dag.storage.count_orphans() > 0:
            logger.log(
                self.log_level,
                "Skipping DAG template publish for schema_id=%s while DAG has unresolved orphan nodes",
                self.schema_id,
            )
            return None

        resolved_schema_id = schema_id or self.schema_id
        resolved_data = await self._resolve_payload(payload)
        resolved_metadata = await self._resolve_metadata(resolved_data, metadata)
        resolved_payload = self.dag_payload_from_data(resolved_data)
        resolved_parent_ids = (
            await self._schema_head_ids(resolved_schema_id) if parent_ids is None else tuple(parent_ids)
        )
        publisher_config = self.publisher.config

        resolved_author = (
            author
            or self.author
            or (None if publisher_config is None else publisher_config.author)
            or self.local_peer_id
        )
        return DagNodePublishRequirements(
            schema_id=resolved_schema_id,
            payload=resolved_payload,
            parent_ids=tuple(sorted(set(resolved_parent_ids))),
            author=resolved_author,
            signer=signer or self._require_signer(),
            created_at_ms=created_at_ms,
            metadata=resolved_metadata,
        )

    async def latest_local_record(self) -> DagRecord[DataT] | None:
        """
        Return the latest local record from the configured cache or DAG heads.

        Used by app code that wants the latest locally published node as
        ``DagRecord`` without manually reading snapshots or scanning DAG heads.
        """
        if self.snapshot_db_key is not None:
            node_id = self._node_id_from_db()
            if node_id is not None:
                record = await self._record_from_node_id(node_id)
                if record is not None:
                    return record

        latest_header = None
        for head_id in await self._schema_head_ids():
            header = await self.dag.get_header(head_id)
            if header is None or header.author != self.local_peer_id:
                continue
            if latest_header is None or (header.created_at_ms, header.node_id) > (
                latest_header.created_at_ms,
                latest_header.node_id,
            ):
                latest_header = header

        if latest_header is None:
            return None

        return await self._record_from_node_id(latest_header.node_id)

    async def latest_local_peer_state(self) -> DagRecord[DataT] | None:
        """
        Return the latest local record using the peer-state style legacy name.

        Kept as a convenience alias for publishers and examples that model DAG
        data as local peer state.
        """
        return await self.latest_local_record()

    async def after_publish(self, payload: DataT, result: DagPublishResult) -> None:
        """
        Hook called after a successful publish.

        The default stores the latest-node snapshot and emits template telemetry.
        Override for app-specific side effects, and call ``super()`` to keep the
        latest-node cache behavior.
        """
        await self._store_latest_local_snapshot(payload, result.node_id)
        if self.telemetry:
            await self.telemetry.emit_async(
                "dag_template_published",
                namespace=self.namespace,
                schema_id=self.schema_id,
                node_id=result.node_id,
                gossip_message_id=result.gossip_message_id,
            )

    async def build_metadata(self, payload: DataT) -> Mapping[str, Any] | None:
        """
        Return header metadata for a payload.

        Used by ``build_publish_requirements`` when metadata was not supplied by
        the caller. ``DagPayloadTemplate`` values serialize through
        ``to_metadata()`` and merge with ``default_metadata``. Subclasses can
        override this to store only indexed fields, add derived fields, or omit
        metadata.
        """
        resolved = dict(self.default_metadata)
        if isinstance(payload, DagPayloadTemplate):
            resolved.update(payload.to_metadata())
        return resolved

    @abstractmethod
    def build_payload(self) -> MaybeAwaitable[DataT]:
        """
        Return the payload used when ``publish`` is called without one.

        Subclasses implement this to collect current app data and build the
        object that will become the DAG node payload.
        """

    def dag_payload_from_data(self, data: DataT) -> Any:
        """
        Return the JSON-compatible DAG body payload for ``data``.

        Used before publishing to convert app-level data into the canonical DAG
        body. ``DagPayloadTemplate`` values serialize through ``to_metadata()``;
        other JSON-compatible values are returned as-is. Override when app data
        needs a custom DAG body representation.
        """
        if isinstance(data, DagPayloadTemplate):
            return data.to_metadata()
        return data

    def data_from_dag_payload(self, payload: Any) -> DataT:
        """
        Return the app-level record data from a stored DAG body payload.

        Used after publishing and when reading latest records.
        ``DagPublisherTemplateSchema`` values deserialize through
        ``data_from_payload()``; other JSON-compatible values are returned as-is.
        Override only when the configured schema cannot rebuild the app object.
        """
        if isinstance(self.payload_schema, DagPublisherTemplateSchema):
            return cast(DataT, self.payload_schema.data_from_payload(payload))
        return cast(DataT, payload)

    async def _schema_head_ids(self, schema_id: str | None = None) -> tuple[str, ...]:
        """
        Return local complete DAG heads for the configured schema.

        Used by ``build_publish_requirements`` as the default parent set so each
        new node builds on the known schema frontier.
        """
        resolved_schema_id = schema_id or self.schema_id
        schema_head_ids: list[str] = []
        for head_id in await self.dag.get_heads():
            header = await self.dag.get_header(head_id)
            if header is None:
                continue
            if header.schema_id != resolved_schema_id:
                continue
            schema_head_ids.append(head_id)
        return tuple(sorted(schema_head_ids))

    async def _resolve_payload(self, payload: DataT | object) -> DataT:
        """
        Return the caller-supplied payload or build one with ``build_payload``.

        Used by ``build_publish_requirements`` and supports both sync and async
        payload builders.
        """
        if payload is not _MISSING:
            return cast(DataT, payload)

        built_payload = self.build_payload()
        if inspect.isawaitable(built_payload):
            built_payload = await cast(Awaitable[DataT], built_payload)
        return cast(DataT, built_payload)

    async def _resolve_metadata(
        self,
        payload: DataT,
        metadata: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Return caller-supplied metadata or build it with ``build_metadata``.

        Used by ``build_publish_requirements`` and supports both sync and async
        metadata builders.
        """
        if metadata is not None:
            return dict(metadata)

        resolved_metadata = self.build_metadata(payload)
        if inspect.isawaitable(resolved_metadata):
            resolved_metadata = await cast(Awaitable[Mapping[str, Any] | None], resolved_metadata)
        return dict(resolved_metadata or {})

    def _node_id_from_db(self) -> str | None:
        """Read the latest local node id from the configured snapshot DB key."""
        if self.snapshot_db_key is None:
            return None

        snapshot = self.db.get_nested(self.snapshot_db_key, self.local_peer_id)
        if not isinstance(snapshot, dict):
            return None

        node_id = snapshot.get("node_id")
        if not isinstance(node_id, str) or not node_id:
            return None

        return node_id

    async def _record_from_node_id(self, node_id: str) -> DagRecord[DataT] | None:
        """
        Load a DAG node and convert it into ``DagRecord``.

        Used by latest-record readers after a node id is found in the snapshot DB
        or selected from DAG heads.
        """
        node = await self.dag.get_node(node_id)
        if node is None:
            return None

        return DagRecord(
            node_id=node.header.node_id,
            peer_id=node.header.author,
            data=self.data_from_dag_payload(node.body.payload),
        )

    async def _store_latest_local_snapshot(self, _payload: DataT, node_id: str) -> None:
        """
        Store the latest successfully published local node in the snapshot DB.

        Used by ``after_publish`` so later reads can avoid scanning DAG heads when
        a fresh local snapshot is available.
        """
        if self.snapshot_db_key is None:
            return

        header = await self.dag.get_header(node_id)
        created_at_ms = -1 if header is None else header.created_at_ms
        metadata = {} if header is None else dict(header.metadata)
        current = {
            "peer_id": self.local_peer_id,
            "node_id": node_id,
            "schema_id": self.schema_id,
            "created_at_ms": created_at_ms,
            **metadata,
        }

        previous = self.db.get_nested(self.snapshot_db_key, self.local_peer_id)
        if isinstance(previous, dict):
            previous_created_at_ms = int(previous.get("created_at_ms", -1))
            previous_node_id = str(previous.get("node_id", ""))
            if (previous_created_at_ms, previous_node_id) >= (created_at_ms, node_id):
                return

        self.db.set_nested(self.snapshot_db_key, self.local_peer_id, current)

    def _require_signer(self) -> Signer:
        """
        Return the configured signer or fail fast if the namespace lacks one.

        Used during initialization and requirement building because template
        publishes must create signed DAG nodes.
        """
        publisher_config = self.publisher.config
        if publisher_config is None or publisher_config.signer is None:
            raise ValueError(
                f"DagPublisherTemplate requires namespace {self.namespace!r} to be configured with a signer"
            )
        return publisher_config.signer

    def _validate_publisher_ready(self) -> None:
        """
        Verify the namespace has a signer and the requested schema is registered.

        Called by ``__init__`` so misconfigured template publishers fail before
        publishing starts.
        """
        self._require_signer()
        try:
            self.context.runtime.schema_registry.require(self.schema_id)
        except SchemaNotFoundError as exc:
            raise ValueError(
                f"DagPublisherTemplate requires namespace {self.namespace!r} to register schema_id={self.schema_id!r}"
            ) from exc

    @staticmethod
    def _context_for_schema(
        dag_system: DagGossipSystem,
        namespace: str,
        schema_id: str,
    ) -> DagGossipTopicContext:
        """
        Find the DAG topic context configured for a namespace and schema.

        Used by ``__init__`` to bind the template to the right publisher, DAG,
        topic config, and schema registry.
        """
        for context in dag_system.contexts_by_topic.values():
            if context.namespace != namespace:
                continue
            schema_ids = {str(schema.schema_id) for schema in context.config.payload_schemas}
            if schema_id in schema_ids:
                return context

        raise ValueError(f"No DAG topic in namespace {namespace!r} is configured for schema_id={schema_id!r}")


class CallableDagPublisherTemplate(DagPublisherTemplate[DataT]):
    """Concrete template that delegates payload/metadata/hooks to callables."""

    def __init__(
        self,
        dag_system: DagGossipSystem,
        *,
        namespace: str,
        schema_id: str,
        payload_factory: PayloadFactory[DataT],
        metadata_factory: MetadataFactory[DataT] | None = None,
        after_publish_hook: AfterPublishHook[DataT] | None = None,
        snapshot_db_key: str | None = None,
        skip_if_orphans: bool = True,
        default_metadata: Mapping[str, Any] | None = None,
        author: str | None = None,
        telemetry: Telemetry | None = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        """
        Build a concrete template publisher from callables instead of a subclass.

        Used for small publishers where factories and hooks are enough to supply
        payloads, metadata, and post-publish behavior.
        """
        self._payload_factory = payload_factory
        self._metadata_factory = metadata_factory
        self._after_publish_hook = after_publish_hook
        super().__init__(
            dag_system=dag_system,
            namespace=namespace,
            schema_id=schema_id,
            snapshot_db_key=snapshot_db_key,
            skip_if_orphans=skip_if_orphans,
            default_metadata=default_metadata,
            author=author,
            telemetry=telemetry,
            log_level=log_level,
        )

    def build_payload(self) -> MaybeAwaitable[DataT]:
        """
        Return a payload from the configured factory.

        Used by inherited ``publish`` when no caller-supplied payload is passed.
        """
        return self._payload_factory()

    async def build_metadata(self, payload: DataT) -> Mapping[str, Any] | None:
        """
        Return metadata from the configured metadata factory, if present.

        Used by inherited requirement building, merging ``default_metadata`` with
        factory output.
        """
        metadata = dict(await super().build_metadata(payload) or {})
        if self._metadata_factory is None:
            return metadata

        factory_metadata = self._metadata_factory(payload)
        if inspect.isawaitable(factory_metadata):
            factory_metadata = await cast(Awaitable[Mapping[str, Any] | None], factory_metadata)
        metadata.update(dict(factory_metadata or {}))
        return metadata

    async def after_publish(self, payload: DataT, result: DagPublishResult) -> None:
        """
        Run the default snapshot behavior and the configured hook.

        Used after inherited publishes complete so callers can add custom
        side effects without losing the latest-node cache.
        """
        await super().after_publish(payload, result)
        if self._after_publish_hook is None:
            return

        hook_result = self._after_publish_hook(payload, result)
        if inspect.isawaitable(hook_result):
            await cast(Awaitable[None], hook_result)
