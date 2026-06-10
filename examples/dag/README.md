# Building A DAG Publisher With `DagPublisherTemplate`

`DagPublisherTemplate` is the base class for application-specific DAG publishers.
Use it when you want your app to define a normal payload object, while the template handles the repetitive DAG work:

- finding schema-local DAG heads for parent links
- checking whether unresolved orphans should pause publishing
- building `DagNodePublishRequirements`
- publishing through the configured `DagGossipSystem`
- storing the latest local node snapshot
- returning latest local data as `DagRecord(node_id, peer_id, data)`

The payload itself should stay simple. It does not need to know about DAG nodes,
headers, signers, or parent links.

See [peer_state_dag_publisher.py](peer_state_dag_publisher.py) for a complete
working example.

## 1. Create Your Payload

Start with a small app-level payload. It can be a dataclass, a Pydantic model,
or any object you can convert to and from JSON-compatible metadata. When you use
Pydantic, inherit `DagPayloadTemplate` to get `to_metadata`, `to_json`,
`from_metadata`, and `from_json`.

This example uses Pydantic:

```python
from __future__ import annotations

from pydantic import ConfigDict

from subnet.utils.dag.dag_publisher_template import DagPayloadTemplate


class StatusPayload(DagPayloadTemplate):
    model_config = ConfigDict(extra="ignore", frozen=True)

    status: str
    epoch: int
    multiaddr: str
```

Notice that `StatusPayload` does not include `peer_id`. The DAG header stores
the publishing peer identity, so the body payload can stay focused on app data.

## 2. Create The Schema

Use `DagPublisherTemplateSchema` directly. It validates the app payload and
materializes records with the peer identity from the DAG node header.

```python
from subnet.utils.dag.dag_publisher_template import DagPublisherTemplateSchema


status_schema = DagPublisherTemplateSchema("status.v1", StatusPayload)
```

`DagPublisherTemplateSchema.validate_payload(...)` validates typed app data with
`payload_type.from_metadata(payload)`. Override
`data_from_payload(...)` only for app-specific conversion or non-`DagPayloadTemplate`
data.

`validate_parent_links` is also handled by `DagPublisherTemplateSchema`. By
default, template nodes can only reference parents with the same schema id.
Override it only if your app intentionally supports cross-schema parents.

`materialize` is handled by `DagPublisherTemplateSchema`. It returns
`DagRecord(node_id, peer_id, data)` using the node header author and
`data_from_payload(...)`, which defaults to `payload_type.from_metadata(...)`
when `payload_type` is supplied.

## 3. Configure `DagGossipSystem`

The template publisher expects the namespace and schema to already exist in a
`DagGossipSystem` topic config. The config must include:

- `namespace`
- `payload_schemas=[DagPublisherTemplateSchema(schema_id, YourPayload)]`
- `schema_id`
- `signer`

```python
from subnet.merkle_dag import Libp2pKeyPairSigner
from subnet.utils.dag.dag_gossip_system import (
    DagGossipSystem,
    DagGossipTopicConfig,
)


schema_id = app_config.status_schema_id
topic = app_config.status_topic
namespace = app_config.dag_namespace
snapshot_key = app_config.status_snapshot_key

dag_system = DagGossipSystem(
    pubsub=pubsub,
    termination_event=termination_event,
    db=db,
    local_peer_id=host.get_id(),
    topics=[
        DagGossipTopicConfig(
            topic=topic,
            namespace=namespace,
            payload_schemas=[DagPublisherTemplateSchema(schema_id, StatusPayload)],
            schema_id=schema_id,
            signer=Libp2pKeyPairSigner(key_pair),
            author=host.get_id().to_string(),
            parent_schema_id=schema_id,
            latest_node_snapshot_db_key=snapshot_key,
        )
    ],
)
```

Start the system before publishing:

```python
nursery.start_soon(dag_system.run)
```

## 4. Build The Publisher

Make your publisher inherit `DagPublisherTemplate[YourPayload]`.

`build_payload` is the only required method. This is where you get all of the
required data to build the payload that the peer will publish to the DAG. If
your app payload inherits `DagPayloadTemplate`, the default DAG body payload is
`payload.to_metadata()`. Override `dag_payload_from_data` only when you need a
custom body representation.

```python
from typing import Any

from subnet.utils.dag.dag_publisher import DagPublishResult
from subnet.utils.dag.dag_gossip_system import DagGossipSystem
from subnet.utils.dag.dag_publisher_template import DagPublisherTemplate


class StatusDagPublisher(DagPublisherTemplate[StatusPayload]):
    def __init__(
        self,
        dag_system: DagGossipSystem,
        *,
        namespace: str,
        schema_id: str,
        snapshot_db_key: str | None,
        multiaddr: str,
    ) -> None:
        super().__init__(
            dag_system=dag_system,
            namespace=namespace,
            schema_id=schema_id,
            snapshot_db_key=snapshot_db_key,
        )
        self.status = "joining"
        self.epoch = 0
        self.multiaddr = multiaddr

    def build_payload(self) -> StatusPayload:
        return StatusPayload(
            status=self.status,
            epoch=self.epoch,
            multiaddr=self.multiaddr,
        )

    async def after_publish(self, payload: StatusPayload, result: DagPublishResult) -> None:
        await super().after_publish(payload, result)
        # Add app-specific logging, telemetry, or side effects here.

    async def publish_status(self, payload: StatusPayload) -> DagPublishResult | None:
        return await self.publish_payload(payload)
```

Set up the publisher:

```python
status_publisher = StatusDagPublisher(
    dag_system,
    namespace=namespace,
    schema_id=schema_id,
    snapshot_db_key=snapshot_key,
    multiaddr=app_config.multiaddr,
)
```

## 5. Publish From The Template

Call `publish()` when the publisher should build the payload itself by calling
`build_payload`.

```python
result = await status_publisher.publish()
```

This path:

- calls `build_payload()`
- calls `build_metadata(payload)` unless metadata was supplied
- calls `dag_payload_from_data(payload)`
- chooses parent ids with `_schema_head_ids()`
- publishes to the local DAG and gossips through the configured topic
- calls `after_publish(payload, result)`

## 6. Publish From External Logic

Call `publish_payload(...)` when some other part of your application already has
the payload and wants to trigger publishing.

```python
payload = StatusPayload(
    status="online",
    epoch=42,
    multiaddr=app_config.multiaddr,
)

result = await status_publisher.publish_payload(
    payload,
)
```

You can wrap this in a domain-specific method, like `publish_status`, so callers
do not need to know template details.

```python
await status_publisher.publish_status(payload)
```

## 7. Use The Lower-Level Publish Primitives

Most code should call `publish()` or `publish_payload()`. If you need lower-level
control, the template can build the DAG requirements without immediately
publishing:

```python
requirements = await status_publisher.build_publish_requirements(
    payload=payload,
)

if requirements is not None:
    result = await status_publisher.publisher.publish_now(requirements)
    if result is not None:
        await status_publisher.after_publish(payload, result)
```

This is useful when an application needs to inspect or modify the requirements
before calling the underlying publisher.

## 8. Add App-Specific Run Logic Only If You Need It

`DagPublisherTemplate` does not include a `run()` loop. Not every application
publishes on an interval. If your publisher should publish periodically, put
that behavior in your concrete publisher or in application code.

```python
import trio


class IntervalStatusDagPublisher(StatusDagPublisher):
    def __init__(
        self,
        *args,
        termination_event: trio.Event,
        publish_interval_seconds: float = 20.0,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.termination_event = termination_event
        self.publish_interval_seconds = publish_interval_seconds

    async def run(self) -> None:
        while not self.termination_event.is_set():
            await self.publish()
            await trio.sleep(self.publish_interval_seconds)
```

The peer-state example uses this pattern because peer state should publish on an
interval. A different publisher might publish only when a transaction finishes,
when a model result is ready, or when a CLI command is called.

## 9. Read The Latest Local Record

The template stores a latest-node snapshot when `after_publish` calls `super()`.
Read it with:

```python
latest = await status_publisher.latest_local_record()

if latest is not None:
    print(latest.node_id)
    print(latest.peer_id)
    print(latest.data.status)
```

`latest_local_peer_state()` is an alias retained for peer-state style
publishers:

```python
latest = await status_publisher.latest_local_peer_state()
```

Both return `DagRecord[StatusPayload] | None`.

## Override Reference

Required:

- `build_payload(self) -> YourPayload`: builds the payload used by `publish()`
  when no payload is supplied.

Override only for custom body shapes or conversions:

- `dag_payload_from_data(self, data) -> Any`: converts your app payload into a
  non-default DAG body payload.
- `data_from_dag_payload(self, payload) -> YourPayload`: converts a stored DAG
  body payload back into your app payload type when the configured schema cannot
  do it.

Optional:

- `build_metadata(self, payload) -> Mapping[str, Any] | None`: sets DAG header
  metadata. The default returns `default_metadata` plus `payload.to_metadata()`
  for `DagPayloadTemplate` values.
- `after_publish(self, payload, result) -> None`: runs after a successful publish.
  Call `await super().after_publish(payload, result)` to keep latest-node
  snapshots and default telemetry.

Inherited helpers you normally do not override:

- `publish(...)`: build or accept a payload, write the DAG node, gossip it, then
  call `after_publish`.
- `publish_payload(...)`: publish a caller-supplied payload from external logic.
- `build_publish_requirements(...)`: build low-level requirements for manual
  publishing.
- `latest_local_record()`: return the latest local `DagRecord`.
- `latest_local_peer_state()`: alias for `latest_local_record()`.
- `_schema_head_ids(...)`: choose complete local DAG heads for the schema.

## Generic Type Syntax

Use the payload type in the template generic:

```python
class StatusDagPublisher(DagPublisherTemplate[StatusPayload]):
    ...
```

That gives IDEs and type checkers the right type for:

- `build_payload`
- `build_metadata`
- `after_publish`
- `publish_payload`
- `DagRecord[StatusPayload]`

## Callable Template Option

For very small publishers, you can use `CallableDagPublisherTemplate` instead of
writing a subclass:

```python
from typing import Any

from subnet.utils.dag.dag_publisher_template import CallableDagPublisherTemplate


publisher = CallableDagPublisherTemplate[dict[str, Any]](
    dag_system,
    namespace=namespace,
    schema_id=schema_id,
    snapshot_db_key=snapshot_key,
    payload_factory=lambda: {
        "peer_id": host.get_id().to_string(),
        "status": "online",
        "epoch": 42,
        "multiaddr": app_config.multiaddr,
    },
)

await publisher.publish()
```

Use the subclass pattern when your payload should remain a simple app object and
the publisher should own the DAG-specific conversion.
