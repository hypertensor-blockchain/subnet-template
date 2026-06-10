# DAG Publisher Template

Use `DagPublisherTemplate` when application data should be written as signed
Merkle DAG nodes and announced over a `DagGossipSystem` topic.

## What You Need

- a running libp2p `Pubsub`
- a `trio.Event` used for shutdown
- the app DB object
- the local peer id, usually `host.get_id()`
- the local key pair wrapped in `Libp2pKeyPairSigner`

## 1. Create A Schema

The final DAG body payload must be JSON-compatible. The signed DAG header stores
the publishing peer identity.

For Pydantic payload objects, inherit `DagPayloadTemplate` to get the standard
`to_metadata`, `to_json`, `from_metadata`, and `from_json` helpers. When your
schema should materialize that payload type directly, pass it as
`payload_type` to `DagPublisherTemplateSchema`.

```python
from typing import Any

from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.utils.dag.dag_publisher_template import DagPublisherTemplateSchema


SCHEMA_ID = "status.v1"


class StatusSchema(DagPublisherTemplateSchema[dict[str, Any]]):
    def __init__(self) -> None:
        super().__init__(SCHEMA_ID)

    def validate_payload(self, payload: Any) -> None:
        super().validate_payload(payload)
        if not isinstance(payload.get("status"), str):
            raise PayloadValidationError("status must be a string")
```

## 2. Configure `DagGossipSystem`

`topic` is the GossipSub topic. `namespace` is the DAG namespace. They can be
the same string, but they do not have to be.

```python
from subnet.merkle_dag import Libp2pKeyPairSigner
from subnet.utils.dag.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig


TOPIC = "status-dag"
NAMESPACE = "status"
SNAPSHOT_KEY = "latest-status-node"

dag_system = DagGossipSystem(
    pubsub=pubsub,
    termination_event=termination_event,
    db=db,
    local_peer_id=host.get_id(),
    topics=[
        DagGossipTopicConfig(
            topic=TOPIC,
            namespace=NAMESPACE,
            payload_schemas=[StatusSchema()],
            schema_id=SCHEMA_ID,
            signer=Libp2pKeyPairSigner(key_pair),
            author=host.get_id().to_string(),
            parent_schema_id=SCHEMA_ID,
            latest_node_snapshot_db_key=SNAPSHOT_KEY,
        )
    ],
)

# If you use the DAG sync protocol, route requests through the system.
sync_protocol.set_request_handler(dag_system.handle_sync_request_bytes)

# Start this in your Trio nursery before publishing.
nursery.start_soon(dag_system.run)
```

## 3. Build A Publisher

Only `build_payload()` is required. Override `build_metadata()` when useful.
If your app object is not already the DAG body payload, also override
`dag_payload_from_data()` and `data_from_dag_payload()`.

```python
from typing import Any

from subnet.utils.dag.dag_gossip_system import DagGossipSystem
from subnet.utils.dag.dag_publisher import DagPublishResult
from subnet.utils.dag.dag_publisher_template import DagPublisherTemplate


class StatusPublisher(DagPublisherTemplate[dict[str, Any]]):
    def __init__(self, dag_system: DagGossipSystem) -> None:
        super().__init__(
            dag_system=dag_system,
            namespace=NAMESPACE,
            schema_id=SCHEMA_ID,
            snapshot_db_key=SNAPSHOT_KEY,
        )
        self.status = "joining"

    def build_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
        }

    async def build_metadata(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": payload["status"]}

    async def after_publish(self, payload: dict[str, Any], result: DagPublishResult) -> None:
        await super().after_publish(payload, result)
        # Add app logging or telemetry here.
```

## 4. Publish

```python
status_publisher = StatusPublisher(dag_system)

# Build the payload from current app state.
result = await status_publisher.publish()

# Or publish a payload supplied by application logic.
result = await status_publisher.publish_payload(
    {"status": "online"},
    metadata={"status": "online"},
)
```

`publish()` returns `DagPublishResult | None`. It returns `None` if publishing
is skipped or fails validation.

## 5. Read The Latest Local Record

```python
latest = await status_publisher.latest_local_record()

if latest is not None:
    print(latest.node_id)
    print(latest.peer_id)
    print(latest.data["status"])
```

## Common Checks

- `DagGossipTopicConfig` must include both `schema_id` and `signer`.
- The publisher `namespace` and `schema_id` must match the topic config.
- The signed DAG header stores the publishing peer identity.
- With `skip_if_orphans=True`, the default, publishing pauses while unresolved
  orphan DAG nodes exist locally.

For a fuller example, see
[`examples/dag/peer_state_dag_publisher.py`](../../../examples/dag/peer_state_dag_publisher.py).
