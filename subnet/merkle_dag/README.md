# Merkle DAG Subsystem

This directory contains the transport-agnostic Merkle DAG replication layer used by the subnet.

It is designed for replicated data synchronization in a decentralized peer-to-peer system where:

- peers may receive updates out of order
- peers may temporarily diverge
- multiple heads are valid
- eventual convergence matters more than total ordering
- all remote data must be verified locally before it is accepted

This is not a blockchain consensus engine. It is a verified, immutable, multi-head data synchronization substrate.

## What This Subsystem Does

The Merkle DAG subsystem gives the application a way to:

- create immutable DAG nodes from typed payloads
- hash those nodes deterministically
- sign and verify nodes
- store nodes by content hash
- accept nodes before their parents arrive
- track orphaned nodes while waiting for missing parents
- reconcile state with peers using summaries, fetches, and ancestor traversal
- rebuild application state later from the DAG

The directory is intentionally independent from `libp2p`, GossipSub, and any specific transport. Networking is integrated by thin adapters outside this directory.

## High-Level Architecture

At a high level, there are three layers:

1. Domain and storage layer
2. Sync and reconciliation layer
3. Network integration layer

Only the first two live in this directory.

```text
Application Code
    |
    | creates payload schemas, publishes events, reads materialized state
    v
Merkle DAG Core
    |
    | validates, stores, replays, reconciles
    v
Storage + Crypto + Serialization
```

And when integrated with the existing network:

```text
DagPublisher ------------------> GossipSub announcement topic
     |                                      |
     | create/store node                    | lightweight head updates
     v                                      v
 MerkleDag <---------------------- GossipReceiverV2
     |                                      |
     | fetch headers/bodies                 | direct request/response
     v                                      v
 MerkleDagSyncCoordinator <---- peer request client / stream adapter
```

## Module Map

### Core domain

- `models.py`
  Defines immutable node, header, body, snapshot, summary, orphan, and wire message models.

- `dag.py`
  The main DAG engine. It creates nodes, accepts remote nodes, tracks orphans, activates nodes once parents exist, and manages head sets.

- `validator.py`
  Validation pipeline for canonical serialization, hash verification, signature verification, payload schema validation, and parent-link validation.

- `payloads.py`
  Payload schema registration and the base helper for dict-like payloads.

- `materializer.py`
  Replays nodes in topological order and lets schemas materialize application state from node history.

### Infrastructure

- `serialization.py`
  Canonical JSON serializer. This is the deterministic serialization source of truth.

- `crypto.py`
  Hash and signature helpers. Current defaults are SHA-256 plus libp2p-compatible signing and verification.

- `storage_memory.py`
  In-memory storage backend, mostly for tests and local development.

- `storage_rocksdb.py`
  Production-oriented storage backend backed by `subnet.utils.db.database.RocksDB`.

- `interfaces.py`
  Protocols for storage, signing, verification, gossip publishing, request clients, peer providers, and payload schemas.

### Sync

- `sync.py`
  Sync coordinator plus wire codec for announcements, inventories, and fetches.

- `adapters.py`
  Small callable-based adapters for plugging app/network code into the sync coordinator.

- `exceptions.py`
  Custom exceptions for serialization, schema, hash, signature, and parent validation errors.

## Data Model

A DAG node is split into a header and a body.

```text
DagNode
├── header: DagNodeHeader
└── body:   DagNodeBody
```

### `DagNodeHeader`

The header is the identity and verification surface.

Important fields:

- `node_id`
  Content-derived hash of the unsigned header bytes
- `namespace`
  Logical DAG namespace
- `schema_id`
  Payload schema used to validate and materialize the body
- `parent_ids`
  Zero or more parent node ids
- `body_hash`
  Hash of canonical payload bytes
- `body_size`
  Size of canonical payload bytes
- `author`
  Application-level author identity
- `public_key`
  Signer public key as hex
- `signature`
  Detached signature over the unsigned header bytes
- `created_at_ms`
  Timestamp supplied by the producer
- `metadata`
  Optional application metadata

### `DagNodeBody`

The body contains:

- `node_id`
- `payload`

The payload is schema-specific but must normalize into JSON-compatible data.

## Why Header/Body Split Exists

The split supports efficient synchronization:

- peers can compare heads and fetch only what they are missing
- headers can travel without bodies if needed
- identities are tied to canonical header bytes
- bodies can be verified independently against `body_hash`

## Deterministic Serialization and Hashing

The subsystem uses canonical JSON from `serialization.py`.

Properties:

- sorted object keys
- no extra whitespace
- ASCII-safe output
- `allow_nan=False`
- recursive normalization of mappings and sequences

This matters because hash and signature verification only work if every peer produces identical bytes from the same logical value.

Hashing flow:

```text
payload -> canonical JSON bytes -> body_hash
unsigned header -> canonical JSON bytes -> node_id
```

Current default hasher:

- `SHA256Hasher`

Digest format:

- `sha256:<hex-digest>`

## Validation Pipeline

Every accepted node goes through local verification. No remote input is trusted.

Validation order:

1. schema exists locally
2. parent ids are sorted and unique
3. no self-parenting
4. body size is sane
5. header bytes hash to the declared `node_id`
6. header signature verifies against `public_key`
7. body bytes hash to `body_hash`
8. body size matches `body_size`
9. payload schema validates the body payload
10. once parents are available, parent-link and domain validation run

Visually:

```text
Incoming Node
    |
    v
Header structural checks
    |
    v
Header hash verification
    |
    v
Signature verification
    |
    v
Body hash + size verification
    |
    v
Schema payload validation
    |
    v
Parents available?
    |               \
    | yes            \ no
    v                 v
Parent/domain         Mark orphan
activation
```

## Orphans, Pending Nodes, and Multiple Heads

This subsystem expects misalignment.

### Orphan

A node becomes an orphan when its header/body are valid but one or more parents are missing locally.

The node is stored, not discarded.

The storage layer remembers:

- the orphan node id
- which parents are missing
- which children are waiting on a given parent

When a missing parent later arrives, the DAG re-attempts activation for the waiting children.

### Multiple heads

A head is any accepted node that currently has no accepted child.

Multiple heads are normal when:

- peers publish concurrently
- the application intentionally allows divergent branches
- convergence happens later via merge-style nodes or application-level logic

The DAG does not force a single chain.

## Write Path: Creating and Publishing a Node

The local write path usually starts in `DagPublisher`, not in `MerkleDag` directly.

```text
app event
   |
   v
DagPublisher
   |
   | validate local parent preconditions
   v
MerkleDag.create_node(...)
   |
   | canonicalize payload
   | hash body
   | build unsigned header
   | hash header
   | sign header
   v
MerkleDag.add_node(...)
   |
   | validate + store + update heads
   v
MerkleDagSyncCoordinator.publish_heads(...)
   |
   v
GossipSub announcement
```

### Why `DagPublisher` exists

`GossipReceiverV2` is receive-only.

`DagPublisher` is the outbound side and is responsible for:

- accepting a publish trigger
- checking that required parents already exist locally
- building a new immutable DAG node
- storing it in the local DAG
- gossiping the updated head set

## Receive Path: Processing Gossip and Reconciliation

The receive side lives outside this directory, but it uses the components here.

```text
Gossip announcement
   |
   v
Decode DagAnnouncement
   |
   v
Compare remote heads to local DAG summary
   |
   v
Need data?
   |         \
   | yes      \ no
   v           v
Inventory/Fetch stop
requests
   |
   v
Receive headers/bodies/ancestors
   |
   v
MerkleDag.add_snapshot / add_node
   |
   v
Orphan or Accept
   |
   v
Update heads and waiting children
```

The coordinator in `sync.py` handles:

- announcement dedup
- summary comparison
- missing head detection
- direct fetch of headers/bodies/ancestors
- peer sync state caching
- optional anti-entropy loops

## Storage Model

The `DagStorage` protocol is the abstraction boundary. The DAG engine never depends on RocksDB directly.

### What the storage layer must support

- header lookup by `node_id`
- body lookup by `node_id`
- full node lookup by `node_id`
- current heads
- orphan tracking
- parent -> child tracking
- missing parent -> waiting child tracking
- announcement dedup
- peer sync state

### RocksDB layout

`RocksDBDagStorage` uses these namespaces:

- `dag_heads`
- `dag_headers`
- `dag_bodies`
- `dag_orphans`
- `dag_seen_announcements`
- `dag_peer_state`
- nested `dag_children`
- nested `dag_waiting`

Important note:

The built-in storage only indexes nodes by DAG structure and sync needs. It does not automatically build secondary indexes for business queries like:

- "find node for user `alice`"
- "find latest order `123`"
- "find all records for tenant `x`"

If you need those queries later, build an application projection or external index from DAG contents.

## Reading Data Back Out of the DAG

This is one of the most important usage patterns.

There are three common ways to read data later:

1. read raw nodes directly by `node_id`
2. read the current heads
3. replay/materialize application state from reachable nodes

### 1. Read raw node data by `node_id`

If you already know a node id, this is the simplest path.

```python
node = await dag.get_node(node_id)
if node is None:
    raise KeyError(node_id)

payload = node.body.payload
author = node.header.author
metadata = node.header.metadata
parents = node.header.parent_ids
```

This is good when:

- you stored the node id elsewhere
- a different component references node ids directly
- you need the exact immutable payload that was written

### 2. Read the current heads

To know the current frontier:

```python
head_ids = await dag.get_heads()
```

Heads are useful when:

- you want to inspect the latest concurrent branches
- you want to reconcile with other peers
- you want to start a replay/materialization from the frontier

### 3. Materialize application state

If your application needs a queryable, derived view of the DAG, use `DagStateMaterializer`.

```python
from subnet.merkle_dag import DagStateMaterializer

materializer = DagStateMaterializer(dag)
states_by_node_id = await materializer.materialize()
```

This returns a mapping:

```text
node_id -> schema.materialize(node, parent_states)
```

This is how you reconstruct application-level meaning from immutable DAG history.

## Example: Defining a Payload Schema

Every payload type should have a schema.

A schema is responsible for:

- canonicalizing payloads
- validating payload structure
- optionally validating parent relationships
- optionally materializing application state

Example:

```python
from typing import Any

from subnet.merkle_dag.exceptions import PayloadValidationError
from subnet.merkle_dag.payloads import MappingPayloadSchema


class ProfileSchema(MappingPayloadSchema):
    def __init__(self) -> None:
        super().__init__("profile")

    def validate_payload(self, payload) -> None:
        super().validate_payload(payload)
        if not isinstance(payload.get("user_id"), str):
            raise PayloadValidationError("profile payload requires string user_id")
        if not isinstance(payload.get("display_name"), str):
            raise PayloadValidationError("profile payload requires string display_name")

    def validate_parent_links(self, node, parents) -> None:
        # Optional: enforce domain-specific ancestry rules
        # Example: all parents must belong to the same user_id
        user_id = node.body.payload["user_id"]
        for parent in parents:
            if parent.body.payload["user_id"] != user_id:
                raise PayloadValidationError("parent user_id mismatch")

    def materialize(self, node, parent_states: tuple[Any, ...]) -> Any:
        # Optional: define the application view derived from this node
        previous = parent_states[-1] if parent_states else {}
        return {
            **previous,
            "user_id": node.body.payload["user_id"],
            "display_name": node.body.payload["display_name"],
        }
```

## Example: Building a Local DAG

This example uses the transport-agnostic components only.

```python
from libp2p.crypto.ed25519 import create_new_key_pair

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagValidator,
    InMemoryDagStorage,
    Libp2pKeyPairSigner,
    Libp2pSignatureVerifier,
    MerkleDag,
    PayloadSchemaRegistry,
    SHA256Hasher,
)


signer = Libp2pKeyPairSigner(create_new_key_pair())
serializer = CanonicalJSONSerializer()
schemas = PayloadSchemaRegistry([ProfileSchema()])
storage = InMemoryDagStorage()
validator = DagValidator(
    serializer=serializer,
    hasher=SHA256Hasher(),
    schema_registry=schemas,
    signature_verifier=Libp2pSignatureVerifier(),
)

dag = MerkleDag(
    namespace="profiles",
    storage=storage,
    validator=validator,
    schema_registry=schemas,
    serializer=serializer,
)
```

## Example: Creating and Storing a Node Without Networking

```python
node = await dag.create_node(
    schema_id="profile",
    payload={
        "user_id": "alice",
        "display_name": "Alice A.",
    },
    parent_ids=(),
    signer=signer,
    author="alice",
)

result = await dag.add_node(node)
print(result.status)        # accepted
print(node.header.node_id)  # content-derived id
```

## Example: Reading Raw Stored Data Later

Imagine another part of the application needs the exact node payload later.

```python
stored_node = await dag.get_node(node.header.node_id)
if stored_node is None:
    raise RuntimeError("node missing")

print(stored_node.body.payload)
print(stored_node.header.metadata)
```

If you only need one half:

```python
header = await dag.get_header(node_id)
body = await dag.get_body(node_id)
```

## Example: Using RocksDB Storage

For persisted storage:

```python
from subnet.merkle_dag import RocksDBDagStorage
from subnet.utils.db.database import RocksDB

db = RocksDB(base_path="/tmp/example_dag")
storage = RocksDBDagStorage(db, serializer)

dag = MerkleDag(
    namespace="profiles",
    storage=storage,
    validator=validator,
    schema_registry=schemas,
    serializer=serializer,
)
```

Later, after process restart:

```python
db = RocksDB(base_path="/tmp/example_dag")
storage = RocksDBDagStorage(db, serializer)

dag = MerkleDag(
    namespace="profiles",
    storage=storage,
    validator=validator,
    schema_registry=schemas,
    serializer=serializer,
)

node = await dag.get_node(existing_node_id)
```

## Example: Reading Data Outside the DAG

This is the key question most application code has:

> If the DAG stores important data, how do I read it later from application code that is not "inside the DAG"?

Short answer:

- either keep the `node_id` and read the node directly
- or build a projection / materialized view from DAG history

### Option A: Keep node ids and fetch raw nodes

If your app stores:

- latest profile node id per user
- latest config node id per subnet
- latest checkpoint node id per task

then later you can do:

```python
latest_profile_node_id = "sha256:..."
node = await dag.get_node(latest_profile_node_id)
profile = node.body.payload
```

This is simplest when your app already has an external pointer.

### Option B: Build a projection

If the app needs queryable current state, replay the DAG into an application view.

```python
materializer = DagStateMaterializer(dag)
states = await materializer.materialize()

for node_id, state in states.items():
    print(node_id, state)
```

You can then store the derived state somewhere else:

```python
states = await materializer.materialize()

for node_id, state in states.items():
    my_projection_db[node_id] = state
```

Or build a business-key index:

```python
states = await materializer.materialize()
profiles_by_user_id = {}

for node_id, state in states.items():
    user_id = state["user_id"]
    profiles_by_user_id[user_id] = {
        "node_id": node_id,
        "profile": state,
    }
```

This is usually the best answer when developers ask:

- "How do I query current user state?"
- "How do I read the latest config?"
- "How do I expose DAG data via an API?"

The DAG stores immutable history.
Your app usually reads from a projection built from that history.

## Recommended Read Pattern for Real Applications

Use both of these:

1. DAG as the immutable source of truth
2. projection store as the fast query surface

```text
Merkle DAG -> replay/materialize -> projection/index -> API/business logic
```

Why:

- DAG is ideal for integrity and reconciliation
- projections are ideal for fast app queries
- secondary indexes belong in app logic, not in the core DAG engine

## Example: Building an External Projection

```python
async def rebuild_profile_projection(dag: MerkleDag) -> dict[str, dict]:
    materializer = DagStateMaterializer(dag)
    states_by_node_id = await materializer.materialize()

    projection: dict[str, dict] = {}
    for node_id, state in states_by_node_id.items():
        user_id = state["user_id"]
        projection[user_id] = {
            "latest_node_id": node_id,
            "profile": state,
        }
    return projection
```

Now application code outside the DAG can do:

```python
projection = await rebuild_profile_projection(dag)
alice_profile = projection["alice"]["profile"]
```

## Example: Publish Through the Event-Driven Publisher

The recommended outbound API is `DagPublisher`.

```python
from subnet.utils.gossipsub.dag_publisher import DagNodePublishRequirements, DagPublisher

publisher = DagPublisher(
    pubsub=pubsub,
    termination_event=termination_event,
    db=db,
    payload_schemas=[ProfileSchema()],
    local_peer_id=host.get_id(),
    namespace="profiles",
    dag_topic="profiles-dag",
)
```

Immediate publish:

```python
result = await publisher.publish_now(
    DagNodePublishRequirements(
        schema_id="profile",
        payload={
            "user_id": "alice",
            "display_name": "Alice A.",
        },
        parent_ids=(),
        author="alice",
        signer=signer,
    )
)

print(result.node_id)
print(result.announcement)
```

Queued/event-driven publish:

```python
event = await publisher.trigger_publish(
    DagNodePublishRequirements(
        schema_id="profile",
        payload={
            "user_id": "alice",
            "display_name": "Alice A.",
        },
        parent_ids=(),
        author="alice",
        signer=signer,
    )
)

print(event.event_id)
```

And in the nursery:

```python
nursery.start_soon(publisher.run)
```

## Example: Receive and Reconcile

The receive-side binder is outside this directory, but this is the intended usage:

```python
receiver = GossipReceiverV2(
    gossipsub=gossipsub,
    pubsub=pubsub,
    termination_event=termination_event,
    db=db,
    payload_schemas=[ProfileSchema()],
    local_peer_id=host.get_id(),
    namespace="profiles",
    dag_topic="profiles-dag",
    request_client=request_client,
    peer_provider=peer_provider,
)

nursery.start_soon(receiver.run)
```

And to serve direct sync requests:

```python
response_bytes = await receiver.handle_sync_request_bytes(from_peer, request_bytes)
```

## Anti-Entropy and Reconciliation

There are two sync modes:

### Live gossip

Peers announce head changes through lightweight `DagAnnouncement` messages.

### Anti-entropy

Peers periodically compare summaries and fetch missing data even if live gossip was missed.

This combination handles:

- delayed peers
- packet loss
- restarts
- late joiners
- short-lived partitions

## What the DAG Does Not Do

It does not:

- enforce total ordering
- pick a winning branch
- provide business-level query indexes
- define merge semantics for your domain
- perform blockchain consensus

Those choices belong to the application schema and the broader system.

## Recommended Developer Workflow

When adding a new DAG-backed data type:

1. define a payload schema
2. decide what the payload means
3. decide parent-link rules
4. decide how application state should be materialized
5. decide whether you need an external projection/index
6. wire `DagPublisher` for local writes
7. wire `GossipReceiverV2` for inbound sync
8. test out-of-order arrival, divergence, and replay

## Practical Design Guidance

### Keep payloads small and explicit

The payload is part of the immutable DAG history. Avoid vague or overloaded payloads.

### Put query indexes outside the core DAG

If you need to answer business queries quickly, build a projection store.

### Store business keys in payload or metadata

If nodes represent domain entities, include stable identifiers like:

- `user_id`
- `document_id`
- `subnet_id`
- `object_key`

This makes replay and projection building much easier.

### Use parent links intentionally

Parent links define causal structure. Think carefully about whether new nodes should:

- extend a single previous version
- merge multiple heads
- represent concurrent facts

## Typical Developer Questions

### How do I store data?

Create a schema, build a node with `MerkleDag.create_node(...)` or `DagPublisher`, then add it to the DAG.

### How do I read data later?

- if you know the `node_id`, use `await dag.get_node(node_id)`
- if you need derived current state, use `DagStateMaterializer`
- if you need fast business queries, build a projection/index outside the DAG

### How do I read the "latest" value?

The core DAG only knows heads, not business semantics. Your app should:

- inspect `await dag.get_heads()`
- or materialize state
- or maintain a projection keyed by your business identifier

### Can I query by app key directly from the DAG?

Not by default. The core storage layer is structural, not domain-indexed.

## Minimal Mental Model

If you only remember one thing, remember this:

```text
The DAG stores immutable verified history.
Your app reads either:
- raw nodes by node id
- or derived state by replay/materialization
```

That separation is the core design of this subsystem.
