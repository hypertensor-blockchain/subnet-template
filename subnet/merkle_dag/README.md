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
- canonicalize, hash, sign, and verify those nodes
- store headers and bodies separately by content hash
- accept complete nodes before their parents arrive
- accept header-only snapshots while waiting for bodies
- track orphaned nodes while waiting for missing parents
- track the current accepted head set
- reconcile state with peers using node gossip, head announcements, inventory, fetches, and ancestor traversal
- rebuild application state later from the DAG

The code in this directory is intentionally independent from py-libp2p, GossipSub, and any concrete request-response transport. Network integration is supplied by thin adapters outside this directory, mainly under `subnet.utils.dag` and `subnet.protocols.dag_sync_protocol`.

## High-Level Architecture

At a high level, there are four layers:

1. domain models, validation, crypto, and serialization
2. storage and DAG activation
3. sync coordination, scheduling, and runtime assembly
4. transport integration outside this directory

```text
Application code
    |
    | payload schemas, publish triggers, materialized reads
    v
DagGossipSystem / DagPublisher          outside this directory
    |
    | create signed nodes, publish full-node gossip
    v
MerkleDagRuntime
    |
    | validates, stores, replays, reconciles
    v
MerkleDag + storage + validator
```

When integrated with the current network helpers:

```text
DagPublisher --------------------> GossipSub DagNodeGossip
     |                                      |
     | create/store node                    | full node for live replication
     v                                      v
 MerkleDag <---------------------- DagGossipSubReceiver
     |                                      |
     | orphan or missing head               | direct request/response
     v                                      v
SyncScheduler / SyncService ----> MerkleDagSyncCoordinator
```

Lightweight `DagAnnouncement` messages still exist for head summary gossip, but the normal publish path gossips a complete `DagNodeGossip` message so connected peers can ingest the node immediately.

## Module Map

### Core Domain

- `models.py`
  Defines immutable node, header, body, snapshot, summary, orphan, peer-state, and wire message models.

- `dag.py`
  The main DAG engine. It creates nodes, accepts complete nodes, accepts header-only snapshots, stores bodies, tracks orphans, activates nodes once complete parents exist, and manages head sets.

- `validator.py`
  Validation pipeline for canonical serialization, hash verification, signature verification, signer-peer derivation, optional remote timestamp checks, payload schema validation, parent-link validation, and domain validators.

- `payloads.py`
  Payload schema registration plus the `MappingPayloadSchema` base helper for dict-like payloads.

- `materializer.py`
  Replays complete nodes in deterministic topological order and lets schemas materialize application state from node history.

### Infrastructure

- `serialization.py`
  Canonical JSON serializer. This is the deterministic serialization source of truth.

- `crypto.py`
  Hash and signature helpers. Current defaults are SHA-256 plus libp2p-compatible signing and verification.

- `storage_memory.py`
  In-memory storage backend for tests and local development.

- `storage_rocksdb.py`
  Production-oriented storage backend backed by `subnet.utils.db.database.RocksDB`, scoped by DAG namespace.

- `interfaces.py`
  Protocols for storage, signing, verification, gossip publishing, request clients, peer providers, domain validators, and payload schemas.

- `adapters.py`
  Callable-based adapters for plugging app or transport code into the sync coordinator.

- `exceptions.py`
  Custom exceptions for serialization, schema, hash, signature, timestamp, source-peer, and parent validation errors.

### Sync And Runtime

- `sync.py`
  Sync message codec and `MerkleDagSyncCoordinator`. Handles head announcements, inventory comparison, direct fetches, recursive missing-parent fetches, peer sync state, and sync request serving.

- `runtime.py`
  Assembly object for one DAG namespace/topic. Builds serializer, hasher, schema registry, storage, validator, DAG, codec, and coordinator.

- `sync_service.py`
  Lifecycle wrapper for one runtime. Serves bytes-based request-response sync calls, supports startup reconciliation with `sync_dag(...)`, and can run optional periodic anti-entropy.

- `sync_scheduler.py`
  Event-driven missing-parent scheduler. It batches orphan notifications and asks the coordinator to fetch still-missing parent nodes from candidate peers.

## Data Model

A complete DAG node is split into a header and a body.

```text
DagNode
├── header: DagNodeHeader
└── body:   DagNodeBody
```

### `DagNodeHeader`

The header is the identity and verification surface.

Important fields:

- `node_id`
  Content-derived hash of the unsigned header bytes.
- `namespace`
  Logical DAG namespace.
- `schema_id`
  Payload schema used to validate and materialize the body.
- `parent_ids`
  Zero or more parent node ids. They must be sorted and unique.
- `body_hash`
  Hash of canonical payload bytes.
- `body_size`
  Size of canonical payload bytes.
- `author`
  Application-level author identity.
- `public_key`
  Signer public key as hex.
- `signature`
  Detached signature over the unsigned header bytes.
- `created_at_ms`
  Timestamp supplied by the producer.
- `version`
  Header version. The current default is `1`.
- `metadata`
  Optional application metadata, normalized and key-sorted.

### `DagNodeBody`

The body contains:

- `node_id`
- `payload`

The payload is schema-specific but must normalize into JSON-compatible data.

### `DagNodeSnapshot`

Snapshots are transfer objects used by direct fetches.

- `DagNodeSnapshot(header=header, body=body)` carries a complete node.
- `DagNodeSnapshot(header=header, body=None)` carries a header-only result.

`MerkleDag.add_snapshot(...)` accepts either form. A header-only snapshot can return `PENDING_BODY` when its parents are known but its body has not arrived yet.

## Wire Messages

All sync wire messages are canonical JSON envelopes with:

- `kind`
- `message_id`
- `namespace`
- `peer_id`
- `created_at_ms`

Current message kinds:

- `node_gossip`
  A GossipSub message carrying a complete `DagNode` for live replication.

- `announcement`
  A GossipSub message carrying current head ids and node count.

- `inventory_request`
  A direct request asking a peer for its current DAG summary.

- `inventory_response`
  A direct response carrying a `DagSummary`.

- `fetch_request`
  A direct request for headers or complete snapshots. It includes `node_ids`, `include_bodies`, and `max_ancestor_depth`.

- `fetch_response`
  A direct response carrying snapshots plus `not_found` ids.

`DagSyncMessageCodec` is the single codec for all of these messages.

## Why Header/Body Split Exists

The split supports efficient synchronization:

- peers can compare heads and fetch only what they are missing
- headers can travel without bodies when a fetch asks for header-only data
- identities are tied to canonical unsigned header bytes
- bodies can be verified independently against `body_hash`
- incomplete data can be tracked without pretending it is an accepted complete node

## Deterministic Serialization And Hashing

The subsystem uses canonical JSON from `serialization.py`.

Properties:

- sorted object keys
- no extra whitespace
- ASCII-safe output
- `allow_nan=False`
- recursive normalization of mappings and sequences
- string-only object keys

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

Independent header validation checks:

1. schema exists locally
2. parent ids are sorted and unique
3. no self-parenting
4. body size is non-negative
5. header bytes hash to the declared `node_id`
6. header signature verifies against `public_key`

Complete body validation checks:

1. body `node_id` matches the header
2. body bytes hash to `body_hash`
3. body size matches `body_size`
4. schema validates the payload
5. schema validates that the signed peer identity may publish the payload

Remote paths can also request timestamp validation:

```python
await dag.add_node(node, from_peer=peer_id, validate_remote_timestamp=True)
```

This rejects headers whose `created_at_ms` is too far in the future. The default `DagValidator` allowance is 60 seconds.

Transport receivers should additionally compare the transport sender to the signed header identity:

```python
dag.validator.validate_header_source_peer(node.header, source_peer_id)
```

Activation validation runs only after the node and all parents are complete locally:

1. parent namespaces match the node namespace
2. schema validates parent links
3. configured domain validators run

Visually:

```text
Incoming complete node
    |
    v
Header hash + signature validation
    |
    v
Body hash + schema validation
    |
    v
Signer peer validation
    |
    v
Remote timestamp/source checks when requested by receiver
    |
    v
Complete parents available?
    |                  \
    | yes               \ no
    v                    v
Activation checks       Mark orphan
    |
    v
Accept, update heads, retry waiting children
```

## Ingest Results

`MerkleDag` ingestion methods return `NodeIngestResult`.

Statuses:

- `ACCEPTED`
  The node is complete, parent/domain checks passed, and heads were updated.

- `DUPLICATE`
  The exact header/body state was already known.

- `ORPHAN`
  The header/body are valid, but one or more complete parents are missing.

- `PENDING_BODY`
  A header-only snapshot was stored, parent headers are available, and the body still needs to arrive.

- `REJECTED`
  The input could not be stored in this DAG. For example, namespace mismatch or body-before-header.

Most structural validation failures raise a typed `MerkleDagError` subclass rather than returning `REJECTED`.

## Orphans, Pending Bodies, And Multiple Heads

This subsystem expects misalignment.

### Orphans

A node becomes an orphan when its own data is valid but one or more parents are not complete locally.

The node is stored, not discarded.

The storage layer remembers:

- the orphan node id
- which parents are missing
- which children are waiting on a given parent

When a missing parent later arrives, the DAG re-attempts activation for the waiting children. Activation can cascade, so accepting one parent may resolve several descendants.

### Pending Bodies

Header-only snapshots are useful for lightweight fetches and ancestor inspection.

If a header arrives without a body:

- the header is validated and stored
- missing parent headers can still make it an `ORPHAN`
- otherwise the result is `PENDING_BODY`
- `add_body(...)` validates the body against the stored header and then tries activation

### Multiple Heads

A head is any accepted node that currently has no accepted child.

Multiple heads are normal when:

- peers publish concurrently
- the application intentionally allows divergent branches
- convergence happens later via merge-style nodes or application-level logic

The DAG does not force a single chain.

## Write Path: Creating And Publishing A Node

Application code can use `MerkleDag` directly in tests or local tools, but the app-facing network path usually starts with `DagGossipSystem.publish(...)` or `DagPublisher`.

Current full-node gossip path:

```text
app event
   |
   v
DagGossipSystem.publish(namespace, payload)
   |
   v
DagPublisher
   |
   | choose parents, check parent headers+bodies exist locally
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
   | validate + store + activate + update heads
   v
GossipSub DagNodeGossip
```

`DagPublisher.publish_heads(...)` and `MerkleDagSyncCoordinator.publish_heads(...)` are still available for lightweight `DagAnnouncement` publishing, but normal local publishes gossip the complete node.

## Receive Path: Gossip And Reconciliation

The receive-side network binder is outside this directory. The current helper is `DagGossipSubReceiver`, and the beginner-facing wrapper is `DagGossipSystem`.

Full node gossip:

```text
GossipSub DagNodeGossip
   |
   v
Decode with DagSyncMessageCodec
   |
   v
Check topic namespace, claimed peer id, and allowed schema id
   |
   v
Validate source peer matches signed header identity
   |
   v
MerkleDag.add_node(..., validate_remote_timestamp=True)
   |
   v
Accepted, duplicate, or orphan
   |
   v
If orphan, schedule missing-parent sync
```

Announcement reconciliation:

```text
GossipSub DagAnnouncement
   |
   v
Deduplicate announcement id
   |
   v
Store PeerSyncState summary
   |
   v
Unknown heads or larger remote count?
   |                    \
   | yes                 \ no
   v                      v
Inventory/fetch requests  Stop
```

Direct sync fetches walk backward from the missing frontier. `fetch_missing(...)` requests the exact missing ids with `max_ancestor_depth=0`; if a fetched node is still orphaned, its missing parents are queued for follow-up fetches.

## Sync Modes

There are four related sync paths:

- live node gossip
  `DagNodeGossip` carries a complete node over GossipSub.

- lightweight announcements
  `DagAnnouncement` carries head ids and node count over GossipSub.

- orphan-driven missing-parent sync
  `SyncScheduler` batches orphan notifications and calls `fetch_missing(...)`.

- anti-entropy reconciliation
  `MerkleDagSyncService` can perform inventory comparison and fetch missing content. Periodic reconciliation is disabled by default and only runs when explicitly enabled.

`sync_dag(...)` is the startup catch-up path. It waits for peers, lets discovery settle, then reconciles until local storage contains the advertised remote heads and their closure.

## Storage Model

The `DagStorage` protocol is the abstraction boundary. The DAG engine never depends on RocksDB directly.

### What Storage Must Support

- header lookup by `node_id`
- body lookup by `node_id`
- full node lookup by `node_id`
- current heads
- orphan tracking
- missing parent -> waiting child tracking
- announcement dedup
- complete node counting and listing
- peer sync state

### RocksDB Layout

`RocksDBDagStorage` scopes each logical map by namespace:

- `dag_heads:<namespace>`
- `dag_headers:<namespace>`
- `dag_bodies:<namespace>`
- `dag_orphans:<namespace>`
- `dag_seen_announcements:<namespace>`
- `dag_peer_state:<namespace>`
- nested `dag_waiting:<namespace>`

Important note:

The built-in storage only indexes nodes by DAG structure and sync needs. It does not automatically build secondary indexes for business queries like:

- "find node for user `alice`"
- "find latest order `123`"
- "find all records for tenant `x`"

If you need those queries later, build an application projection or external index from DAG contents.

## Reading Data Back Out Of The DAG

There are three common ways to read data later:

1. read raw nodes directly by `node_id`
2. read the current heads
3. replay and materialize application state from reachable nodes

### 1. Read Raw Node Data By `node_id`

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

### 2. Read The Current Heads

To know the current frontier:

```python
head_ids = await dag.get_heads()
```

Heads are useful when:

- you want to inspect the latest concurrent branches
- you want to reconcile with other peers
- you want to start replay/materialization from the frontier

### 3. Materialize Application State

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

## Example: Defining A Payload Schema

Every payload type should have a schema.

A schema is responsible for:

- canonicalizing payloads
- validating payload structure
- optionally validating that the signing peer may publish the payload
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

    def validate_signer_peer(self, node, signer_peer_id: str) -> None:
        # Optional: require payload ownership to match the signed libp2p identity.
        if node.body.payload.get("peer_id") not in (None, signer_peer_id):
            raise PayloadValidationError("profile peer_id does not match signer")

    def validate_parent_links(self, node, parents) -> None:
        # Optional: enforce domain-specific ancestry rules.
        user_id = node.body.payload["user_id"]
        for parent in parents:
            if parent.body.payload["user_id"] != user_id:
                raise PayloadValidationError("parent user_id mismatch")

    def materialize(self, node, parent_states: tuple[Any, ...]) -> Any:
        previous = parent_states[-1] if parent_states else {}
        return {
            **previous,
            "user_id": node.body.payload["user_id"],
            "display_name": node.body.payload["display_name"],
        }
```

## Example: Building A Local DAG

This example uses the transport-agnostic components only.

```python
from libp2p.crypto.ed25519 import create_new_key_pair

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagNodeSnapshot,
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
storage = InMemoryDagStorage(namespace="profiles")
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

## Example: Creating And Storing A Node Without Networking

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
print(result.status.value)  # accepted
print(node.header.node_id)  # content-derived id
```

## Example: Header-Only Snapshot Then Body

```python
snapshot = node.to_snapshot()

header_result = await dag.add_snapshot(
    DagNodeSnapshot(header=snapshot.header, body=None),
    from_peer=remote_peer_id,
    validate_remote_timestamp=True,
)

print(header_result.status.value)  # pending_body, orphan, duplicate, or rejected

body_result = await dag.add_body(snapshot.body)
print(body_result.status.value)
```

Fetch responses usually include bodies, but this path is available when a transport wants to stage headers separately.

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
storage = RocksDBDagStorage(db, serializer, namespace="profiles")

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
storage = RocksDBDagStorage(db, serializer, namespace="profiles")

dag = MerkleDag(
    namespace="profiles",
    storage=storage,
    validator=validator,
    schema_registry=schemas,
    serializer=serializer,
)

node = await dag.get_node(existing_node_id)
```

For most networked application wiring, prefer `MerkleDagRuntime`; it builds namespace-scoped RocksDB storage for you.

## Example: Publish Through `DagGossipSystem`

The recommended outbound API for app code using the DAG gossip helpers is `DagGossipSystem.publish(...)`. Application logic decides when to publish and passes the configured DAG namespace; the system writes the node locally and gossips it on the namespace's configured GossipSub topic.

```python
from subnet.utils.dag.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig

dag_system = DagGossipSystem(
    pubsub=pubsub,
    termination_event=termination_event,
    db=db,
    local_peer_id=host.get_id(),
    topics=[
        DagGossipTopicConfig(
            topic="profiles-dag",
            namespace="profiles",
            payload_schemas=[ProfileSchema()],
            schema_id="profile",
            signer=signer,
        )
    ],
)

result = await dag_system.publish(
    "profiles",
    {
        "user_id": "alice",
        "display_name": "Alice A.",
    },
    author="alice",
)

print(result.node_id)
print(result.gossip_message_id)
```

Receiving-only namespaces can still publish with explicit requirements:

```python
from subnet.utils.dag.dag_publisher import DagNodePublishRequirements

result = await dag_system.publish(
    "profiles",
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
```

## Example: Receive And Reconcile

The recommended app-facing wrapper owns receive loops, publishing, missing-parent repair, and direct sync request routing.

```python
from subnet.protocols.dag_sync_protocol import (
    DagPeerSetProvider,
    MerkleDagSyncProtocol,
    SyncProtocolPeerRequestClient,
)
from subnet.utils.dag.dag_gossip_system import DagGossipSystem, DagGossipTopicConfig

sync_protocol = MerkleDagSyncProtocol(host=host, db=db, pubsub=pubsub, gossipsub=gossipsub)
request_client = SyncProtocolPeerRequestClient(sync_protocol)
peer_provider = DagPeerSetProvider(sync_protocol)

dag_system = DagGossipSystem(
    pubsub=pubsub,
    termination_event=termination_event,
    db=db,
    local_peer_id=host.get_id(),
    topics=[
        DagGossipTopicConfig(
            topic="profiles-dag",
            namespace="profiles",
            payload_schemas=[ProfileSchema()],
            schema_id="profile",
            signer=signer,
            request_client=request_client,
            peer_provider=peer_provider,
        )
    ],
)

sync_protocol.set_request_handler(dag_system.handle_sync_request_bytes)
nursery.start_soon(dag_system.run)
```

To run startup reconciliation for a namespace:

```python
synced_peers = await dag_system.sync_dag("profiles")
```

If you only need the lower-level receive template, use `DagGossipSubReceiver` from `subnet.utils.dag.gossip_dag_receiver` and register `receiver.handle_sync_request_bytes` with the direct sync protocol.

## Example: Reading Data Outside The DAG

This is the key question most application code has:

> If the DAG stores important data, how do I read it later from application code that is not "inside the DAG"?

Short answer:

- either keep the `node_id` and read the node directly
- or build a projection or materialized view from DAG history

### Option A: Keep Node Ids And Fetch Raw Nodes

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

### Option B: Build A Projection

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

## Recommended Read Pattern For Real Applications

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

## Example: Building An External Projection

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

## What The DAG Does Not Do

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
3. decide signer-peer ownership rules, if any
4. decide parent-link rules
5. decide how application state should be materialized
6. decide whether you need an external projection or index
7. wire `DagGossipSystem` or `DagPublisher` for local writes
8. wire `DagGossipSubReceiver` or `DagGossipSystem` for inbound sync
9. configure a direct request client and peer provider when peers must fetch missing data
10. test out-of-order arrival, divergence, replay, and restart catch-up

## Practical Design Guidance

### Keep Payloads Small And Explicit

The payload is part of the immutable DAG history. Avoid vague or overloaded payloads.

### Put Query Indexes Outside The Core DAG

If you need to answer business queries quickly, build a projection store.

### Store Business Keys In Payload Or Metadata

If nodes represent domain entities, include stable identifiers like:

- `user_id`
- `document_id`
- `subnet_id`
- `object_key`

This makes replay and projection building much easier.

### Use Parent Links Intentionally

Parent links define causal structure. Think carefully about whether new nodes should:

- extend a single previous version
- merge multiple heads
- represent concurrent facts

### Prefer Complete Parents For Local Publishing

`DagPublisher` refuses to publish a node until required parent headers and bodies exist locally. This keeps local publishes from creating avoidable orphans.

## Typical Developer Questions

### How Do I Store Data?

Create a schema, build a node with `MerkleDag.create_node(...)` and `MerkleDag.add_node(...)`, or publish through `DagGossipSystem.publish(...)`.

### How Do I Read Data Later?

- if you know the `node_id`, use `await dag.get_node(node_id)`
- if you need derived current state, use `DagStateMaterializer`
- if you need fast business queries, build a projection/index outside the DAG

### How Do I Read The "Latest" Value?

The core DAG only knows heads, not business semantics. Your app should:

- inspect `await dag.get_heads()`
- materialize state
- maintain a projection keyed by your business identifier

### Can I Query By App Key Directly From The DAG?

Not by default. The core storage layer is structural, not domain-indexed.

### What Happens When Parents Arrive Late?

The child is stored as an orphan. When a parent arrives, activation is retried for any waiting children. If all required parents are complete and domain validation passes, the child is accepted and heads are updated.

### What Happens When Live Gossip Is Missed?

Announcements, startup sync, orphan-driven fetches, and optional periodic anti-entropy all use direct inventory/fetch requests to repair missing content.

## Minimal Mental Model

If you only remember one thing, remember this:

```text
The DAG stores immutable verified history.
Your app reads either:
- raw nodes by node id
- or derived state by replay/materialization
```

That separation is the core design of this subsystem.
