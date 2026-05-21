# ServerBase Developer Guide

This directory contains two styles of server implementation:

- `server_template.py`: the reusable server lifecycle template.

When people say "SubnetTemplate" in this codebase, they usually mean
`ServerBase`: the class in `subnet/server/server_template.py` that starts
and owns the reusable libp2p networking lifecycle. This guide uses the code name
`ServerBase`.

The main idea is simple:

- `ServerBase` owns common P2P infrastructure.
- `ApplicationBase` owns your application behavior.
- Your concrete `Server` class wires those two things together with the correct
  options for your subnet.

After reading this, a developer should know where to put application logic, how
to create a new server, how the template starts and stops services, and which
hook to use for each kind of work.

## What ServerBase Gives You

`ServerBase` removes the repeated work needed to build a subnet server:

- Create and run a libp2p host.
- Build listen addresses.
- Configure optional proof-of-stake transports.
- Create and run a KadDHT.
- Optionally create and run Pubsub and GossipSub.
- Connect to bootstrap peers.
- Add known peers to the DHT routing table.
- Optionally start ping handling.
- Optionally start random-walk discovery logging.
- Optionally create a `SubnetInfoTracker`.
- Optionally start connection maintenance.
- Optionally start consensus.
- Provide a shared `P2PNetworkContext` to application logic.
- Cancel all background tasks on shutdown.
- Call application cleanup while the network context is still available.

The template is intentionally not where business logic belongs. Anything that is
specific to your subnet, protocol, DAG topic, scoring, publisher, validator,
request handler, or app state belongs in a `ApplicationBase` subclass.

## The Mental Model

Build a server in three layers.

### 1. Application Layer

Subclass `ApplicationBase`. This is where app-specific logic lives.

Use it to:

- Register direct libp2p stream protocols.
- Start telemetry tasks.
- Subscribe to Pubsub topics.
- Create DAG systems and publishers.
- Perform startup sync.
- Start recurring app workers.
- Publish final status in cleanup.

### 2. Server Wrapper Layer

Subclass `ServerBase`. This is usually a small constructor that:

- Accepts user-facing server arguments.
- Creates your `ApplicationBase`.
- Calls `super().__init__(...)` with the right template options.
- Stores useful server attributes for logs or introspection.

See `examples/server/server.py` for the current complete example.

### 3. Runtime Layer

CLI or process code creates your server and runs it with Trio:

```python
server = Server(
    port=38960,
    key_pair=key_pair,
    db=db,
    subnet_id=1,
    subnet_node_id=1,
    hypertensor=hypertensor,
    bootstrap_addrs=bootstrap_addrs,
)

trio.run(server.run)
```

## Files To Read First

Start with these files in this order:

1. `subnet/server/server_template.py`
   The reusable lifecycle code.

2. `examples/server/server.py`
   A real subnet server using the template.

3. `examples/server/server_dag.py`
   A DAG-focused example server with the same template shape.

4. `subnet/protocols/protocol_base.py`
   The recommended base for direct request/response protocols.

5. `examples/dag/README.md`
   A guide for building DAG publishers.

## Step By Step: Build A New Server

### Step 1: Decide What Your App Needs

Before writing code, answer these questions:

- Does the app need Pubsub or GossipSub?
- Does it need DAG gossip?
- Does it need direct request/response protocols?
- Does it need on-chain peer information?
- Should it maintain only registered on-chain peers?
- Should it use proof-of-stake transport verification?
- Should it run consensus?
- Should bootstrap nodes behave differently from validator nodes?

These answers decide the `ServerBase` flags and the work done in your
`ApplicationBase` hooks.

### Step 2: Create A ApplicationBase

Start with an application class. Keep constructor arguments app-specific. Do not
pass every server option unless the application actually needs it.

```python
from libp2p.crypto.keys import KeyPair

from subnet.server.server_template import ApplicationBase, P2PNetworkContext
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB


class MyApplication(ApplicationBase):
    def __init__(
        self,
        *,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int,
        subnet_node_id: int,
        is_bootstrap: bool,
        telemetry: Telemetry | None = None,
    ) -> None:
        super().__init__()
        self.key_pair = key_pair
        self.db = db
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.is_bootstrap = is_bootstrap
        self.telemetry = telemetry

    async def setup(self, context: P2PNetworkContext) -> None:
        if self.telemetry is not None:
            context.nursery.start_soon(self.telemetry.run)

    async def start_application(
        self,
        context: P2PNetworkContext,
    ) -> None:
        pass

    async def cleanup(self, context: P2PNetworkContext) -> None:
        pass
```

### Step 3: Use `setup` For Local Network Registration

`setup` is called after the host, DHT, and optional Pubsub services are running,
but before bootstrap peers are dialed and before connection maintenance starts.

Use `setup` for work that only needs local services to exist:

- Start telemetry.
- Validate that required services exist.

Do not block forever in `setup`. If you need a background loop, start it with
`context.nursery.start_soon(...)`.

Example:

```python
from subnet.protocols.mock_protocol_v2 import MockProtocolV2
from subnet.telemetry.telemetry import Telemetry


class MyApplication(ApplicationBase):
    def __init__(self, telemetry: Telemetry | None = None):
        self.telemetry = telemetry

    async def setup(self, context: P2PNetworkContext) -> None:
        if self.telemetry is not None:
            context.nursery.start_soon(self.telemetry.run)

        self.mock_protocol = MockProtocolV2(
            host=context.host,
            telemetry=self.telemetry,
        )

        if context.peer_multiaddr is not None:
            logger.info("Running peer on %s", context.peer_multiaddr)
```

### Step 4: Use `start_application` For Peer-Dependent Work

This hook is called after:

1. The application `setup` hook has completed.
2. Bootstrap peers have been dialed.
3. Bootstrap peers have been added to the DHT routing table.
4. Optional connection maintenance has been started.

Use this hook for work that benefits from peers already being available:

- Start role specific logc.
- Start DAG gossip systems.
- Register DAG sync request handlers.
- Perform startup DAG sync.
- Start publishers that expect the network to be ready.
- Start app workers that should run after bootstrap.

This is the hook used by `server_dag.py` to start DAG sync and peer-state
publishing.

```python
async def start_application(
    self,
    context: P2PNetworkContext,
) -> None:
    if context.pubsub is None or context.gossipsub is None:
        raise RuntimeError("MyApplication requires Pubsub and GossipSub")

    self.dag_system = build_my_dag_system(
        host=context.host,
        dht=context.dht,
        pubsub=context.pubsub,
        gossipsub=context.gossipsub,
        termination_event=context.termination_event,
    )
    context.nursery.start_soon(self.dag_system.run)

    if self.is_bootstrap:
        return

    await self.dag_system.sync_dag(
        "my-namespace",
        min_peer_count=2,
        wait_timeout=180.0,
        poll_interval=1.0,
        settle_time=10.0,
    )

    self.publisher = build_my_publisher(self.dag_system)
    context.nursery.start_soon(self.publisher.run)
```

### Step 5: Override `run` Only When The App Owns The Main Loop

The default `ApplicationBase.run` waits until `context.termination_event` is set.
That is enough for most servers because background tasks are started in the
nursery.

Override `run` only if your application needs to own the main blocking behavior:

- Wait for a specific app condition.
- Run a foreground scheduler.
- Stop the server when an app-level task completes.

If `run` returns, the server starts shutdown.

```python
async def run(self, context: P2PNetworkContext) -> None:
    while not context.termination_event.is_set():
        await self.do_foreground_work()
        await trio.sleep(1.0)
```

### Step 6: Use `cleanup` For Final App Work

`cleanup` is called while the network context still exists, after
`context.stop()` has been requested.

Use it for short finalization:

- Publish final state.
- Flush app-level buffers.
- Stop app-owned resources that are not managed by the Trio nursery.
- Log shutdown information.

Keep cleanup bounded. A long or stuck cleanup delays server shutdown.

```python
async def cleanup(self, context: P2PNetworkContext) -> None:
    if self.publisher is not None:
        await self.publisher.publish_offline_status()
```

### Step 7: Wrap The Application In A Server

The concrete server class is mostly configuration. It creates your application,
then calls `ServerBase.__init__`.

```python
from collections.abc import Sequence
import logging

from libp2p.crypto.keys import KeyPair
from libp2p.rcmgr.manager import ResourceManager

from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.server.server_template import ServerBase
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.db.database import RocksDB


class MyServer(ServerBase):
    def __init__(
        self,
        *,
        ip: str | None = None,
        port: int,
        bootstrap_addrs: Sequence[str] | None = None,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int,
        subnet_slot: int = 3,
        subnet_node_id: int,
        hypertensor: Hypertensor | LocalMockHypertensor,
        is_bootstrap: bool = False,
        telemetry: Telemetry | None = None,
        enable_mDNS: bool = False,
        enable_upnp: bool = False,
        enable_autotls: bool = False,
        resource_manager: ResourceManager | None = None,
        psk: str | None = None,
    ) -> None:
        application = MyApplication(
            key_pair=key_pair,
            db=db,
            subnet_id=subnet_id,
            subnet_node_id=subnet_node_id,
            is_bootstrap=is_bootstrap,
            telemetry=telemetry,
        )

        super().__init__(
            ip=ip or "0.0.0.0",
            port=port,
            application=application,
            key_pair=key_pair,
            bootstrap_addrs=bootstrap_addrs,
            use_available_interfaces=True,
            enable_pubsub=True,
            enable_random_walk=True,
            enable_mDNS=enable_mDNS,
            enable_upnp=enable_upnp,
            enable_autotls=enable_autotls,
            resource_manager=resource_manager,
            psk=psk,
            max_connections_per_peer=6,
            enable_ping=True,
            enable_proof_of_stake=True,
            db=db,
            subnet_id=subnet_id,
            subnet_slot=subnet_slot,
            subnet_node_id=subnet_node_id,
            hypertensor=hypertensor,
            is_bootstrap=is_bootstrap,
            enable_subnet_info_tracker=True,
            enable_connection_maintenance=True,
            strict_maintain_connections=True,
            telemetry=telemetry,
            maintain_connections_log_level=logging.DEBUG,
        )
```

### Step 8: Run The Server

Server execution is asynchronous and uses Trio.

```python
import trio


server = MyServer(...)
trio.run(server.run)
```

Do not call `await server.run()` from synchronous CLI code. Use `trio.run`.

## P2PNetworkContext

`P2PNetworkContext` is the object passed to every `ApplicationBase` hook. It is a
snapshot of the running network services and lifecycle controls.

| Field | Type | Always present | Purpose |
| --- | --- | --- | --- |
| `host` | `IHost` | Yes | The libp2p host. Use it for peer ID, streams, peerstore, connections, and stream handlers. |
| `dht` | `KadDHT` | Yes | The KadDHT service used for peer discovery and routing table access. |
| `nursery` | `trio.Nursery` | Yes | Start app background tasks here. The template cancels it on shutdown. |
| `termination_event` | `trio.Event` | Yes | Shared shutdown signal. Long-running app loops should watch it. |
| `listen_addrs` | `tuple[Multiaddr, ...]` | Yes | Addresses the host was asked to listen on. |
| `optimal_addr` | `Multiaddr` or `None` | Usually | Best local binding address found by libp2p utilities. |
| `peer_multiaddr` | `str` or `None` | Usually | `optimal_addr` plus `/p2p/<peer_id>`, useful for logs and bootstrap output. |
| `subnet_info_tracker` | `SubnetInfoTracker` or `None` | No | Present when template options require on-chain subnet peer info. |
| `pubsub` | `Pubsub` or `None` | No | Present when `enable_pubsub=True` or custom Pubsub was provided. |
| `gossipsub` | `GossipSub` or `None` | No | Present when `enable_pubsub=True` or custom GossipSub was provided. |

`context.stop()` sets `termination_event`. Use it when app logic decides the
server should shut down.

## ApplicationBase Hook Reference

### `__init__(*args, **kwargs)`

The base class stores `args` and `kwargs`, but application subclasses usually
call `super().__init__()` and then store explicit typed attributes.

Keep this constructor focused on application dependencies. Examples:

- `db`
- `key_pair`
- `subnet_id`
- `subnet_node_id`
- `hypertensor`
- `telemetry`
- app-specific timing options
- app-specific topic names or schema IDs

Avoid putting host, DHT, Pubsub, or GossipSub into the constructor. Those are
runtime services and should come from `P2PNetworkContext`.

### `setup(context)`

Called once after local network services are ready.

At this point:

- Host is running.
- DHT service has started.
- Pubsub and GossipSub have started if enabled.
- The app has access to the shared nursery.
- Bootstrap peers have not been dialed yet.
- Connection maintenance has not started yet.
- Consensus has not been started yet.

Good work for `setup`:

- Validate required template services.
- Start telemetry.
- Register stream handlers.
- Instantiate direct protocol classes.
- Subscribe to topics if the subscription does not require connected peers.
- Initialize local in-memory application state.

Bad work for `setup`:

- Long startup sync that waits for peers.
- Infinite loops that block the hook.
- Work that requires bootstrap peers to already be connected.

### `start_application(context)`

Called after bootstrap and optional connection maintenance startup.

At this point:

- `setup` has finished.
- Bootstrap nodes have been dialed if configured.
- Known peerstore peers have been added to the DHT routing table.
- Connection maintenance has been started if enabled.
- Consensus has not yet been started by the template.

Good work for this hook:

- Peer-dependent startup sync.
- DAG sync setup.
- DAG gossip system startup.
- Publisher startup.
- App workers that assume initial peers may exist.

This hook may do bounded async work. For example, `server_dag.py` awaits initial
DAG sync before starting the peer-state publisher.

### `run(context)`

Called after:

- `setup`
- bootstrap
- connection maintenance startup
- `start_application`
- consensus startup

The default implementation waits forever until `context.termination_event` is
set.

Most applications do not need to override it. Start background tasks in
`setup` or `start_application`, then let the default `run`
hold the server open.

Override it if the application itself owns the main lifecycle. If your override
returns, the template starts shutdown.

### `cleanup(context)`

Called in a `finally` block from `_run_application` after `setup` has completed.
It runs when bootstrap, connection maintenance startup, app post-bootstrap
startup, consensus startup, or `run` raises or returns.

If `setup` itself raises, application cleanup is not called by `_run_application`
because the protected lifecycle block has not started yet. The outer
`ServerBase.run` cleanup still requests shutdown and cancels the nursery.

Before `cleanup` is called, the template calls `context.stop()`. The network
context is still available, but other tasks may already be reacting to
shutdown.

Use `cleanup` for short, best-effort final work. Handle exceptions inside
cleanup if failure should not hide the original shutdown reason.

## ServerBase Lifecycle

This is the high-level order inside `ServerBase.run()`:

1. Apply libp2p patches if `apply_libp2p_patches=True`.
2. Build listen addresses with `_build_listen_addrs()`.
3. Create the libp2p host with `create_host(...)`.
4. Create the shared `termination_event`.
5. Create `SubnetInfoTracker` if required.
6. Find the optimal binding address.
7. Build `peer_multiaddr` for logs and bootstrap instructions.
8. Start the host with `host.run(...)`.
9. Open the shared Trio nursery.
10. Start peerstore cleanup.
11. Register built-in stream handlers such as ping.
12. Create the DHT with `create_dht(...)`.
13. Start the DHT background service.
14. Optionally start random-walk discovery logging.
15. Optionally start Pubsub and GossipSub.
16. Build `P2PNetworkContext`.
17. Run the application lifecycle through `_run_application(context)`.
18. On exit, set the termination event, cancel the nursery, and log shutdown.

The application lifecycle inside `_run_application(context)` is:

1. `await application.setup(context)`
2. `await _connect_bootstrap_peers(context.host, context.dht)`
3. `_start_connection_maintenance(context)`
4. `await application.start_application(context)`
5. `_start_consensus(context)`
6. `await application.run(context)`
7. `context.stop()`
8. `await application.cleanup(context)`

If `setup` succeeds and any later application lifecycle step raises, the
application `finally` block calls `context.stop()` and `application.cleanup`.
The outer `ServerBase.run` `finally` block also requests shutdown and
cancels the nursery. If `setup` raises, only the outer template shutdown path is
guaranteed.

## ServerBase Function Reference

### `__init__(...)`

Stores all server configuration. It does not start networking. It is safe to
instantiate a server object without opening sockets.

Important groups of options:

Identity and listening:

- `key_pair`: libp2p identity keypair. Required.
- `port`: listen port. Required.
- `ip`: IP used when `listen_addrs` is not provided.
- `listen_addrs`: explicit multiaddrs. If provided, these win over `ip`.
- `use_available_interfaces`: when true, listen on all available interface
  addresses discovered by libp2p utilities.
- `listen_protocol`: usually `"tcp"`.

Bootstrap and discovery:

- `bootstrap_addrs`: remote `/ip4/.../tcp/.../p2p/...` addresses to dial.
- `dht_mode`: DHT mode. Defaults to `DHTMode.SERVER`.
- `enable_random_walk`: enables DHT random walk discovery.
- `dht_validator`: DHT record validator. Defaults to public-key validation.

Pubsub:

- `enable_pubsub`: create Pubsub and GossipSub services.
- `pubsub`: custom Pubsub instance. Providing one also enables Pubsub.
- `gossipsub`: custom GossipSub router. Providing one also enables Pubsub.

Host features:

- `enable_mDNS`: enable local network mDNS discovery.
- `enable_upnp`: enable UPnP.
- `enable_autotls`: enable AutoTLS.
- `resource_manager`: optional libp2p resource manager.
- `psk`: optional pre-shared key.
- `peerstore_db_path`: reserved for persistent peerstore support. Current code
  raises `NotImplementedError` if it is used.
- `peerstore_cleanup_interval`: seconds between peerstore cleanup runs.
- `max_connections_per_peer`: connection cap applied to the host swarm.

Built-in protocols:

- `enable_ping`: register the ping stream handler.
- `ping_protocol_id`: protocol ID for ping. Defaults to `/ipfs/ping/1.0.0`.

Subnet and chain features:

- `enable_proof_of_stake`: wrap secure transports with proof-of-stake checks.
- `db`: database used by consensus and application code.
- `subnet_id`: subnet ID.
- `subnet_slot`: subnet slot used by `SubnetInfoTracker`.
- `subnet_node_id`: current node ID in the subnet.
- `hypertensor`: chain client or local mock chain client.
- `is_bootstrap`: identifies bootstrap node behavior.
- `enable_subnet_info_tracker`: force creation of `SubnetInfoTracker`.
- `enable_consensus`: start consensus support.
- `enable_connection_maintenance`: start a background connection maintainer.
- `strict_maintain_connections`: if true, maintain peers against on-chain
  subnet info. If false, use basic peerstore-based maintenance.

Operational options:

- `telemetry`: telemetry passed into connection
  maintenance.
- `maintain_connections_log_level`: log level for connection maintenance.
- `apply_libp2p_patches`: apply local libp2p patches before startup.
- `log_level`: template lifecycle log level.

### `run()`

Starts the server and blocks until shutdown.

What it owns:

- Patches.
- Host lifecycle.
- DHT lifecycle.
- Pubsub lifecycle.
- Shared nursery.
- Application lifecycle.
- Final cancellation.

Call it with Trio:

```python
trio.run(server.run)
```

Do not call `run` twice on the same server instance. Create a new server object
for a new run.

### `create_host(listen_addrs)`

Creates the libp2p host with:

- the configured identity keypair
- listen addresses
- optional proof-of-stake secure transports
- optional mDNS, UPnP, AutoTLS, resource manager, and PSK
- optional max-connections-per-peer swarm setting

Override this only when you need custom host construction. Most applications
should use the default.

Current persistent peerstore behavior:

- If `peerstore_db_path is None`, the host uses libp2p defaults.
- If `peerstore_db_path` is set, the method raises `NotImplementedError`
  because persistent peerstore support is not wired in.

### `_secure_transports_by_protocol()`

Builds proof-of-stake-wrapped secure transports when
`enable_proof_of_stake=True`.

When proof of stake is disabled, it returns `None` and libp2p uses default
secure transports.

When proof of stake is enabled:

- `hypertensor` is required.
- A `ProofOfStake` object is created for `subnet_id`.
- Noise transport is wrapped with `POSTransport`.
- Secio transport is wrapped with `POSTransport`.
- The returned mapping is passed to `new_host(...)`.

Use this when the network should reject peers that do not satisfy on-chain
proof-of-stake checks.

### `_set_builtin_stream_handlers(host)`

Registers stream handlers owned by the template.

Currently:

- If `enable_ping=True`, registers `handle_ping` under `ping_protocol_id`.

Application-specific handlers should not be added here. Register them in
`ApplicationBase.setup`.

### `create_dht(host)`

Creates the KadDHT service.

Defaults:

- mode: `self.dht_mode`, usually `DHTMode.SERVER`
- random walk: `self.enable_random_walk`
- validator: `self.dht_validator`

Override this only when you need different DHT construction.

### `create_gossipsub()`

Creates the default GossipSub router used when Pubsub is enabled.

Current defaults include:

- protocol: `GOSSIPSUB_PROTOCOL_ID`
- degree: `7`
- degree low: `5`
- degree high: `10`
- TTL: `60`
- gossip window: `2`
- gossip history: `5`
- heartbeat initial delay: `0.5`
- heartbeat interval: `2`

Override this if the application needs different mesh or heartbeat settings.

### `create_pubsub(host, gossipsub)`

Creates the Pubsub service around the host and GossipSub router.

Most applications should not override it. If you need a custom Pubsub instance,
you can either override this method or pass `pubsub=` into the constructor.

### `_build_listen_addrs()`

Determines which addresses the host should listen on.

Order of precedence:

1. If `listen_addrs` is provided, convert each item to `Multiaddr` and use it.
2. Else if `use_available_interfaces=True`, ask libp2p utilities for all
   available interface addresses for `port` and `listen_protocol`.
3. Else use `/ip4/{ip}/tcp/{port}`.

Use explicit `listen_addrs` when you need full control. Use
`use_available_interfaces=True` for nodes that should bind to every available
interface.

### `_get_optimal_binding_address()`

Asks libp2p utilities for the best local address to show to other peers. If this
fails, it logs at debug level and returns `None`.

The result is used to build `context.peer_multiaddr`:

```text
<optimal_addr>/p2p/<local_peer_id>
```

This is useful for logs and bootstrap instructions. It is not required for the
host to run.

### `_connect_bootstrap_peers(host, dht)`

Connects to configured bootstrap peers.

Behavior:

1. If `bootstrap_addrs` is not empty, call `connect_to_bootstrap_nodes(...)`.
2. `connect_to_bootstrap_nodes` parses each `/p2p/` multiaddr, stores addresses
   in the peerstore, and dials the peer.
3. If no bootstrap connection succeeds, it raises an exception.
4. After dialing, every peer in the host peerstore is added to the DHT routing
   table.

Bootstrap addresses must include the peer ID:

```text
/ip4/127.0.0.1/tcp/38960/p2p/12D3KooW...
```

### `_start_optional_pubsub(host, stack)`

Starts Pubsub and GossipSub only when `enable_pubsub` is true.

Behavior:

1. If Pubsub is disabled, return `(None, None)`.
2. Use provided `self.gossipsub` or create one with `create_gossipsub()`.
3. Use provided `self.pubsub` or create one with `create_pubsub(...)`.
4. Start both as Trio background services through the async exit stack.
5. Wait until Pubsub is ready.
6. Return `(pubsub, gossipsub)`.

If your application requires Pubsub, validate it in `setup`:

```python
if context.pubsub is None or context.gossipsub is None:
    raise RuntimeError("This application requires Pubsub and GossipSub")
```

### `_run_application(context)`

Runs the application hook sequence.

Exact order:

1. `await application.setup(context)`
2. `await _connect_bootstrap_peers(context.host, context.dht)`
3. `_start_connection_maintenance(context)`
4. `await application.start_application(context)`
5. `_start_consensus(context)`
6. `await application.run(context)`

Final behavior:

1. `context.stop()`
2. `await application.cleanup(context)`

This method is why application code can stay small. The app does not have to
know how to start the host, DHT, Pubsub, bootstrap, or maintenance machinery.

### `_start_consensus(context)`

Starts consensus support when `enable_consensus=True`.

Current behavior:

- If consensus is disabled or this is a bootstrap node, it returns immediately.
- It requires `SubnetInfoTracker`.
- It requires `db`.
- It requires `hypertensor`.
- It creates `Consensus(...)` and stores it as `self.consensus`.
- It starts `self.consensus._main_loop` in the shared nursery.

### `_start_connection_maintenance(context)`

Starts connection maintenance when `enable_connection_maintenance=True`.

If maintenance is disabled, it returns immediately.

Strict mode:

- Requires `SubnetInfoTracker`.
- Starts `maintain_connections(...)` in the nursery.
- Uses on-chain peer IDs from the tracker.
- Disconnects peers that are no longer registered on-chain.
- Reconnects to compatible registered peers.
- Can maintain GossipSub peer relationships.

Basic mode:

- Starts `basic_maintain_connections(...)` in the nursery.
- Uses connected peers and peerstore addresses.
- Does not require on-chain subnet info.

Use strict mode for production subnet nodes that should follow on-chain peer
membership. Use basic mode for generic P2P apps or local experiments that do
not have chain-backed peer membership.

### `_create_subnet_info_tracker(termination_event)`

Creates `SubnetInfoTracker` when the template needs on-chain subnet information.

It creates the tracker if any of these are true:

- `enable_subnet_info_tracker=True`
- `enable_consensus=True`
- `enable_connection_maintenance=True` and `strict_maintain_connections=True`

If a tracker is required, `hypertensor` must be provided.

### `_requires_subnet_info_tracker()`

Returns whether the current options require a `SubnetInfoTracker`.

This is purely a configuration check used during startup.

### `_require_subnet_info_tracker()`

Returns `self.subnet_info_tracker` if it exists. Raises `RuntimeError` if not.

Methods use this when a feature cannot run without a tracker, such as strict
connection maintenance or consensus startup.

## How `server_dag.py` Uses The Template

`server_dag.py` is the best local example of how to build a real server.

### `SubnetApplication`

The application stores subnet-specific dependencies:

- `port`
- `key_pair`
- `db`
- `subnet_id`
- `subnet_node_id`
- `hypertensor`
- `is_bootstrap`
- telemetry
- DAG sync timing options
- log levels

In `setup` it:

- Requires Pubsub and GossipSub.
- Starts telemetry if provided.
- Logs the local peer multiaddr.

In `start_application` it:

- Requires Pubsub and GossipSub.
- Creates `MerkleDagSyncProtocol`.
- Wraps it with a request client and peer provider.
- Creates `DagGossipSystem` for the peer-state topic.
- Registers the DAG sync request handler.
- Starts the DAG gossip system in the nursery.
- Returns early for bootstrap nodes.
- Runs initial DAG startup sync for non-bootstrap nodes.
- Creates `PeerStateDagPublisher`.
- Starts the publisher in the nursery.

In `cleanup` it logs shutdown. This is the right place to add final status
publishing if needed.

### `ServerV2`

The server wrapper:

- Creates `SubnetApplication`.
- Calls `ServerBase.__init__`.
- Enables Pubsub.
- Enables random walk.
- Enables ping.
- Enables proof of stake by default.
- Enables subnet info tracking.
- Enables connection maintenance.
- Passes telemetry into connection maintenance.
- Stores subnet attributes for logs and inspection.

`ServerV2.run` only logs around `super().run()`.

## Where To Put Common Logic

| Need | Put it here | Why |
| --- | --- | --- |
| New direct stream protocol | `ApplicationBase.setup` | Host is running and can register handlers. |
| Protobuf request/response logic | `subnet/protocols/protocol_base.py` subclass | The protocol template owns stream framing and cleanup. |
| Pubsub topic validation | App setup or a Pubsub helper | Pubsub exists once `setup` is called. |
| DAG gossip system | `start_application` | Bootstrap and maintenance have started, so peers may be available. |
| Initial peer sync | `start_application` | This can wait for peers before publishers start. |
| Periodic background publisher | Start in `setup` or `start_application` with `context.nursery.start_soon` | Nursery tasks are canceled by the template. |
| Main foreground loop | Override `ApplicationBase.run` | Returning from `run` controls server shutdown. |
| Final state publish | `ApplicationBase.cleanup` | The network context is still available. |
| Host transport customization | Override `ServerBase.create_host` | Host construction is template-owned. |
| DHT configuration | Override `ServerBase.create_dht` | DHT construction is template-owned. |
| GossipSub mesh tuning | Override `ServerBase.create_gossipsub` | GossipSub defaults live there. |
| Server option defaults | Concrete `ServerBase` subclass | The wrapper should present a clean constructor. |

## Building Application Logic

Application logic should be written around small components that are created and
started from the hooks.

### Direct Request/Response Protocols

Use `ProtocolBase` or `ProtobufRequestResponseProtocolTemplate` from
`subnet/protocols/protocol_base.py` instead of manually handling stream frames.

Pattern:

1. Define protobuf messages.
2. Generate Python protobuf files.
3. Subclass `ProtobufRequestResponseProtocolTemplate`.
4. Implement `handle_request`.
5. Instantiate the protocol in `ApplicationBase.setup`.

Example:

```python
class MyApplication(ApplicationBase):
    async def setup(self, context: P2PNetworkContext) -> None:
        self.task_protocol = TaskProtocol(context.host)
```

The protocol class registers its stream handler when constructed, so the app
does not need to call `context.host.set_stream_handler(...)` itself.

### Pubsub And GossipSub

Enable Pubsub in the server wrapper:

```python
super().__init__(
    ...,
    enable_pubsub=True,
)
```

Validate it in the app:

```python
if context.pubsub is None or context.gossipsub is None:
    raise RuntimeError("This app requires Pubsub and GossipSub")
```

Start long-running Pubsub consumers with the nursery. Long loops should stop
when `context.termination_event` is set.

### DAG Gossip

DAG applications usually need both Pubsub and peer sync.

The standard pattern is:

1. Enable Pubsub in the server wrapper.
2. In `start_application`, create the DAG sync protocol.
3. Create `DagGossipSystem`.
4. Attach the sync request handler.
5. Start `dag_system.run` in the nursery.
6. For non-bootstrap nodes, run startup `sync_dag`.
7. Start publishers after sync.

This is exactly the shape used in `examples/server/server_dag.py`.

### Telemetry

Telemetry is an app concern, but it is usually started in `setup`:

```python
if self.telemetry:
    context.nursery.start_soon(self.telemetry.run)
```

Pass telemetry into template-owned systems when available:

```python
super().__init__(
    ...,
    telemetry=telemetry,
)
```

Pass telemetry into app-owned systems directly from your application object.

### Bootstrap Nodes

Use `is_bootstrap` to disable behavior that only validator/client nodes should
perform.

Common bootstrap behavior:

- Listen and serve discovery.
- Participate in DHT.
- Run Pubsub if needed for mesh health.
- Do not publish peer state.
- Do not run app-specific validator loops.
- Do not run startup sync that waits for other peers unless desired.

Example:

```python
if self.is_bootstrap:
    return

context.nursery.start_soon(self.publisher.run)
```

### Chain-Backed Subnet Nodes

For production subnet nodes, the server wrapper usually enables:

```python
enable_proof_of_stake=True
enable_subnet_info_tracker=True
enable_connection_maintenance=True
strict_maintain_connections=True
```

These options require `hypertensor`. Strict connection maintenance also needs
the current subnet fields:

- `subnet_id`
- `subnet_slot`
- `subnet_node_id`

The CLI should validate registration and key ownership before constructing the
server. `subnet/cli/run_node.py` and `subnet/cli/run_server_example.py` are
examples of that outer validation layer.

## Configuration Recipes

### Local P2P App Without Chain Requirements

Use this for early development when you only need host, DHT, and maybe Pubsub:

```python
super().__init__(
    port=port,
    application=application,
    key_pair=key_pair,
    bootstrap_addrs=bootstrap_addrs,
    enable_pubsub=True,
    enable_proof_of_stake=False,
    enable_subnet_info_tracker=False,
    enable_consensus=False,
    enable_connection_maintenance=True,
    strict_maintain_connections=False,
)
```

### Production-Like Subnet Node

Use this when the node should follow on-chain peer membership:

```python
super().__init__(
    port=port,
    application=application,
    key_pair=key_pair,
    bootstrap_addrs=bootstrap_addrs,
    enable_pubsub=True,
    enable_proof_of_stake=True,
    db=db,
    subnet_id=subnet_id,
    subnet_slot=subnet_slot,
    subnet_node_id=subnet_node_id,
    hypertensor=hypertensor,
    enable_subnet_info_tracker=True,
    enable_connection_maintenance=True,
    strict_maintain_connections=True,
)
```

### Bootstrap Node

Use the same server class, but pass `is_bootstrap=True`. The application should
branch on that flag and skip non-bootstrap behavior.

```python
server = Server(
    ...,
    is_bootstrap=True,
    bootstrap_addrs=None,
)
```

### Custom Listen Address

Use explicit listen addresses when the default address builder is not enough:

```python
super().__init__(
    ...,
    listen_addrs=("/ip4/0.0.0.0/tcp/38960",),
)
```

If `listen_addrs` is provided, `ip` and `use_available_interfaces` are ignored
by `_build_listen_addrs`.

## Shutdown And Error Handling

The template is designed around Trio structured concurrency.

Important rules:

- Start long-running app tasks with `context.nursery.start_soon`.
- Do not create untracked background tasks outside the nursery.
- Long-running loops should check `context.termination_event`.
- Call `context.stop()` when app logic wants to stop the server.
- If an application hook raises, the template shuts the server down.
- On shutdown, the template sets the termination event and cancels the nursery.
- After `setup` has completed, `cleanup` is called from `_run_application` after
  `context.stop()`.

Example loop:

```python
async def publish_loop(self, context: P2PNetworkContext) -> None:
    while not context.termination_event.is_set():
        await self.publish_once()
        await trio.sleep(20.0)
```

Start it with:

```python
context.nursery.start_soon(self.publish_loop, context)
```

## Testing A New Server

Test the layers separately.

### Application Hook Tests

Create a fake or lightweight `P2PNetworkContext` and call hooks directly.

Test:

- `setup` validates required services.
- protocols are created.
- background tasks are started when expected.
- bootstrap nodes skip publisher startup.
- non-bootstrap nodes start publishers.
- cleanup handles missing optional resources.

### Server Constructor Tests

Instantiate your concrete server and assert template options:

- `server.enable_pubsub`
- `server.enable_proof_of_stake`
- `server.enable_connection_maintenance`
- `server.strict_maintain_connections`
- `server.subnet_id`
- `server.application`

### Local Runtime Smoke Test

For local testing without live chain requirements, disable proof of stake and
strict chain-backed maintenance, or use `LocalMockHypertensor` if your app
requires a `hypertensor` object.

Use one bootstrap node and one joining node:

```text
python -m subnet.cli.run_server_example --private_key_path bootnode.key --port 38960 --subnet_id 1 --no_blockchain_rpc --is_bootstrap

python -m subnet.cli.run_server_example --private_key_path alith.key --port 38961 --bootstrap /ip4/127.0.0.1/tcp/38960/p2p/<BOOTNODE_PEER_ID> --subnet_id 1 --subnet_node_id 1 --no_blockchain_rpc
```

## Common Mistakes

### Blocking Inside `setup`

`setup` should register local behavior and return. If it waits forever,
bootstrap, connection maintenance, consensus startup, and `run` never
happen.

### Forgetting To Enable Pubsub

If the application expects `context.pubsub`, the server wrapper must pass
`enable_pubsub=True` or provide custom `pubsub` and `gossipsub` objects.

### Starting Background Tasks Outside The Nursery

Tasks started outside `context.nursery` may outlive the server or fail without
being tied to shutdown. Use the context nursery.

### Running Peer-Dependent Sync Too Early

Do not do startup sync in `setup` if it needs connected peers. Use
`start_application`.

### Enabling Proof Of Stake Without Hypertensor

`enable_proof_of_stake=True` requires `hypertensor`. Startup raises `ValueError`
without it.

### Enabling Strict Maintenance Without Hypertensor

Strict connection maintenance requires `SubnetInfoTracker`, which requires
`hypertensor`.

### Passing `peerstore_db_path`

Persistent peerstore support is not implemented. Passing `peerstore_db_path`
currently raises `NotImplementedError`.

### Expecting Bootstrap Nodes To Run Consensus

The current `_start_consensus` method returns early for bootstrap nodes. A node
must have `enable_consensus=True` and `is_bootstrap=False` before the template
creates `Consensus` and starts its main loop.

## Developer Checklist

Use this checklist when adding a new server:

1. Create a `ApplicationBase` subclass.
2. Store only app-specific dependencies in the application constructor.
3. Validate required context services in `setup`.
4. Register direct protocols in `setup`.
5. Start local-only background tasks in `setup`.
6. Put peer-dependent startup work in `start_application`.
7. Start long-running app workers with `context.nursery.start_soon`.
8. Override `run` only if the app owns the foreground lifecycle.
9. Keep `cleanup` short and best-effort.
10. Create a concrete `ServerBase` subclass.
11. Pass the correct template flags for Pubsub, proof of stake, tracker,
    consensus, and connection maintenance.
12. Run with `trio.run(server.run)`.
13. Smoke test bootstrap and joining node behavior.
14. Test bootstrap nodes skip publishing if that is intended.
15. Test non-bootstrap nodes start sync and publishers.

## Quick Reference

Minimal application:

```python
class App(ApplicationBase):
    async def setup(self, context: P2PNetworkContext) -> None:
        pass
```

Minimal server:

```python
class Server(ServerBase):
    def __init__(self, *, port: int, key_pair: KeyPair) -> None:
        super().__init__(
            port=port,
            key_pair=key_pair,
            application=App(),
        )
```

Pubsub app:

```python
super().__init__(
    ...,
    enable_pubsub=True,
)
```

Strict subnet node:

```python
super().__init__(
    ...,
    db=db,
    hypertensor=hypertensor,
    subnet_id=subnet_id,
    subnet_slot=subnet_slot,
    subnet_node_id=subnet_node_id,
    enable_proof_of_stake=True,
    enable_subnet_info_tracker=True,
    enable_connection_maintenance=True,
    strict_maintain_connections=True,
)
```

Run:

```python
trio.run(server.run)
```
