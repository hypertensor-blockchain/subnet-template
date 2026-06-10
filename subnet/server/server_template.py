from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from functools import partial
import logging
from typing import Any, cast

from libp2p import new_host
from libp2p.abc import IHost
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.custom_types import ISecureTransport, TProtocol
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT
from libp2p.network.swarm import Swarm
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.rcmgr.manager import ResourceManager
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.records.validator import NamespacedValidator
from libp2p.security.noise.transport import (
    Transport as NoiseTransport,
)
import libp2p.security.secio.transport as secio
from libp2p.security.secio.transport import Transport as SecioTransport
from libp2p.tools.async_service import background_trio_service
from libp2p.utils.address_validation import get_available_interfaces, get_optimal_binding_address
from multiaddr import Multiaddr
import trio

from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.consensus.consensus_v2 import Consensus
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.connections.bootstrap import connect_to_bootstrap_nodes
from subnet.utils.connections.connection import (
    basic_maintain_connections,
    demonstrate_random_walk_discovery,
    maintain_connections,
)
from subnet.utils.hypertensor.subnet_info_tracker_v5 import SubnetInfoTracker
from subnet.utils.patches import apply_all_patches
from subnet.utils.pos.pos_transport import (
    PROTOCOL_ID as POS_PROTOCOL_ID,
    POSTransport,
)
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.protocols.ping import handle_ping

logger = logging.getLogger(__name__)

PING_PROTOCOL_ID = TProtocol("/ipfs/ping/1.0.0")
DHTProvideKey = str | bytes


@dataclass(frozen=True, slots=True)
class P2PNetworkContext:
    """
    Runtime objects app logic can use after the peer network has started.

    ``host`` and ``dht`` are always available. ``pubsub`` and ``gossipsub`` are
    only set when ``ServerBase(enable_pubsub=True)`` is used.
    """

    host: IHost
    dht: KadDHT
    nursery: trio.Nursery
    termination_event: trio.Event
    listen_addrs: tuple[Multiaddr, ...]
    optimal_addr: Multiaddr | None = None
    peer_multiaddr: str | None = None
    subnet_info_tracker: SubnetInfoTracker | None = None
    pubsub: Pubsub | None = None
    gossipsub: GossipSub | None = None

    def stop(self) -> None:
        """Ask the server template to stop all app and network tasks."""
        self.termination_event.set()


class ApplicationBase:
    """
    Base class for application logic that runs on top of the P2P template.

    Subclasses usually override ``setup`` to register stream handlers, subscribe
    to gossip topics, or start background tasks with ``context.nursery``. Override
    ``run`` only when the application needs to own the main blocking loop.

    Example:

        class MyApp(ApplicationBase):
            def __init__(self, greeting: str) -> None:
                super().__init__(greeting)
                self.greeting = greeting

            async def setup(self, context: P2PNetworkContext) -> None:
                context.host.set_stream_handler(PROTOCOL_ID, self.handle_stream)
                context.nursery.start_soon(self.worker, context)

    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.args = args
        self.kwargs = kwargs

    async def setup(self, context: P2PNetworkContext) -> None:
        """Called once after host, DHT, and optional pubsub are ready."""

    async def start_application(self, context: P2PNetworkContext) -> None:
        """Called after bootstrap and optional connection maintenance startup."""

    async def run(self, context: P2PNetworkContext) -> None:
        """Block until the app or server should stop."""
        await context.termination_event.wait()

    async def cleanup(self, context: P2PNetworkContext) -> None:
        """Called once while the network context is still available."""


class ServerBase:
    """
    Minimal P2P server template for application-specific peers.

    The template always starts a libp2p host and KadDHT in server mode so peers
    can listen, dial bootstrap nodes, and participate in peer discovery. Pubsub
    and GossipSub are optional and are only created when ``enable_pubsub=True``.

    Application code belongs in a ``ApplicationBase`` subclass so builders do not
    need to reimplement host, DHT, bootstrap, or service lifecycle wiring.
    """

    def __init__(
        self,
        *,
        port: int,
        application: ApplicationBase | None = None,
        key_pair: KeyPair,
        ip: str = "0.0.0.0",
        listen_addrs: Sequence[str | Multiaddr] | None = None,
        use_available_interfaces: bool = False,
        listen_protocol: str = "tcp",
        bootstrap_addrs: Sequence[str] | None = None,
        enable_pubsub: bool = False,
        pubsub: Pubsub | None = None,
        gossipsub: GossipSub | None = None,
        dht_mode: DHTMode = DHTMode.SERVER,
        enable_random_walk: bool = True,
        dht_validator: NamespacedValidator | None = None,
        dht_provide_keys: DHTProvideKey | Sequence[DHTProvideKey] | None = None,
        enable_mDNS: bool = False,
        enable_upnp: bool = False,
        enable_autotls: bool = False,
        resource_manager: ResourceManager | None = None,
        psk: str | None = None,
        peerstore_db_path: str | None = None,
        peerstore_cleanup_interval: int = 60,
        max_connections_per_peer: int | None = 6,
        ping_protocol_id: TProtocol = PING_PROTOCOL_ID,
        enable_proof_of_stake: bool = False,
        db: Any | None = None,
        subnet_id: int = 0,
        subnet_slot: int = 3,
        subnet_node_id: int = 0,
        hypertensor: Any | None = None,
        is_bootstrap: bool = False,
        enable_subnet_info_tracker: bool = False,
        enable_consensus: bool = False,
        log_random_walk: bool = False,
        random_walk_log_interval: int = 30,
        enable_connection_maintenance: bool = False,
        strict_maintain_connections: bool = True,
        telemetry: Telemetry | None = None,
        maintain_connections_log_level: int = logging.DEBUG,
        apply_libp2p_patches: bool = True,
        log_level: int = logging.INFO,
    ) -> None:
        self.port = port
        self.application = application or ApplicationBase()
        self.key_pair = key_pair
        self.ip = ip
        self.listen_addrs = tuple(listen_addrs) if listen_addrs is not None else None
        self.use_available_interfaces = use_available_interfaces
        self.listen_protocol = listen_protocol
        self.bootstrap_addrs = tuple(bootstrap_addrs or ())
        self.enable_pubsub = enable_pubsub or pubsub is not None or gossipsub is not None
        self.pubsub = pubsub
        self.gossipsub = gossipsub
        self.dht_mode = dht_mode
        self.enable_random_walk = enable_random_walk
        self.dht_validator = dht_validator or NamespacedValidator({"pk": PublicKeyValidator()})
        self.dht_provide_keys = self._normalize_dht_provide_keys(dht_provide_keys)
        self.enable_mDNS = enable_mDNS
        self.enable_upnp = enable_upnp
        self.enable_autotls = enable_autotls
        self.resource_manager = resource_manager
        self.psk = psk
        self.peerstore_db_path = peerstore_db_path
        self.peerstore_cleanup_interval = peerstore_cleanup_interval
        self.max_connections_per_peer = max_connections_per_peer
        self.ping_protocol_id = ping_protocol_id
        self.enable_proof_of_stake = enable_proof_of_stake
        self.db = db
        self.subnet_id = subnet_id
        self.subnet_slot = subnet_slot
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.is_bootstrap = is_bootstrap
        self.enable_subnet_info_tracker = enable_subnet_info_tracker
        self.subnet_info_tracker: SubnetInfoTracker | None = None
        self.enable_consensus = enable_consensus
        self.consensus: Consensus | None = None
        self.log_random_walk = log_random_walk
        self.random_walk_log_interval = random_walk_log_interval
        self.enable_connection_maintenance = enable_connection_maintenance
        self.strict_maintain_connections = strict_maintain_connections
        self.telemetry = telemetry
        self.maintain_connections_log_level = maintain_connections_log_level
        self.apply_libp2p_patches = apply_libp2p_patches
        self.log_level = log_level

    async def run(self) -> None:
        """Start the P2P services and run the configured application."""
        if self.apply_libp2p_patches:
            apply_all_patches()

        listen_addrs = self._build_listen_addrs()
        host = self.create_host(listen_addrs)
        termination_event = trio.Event()
        self.subnet_info_tracker = self._create_subnet_info_tracker(termination_event)
        optimal_addr = self._get_optimal_binding_address()
        peer_multiaddr = f"{optimal_addr}/p2p/{host.get_id().to_string()}" if optimal_addr is not None else None

        logger.log(self.log_level, "Starting P2P host on %s", [str(addr) for addr in listen_addrs])
        async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
            try:
                nursery.start_soon(host.get_peerstore().start_cleanup_task, self.peerstore_cleanup_interval)
                self._set_builtin_stream_handlers(host)

                dht = self.create_dht(host)
                async with background_trio_service(dht):
                    if self.log_random_walk:
                        nursery.start_soon(
                            demonstrate_random_walk_discovery,
                            dht,
                            self.random_walk_log_interval,
                        )

                    async with AsyncExitStack() as stack:
                        pubsub, gossipsub = await self._start_optional_pubsub(host, stack)
                        context = P2PNetworkContext(
                            host=host,
                            dht=dht,
                            nursery=nursery,
                            termination_event=termination_event,
                            listen_addrs=listen_addrs,
                            optimal_addr=optimal_addr,
                            peer_multiaddr=peer_multiaddr,
                            subnet_info_tracker=self.subnet_info_tracker,
                            pubsub=pubsub,
                            gossipsub=gossipsub,
                        )
                        await self._run_application(context)
            finally:
                termination_event.set()
                nursery.cancel_scope.cancel()
                logger.log(self.log_level, "P2P server template stopped")

    def create_host(self, listen_addrs: Sequence[Multiaddr]) -> IHost:
        """Create the libp2p host. Override for custom transports or security."""
        if self.peerstore_db_path is not None:
            #
            # *libp2p doesn't have persistent peerstore integrated yet*
            #
            # peerstore = create_async_peerstore(
            #     db_path=self.peerstore_db_path,
            #     backend="leveldb",
            # )
            # peerstore = create_sync_peerstore(
            #     db_path=self.peerstore_db_path,
            #     backend="leveldb",
            # )
            raise NotImplementedError("Persistent peerstore not implemented.")
        else:
            peerstore = None

        host = new_host(
            key_pair=self.key_pair,
            listen_addrs=listen_addrs,
            sec_opt=self._secure_transports_by_protocol(),
            peerstore_opt=peerstore,
            enable_upnp=self.enable_upnp,
            enable_mDNS=self.enable_mDNS,
            enable_autotls=self.enable_autotls,
            # enable_quic=True,
            # quic_transport_opt=QUICTransportConfig(),
            resource_manager=self.resource_manager,
            psk=self.psk,
        )
        if self.max_connections_per_peer is not None:
            cast(Swarm, host.get_network()).connection_config.max_connections_per_peer = self.max_connections_per_peer
        logger.info("Host ID: %s", host.get_id())
        return host

    def _secure_transports_by_protocol(self) -> Mapping[TProtocol, ISecureTransport] | None:
        if not self.enable_proof_of_stake:
            return None
        if self.hypertensor is None:
            raise ValueError("enable_proof_of_stake=True requires hypertensor")

        proof_of_stake = ProofOfStake(
            subnet_id=self.subnet_id,
            hypertensor=self.hypertensor,
            min_class=0,
        )

        pos_noise_transport = POSTransport(
            transport=NoiseTransport(
                self.key_pair,
                noise_privkey=create_new_x25519_key_pair().private_key,
            ),
            pos=proof_of_stake,
            log_level=logging.INFO if self.is_bootstrap else logging.DEBUG,
        )
        pos_secio_transport = POSTransport(
            transport=SecioTransport(self.key_pair),
            pos=proof_of_stake,
            log_level=logging.INFO if self.is_bootstrap else logging.DEBUG,
        )

        return {
            POS_PROTOCOL_ID: pos_noise_transport,
            TProtocol(secio.ID): pos_secio_transport,
        }

    def create_dht(self, host: IHost) -> KadDHT:
        """Create the DHT required for peer discovery."""
        return KadDHT(
            host,
            self.dht_mode,
            enable_random_walk=self.enable_random_walk,
            validator=self.dht_validator,
        )

    def create_gossipsub(self) -> GossipSub:
        """Create the optional GossipSub router used when pubsub is enabled."""
        return GossipSub(
            protocols=[GOSSIPSUB_PROTOCOL_ID],
            degree=7,
            degree_low=5,
            degree_high=10,
            direct_peers=None,
            time_to_live=60,
            gossip_window=2,
            gossip_history=5,
            heartbeat_initial_delay=0.5,
            heartbeat_interval=2,
        )

    def create_pubsub(self, host: IHost, gossipsub: GossipSub) -> Pubsub:
        """Create the optional Pubsub service used when pubsub is enabled."""
        return Pubsub(host=host, router=gossipsub)

    def _build_listen_addrs(self) -> tuple[Multiaddr, ...]:
        if self.listen_addrs is not None:
            return tuple(addr if isinstance(addr, Multiaddr) else Multiaddr(addr) for addr in self.listen_addrs)
        if self.use_available_interfaces:
            return tuple(get_available_interfaces(self.port, protocol=self.listen_protocol))
        return (Multiaddr(f"/ip4/{self.ip}/tcp/{self.port}"),)

    def _get_optimal_binding_address(self) -> Multiaddr | None:
        try:
            return get_optimal_binding_address(self.port, protocol=self.listen_protocol)
        except Exception:
            logger.debug("Failed to get optimal binding address", exc_info=True)
            return None

    async def _connect_bootstrap_peers(self, host: IHost, dht: KadDHT) -> None:
        if self.bootstrap_addrs:
            logger.info("Connecting to bootstrap nodes")
            await connect_to_bootstrap_nodes(host, list(self.bootstrap_addrs))
            logger.info("Connecting to bootstrap nodes complete")

        logger.info("Adding peers to DHT routing table")
        for peer_id in host.get_peerstore().peer_ids():
            await dht.routing_table.add_peer(peer_id)
        logger.info("Adding peers to DHT routing table complete")

    @staticmethod
    def _normalize_dht_provide_keys(
        dht_provide_keys: DHTProvideKey | Sequence[DHTProvideKey] | None,
    ) -> tuple[DHTProvideKey, ...]:
        if dht_provide_keys is None:
            return ()
        if isinstance(dht_provide_keys, (str, bytes)):
            return (dht_provide_keys,)

        normalized = tuple(dht_provide_keys)
        for key in normalized:
            if not isinstance(key, (str, bytes)):
                raise TypeError("dht_provide_keys must be a str, bytes, or sequence of str/bytes")
        return normalized

    async def _provide_dht_keys(self, dht: KadDHT) -> None:
        if not self.dht_provide_keys:
            return

        logger.info("Advertising %d DHT provider key(s)", len(self.dht_provide_keys))
        for key in self.dht_provide_keys:
            provided = await self._provide_dht_key(dht, key)
            if not provided:
                logger.warning(
                    "DHT provider advertisement did not reach any peers for key %s",
                    self._format_dht_key(key),
                )
        logger.info("Advertising DHT provider key(s) complete")

    async def _provide_dht_key(self, dht: KadDHT, key: DHTProvideKey) -> bool:
        if isinstance(key, str):
            return await dht.provide(key)

        try:
            return await cast(Any, dht).provide(key)
        except AttributeError as exc:
            if getattr(exc, "name", None) != "encode":
                raise

        provider_store = getattr(dht, "provider_store", None)
        provider_provide = getattr(provider_store, "provide", None)
        if provider_provide is None:
            raise AttributeError("DHT provider_store does not expose provide")
        return await provider_provide(key)

    @staticmethod
    def _format_dht_key(key: DHTProvideKey) -> str:
        if isinstance(key, bytes):
            return key.hex()
        return key

    async def _start_optional_pubsub(
        self,
        host: IHost,
        stack: AsyncExitStack,
    ) -> tuple[Pubsub | None, GossipSub | None]:
        if not self.enable_pubsub:
            return None, None

        gossipsub = self.gossipsub or self.create_gossipsub()
        pubsub = self.pubsub or self.create_pubsub(host, gossipsub)

        await stack.enter_async_context(background_trio_service(pubsub))
        await stack.enter_async_context(background_trio_service(gossipsub))
        await pubsub.wait_until_ready()

        logger.log(self.log_level, "Optional Pubsub and GossipSub services started")
        return pubsub, gossipsub

    async def _run_application(self, context: P2PNetworkContext) -> None:
        logger.info("Setting up application logic")
        await self.application.setup(context)
        logger.info("Setting up application logic complete")
        try:
            await self._connect_bootstrap_peers(context.host, context.dht)
            await self._provide_dht_keys(context.dht)
            self._start_connection_maintenance(context)
            logger.info("Starting application logic")
            await self.application.start_application(context)
            logger.info("Starting application logic complete")
            self._start_consensus(context)
            await self.application.run(context)
        finally:
            context.stop()
            await self.application.cleanup(context)

    def _start_consensus(self, context: P2PNetworkContext) -> None:
        if not self.enable_consensus or self.is_bootstrap:
            logger.info("Skipping starting consensus")
            return

        subnet_info_tracker = self._require_subnet_info_tracker()
        if self.db is None:
            raise RuntimeError("enable_consensus requires db")
        if self.hypertensor is None:
            raise RuntimeError("enable_consensus requires hypertensor")

        logger.info("Starting consensus")
        self.consensus = Consensus(
            db=self.db,
            subnet_id=self.subnet_id,
            subnet_node_id=self.subnet_node_id,
            subnet_info_tracker=subnet_info_tracker,
            hypertensor=self.hypertensor,
        )
        context.nursery.start_soon(self.consensus._main_loop)
        logger.info("Starting consensus complete")

    def _start_connection_maintenance(self, context: P2PNetworkContext) -> None:
        if not self.enable_connection_maintenance:
            return

        if self.strict_maintain_connections:
            logger.info("Starting strict connection maintenance")
            subnet_info_tracker = self._require_subnet_info_tracker()
            context.nursery.start_soon(
                partial(
                    maintain_connections,
                    context.host,
                    subnet_info_tracker,
                    gossipsub=context.gossipsub,
                    pubsub=context.pubsub,
                    dht=context.dht,
                    telemetry=self.telemetry,
                    log_level=self.maintain_connections_log_level,
                )
            )
            logger.info("Starting strict connection maintenance complete")
            return

        logger.info("Starting basic connection maintenance")
        context.nursery.start_soon(
            partial(
                basic_maintain_connections,
                context.host,
                telemetry=self.telemetry,
                log_level=self.maintain_connections_log_level,
                gossipsub=context.gossipsub,
                pubsub=context.pubsub,
                dht=context.dht,
            )
        )
        logger.info("Starting basic connection maintenance complete")

    def _create_subnet_info_tracker(self, termination_event: trio.Event) -> SubnetInfoTracker | None:
        if not self._requires_subnet_info_tracker():
            return None
        if self.hypertensor is None:
            raise RuntimeError("SubnetInfoTracker requires hypertensor")

        return SubnetInfoTracker(
            termination_event,
            self.subnet_id,
            self.subnet_slot,
            self.hypertensor,
        )

    def _requires_subnet_info_tracker(self) -> bool:
        return (
            self.enable_subnet_info_tracker
            or self.enable_consensus
            or (self.enable_connection_maintenance and self.strict_maintain_connections)
        )

    def _require_subnet_info_tracker(self) -> SubnetInfoTracker:
        if self.subnet_info_tracker is not None:
            return self.subnet_info_tracker
        raise RuntimeError("SubnetInfoTracker is required but was not created")


__all__ = ["ApplicationBase", "P2PNetworkContext", "ServerBase"]
