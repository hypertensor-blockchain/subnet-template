from collections.abc import Mapping
import logging
from typing import TYPE_CHECKING, List, cast

from libp2p import (
    new_host,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from libp2p.custom_types import ISecureTransport, TProtocol
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p.network.swarm import Swarm
from libp2p.peer.persistent import create_async_peerstore, create_sync_peerstore, create_sync_rocksdb_peerstore
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.records.validator import NamespacedValidator
from libp2p.security.noise.transport import (
    PROTOCOL_ID as NOISE_PROTOCOL_ID,
    Transport as NoiseTransport,
)
import libp2p.security.secio.transport as secio
from libp2p.security.secio.transport import Transport as SecioTransport
from libp2p.tools.async_service import background_trio_service
import trio

from subnet.config import GOSSIPSUB_PROTOCOL_ID
from subnet.consensus.consensus import Consensus
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.utils.connection import (
    demonstrate_random_walk_discovery,
    maintain_connections,
)
from subnet.utils.connections.bootstrap import connect_to_bootstrap_nodes
from subnet.utils.db.database import RocksDB
from subnet.utils.gossipsub.gossip_receiver import GossipReceiver
from subnet.utils.hypertensor.subnet_info_tracker_v3 import SubnetInfoTracker
from subnet.utils.patches import apply_all_patches
from subnet.utils.pos.pos_transport import (
    PROTOCOL_ID as POS_PROTOCOL_ID,
    POSTransport,
)
from subnet.utils.pos.proof_of_stake import ProofOfStake
from subnet.utils.protocols.ping import handle_ping
from subnet.utils.pubsub.custom_score_params import custom_score_params
from subnet.utils.pubsub.heartbeat import (
    HEARTBEAT_TOPIC,
    publish_loop,
)
from subnet.utils.pubsub.pubsub_validation import (
    SyncHeartbeatMsgValidator,
    SyncPubsubTopicValidator,
)

if TYPE_CHECKING:
    from libp2p.network.swarm import Swarm

# Apply patches for stability
apply_all_patches()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")

PING_PROTOCOL_ID = TProtocol("/ipfs/ping/1.0.0")


class Server:
    def __init__(
        self,
        *,
        port: int,
        peerstore_db_path: str | None = None,
        bootstrap_addrs: List[str] | None = None,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int = 0,
        subnet_slot: int = 3,
        subnet_node_id: int = 0,
        hypertensor: Hypertensor | LocalMockHypertensor,
        is_bootstrap: bool = False,
        **kwargs,
    ):
        logger.info(f"Server starting subnet_id={subnet_id}")
        self.port = port
        self.bootstrap_addrs = bootstrap_addrs
        self.key_pair = key_pair
        self.subnet_id = subnet_id
        self.subnet_slot = subnet_slot
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.db = db
        self.is_bootstrap = is_bootstrap
        self.peerstore_db_path = peerstore_db_path

    async def run(self):
        logger.info(f"Server running subnet_id={self.subnet_id}")
        from libp2p.utils.address_validation import (
            get_available_interfaces,
            get_optimal_binding_address,
        )

        listen_addrs = get_available_interfaces(self.port)

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
            transport=SecioTransport(
                self.key_pair,
            ),
            pos=proof_of_stake,
            log_level=logging.INFO if self.is_bootstrap else logging.DEBUG,
        )

        secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
            POS_PROTOCOL_ID: pos_noise_transport,
            TProtocol(secio.ID): pos_secio_transport,
        }

        if self.peerstore_db_path is not None:
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

        # Create a new libp2p host
        # host = new_host(key_pair=self.key_pair)
        host = new_host(key_pair=self.key_pair, sec_opt=secure_transports_by_protocol, peerstore_opt=peerstore)

        # Increase connection limits to prevent aggressive pruning (EOF/0-byte reads)
        # This is done manually because new_host() only exposes this via QUIC config.
        # We cast to Swarm so the IDE/type checker recognizes the connection_config.
        cast("Swarm", host.get_network()).connection_config.max_connections_per_peer = 100

        # Log available protocols
        logger.info(f"Host ID: {host.get_id()}")
        logger.info(
            f"Host multiselect protocols: {host.get_mux().get_protocols() if hasattr(host, 'get_mux') else 'N/A'}"
        )

        termination_event = trio.Event()  # Event to signal termination
        async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
            logger.info(f"Listening address: {listen_addrs}")
            # Start the peer-store cleanup task, TTL
            nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)

            # Set stream handler for ping protocol (used by overwatch nodes)
            host.set_stream_handler(PING_PROTOCOL_ID, handle_ping)

            dht = KadDHT(
                host,
                DHTMode.SERVER,
                enable_random_walk=True,
                validator=NamespacedValidator({"pk": PublicKeyValidator()}),
            )

            gossipsub = GossipSub(
                protocols=[GOSSIPSUB_PROTOCOL_ID],
                degree=3,  # Number of peers to maintain in mesh
                degree_low=2,  # Lower bound for mesh peers
                degree_high=4,  # Upper bound for mesh peers
                direct_peers=None,  # Direct peers
                time_to_live=60,  # TTL for message cache in seconds
                gossip_window=2,  # Smaller window for faster gossip
                gossip_history=5,  # Keep more history
                heartbeat_initial_delay=2.0,  # Start heartbeats sooner
                heartbeat_interval=5,  # More frequent heartbeats for testing
                # score_params=custom_score_params(),
            )
            pubsub = Pubsub(host, gossipsub)

            # Start the background services
            async with background_trio_service(dht):
                subnet_info_tracker = SubnetInfoTracker(
                    termination_event,
                    self.subnet_id,
                    self.subnet_slot,
                    self.hypertensor,
                )

                # Display the random walk
                nursery.start_soon(demonstrate_random_walk_discovery, dht, 30)

                async with background_trio_service(pubsub):
                    async with background_trio_service(gossipsub):
                        logger.info("Pubsub and GossipSub services started.")
                        await pubsub.wait_until_ready()
                        logger.info("Pubsub ready.")

                        pubsub.set_topic_validator(
                            HEARTBEAT_TOPIC,
                            SyncPubsubTopicValidator.from_predicate_class(
                                SyncHeartbeatMsgValidator,
                                host.get_id(),
                                subnet_info_tracker,
                                self.hypertensor,
                                self.subnet_id,
                                proof_of_stake,
                            ).validate,
                            is_async_validator=False,
                        )

                        # Connect to bootstrap nodes AFTER starting services
                        # This avoids AttributeError on incoming streams
                        if self.bootstrap_addrs is not None:
                            await connect_to_bootstrap_nodes(host, self.bootstrap_addrs)

                        optimal_addr = get_optimal_binding_address(self.port)
                        optimal_addr_with_peer = f"{optimal_addr}/p2p/{host.get_id().to_string()}"
                        logger.info(f"\nRunning peer on {optimal_addr_with_peer}\n")

                        for peer_id in host.get_peerstore().peer_ids():
                            await dht.routing_table.add_peer(peer_id)

                        # Start gossip receiver
                        gossip_receiver = GossipReceiver(
                            gossipsub=gossipsub,
                            pubsub=pubsub,
                            termination_event=termination_event,
                            db=self.db,
                            topics=[HEARTBEAT_TOPIC],
                            subnet_info_tracker=subnet_info_tracker,
                            hypertensor=self.hypertensor,
                        )
                        nursery.start_soon(gossip_receiver.run)

                        # Keep nodes connected to each other
                        # NOTE: Start this after host, gossipsub, and pubsub are initialized
                        nursery.start_soon(
                            maintain_connections,
                            host,
                            subnet_info_tracker,
                            gossipsub,
                            pubsub,
                            dht,
                        )

                        if not self.is_bootstrap:
                            # Start heartbeat publisher
                            nursery.start_soon(
                                publish_loop,
                                pubsub,
                                HEARTBEAT_TOPIC,
                                termination_event,
                                self.subnet_id,
                                self.subnet_node_id,
                                subnet_info_tracker,
                                self.hypertensor,
                            )

                            # Start consensus
                            consensus = Consensus(
                                db=self.db,
                                subnet_id=self.subnet_id,
                                subnet_node_id=self.subnet_node_id,
                                subnet_info_tracker=subnet_info_tracker,
                                hypertensor=self.hypertensor,
                                skip_activate_subnet=False,
                                start=True,
                            )
                            nursery.start_soon(consensus._main_loop)

                        await termination_event.wait()

            nursery.cancel_scope.cancel()

        print("Application shutdown complete")
