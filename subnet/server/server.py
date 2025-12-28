from typing import List

from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
import trio
from libp2p import (
    new_host,
)
from collections.abc import Mapping
from libp2p.custom_types import TProtocol
from libp2p.abc import ISecureTransport
from libp2p.security.noise.transport import Transport as NoiseTransport
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from subnet.network.pos.pos_transport import (
    POSTransport,
    PROTOCOL_ID as POS_PROTOCOL_ID,
)
from subnet.network.pos.proof_of_stake import ProofOfStake
from libp2p.security.noise.transport import (
    PROTOCOL_ID as NOISE_PROTOCOL_ID,
    Transport as NoiseTransport,
)
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.tools.async_service import background_trio_service
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.stream_muxer.mplex.mplex import MPLEX_PROTOCOL_ID, Mplex
from libp2p.crypto.keys import KeyPair
from libp2p.records.validator import NamespacedValidator
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.consensus.consensus import Consensus
from subnet.utils.bootstrap import connect_to_bootstrap_nodes
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.db.database import RocksDB
from subnet.utils.heartbeat import (
    HEARTBEAT_TOPIC,
    publish_loop,
)
from subnet.utils.gossip_receiver import GossipReceiver
from subnet.utils.subnet_info_tracker import SubnetInfoTracker
import logging
from subnet.utils.custom_score_params import custom_score_params
from subnet.utils.random_walk import maintain_connections

GOSSIPSUB_PROTOCOL_ID = TProtocol("/meshsub/1.0.0")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")


class Server:
    def __init__(
        self,
        *,
        port: int,
        bootstrap_addrs: List[str] | None = None,
        key_pair: KeyPair,
        db: RocksDB,
        subnet_id: int = 0,
        subnet_node_id: int = 0,
        hypertensor: Hypertensor | LocalMockHypertensor,
        **kwargs,
    ):
        self.port = port
        self.bootstrap_addrs = bootstrap_addrs
        self.key_pair = key_pair
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.hypertensor = hypertensor
        self.db = db

    async def run(self):
        print("running server gossip")
        from libp2p.utils.address_validation import (
            get_available_interfaces,
            get_optimal_binding_address,
        )

        listen_addrs = get_available_interfaces(self.port)

        # pos_transport = POSTransport(
        #     noise_transport=NoiseTransport(
        #         self.key_pair,
        #         noise_privkey=create_new_x25519_key_pair().private_key,
        #     ),
        #     pos=ProofOfStake(
        #         subnet_id=self.subnet_id,
        #         hypertensor=self.hypertensor,
        #         min_class=0,
        #     ),
        # )

        # secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
        #     POS_PROTOCOL_ID: pos_transport,
        # }

        # host = new_host(
        #     key_pair=self.key_pair,
        #     sec_opt=secure_transports_by_protocol,
        #     muxer_opt={MPLEX_PROTOCOL_ID: Mplex},  # Yamux bug, use Mplex
        # )

        # Create a new libp2p host
        host = new_host(
            key_pair=self.key_pair,
            muxer_opt={MPLEX_PROTOCOL_ID: Mplex},
        )
        # Log available protocols
        logger.info(f"Host ID: {host.get_id()}")
        logger.info(
            f"Host multiselect protocols: "
            f"{host.get_mux().get_protocols() if hasattr(host, 'get_mux') else 'N/A'}"
        )

        # Create and start gossipsub with optimized parameters for testing
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
            score_params=custom_score_params(),
        )

        pubsub = Pubsub(host, gossipsub)
        termination_event = trio.Event()  # Event to signal termination
        async with host.run(listen_addrs=listen_addrs), trio.open_nursery() as nursery:
            # Start the peer-store cleanup task
            nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)
            nursery.start_soon(maintain_connections, host)

            logger.info(f"Node started with peer ID: {host.get_id()}")
            logger.info("Initializing PubSub and GossipSub...")
            async with background_trio_service(pubsub):
                async with background_trio_service(gossipsub):
                    logger.info("Pubsub and GossipSub services started.")
                    await pubsub.wait_until_ready()
                    logger.info("Pubsub ready.")

                    if self.bootstrap_addrs:
                        await connect_to_bootstrap_nodes(host, self.bootstrap_addrs)

                    # Server mode
                    # Get all available addresses with peer ID
                    all_addrs = host.get_addrs()

                    logger.info("Listener ready, listening on:")
                    for addr in all_addrs:
                        logger.info(f"{addr}")

                    # Use optimal address for the client command
                    optimal_addr = get_optimal_binding_address(self.port)
                    optimal_addr_with_peer = (
                        f"{optimal_addr}/p2p/{host.get_id().to_string()}"
                    )
                    logger.info(
                        f"\nRun this from the same folder in another console:\n\n"
                        f"python -m subnet.cli.pubsub -d {optimal_addr_with_peer}\n"
                    )
                    logger.info("Waiting for peers...")

                    subnet_info_tracker = SubnetInfoTracker(
                        termination_event,
                        self.subnet_id,
                        self.hypertensor,
                        epoch_update_intervals=[
                            0.0,
                            0.5,
                        ],  # Update at the start and middle of each subnet epoch
                    )
                    nursery.start_soon(subnet_info_tracker.run)

                    # Start gossip receiver
                    gossip_receiver = GossipReceiver(
                        gossipsub=gossipsub,
                        pubsub=pubsub,
                        termination_event=termination_event,
                        db=self.db,
                        topics=[HEARTBEAT_TOPIC],
                        subnet_info_tracker=subnet_info_tracker,
                    )
                    nursery.start_soon(gossip_receiver.run)

                    # Start heartbeat publisher
                    nursery.start_soon(
                        publish_loop,
                        pubsub,
                        HEARTBEAT_TOPIC,
                        termination_event,
                        self.subnet_id,
                        self.subnet_node_id,
                        self.hypertensor,
                    )

                    # Start consensus
                    # consensus = Consensus(
                    #     db=self.db,
                    #     subnet_id=self.subnet_id,
                    #     subnet_node_id=self.subnet_node_id,
                    #     subnet_info_tracker=subnet_info_tracker,
                    #     hypertensor=self.hypertensor,
                    #     skip_activate_subnet=False,
                    #     start=True,
                    # )
                    # nursery.start_soon(consensus._main_loop)

                    dht = KadDHT(
                        host,
                        DHTMode.SERVER,
                        enable_random_walk=True,
                        validator=NamespacedValidator({"pk": PublicKeyValidator()}),
                    )

                    # take all peer ids from the host and add them to the dht
                    for peer_id in host.get_peerstore().peer_ids():
                        await dht.routing_table.add_peer(peer_id)
                    logger.info(
                        f"Connected to bootstrap nodes: {host.get_connected_peers()}"
                    )

                    async with background_trio_service(dht):
                        logger.info("DHT service started with random walk enabled")
                        await termination_event.wait()  # Wait for termination signal

            # Ensure all tasks are completed before exiting
            nursery.cancel_scope.cancel()

        print("Application shutdown complete")  # Print shutdown message
