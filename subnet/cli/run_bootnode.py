import argparse
import logging
import os
import base58
import trio
from libp2p.peer.id import ID as PeerID
import secrets
from subnet.utils.bootstrap import connect_to_bootstrap_nodes
from libp2p import (
    new_host,
)
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p.crypto.ed25519 import (
    create_new_key_pair,
)
from libp2p.custom_types import (
    TProtocol,
)
from libp2p.peer.peerinfo import (
    info_from_p2p_addr,
)
from libp2p.pubsub.gossipsub import (
    GossipSub,
)
from libp2p.pubsub.pubsub import (
    Pubsub,
)
from libp2p.stream_muxer.mplex.mplex import (
    MPLEX_PROTOCOL_ID,
    Mplex,
)
from libp2p.tools.async_service.trio_service import (
    background_trio_service,
)
from libp2p.utils.address_validation import (
    find_free_port,
)
from libp2p.pubsub.score import ScoreParams, TopicScoreParams
from collections.abc import Mapping
from libp2p.abc import ISecureTransport
from subnet.db.database import RocksDB
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom
from substrateinterface import Keypair as SubstrateKeypair, KeypairType
from subnet.utils.heartbeat import HEARTBEAT_TOPIC
from subnet.utils.gossip_receiver import GossipReceiver
from libp2p.peer.pb import crypto_pb2
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from libp2p.crypto.secp256k1 import Secp256k1PrivateKey
from libp2p.crypto.keys import KeyPair
from libp2p.records.pubkey import PublicKeyValidator
from libp2p.records.validator import NamespacedValidator
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
from subnet.utils.subnet_info_tracker import SubnetInfoTracker
import random
from dotenv import load_dotenv
from pathlib import Path
from subnet.utils.custom_score_params import custom_score_params
from subnet.utils.random_walk import maintain_connections

load_dotenv(os.path.join(Path.cwd(), ".env"))

PHRASE = os.getenv("PHRASE")

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set default to DEBUG for more verbose output
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("pubsub-bootnode")
GOSSIPSUB_PROTOCOL_ID = TProtocol("/meshsub/1.0.0")


async def run(args: argparse.Namespace) -> None:
    if args.private_key_path is None:
        key_pair = create_new_key_pair(secrets.token_bytes(32))
    else:
        with open(f"{args.private_key_path}", "rb") as f:
            data = f.read()
        private_key = crypto_pb2.PrivateKey.FromString(data)
        if private_key.Type == crypto_pb2.KeyType.Ed25519:
            private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
        elif private_key.Type == crypto_pb2.KeyType.Secp256k1:
            private_key = Secp256k1PrivateKey.from_bytes(private_key.Data)
        else:
            raise ValueError("Unsupported key type")

    public_key = private_key.get_public_key()
    key_pair = KeyPair(private_key, public_key)

    if not args.base_path:
        base_path = f"/tmp/{random.randint(100, 1000000)}"
    else:
        base_path = args.base_path

    db = RocksDB(base_path, args.subnet_id)

    if not args.no_blockchain_rpc:
        if args.local_rpc:
            rpc = os.getenv("LOCAL_RPC")
        else:
            rpc = os.getenv("DEV_RPC")

        if args.phrase is not None:
            hypertensor = Hypertensor(rpc, args.phrase)
            substrate_keypair = SubstrateKeypair.create_from_mnemonic(
                args.phrase, crypto_type=KeypairType.ECDSA
            )
            hotkey = substrate_keypair.ss58_address
            logger.info(f"hotkey: {hotkey}")
        elif args.tensor_private_key is not None:
            hypertensor = Hypertensor(
                rpc, args.tensor_private_key, KeypairFrom.PRIVATE_KEY
            )
            substrate_keypair = SubstrateKeypair.create_from_private_key(
                args.tensor_private_key, crypto_type=KeypairType.ECDSA
            )
            hotkey = substrate_keypair.ss58_address
            logger.info(f"hotkey: {hotkey}")
        else:
            # Default to using PHRASE if no other options are provided
            hypertensor = Hypertensor(rpc, PHRASE)
    else:
        # Run mock hypertensor blockchain for testing
        hypertensor = LocalMockHypertensor(
            subnet_id=args.subnet_id,
            peer_id=PeerID.from_pubkey(key_pair.public_key),
            subnet_node_id=0,
            coldkey="",
            hotkey="",
            bootnode_peer_id="",
            client_peer_id="",
            reset_db=True if not args.bootstrap_addrs else False,
        )

    from libp2p.utils.address_validation import (
        get_available_interfaces,
        get_optimal_binding_address,
    )

    if args.port is None or args.port == 0:
        port = find_free_port()
        logger.info(f"Using random available port: {port}")
    else:
        port = args.port

    listen_addrs = get_available_interfaces(port)

    # pos_transport = POSTransport(
    #     noise_transport=NoiseTransport(
    #         key_pair,
    #         noise_privkey=create_new_x25519_key_pair().private_key,
    #     ),
    #     pos=ProofOfStake(
    #         subnet_id=args.subnet_id,
    #         hypertensor=hypertensor,
    #         min_class=0,
    #     ),
    # )

    # secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
    #     POS_PROTOCOL_ID: pos_transport,
    # }

    # host = new_host(
    #     key_pair=key_pair,
    #     sec_opt=secure_transports_by_protocol,
    #     muxer_opt={MPLEX_PROTOCOL_ID: Mplex},  # Yamux bug, use Mplex
    # )

    # Create a new libp2p host
    host = new_host(
        key_pair=key_pair,
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

                if args.destination:
                    await connect_to_bootstrap_nodes(host, [args.destination])

                # Server mode
                # Get all available addresses with peer ID
                all_addrs = host.get_addrs()

                logger.info("Listener ready, listening on:")
                for addr in all_addrs:
                    logger.info(f"{addr}")

                # Use optimal address for the client command
                optimal_addr = get_optimal_binding_address(port)
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
                    args.subnet_id,
                    hypertensor,
                    epoch_update_intervals=[0.0, 0.5],
                )
                nursery.start_soon(subnet_info_tracker.run)

                # Start gossip receiver
                gossip_receiver = GossipReceiver(
                    gossipsub=gossipsub,
                    pubsub=pubsub,
                    termination_event=termination_event,
                    db=db,
                    topics=[HEARTBEAT_TOPIC],
                    subnet_info_tracker=subnet_info_tracker,
                )
                nursery.start_soon(gossip_receiver.run)

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


def main() -> None:
    description = """
    This program demonstrates a pubsub p2p chat application using libp2p with
    the gossipsub protocol as the pubsub router.
    To use it, first run 'python pubsub.py -p <PORT> -t <TOPIC>',
    where <PORT> is the port number,
    -d <DESTINATION>', where <DESTINATION> is the multiaddress of the previous
    listener host. Messages typed in either terminal will be received by all peers
    subscribed to the same topic.

python -m subnet.cli.run_bootnode \
--private_key_path bootnode-ed25519.key \
--port 31190 \
--subnet_id 1 \
--no_blockchain_rpc

    """

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "-d",
        "--destination",
        type=str,
        help="Address of peer to connect to",
        default=None,
    )

    parser.add_argument(
        "--bootstrap",
        "-b",
        action="append",
        dest="bootstrap_addrs",
        default=[],
        metavar="MULTIADDR",
        help="Bootstrap peer multiaddress (can be specified multiple times). "
        "Format: /ip4/<IP>/tcp/<PORT>/p2p/<PEER_ID>",
    )

    parser.add_argument(
        "--private_key_path",
        type=str,
        default=None,
        help="Path to the peers private key file. ",
    )

    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="Port to listen on",
        default=None,
    )

    parser.add_argument(
        "--base-path",
        type=str,
        help="Base path for database",
        default=None,
    )

    parser.add_argument(
        "--subnet_id", type=int, default=1, help="Subnet ID this node belongs to. "
    )

    parser.add_argument(
        "--no_blockchain_rpc", action="store_true", help="[Testing] Run with no RPC"
    )

    parser.add_argument(
        "--local_rpc",
        action="store_true",
        help="[Testing] Run in local RPC mode, uses LOCAL_RPC",
    )

    parser.add_argument(
        "--tensor_private_key",
        type=str,
        required=False,
        help="[Testing] Hypertensor blockchain private key",
    )

    parser.add_argument(
        "--phrase",
        type=str,
        required=False,
        help="[Testing] Coldkey phrase that controls actions which include funds, such as registering, and staking",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Set debug level if verbose flag is provided
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    logger.info("Running pubsub chat example...")

    try:
        trio.run(run, args)
    except KeyboardInterrupt:
        logger.info("Application terminated by user")


if __name__ == "__main__":
    main()
