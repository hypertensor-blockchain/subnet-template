from typing import List

import sys
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p.peer.id import ID as PeerID
from libp2p.tools.utils import (
    info_from_p2p_addr,
)
from multiaddr import (
    Multiaddr,
)
import secrets
import trio
from libp2p.crypto.secp256k1 import (
    create_new_key_pair,
)
from libp2p import (
    new_host,
)
from collections.abc import (
    Mapping,
)
from libp2p.custom_types import (
    TProtocol,
)
from libp2p.abc import (
    IHost,
    ISecureTransport,
)
from libp2p.security.noise.transport import (
    Transport as NoiseTransport,
)
from libp2p.crypto.x25519 import create_new_key_pair as create_new_x25519_key_pair
from subnet.network.pos.pos_transport import (
    POSTransport,
    PROTOCOL_ID as POS_PROTOCOL_ID,
)
from subnet.network.pos.proof_of_stake import ProofOfStake
import libp2p.security.secio.transport as secio
from libp2p.security.noise.transport import (
    PROTOCOL_ID as NOISE_PROTOCOL_ID,
    Transport as NoiseTransport,
)
from libp2p.security.insecure.transport import (
    PLAINTEXT_PROTOCOL_ID,
    InsecureTransport,
)
from libp2p.records.pubkey import PublicKeyValidator
from subnet.protocols.mock_protocol import (
    MockProtocol,
)
from libp2p.tools.async_service import (
    background_trio_service,
)
from libp2p.crypto.keys import KeyPair
from libp2p.peer.pb import crypto_pb2
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from libp2p.records.validator import NamespacedValidator
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.consensus.consensus import Consensus
from subnet.utils.bootstrap import connect_to_bootstrap_nodes
from subnet.hypertensor.chain_functions import Hypertensor

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("server/1.0.0")


class Server:
    """ """

    def __init__(
        self,
        *,
        port: int,
        bootstrap_addrs: List[str] | None = None,
        key_pair: KeyPair,
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

    async def run(self):
        try:
            bootstrap_nodes = []

            if self.bootstrap_addrs:
                for addr in self.bootstrap_addrs:
                    bootstrap_nodes.append(addr)

            pos_transport = POSTransport(
                noise_transport=NoiseTransport(
                    self.key_pair,
                    noise_privkey=create_new_x25519_key_pair().private_key,
                ),
                pos=ProofOfStake(
                    subnet_id=self.subnet_id,
                    hypertensor=self.hypertensor,
                    min_class=0,
                ),
            )

            secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
                POS_PROTOCOL_ID: pos_transport,
            }

            host = new_host(
                key_pair=self.key_pair, sec_opt=secure_transports_by_protocol
            )

            # host = new_host(key_pair=self.key_pair)

            from libp2p.utils.address_validation import (
                get_available_interfaces,
                get_optimal_binding_address,
            )

            listen_addrs = get_available_interfaces(self.port)

            async with (
                host.run(listen_addrs=listen_addrs),
                trio.open_nursery() as nursery,
            ):
                # Start the peer-store cleanup task
                nursery.start_soon(host.get_peerstore().start_cleanup_task, 60)

                peer_id = host.get_id().pretty()

                # Get all available addresses with peer ID
                all_addrs = host.get_addrs()

                logger.info("Listener ready, listening on:")
                for addr in all_addrs:
                    logger.info(f"{addr}")

                # Use optimal address for the bootstrap command
                optimal_addr = get_optimal_binding_address(self.port)
                optimal_addr_with_peer = (
                    f"{optimal_addr}/p2p/{host.get_id().to_string()}"
                )
                bootstrap_cmd = f"--bootstrap {optimal_addr_with_peer}"
                logger.info("To connect to this node, use: %s", bootstrap_cmd)

                await connect_to_bootstrap_nodes(host, bootstrap_nodes)
                # dht = KadDHT(host, DHTMode.SERVER, enable_random_walk=True)
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

                nursery.start_soon(start_heartbeat, dht, self.key_pair)
                # nursery.start_soon(print_heartbeats, dht)

                consensus = Consensus(
                    dht=dht,
                    subnet_id=self.subnet_id,
                    subnet_node_id=self.subnet_node_id,
                    hypertensor=self.hypertensor,
                    skip_activate_subnet=False,
                    start=True,
                )
                nursery.start_soon(consensus._main_loop)

                # Start the DHT service
                async with background_trio_service(dht):
                    logger.info(f"DHT service started in {DHTMode.SERVER.value} mode")

                    # Keep the node running
                    while True:
                        logger.info(
                            "Status - Connected peers: %d,"
                            "Peers in store: %d, Values in store: %d",
                            len(dht.host.get_connected_peers()),
                            len(dht.host.get_peerstore().peer_ids()),
                            len(dht.value_store.store),
                        )
                        await trio.sleep(10)

        except Exception as e:
            logger.error(f"Server node error: {e}", exc_info=True)
            sys.exit(1)


async def start_heartbeat(dht: KadDHT, keypair: KeyPair):
    peer_id = PeerID.from_pubkey(keypair.public_key)
    key = f"/pk/{peer_id.to_bytes().hex()}"
    value = keypair.public_key.serialize()

    logger.info(f"[Heartbeat] Key: {key}")
    logger.info(f"[Heartbeat] Value: {value}")
    while True:
        logger.info("[Heartbeat] Declaring peer")
        await dht.put_value(key, value)
        await trio.sleep(15)


# async def print_heartbeats(dht: KadDHT):
#     """
#     It's impossible to get every Peer ID in the (a large) network so we test using
#     the test IDs to get the heartbeat

#     In production: We use the blockchain to get the list of all peer IDs
#     """
#     peer_ids = [
#         "12D3KooWAkRWUdmXy5tkGQ1oUKxx2W4sXxsWr4ekrcvLCbA3BQTf",  # Alith
#         "12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF",  # Baltathar
#         "12D3KooWBqJu85tnb3WciU3LcXhCmTdkvMi4k1Zq3BshUPhVfTui",  # Charleth
#     ]
#     while True:
#         for base58_peer_id in peer_ids:
#             peer_id = PeerID.from_base58(base58_peer_id)
#             key = f"/pk/{peer_id.to_bytes().hex()}"
#             retrieved_value = await dht.get_value(key)
#             logger.info(f"[Print Heartbeat] {base58_peer_id}: {retrieved_value}")

#         await trio.sleep(15)


# async def mock_protocol_call(mock_protocol: MockProtocol, multiaddr: Multiaddr):
#     while True:
#         mp_returned = await mock_protocol.call_remote(multiaddr, "Hello, world!")
#         logger.info("Mock protocol returned: %s", mp_returned)
#         await trio.sleep(60)
