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
from subnet.utils.bootstrap import connect_to_bootstrap_nodes

from subnet.protocols.mock_protocol import (
    MockProtocol,
)
import logging

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
        **kwargs,
    ):
        self.port = port
        self.bootstrap_addrs = bootstrap_addrs

    async def run(self):
        """
        Keep server running forever
        """
        try:
            bootstrap_nodes = []

            if self.bootstrap_addrs:
                for addr in self.bootstrap_addrs:
                    bootstrap_nodes.append(addr)

            logger.info("Connecting to bootstrap nodes: %s", bootstrap_nodes)

            key_pair = create_new_key_pair(secrets.token_bytes(32))

            # Generate X25519 keypair for Noise
            noise_key_pair = create_new_x25519_key_pair()

            secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
                NOISE_PROTOCOL_ID: NoiseTransport(
                    key_pair, noise_privkey=noise_key_pair.private_key
                ),
                TProtocol(secio.ID): secio.Transport(key_pair),
                TProtocol(PLAINTEXT_PROTOCOL_ID): InsecureTransport(
                    key_pair, peerstore=None
                ),
            }

            # pos_transport = POSTransport(
            #     noise_transport=NoiseTransport(
            #         key_pair, noise_privkey=noise_key_pair.private_key
            #     ),
            #     pos=None,
            # )

            # secure_transports_by_protocol: Mapping[TProtocol, ISecureTransport] = {
            #     POS_PROTOCOL_ID: pos_transport,
            # }

            # host = new_host(key_pair=key_pair, sec_opt=secure_transports_by_protocol)
            host = new_host(key_pair=key_pair)

            # mock_protocol = MockProtocol(host)

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
                logger.info(f"Your Peer ID is: {peer_id}")

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
                self.dht = KadDHT(host=host, mode=DHTMode.SERVER)
                logger.info("DHT started")

                # take all peer ids from the host and add them to the dht
                for id in host.get_peerstore().peer_ids():
                    await self.dht.routing_table.add_peer(id)

                # Start heartbeat
                val_key = f"heartbeat/{peer_id}"
                msg = "Validator"
                val_data = msg.encode()

                logger.info(f"val_key is: {val_key}")

                # nursery.start_soon(heartbeat, self.dht, val_key.encode(), val_data)

                # if len(bootstrap_nodes) != 0:
                #     nursery.start_soon(
                #         mock_protocol_call,
                #         mock_protocol,
                #         Multiaddr(bootstrap_nodes[0]),
                #     )

                while True:
                    logger.info(
                        "Status - Connected peers: %d,"
                        "Peers in store: %d, Values in store: %d",
                        len(self.dht.host.get_connected_peers()),
                        len(self.dht.host.get_peerstore().peer_ids()),
                        len(self.dht.value_store.store),
                    )

                    # Detailed logging
                    connected_peers = self.dht.host.get_connected_peers()
                    peerstore_peers = self.dht.host.get_peerstore().peer_ids()
                    value_store = self.dht.value_store.store

                    logger.info("=" * 80)
                    logger.info("STATUS UPDATE")
                    logger.info("=" * 80)

                    logger.info(f"Connected peers ({len(connected_peers)}):")
                    for peer in connected_peers:
                        logger.info(f"  - {peer.pretty()}")

                    logger.info(f"\nPeers in store ({len(peerstore_peers)}):")
                    for peer_id in peerstore_peers:
                        logger.info(f"  - {peer_id.pretty()}")

                    logger.info(f"\nValues in store ({len(value_store)}):")
                    for key, value in value_store.items():
                        logger.info(f"  - Key: {key}")
                        logger.info(f"    Value: {value}")

                    logger.info("=" * 80)

                    await trio.sleep(10)

        except Exception as e:
            logger.error(f"Server node error: {e}", exc_info=True)
            sys.exit(1)


async def heartbeat(dht: KadDHT, key: bytes, value):
    while True:
        logger.info("Heartbeat key: %s", key)
        logger.info("Heartbeat value: %s", value)
        await dht.put_value(key, value)
        await trio.sleep(60)


async def mock_protocol_call(mock_protocol: MockProtocol, multiaddr: Multiaddr):
    while True:
        mp_returned = await mock_protocol.call_remote(multiaddr, "Hello, world!")
        logger.info("Mock protocol returned: %s", mp_returned)
        await trio.sleep(60)
