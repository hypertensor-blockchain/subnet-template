"""Mock Protocol for communication between peers."""

import logging

from libp2p.abc import (
    IHost,
    INetStream,
)
from libp2p.crypto.ecdsa import ECDSAPublicKey
from libp2p.crypto.ed25519 import Ed25519PublicKey
from libp2p.crypto.rsa import RSAPublicKey
from libp2p.crypto.secp256k1 import Secp256k1PublicKey
from libp2p.network.stream.exceptions import (
    StreamEOF,
)
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2
from libp2p.tools.utils import (
    info_from_p2p_addr,
)
from multiaddr import (
    Multiaddr,
)
import varint

from subnet.db.database import RocksDB
from subnet.protocols.pb.gossip_fallback_pb2 import (
    GossipFallbackRequest,
    GossipFallbackResponse,
)
from subnet.utils.hypertensor.subnet_info_tracker import SubnetInfoTracker
from subnet.utils.pubsub.heartbeat import HEARTBEAT_TOPIC, HeartbeatData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("gossip_fallback/1.0.0")

# Protocol ID - this must match between all peers using this protocol
PROTOCOL_ID = "/gossip_fallback/1.0.0"
MAX_READ_LEN = 2**32 - 1


class GossipFallback:
    """
    A fallback protocol for communication between peers to retrieve gossip messages they didn't receive
    """

    def __init__(self, host: IHost, subnet_info_tracker: SubnetInfoTracker, db: RocksDB):
        """
        Initialize the GossipFallback.

        Args:
            host: The libp2p host instance
            subnet_info_tracker: The subnet info tracker instance
            db: The database instance

        """
        self.host = host
        self.subnet_info_tracker = subnet_info_tracker
        self.db = db

        # Register the protocol with the host
        self.host.set_stream_handler(PROTOCOL_ID, self._handle_incoming_stream)
        logger.info(f"GossipFallback initialized with protocol ID: {PROTOCOL_ID}")

    async def call_remote(
        self,
        destination: Multiaddr,
        peer_id: PeerID,
        epoch: int,
    ) -> bytes:
        """
        Call the rpc_respond function on a remote peer.

        Args:
            destination: The destination peer ID to call
            peer_id: The peer ID to call
            epoch: The epoch to call

        Returns:
            Response bytes from the remote peer

        """
        try:
            msg = GossipFallbackRequest(
                public_key=peer_id.extract_public_key().serialize(),
                epoch=epoch,
            )
            logger.info(f"GossipFallback call_remote: {msg}")

            maddr = destination
            info = info_from_p2p_addr(maddr)
            stream_peer_id = info.peer_id

            logger.info(f"GossipFallback Connecting to: {stream_peer_id}")

            await self.host.connect(info)
            logger.info(f"GossipFallback Opening Stream to: {stream_peer_id}")

            stream = await self.host.new_stream(stream_peer_id, [PROTOCOL_ID])

            logger.info(f"GossipFallback Opened stream to {stream_peer_id}, sending message '{msg}'")

            await stream.write(msg.encode("utf-8"))
            logger.info(f"GossipFallback Sent message to {stream_peer_id}: '{msg}'")

            response = await stream.read()
            logger.info(f"GossipFallback Received response from {stream_peer_id}: '{response}'")

            # Close the stream
            await stream.close()

            logger.info(f"GossipFallback RPC call to {stream_peer_id} succeeded, received '{response}'")
            return response

        except Exception as e:
            logger.error(f"GossipFallback Failed to call rpc_respond on peer {stream_peer_id}: {e}")

    async def _handle_incoming_stream(self, stream: INetStream) -> None:
        try:
            peer_id = stream.muxed_conn.peer_id
            logger.info(f"GossipFallback Received connection from {peer_id}")
            # Wait until EOF
            msg = await stream.read(MAX_READ_LEN)
            print(f"Read message {msg}")
            logger.info(f"GossipFallback Echoing message: {msg.decode('utf-8')}")
            await stream.write(msg)
        except StreamEOF:
            logger.warning("GossipFallback Stream closed by remote peer.", exc_info=True)
        except Exception as e:
            logger.warning(f"GossipFallback Error in echo handler: {e}", exc_info=True)
        finally:
            logger.info("GossipFallback Finally losing stream")
            await stream.close()

    async def _handle_incoming_stream_v2(self, stream: INetStream) -> None:
        """
        Handle incoming stream using protobuf.

        This will listen for incoming streams and parse the protobuf message.

        This function can handle multiple uses cases that should be handled by the protobuf logic
        """
        try:
            peer_id = stream.muxed_conn.peer_id
            print(f"Received connection from {peer_id}")

            # Read varint-prefixed length for the message
            length_prefix = b""
            while True:
                byte = await stream.read(1)
                if not byte:
                    logger.warning("Stream closed while reading varint length")
                    await stream.close()
                    return
                length_prefix += byte
                if byte[0] & 0x80 == 0:
                    break
            msg_length = varint.decode_bytes(length_prefix)

            # Read the message bytes
            msg_bytes = await stream.read(msg_length)
            if len(msg_bytes) < msg_length:
                logger.warning("Failed to read full message from stream")
                await stream.close()
                return

            try:
                # Parse as protobuf
                message = GossipFallbackRequest()
                message.ParseFromString(msg_bytes)

                logger.debug(f"Received DHT message from {peer_id}, type: {message}")

                epoch = message.epoch
                public_key = message.public_key

                if public_key.Type == crypto_pb2.KeyType.RSA:
                    public_key = RSAPublicKey.from_bytes(public_key.Data)
                elif public_key.Type == crypto_pb2.KeyType.Ed25519:
                    public_key = Ed25519PublicKey.from_bytes(public_key.Data)
                elif public_key.Type == crypto_pb2.KeyType.Secp256k1:
                    public_key = Secp256k1PublicKey.from_bytes(public_key.Data)
                elif public_key.Type == crypto_pb2.KeyType.ECDSA:
                    public_key = ECDSAPublicKey.from_bytes(public_key.Data)
                else:
                    raise Exception("Unsupported key type")

                peer_id = PeerID.from_pubkey(public_key)
                key = f"{epoch}:{peer_id}"
                heartbeat = self.db.nmap_get(HEARTBEAT_TOPIC, key)

                if heartbeat is None:
                    raise Exception("Heartbeat doesn't exist")

                heartbeat_data = HeartbeatData.from_json(heartbeat)

                # TODO: Check if peer_id is an on-chain peer ID

                msg = GossipFallbackResponse(
                    epoch=epoch,
                    subnet_id=heartbeat_data.subnet_id,
                    subnet_node_id=heartbeat_data.subnet_node_id,
                )

            except Exception as proto_err:
                logger.warning(f"Failed to parse protobuf message: {proto_err}")

            await stream.close()
        except Exception as e:
            logger.error(f"Error handling DHT stream: {e}")
            await stream.close()
