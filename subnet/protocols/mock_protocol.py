"""Mock Protocol for communication between peers."""

import logging

from libp2p.abc import (
    IHost,
    INetStream,
)
from libp2p.network.stream.exceptions import (
    StreamEOF,
)
from libp2p.peer.id import ID as PeerID
from libp2p.tools.utils import (
    info_from_p2p_addr,
)
from multiaddr import (
    Multiaddr,
)
import varint

from subnet.protocols.pb.mock_protocol_pb2 import (
    MockProtocolMessage,
)
from subnet.utils.hypertensor.subnet_info_tracker import SubnetInfoTracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mock_protocol/1.0.0")

# Protocol ID - this must match between all peers using this protocol
PROTOCOL_ID = "/subnet/mock_protocol/1.0.0"
MAX_READ_LEN = 2**32 - 1


class MockProtocol:
    """
    A simplified mock protocol for communication between peers.

    Peers register a single rpc_respond handler that processes incoming requests.
    """

    def __init__(self, host: IHost, subnet_info_tracker: SubnetInfoTracker):
        """
        Initialize the MockProtocol.

        Args:
            host: The libp2p host instance
            subnet_info_tracker: The subnet info tracker instance

        """
        self.host = host
        self.subnet_info_tracker = subnet_info_tracker

        # Register the protocol with the host
        self.host.set_stream_handler(PROTOCOL_ID, self._handle_incoming_stream)
        logger.info(f"MockProtocol initialized with protocol ID: {PROTOCOL_ID}")

    async def call_remote(
        self,
        destination: Multiaddr,
        msg: str,
    ) -> bytes:
        """
        Call the rpc_respond function on a remote peer.

        Args:
            destination: The destination peer ID to call
            msg: Message to send

        Returns:
            Response bytes from the remote peer

        """
        try:
            logger.info(f"MockProtocol call_remote: {msg}")

            maddr = destination
            info = info_from_p2p_addr(maddr)
            peer_id = info.peer_id

            logger.info(f"MockProtocol Connecting to: {peer_id}")

            await self.host.connect(info)
            logger.info(f"MockProtocol Opening Stream to: {peer_id}")

            stream = await self.host.new_stream(peer_id, [PROTOCOL_ID])

            logger.info(f"MockProtocol Opened stream to {peer_id}, sending message '{msg}'")

            await stream.write(msg.encode("utf-8"))
            logger.info(f"MockProtocol Sent message to {peer_id}: '{msg}'")

            response = await stream.read()
            logger.info(f"MockProtocol Received response from {peer_id}: '{response}'")

            # Close the stream
            await stream.close()

            logger.info(f"MockProtocol RPC call to {peer_id} succeeded, received '{response}'")
            return response

        except Exception as e:
            logger.error(f"MockProtocol Failed to call rpc_respond on peer {peer_id}: {e}")

    async def _handle_incoming_stream(self, stream: INetStream) -> None:
        logger.info("MockProtocol _handle_incoming_stream")
        try:
            peer_id = stream.muxed_conn.peer_id
            logger.info(f"MockProtocol Received connection from {peer_id}")
            # Wait until EOF
            msg = await stream.read(MAX_READ_LEN)
            print(f"Read message {msg}")
            logger.info(f"MockProtocol Echoing message: {msg.decode('utf-8')}")
            await stream.write(msg)
        except StreamEOF:
            logger.warning("MockProtocol Stream closed by remote peer.", exc_info=True)
        except Exception as e:
            logger.warning(f"MockProtocol Error in echo handler: {e}", exc_info=True)
        finally:
            logger.info("MockProtocol Finally losing stream")
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
                message = MockProtocolMessage()
                message.ParseFromString(msg_bytes)

                logger.debug(f"Received DHT message from {peer_id}, type: {message.type}")

                if message.type == MockProtocolMessage.MockProtocolMessageType.FIND_NODE:
                    # Handle find_node message
                    pass

            except Exception as proto_err:
                logger.warning(f"Failed to parse protobuf message: {proto_err}")

            await stream.close()
        except Exception as e:
            logger.error(f"Error handling DHT stream: {e}")
            await stream.close()
