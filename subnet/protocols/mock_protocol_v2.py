"""Mock Protocol for communication between peers."""

import logging

from libp2p.abc import (
    IHost,
)
from multiaddr import (
    Multiaddr,
)

from subnet.protocols.pb.mock_protocol_pb2 import (
    MockProtocolMessage,
)
from subnet.protocols.protocol_base import (
    IncomingRequestContext,
    ProtobufRequestResponseProtocolTemplate,
)
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.logging_config import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger("mock_protocol/1.0.0")

# Protocol ID - this must match between all peers using this protocol
PROTOCOL_ID = "/subnet/mock_protocol/1.0.0"


class MockProtocolV2(ProtobufRequestResponseProtocolTemplate[MockProtocolMessage, MockProtocolMessage]):
    """
    A mock direct request/response protocol using the reusable protobuf template.

    The parent class owns the stream lifecycle, varint framing, protobuf
    serialization, exact reads, and response framing. This class only contains
    protocol-specific request handling. See ``subnet.protocols.protocol_base``
    for a complete example of defining protobufs and creating a custom protocol.
    """

    def __init__(
        self,
        host: IHost,
        telemetry: Telemetry | None = None,
    ):
        """
        Initialize the MockProtocolV2.

        Args:
            host: The libp2p host instance
            telemetry: Optional telemetry events

        """
        self.host = host
        self.telemetry = telemetry

        super().__init__(
            host=host,
            protocol_id=PROTOCOL_ID,
            request_message_type=MockProtocolMessage,
            response_message_type=MockProtocolMessage,
            log=logger,
        )
        logger.info(f"MockProtocol initialized with protocol ID: {PROTOCOL_ID}")

    async def call_remote(
        self,
        destination: Multiaddr | str,
        request: MockProtocolMessage | None = None,
        *,
        peer_id: str | None = None,
    ) -> MockProtocolMessage:
        """
        Send a mock protobuf request to a remote peer.

        Args:
            destination: The destination multiaddr to call.
            request: Optional protobuf message. Defaults to TASK_REQUEST.
            peer_id: Optional peer ID to append when destination is missing a /p2p component.

        Returns:
            Parsed protobuf response from the remote peer.

        """
        outbound_request = request or MockProtocolMessage(type=MockProtocolMessage.TASK_REQUEST)
        logger.info("MockProtocol call_remote: %s", outbound_request)
        return await super().call_remote(destination, outbound_request, peer_id=peer_id)

    async def handle_request(
        self,
        context: IncomingRequestContext,
        request: MockProtocolMessage,
    ) -> MockProtocolMessage:
        """Handle protocol-specific mock requests."""
        logger.info("MockProtocol received message type %s from %s", request.type, context.peer_id)

        if self.telemetry:
            await self.telemetry.emit_async(
                "mock_protocol_request_received",
                peer_id=context.peer_id,
                message_type=request.type,
            )

        if request.type == MockProtocolMessage.TASK_REQUEST:
            return MockProtocolMessage(type=MockProtocolMessage.TASK_REQUEST)

        raise ValueError(f"Unsupported MockProtocolMessage type: {request.type}")
