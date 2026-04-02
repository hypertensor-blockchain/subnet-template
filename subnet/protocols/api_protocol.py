"""API Protocol for communication between peers that have APIs."""

import json
import logging
import os

import httpx
from libp2p.abc import (
    IHost,
    INetStream,
)
from libp2p.network.stream.exceptions import (
    StreamEOF,
)
from libp2p.tools.utils import (
    info_from_p2p_addr,
)
from multiaddr import (
    Multiaddr,
)
import varint

from subnet.protocols.pb.api_protocol_pb2 import (
    ApiProtocolMessage,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("api_protocol/1.0.0")

# Protocol ID - this must match between all peers using this protocol
PROTOCOL_ID = "/subnet/api_protocol/1.0.0"
MAX_READ_LEN = 2**32 - 1


class ApiProtocolConfig:
    """
    Configuration for the ApiProtocol.
    Allows for in-memory routes and an optional persistent JSON config file
    that can be updated on the fly to change API routes without restarting.
    """

    def __init__(self, routes: dict = None, config_file: str = None):
        self.routes = routes or {}
        self.config_file = config_file

    def get_route(self, route_name: str) -> str | None:
        """Get the URL for a route. Checks the JSON config file first if it exists, then falls back to memory."""
        if self.config_file and os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r") as f:
                    file_routes = json.load(f)
                    if isinstance(file_routes, dict) and route_name in file_routes:
                        return file_routes[route_name]
            except Exception as e:
                logger.warning(f"Failed to read/parse ApiProtocolConfig file {self.config_file}: {e}")

        return self.routes.get(route_name)


class ApiProtocol:
    """
    An API protocol for communication between peers. Remote peers call this protocol which
    sends out an API request to an API. This API can be another server or a local API that
    is running the logic, such as inference, training, or any other logic.

    Peers register a single api_respond handler that processes incoming requests.
    """

    def __init__(self, host: IHost, config: ApiProtocolConfig = None):
        """
        Initialize the ApiProtocol.

        Args:
            host: The libp2p host instance
            config: Configuration defining the API routes

        """
        self.host = host
        self.config = config or ApiProtocolConfig()

        # Register the protocol with the host
        self.host.set_stream_handler(PROTOCOL_ID, self._handle_incoming_stream)
        logger.info(f"ApiProtocol initialized with protocol ID: {PROTOCOL_ID}")

    async def _send_request(
        self, stream: INetStream, route: str, method: str, headers: dict, body: bytes, response_type: int
    ):
        message = ApiProtocolMessage(
            route=route, method=method, headers=headers or {}, body=body, response_type=response_type
        )
        msg_bytes = message.SerializeToString()
        length_prefix = varint.encode(len(msg_bytes))
        await stream.write(length_prefix + msg_bytes)

    async def call_remote(
        self,
        destination: Multiaddr,
        route: str,
        method: str = "GET",
        headers: dict = None,
        body: bytes = b"",
    ) -> bytes:
        """
        Call a remote peer's API and wait for a single unary response.
        """
        peer_id = None
        try:
            logger.info(f"ApiProtocol call_remote: {route} {method}")
            maddr = destination
            info = info_from_p2p_addr(maddr)
            peer_id = info.peer_id

            await self.host.connect(info)
            stream = await self.host.new_stream(peer_id, [PROTOCOL_ID])

            await self._send_request(stream, route, method, headers, body, ApiProtocolMessage.UNARY)

            # Wait for response (unary means one chunk containing the whole response, or we read until EOF)
            response = await stream.read(MAX_READ_LEN)
            await stream.close()
            return response
        except Exception as e:
            logger.error(f"ApiProtocol Failed to call_remote on peer {peer_id}: {e}")
            raise

    async def stream_remote(
        self,
        destination: Multiaddr,
        route: str,
        method: str = "GET",
        headers: dict = None,
        body: bytes = b"",
    ):
        """
        Call a remote peer's API and yield the streaming response.
        """
        peer_id = None
        try:
            logger.info(f"ApiProtocol stream_remote: {route} {method}")
            maddr = destination
            info = info_from_p2p_addr(maddr)
            peer_id = info.peer_id

            await self.host.connect(info)
            stream = await self.host.new_stream(peer_id, [PROTOCOL_ID])

            await self._send_request(stream, route, method, headers, body, ApiProtocolMessage.STREAM)

            while True:
                try:
                    # In a real framing format, we'd read chunks properly.
                    # Since we just pipe HTTP chunks to the libp2p stream, read what's available.
                    chunk = await stream.read(4096)
                    if not chunk:
                        break
                    yield chunk
                except StreamEOF:
                    break

            await stream.close()
        except Exception as e:
            logger.error(f"ApiProtocol Failed to stream_remote on peer {peer_id}: {e}")
            raise

    async def _handle_incoming_stream(self, stream: INetStream) -> None:
        """
        Handle incoming stream using protobuf and route to API.
        """
        try:
            peer_id = stream.muxed_conn.peer_id

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
                message = ApiProtocolMessage()
                message.ParseFromString(msg_bytes)

                logger.info(
                    f"Received API request from {peer_id}, route: {message.route}, type: {message.response_type}"
                )

                target_url = self.config.get_route(message.route)

                if not target_url:
                    logger.warning(f"Route not found: {message.route}")
                    await stream.write(b"Route not found")
                    await stream.close()
                    return

                async with httpx.AsyncClient() as client:
                    async with client.stream(
                        method=message.method,
                        url=target_url,
                        headers=dict(message.headers),
                        content=message.body if message.body else None,
                    ) as resp:
                        if message.response_type == ApiProtocolMessage.UNARY:
                            response_body = await resp.aread()
                            await stream.write(response_body)
                        elif message.response_type == ApiProtocolMessage.STREAM:
                            async for chunk in resp.aiter_bytes():
                                await stream.write(chunk)
                        else:
                            logger.warning("Unknown response type requested.")

            except Exception as proto_err:
                logger.warning(f"Failed to process API request: {proto_err}")
                await stream.write(f"Error processing request: {proto_err}".encode("utf-8"))

            await stream.close()
        except Exception as e:
            logger.error(f"Error handling DHT stream: {e}")
            try:
                await stream.close()
            except Exception:
                pass
