"""API Protocol for communication between peers that have APIs."""

from dataclasses import dataclass
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
from subnet.telemetry.telemetry import Telemetry
from subnet.utils.logging_config import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger("api_protocol/1.0.0")

# Protocol ID - this must match between all peers using this protocol
PROTOCOL_ID = "/subnet/api_protocol/1.0.0"
MAX_READ_LEN = 2**32 - 1


@dataclass(frozen=True, slots=True)
class ApiRouteConfig:
    """Configuration for one route exposed through ``ApiProtocol``."""

    url: str
    stream: bool = False


class ApiProtocolConfig:
    """
    Configuration for the ApiProtocol.

    Allows for in-memory routes and an optional persistent JSON config file
    that can be updated on the fly to change API routes without restarting.

    The config maps public route names used by peer calls to local or external
    HTTP URLs. Each route also declares whether it supports streaming with the
    ``stream`` flag. For example, when a remote peer calls
    ``route="inference"``, ``ApiProtocol`` looks up ``"inference"`` here and
    forwards the request to the configured URL.

    In-memory setup:
        Use ``routes`` when the route map is known when the node starts::

            config = ApiProtocolConfig(
                routes={
                    "health": {
                        "url": "http://127.0.0.1:8000/health",
                        "stream": False,
                    },
                    "inference": {
                        "url": "http://127.0.0.1:8000/v1/inference",
                        "stream": True,
                    },
                    "events": {
                        "url": "http://127.0.0.1:8000/v1/events",
                        "stream": False,
                    },
                },
            )
            api_protocol = ApiProtocol(host=host, config=config)

        Peers then call the route by name::

            response = await api_protocol.call_remote(
                destination=peer_multiaddr,
                route="inference",
                method="POST",
                headers={"content-type": "application/json"},
                body=b'{"prompt": "hello"}',
            )

    Config-file setup:
        Use ``config_file`` when you want to update routes without restarting
        the node. The file should be a JSON object whose keys are route names.
        Each value must include a target ``url`` and a boolean ``stream`` flag::

            {
              "health": {
                "url": "http://127.0.0.1:8000/health",
                "stream": false
              },
              "inference": {
                "url": "http://127.0.0.1:8000/v1/inference",
                "stream": false
              },
              "events": {
                "url": "http://127.0.0.1:8000/v1/events",
                "stream": true
              }
            }

        Then create the config with the path to that file::

            config = ApiProtocolConfig(
                config_file="api_routes.json",
            )
            api_protocol = ApiProtocol(host=host, config=config)

        ``get_route`` reads the JSON file on each lookup, so changes to the
        file are picked up at runtime. If both ``routes`` and ``config_file``
        are provided, the config file takes precedence for keys it contains and
        ``routes`` is used as the fallback.

        The legacy shorthand ``{"health": "http://127.0.0.1:8000/health"}``
        is still accepted and is treated as ``stream=False``. New route configs
        should use the explicit object format above.
    """

    def __init__(self, routes: dict = None, config_file: str = None):
        self.routes = routes or {}
        self.config_file = config_file

    def get_route(self, route_name: str) -> str | None:
        """Get the URL for a route. Checks the JSON config file first if it exists, then falls back to memory."""
        route_config = self.get_route_config(route_name)
        if route_config is None:
            return None
        return route_config.url

    def get_route_config(self, route_name: str) -> ApiRouteConfig | None:
        """Get the full route config, checking the config file before in-memory routes."""
        raw_route = self._get_file_route(route_name)
        if raw_route is not None:
            try:
                return self._parse_route_config(route_name, raw_route)
            except ValueError as e:
                logger.warning(f"Invalid ApiProtocolConfig route {route_name!r} in {self.config_file}: {e}")

        raw_route = self.routes.get(route_name)
        if raw_route is None:
            return None

        try:
            return self._parse_route_config(route_name, raw_route)
        except ValueError as e:
            logger.warning(f"Invalid ApiProtocolConfig route {route_name!r}: {e}")
            return None

    def _get_file_route(self, route_name: str):
        if self.config_file and os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r") as f:
                    file_routes = json.load(f)
                    if isinstance(file_routes, dict) and route_name in file_routes:
                        return file_routes[route_name]
            except Exception as e:
                logger.warning(f"Failed to read/parse ApiProtocolConfig file {self.config_file}: {e}")
        return None

    @staticmethod
    def _parse_route_config(route_name: str, raw_route) -> ApiRouteConfig:
        if isinstance(raw_route, ApiRouteConfig):
            return raw_route

        if isinstance(raw_route, str):
            if not raw_route:
                raise ValueError("route URL must be a non-empty string")
            return ApiRouteConfig(url=raw_route, stream=False)

        if not isinstance(raw_route, dict):
            raise ValueError("route config must be a URL string or an object with url and stream fields")

        raw_url = raw_route.get("url")
        if not isinstance(raw_url, str) or not raw_url:
            raise ValueError("route config requires a non-empty string url")

        raw_stream = raw_route.get("stream", False)
        if not isinstance(raw_stream, bool):
            raise ValueError("route config stream field must be true or false")

        return ApiRouteConfig(url=raw_url, stream=raw_stream)


class ApiProtocol:
    """
    An API protocol for communication between peers. Remote peers call this protocol which
    sends out an API request to an API. This API can be another server or a local API that
    is running the logic, such as inference, training, or any other logic.

    Peers register a single api_respond handler that processes incoming requests.
    """

    def __init__(self, host: IHost, config: ApiProtocolConfig, telemetry: Telemetry | None = None):
        """
        Initialize the ApiProtocol.

        Args:
            host: The libp2p host instance
            config: Configuration defining the API routes
            telemetry: Optional telemetry URL

        """
        self.host = host
        self.config = config or ApiProtocolConfig()
        self.telemetry = telemetry

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

            if self.telemetry:
                await self.telemetry.emit_async("api_call_remote", route=route, method=method, peer_id=peer_id)

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

            if self.telemetry:
                await self.telemetry.emit_async("api_stream_remote", route=route, method=method, peer_id=peer_id)

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

                route_config = self.config.get_route_config(message.route)

                if route_config is None:
                    logger.warning(f"Route not found: {message.route}")
                    await stream.write(b"Route not found")
                    await stream.close()
                    return

                if message.response_type == ApiProtocolMessage.STREAM and not route_config.stream:
                    logger.warning(f"Route does not support streaming: {message.route}")
                    await stream.write(f"Route does not support streaming: {message.route}".encode("utf-8"))
                    await stream.close()
                    return

                if self.telemetry:
                    await self.telemetry.emit_async(
                        "api_request_received",
                        route=message.route,
                        method=message.method,
                        peer_id=peer_id,
                        stream_requested=message.response_type == ApiProtocolMessage.STREAM,
                        stream_supported=route_config.stream,
                    )

                async with httpx.AsyncClient() as client:
                    async with client.stream(
                        method=message.method,
                        url=route_config.url,
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
