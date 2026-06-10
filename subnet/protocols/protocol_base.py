"""
Base classes for direct libp2p request/response protocols.

Use this module when you want developers to write only protocol-specific logic
while the base class owns stream setup, varint framing, exact reads, protobuf
serialization, response framing, and stream cleanup.

Full protobuf protocol example:

1. Define the protobuf messages in ``subnet/protocols/pb/task_protocol.proto``:

       syntax = "proto3";

       package task_protocol.pb;

       message TaskRequest {
         string task_id = 1;
         bytes payload = 2;
       }

       message TaskResponse {
         bool accepted = 1;
         bytes result = 2;
         string error = 3;
       }

2. Generate the Python protobuf module from the repository root:

       protoc --python_out=. --mypy_out=. subnet/protocols/pb/task_protocol.proto

   This creates ``subnet/protocols/pb/task_protocol_pb2.py`` and, when
   ``mypy-protobuf`` is installed, ``subnet/protocols/pb/task_protocol_pb2.pyi``.
   If you are not generating type stubs, omit ``--mypy_out=.``. To regenerate
   it through the Makefile, add the proto path to the ``PB`` variable.

3. Create a protocol class by subclassing
   ``ProtobufRequestResponseProtocolTemplate``:

       import logging

       from libp2p.abc import IHost
       from multiaddr import Multiaddr

       from subnet.protocols.pb.task_protocol_pb2 import (
           TaskRequest,
           TaskResponse,
       )
       from subnet.protocols.protocol_base import (
           IncomingRequestContext,
           ProtobufRequestResponseProtocolTemplate,
       )

       logger = logging.getLogger("task_protocol/1.0.0")
       PROTOCOL_ID = "/subnet/task_protocol/1.0.0"


       async def run_task(payload: bytes) -> bytes:
           return payload.upper()


       class TaskProtocol(
           ProtobufRequestResponseProtocolTemplate[TaskRequest, TaskResponse],
       ):
           def __init__(self, host: IHost) -> None:
               super().__init__(
                   host=host,
                   protocol_id=PROTOCOL_ID,
                   request_message_type=TaskRequest,
                   response_message_type=TaskResponse,
                   log=logger,
               )

           async def call_remote(
               self,
               destination: Multiaddr | str,
               task_id: str,
               payload: bytes,
           ) -> TaskResponse:
               request = TaskRequest(task_id=task_id, payload=payload)
               return await super().call_remote(destination, request)

           async def handle_request(
               self,
               context: IncomingRequestContext,
               request: TaskRequest,
           ) -> TaskResponse:
               logger.info(
                   "Received task %s from %s",
                   request.task_id,
                   context.peer_id,
               )
               result = await run_task(request.payload)
               return TaskResponse(accepted=True, result=result)

4. Instantiate the protocol once after your libp2p host is created. The base
   class registers the stream handler automatically:

       task_protocol = TaskProtocol(host)
       response = await task_protocol.call_remote(
           remote_multiaddr,
           task_id="score-batch-001",
           payload=b"...",
       )

Developers implementing ``TaskProtocol`` do not need to manually read varint
length prefixes, loop over partial reads, parse raw stream bytes, write framed
responses, dial peers, or remember to close the libp2p stream. For non-protobuf
payloads, subclass ``ProtocolBase`` directly and implement the encode/decode
hooks yourself.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass
import inspect
import logging
from typing import Generic, TypeVar, cast

from google.protobuf.message import Message
from libp2p.abc import IHost, INetStream
from libp2p.tools.utils import info_from_p2p_addr
from multiaddr import Multiaddr
import varint

logger = logging.getLogger(__name__)

MAX_FRAME_LEN = 2**32 - 1
MAX_STREAM_WRITE_LEN = 60 * 1024
MAX_VARINT_PREFIX_LEN = 10

RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")
RequestMessageT = TypeVar("RequestMessageT", bound=Message)
ResponseMessageT = TypeVar("ResponseMessageT", bound=Message)


@dataclass(frozen=True, slots=True)
class IncomingRequestContext:
    """Metadata available while handling a direct peer request."""

    peer_id: str
    protocol_id: str
    stream: INetStream


class ProtocolBase(Generic[RequestT, ResponseT], ABC):
    """
    Base class for unary direct peer protocols over libp2p streams.

    Subclasses provide only protocol-specific encode/decode and request handling
    hooks. This template owns the stream lifecycle, varint frame parsing, exact
    reads, response framing, and peer dialing.
    """

    def __init__(
        self,
        host: IHost,
        protocol_id: str,
        *,
        log: logging.Logger | None = None,
        max_frame_len: int = MAX_FRAME_LEN,
        max_stream_write_len: int = MAX_STREAM_WRITE_LEN,
        auto_register: bool = True,
    ) -> None:
        if not protocol_id:
            raise ValueError("protocol_id must be a non-empty string")
        if max_frame_len < 0:
            raise ValueError("max_frame_len must be non-negative")
        if max_stream_write_len <= 0:
            raise ValueError("max_stream_write_len must be greater than zero")

        self.host = host
        self.protocol_id = protocol_id
        self.logger = log or logger
        self.max_frame_len = max_frame_len
        self.max_stream_write_len = max_stream_write_len

        if auto_register:
            self.register_stream_handler()

    def register_stream_handler(self) -> None:
        """Register this protocol's inbound stream handler with the host."""
        self.host.set_stream_handler(self.protocol_id, self._handle_incoming_stream)

    async def call_remote(
        self,
        destination: Multiaddr | str,
        request: RequestT,
        *,
        peer_id: str | None = None,
    ) -> ResponseT:
        """Dial a peer, send one framed request, and return one framed response."""
        stream: INetStream | None = None
        stream_peer_id = peer_id

        try:
            dial_addr = self._normalize_dial_addr(destination, peer_id=peer_id)
            info = info_from_p2p_addr(dial_addr)
            stream_peer_id = self._coerce_peer_id(info.peer_id)

            await self.host.connect(info)
            stream = await self.host.new_stream(info.peer_id, [self.protocol_id])

            await self._write_frame(stream, self.encode_request(request))
            response_bytes = await self._read_frame(stream)
            return self.decode_response(response_bytes)
        except Exception:
            self.logger.exception("Failed %s request to peer %s", self.protocol_id, stream_peer_id)
            raise
        finally:
            if stream is not None:
                await self._close_stream(stream, stream_peer_id)

    async def _handle_incoming_stream(self, stream: INetStream) -> None:
        """Read one framed request, dispatch it, and write one framed response."""
        from_peer = self._coerce_peer_id(stream.muxed_conn.peer_id)
        context = IncomingRequestContext(peer_id=from_peer, protocol_id=self.protocol_id, stream=stream)

        try:
            request = self.decode_request(await self._read_frame(stream))
            response = self.handle_request(context, request)
            if inspect.isawaitable(response):
                response = await cast(Awaitable[ResponseT], response)
            await self._write_frame(stream, self.encode_response(response))
        except Exception:
            self.logger.exception("Error handling %s stream from %s", self.protocol_id, from_peer)
        finally:
            await self._close_stream(stream, from_peer)

    @abstractmethod
    def encode_request(self, request: RequestT) -> bytes:
        """Serialize an outbound request."""

    @abstractmethod
    def decode_request(self, payload: bytes) -> RequestT:
        """Parse an inbound request payload."""

    @abstractmethod
    def handle_request(
        self,
        context: IncomingRequestContext,
        request: RequestT,
    ) -> ResponseT | Awaitable[ResponseT]:
        """Return the response for an inbound request."""

    @abstractmethod
    def encode_response(self, response: ResponseT) -> bytes:
        """Serialize an outbound response."""

    @abstractmethod
    def decode_response(self, payload: bytes) -> ResponseT:
        """Parse an inbound response payload."""

    async def _read_frame(self, stream: INetStream) -> bytes:
        length_prefix = b""
        while True:
            byte = await stream.read(1)
            if not byte:
                raise EOFError("Stream closed while reading message length")
            length_prefix += byte
            if len(length_prefix) > MAX_VARINT_PREFIX_LEN:
                raise ValueError("Frame length prefix is too long")
            if byte[0] & 0x80 == 0:
                break

        msg_length = varint.decode_bytes(length_prefix)
        if msg_length > self.max_frame_len:
            raise ValueError(f"Frame length {msg_length} exceeds maximum {self.max_frame_len}")
        return await self._read_exact(stream, msg_length)

    async def _read_exact(self, stream: INetStream, size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size

        while remaining > 0:
            chunk = await stream.read(remaining)
            if not chunk:
                raise EOFError("Stream closed before full frame was received")
            chunks.append(chunk)
            remaining -= len(chunk)

        return b"".join(chunks)

    async def _write_frame(self, stream: INetStream, payload: bytes) -> None:
        if len(payload) > self.max_frame_len:
            raise ValueError(f"Frame length {len(payload)} exceeds maximum {self.max_frame_len}")

        await stream.write(varint.encode(len(payload)))
        for offset in range(0, len(payload), self.max_stream_write_len):
            await stream.write(payload[offset : offset + self.max_stream_write_len])

    async def _close_stream(self, stream: INetStream, peer_id: str | None) -> None:
        try:
            await stream.close()
        except Exception:
            self.logger.debug("Failed to close %s stream for peer %s", self.protocol_id, peer_id, exc_info=True)

    @staticmethod
    def _coerce_peer_id(peer_id: object) -> str:
        to_string = getattr(peer_id, "to_string", None)
        if callable(to_string):
            return to_string()
        return str(peer_id)

    @staticmethod
    def _normalize_dial_addr(destination: Multiaddr | str, *, peer_id: str | None) -> Multiaddr:
        addr = str(destination).rstrip("/")
        if "/ipfs/" in addr:
            addr = addr.replace("/ipfs/", "/p2p/")
        if "/p2p/" not in addr:
            if peer_id is None:
                raise ValueError(f"Destination {addr!r} is missing a /p2p/<peer_id> component")
            addr = f"{addr}/p2p/{peer_id}"
        return Multiaddr(addr)


class ProtobufRequestResponseProtocolTemplate(
    ProtocolBase[RequestMessageT, ResponseMessageT],
):
    """
    Request/response template for protobuf-backed direct peer protocols.

    Subclasses only need to implement ``handle_request`` when protobuf request
    and response classes are supplied.
    """

    def __init__(
        self,
        host: IHost,
        protocol_id: str,
        *,
        request_message_type: type[RequestMessageT],
        response_message_type: type[ResponseMessageT],
        log: logging.Logger | None = None,
        max_frame_len: int = MAX_FRAME_LEN,
        max_stream_write_len: int = MAX_STREAM_WRITE_LEN,
        auto_register: bool = True,
    ) -> None:
        self.request_message_type = request_message_type
        self.response_message_type = response_message_type
        super().__init__(
            host=host,
            protocol_id=protocol_id,
            log=log,
            max_frame_len=max_frame_len,
            max_stream_write_len=max_stream_write_len,
            auto_register=auto_register,
        )

    def encode_request(self, request: RequestMessageT) -> bytes:
        return request.SerializeToString()

    def decode_request(self, payload: bytes) -> RequestMessageT:
        message = self.request_message_type()
        message.ParseFromString(payload)
        return message

    def encode_response(self, response: ResponseMessageT) -> bytes:
        return response.SerializeToString()

    def decode_response(self, payload: bytes) -> ResponseMessageT:
        message = self.response_message_type()
        message.ParseFromString(payload)
        return message
