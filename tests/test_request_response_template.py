from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair as create_ed25519_key_pair
from libp2p.peer.id import ID
from multiaddr import Multiaddr
import varint

from subnet.protocols.mock_protocol_v2 import (
    PROTOCOL_ID as MOCK_PROTOCOL_ID,
    MockProtocolV2,
)
from subnet.protocols.pb.mock_protocol_pb2 import MockProtocolMessage
from subnet.protocols.protocol_base import (
    IncomingRequestContext,
    ProtocolBase,
)


class DummyMuxedConn:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id


class FakeStream:
    def __init__(
        self,
        *,
        incoming: bytes = b"",
        peer_id: str = "peer-remote",
        max_read_chunk: int | None = None,
    ):
        self._incoming = bytearray(incoming)
        self.max_read_chunk = max_read_chunk
        self.written = bytearray()
        self.write_calls: list[bytes] = []
        self.closed = False
        self.muxed_conn = DummyMuxedConn(peer_id)

    async def read(self, size: int = -1) -> bytes:
        if not self._incoming:
            return b""
        if size < 0:
            size = len(self._incoming)
        if self.max_read_chunk is not None:
            size = min(size, self.max_read_chunk)
        data = bytes(self._incoming[:size])
        del self._incoming[:size]
        return data

    async def write(self, payload: bytes) -> None:
        self.write_calls.append(payload)
        self.written.extend(payload)

    async def close(self) -> None:
        self.closed = True


class FakeHost:
    def __init__(self, *, stream: FakeStream | None = None):
        self.stream = stream
        self.protocol_id = None
        self.handler = None
        self.connected = []
        self.opened_streams = []

    def set_stream_handler(self, protocol_id, handler) -> None:
        self.protocol_id = protocol_id
        self.handler = handler

    async def connect(self, info) -> None:
        self.connected.append(info)

    async def new_stream(self, peer_id, protocols):
        self.opened_streams.append((peer_id, tuple(protocols)))
        return self.stream


class TextProtocol(ProtocolBase[str, str]):
    def __init__(self, host: FakeHost):
        self.handled: list[tuple[str, str]] = []
        super().__init__(host, "/test/text/1.0.0")

    def encode_request(self, request: str) -> bytes:
        return request.encode("utf-8")

    def decode_request(self, payload: bytes) -> str:
        return payload.decode("utf-8")

    async def handle_request(self, context: IncomingRequestContext, request: str) -> str:
        self.handled.append((context.peer_id, request))
        return f"{request.upper()}:{context.peer_id}"

    def encode_response(self, response: str) -> bytes:
        return response.encode("utf-8")

    def decode_response(self, payload: bytes) -> str:
        return payload.decode("utf-8")


def _peer_id(seed_byte: int) -> str:
    return ID.from_pubkey(create_ed25519_key_pair(bytes([seed_byte]) * 32).public_key).to_string()


def _frame(payload: bytes) -> bytes:
    return varint.encode(len(payload)) + payload


def _read_frame(payload: bytes) -> bytes:
    prefix = bytearray()
    index = 0
    while True:
        prefix.append(payload[index])
        index += 1
        if prefix[-1] & 0x80 == 0:
            break
    length = varint.decode_bytes(bytes(prefix))
    return payload[index : index + length]


@pytest.mark.asyncio
async def test_template_handles_incoming_framed_request_with_partial_reads():
    inbound_stream = FakeStream(incoming=_frame(b"hello"), max_read_chunk=2)
    protocol = TextProtocol(FakeHost())

    await protocol._handle_incoming_stream(inbound_stream)

    assert protocol.handled == [("peer-remote", "hello")]
    assert _read_frame(bytes(inbound_stream.written)) == b"HELLO:peer-remote"
    assert inbound_stream.closed is True


@pytest.mark.asyncio
async def test_template_call_remote_writes_and_reads_framed_payloads():
    remote_peer_id = _peer_id(22)
    response_stream = FakeStream(incoming=_frame(b"pong"))
    host = FakeHost(stream=response_stream)
    protocol = TextProtocol(host)
    destination = Multiaddr(f"/ip4/127.0.0.1/tcp/9002/p2p/{remote_peer_id}")

    response = await protocol.call_remote(destination, "ping")

    assert response == "pong"
    assert _read_frame(bytes(response_stream.written)) == b"ping"
    assert host.protocol_id == "/test/text/1.0.0"
    assert len(host.connected) == 1
    assert len(host.opened_streams) == 1
    assert host.opened_streams[0][1] == ("/test/text/1.0.0",)
    assert response_stream.closed is True


@pytest.mark.asyncio
async def test_mock_protocol_v2_uses_protobuf_template_handler():
    request = MockProtocolMessage(type=MockProtocolMessage.TASK_REQUEST)
    inbound_stream = FakeStream(incoming=_frame(request.SerializeToString()))
    host = FakeHost()
    protocol = MockProtocolV2(host)

    await protocol._handle_incoming_stream(inbound_stream)

    response = MockProtocolMessage()
    response.ParseFromString(_read_frame(bytes(inbound_stream.written)))

    assert host.protocol_id == MOCK_PROTOCOL_ID
    assert response.type == MockProtocolMessage.TASK_REQUEST
    assert inbound_stream.closed is True
