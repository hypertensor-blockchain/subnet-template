from __future__ import annotations

import json

import pytest
import varint

from subnet.protocols.api_protocol import (
    ApiProtocol,
    ApiProtocolConfig,
    ApiRouteConfig,
)
from subnet.protocols.pb.api_protocol_pb2 import ApiProtocolMessage


class DummyMuxedConn:
    def __init__(self, peer_id: str = "peer-remote"):
        self.peer_id = peer_id


class FakeStream:
    def __init__(self, incoming: bytes):
        self._incoming = bytearray(incoming)
        self.written = bytearray()
        self.closed = False
        self.muxed_conn = DummyMuxedConn()

    async def read(self, size: int = -1) -> bytes:
        if not self._incoming:
            return b""
        if size < 0:
            size = len(self._incoming)
        data = bytes(self._incoming[:size])
        del self._incoming[:size]
        return data

    async def write(self, payload: bytes) -> None:
        self.written.extend(payload)

    async def close(self) -> None:
        self.closed = True


class FakeHost:
    def __init__(self):
        self.handler = None

    def set_stream_handler(self, _protocol_id, handler) -> None:
        self.handler = handler


class FakeHttpResponse:
    def __init__(self, chunks: tuple[bytes, ...]):
        self.chunks = chunks

    async def aread(self) -> bytes:
        return b"".join(self.chunks)

    async def aiter_bytes(self):
        for chunk in self.chunks:
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakeHttpClient:
    def __init__(self, chunks: tuple[bytes, ...]):
        self.chunks = chunks
        self.calls = []

    def stream(self, **kwargs):
        self.calls.append(kwargs)
        return FakeHttpResponse(self.chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


def _framed_message(message: ApiProtocolMessage) -> bytes:
    payload = message.SerializeToString()
    return varint.encode(len(payload)) + payload


def test_api_protocol_config_reads_route_objects_from_file(tmp_path):
    config_file = tmp_path / "api_routes.json"
    config_file.write_text(
        json.dumps(
            {
                "events": {
                    "url": "http://127.0.0.1:8000/events",
                    "stream": True,
                },
            },
        ),
    )
    config = ApiProtocolConfig(
        routes={
            "events": {
                "url": "http://127.0.0.1:8000/fallback",
                "stream": False,
            },
        },
        config_file=str(config_file),
    )

    route_config = config.get_route_config("events")

    assert route_config == ApiRouteConfig(url="http://127.0.0.1:8000/events", stream=True)
    assert config.get_route("events") == "http://127.0.0.1:8000/events"


def test_api_protocol_config_supports_legacy_url_shorthand_as_non_streaming():
    config = ApiProtocolConfig(routes={"health": "http://127.0.0.1:8000/health"})

    assert config.get_route_config("health") == ApiRouteConfig(
        url="http://127.0.0.1:8000/health",
        stream=False,
    )


@pytest.mark.asyncio
async def test_api_protocol_rejects_stream_request_for_non_streaming_route():
    protocol = ApiProtocol(
        FakeHost(),
        config=ApiProtocolConfig(
            routes={
                "health": {
                    "url": "http://127.0.0.1:8000/health",
                    "stream": False,
                },
            },
        ),
    )
    stream = FakeStream(
        _framed_message(
            ApiProtocolMessage(
                route="health",
                method="GET",
                response_type=ApiProtocolMessage.STREAM,
            ),
        ),
    )

    await protocol._handle_incoming_stream(stream)

    assert bytes(stream.written) == b"Route does not support streaming: health"
    assert stream.closed is True


@pytest.mark.asyncio
async def test_api_protocol_streams_when_route_allows_streaming(monkeypatch):
    http_client = FakeHttpClient(chunks=(b"chunk-1", b"chunk-2"))
    monkeypatch.setattr("subnet.protocols.api_protocol.httpx.AsyncClient", lambda: http_client)
    protocol = ApiProtocol(
        FakeHost(),
        config=ApiProtocolConfig(
            routes={
                "events": {
                    "url": "http://127.0.0.1:8000/events",
                    "stream": True,
                },
            },
        ),
    )
    stream = FakeStream(
        _framed_message(
            ApiProtocolMessage(
                route="events",
                method="GET",
                response_type=ApiProtocolMessage.STREAM,
            ),
        ),
    )

    await protocol._handle_incoming_stream(stream)

    assert bytes(stream.written) == b"chunk-1chunk-2"
    assert http_client.calls[0]["url"] == "http://127.0.0.1:8000/events"
    assert stream.closed is True
