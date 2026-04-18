import json
from dataclasses import dataclass

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from pydantic import BaseModel

from subnet.telemetry.telemetry import Telemetry


def _build_telemetry(*, max_queue: int = 1000) -> Telemetry:
    return Telemetry(
        url="ws://127.0.0.1:8080/ingest",
        subnet_id=1,
        subnet_node_id=2,
        key_pair=create_new_key_pair(bytes([7]) * 32),
        max_queue=max_queue,
    )


class _ModelPayload(BaseModel):
    name: str


@dataclass
class _DataclassPayload:
    count: int


class _PeerLike:
    def to_string(self) -> str:
        return "peer-123"


def test_emit_returns_false_when_queue_is_full(caplog: pytest.LogCaptureFixture) -> None:
    telemetry = _build_telemetry(max_queue=1)

    assert telemetry.emit("first_event") is True

    with caplog.at_level("WARNING"):
        assert telemetry.emit("second_event") is False

    assert "Telemetry queue full (1); dropped event: second_event" in caplog.text


def test_build_payload_normalizes_values_before_queueing() -> None:
    telemetry = _build_telemetry()

    payload = telemetry._build_payload(
        "event_with_models",
        {
            "peer": _PeerLike(),
            "model": _ModelPayload(name="alice"),
            "dataclass_payload": _DataclassPayload(count=3),
            "raw": b"\xff",
        },
    )

    assert payload["event"] == "event_with_models"
    assert payload["data"] == {
        "peer": "peer-123",
        "model": {"name": "alice"},
        "dataclass_payload": {"count": 3},
        "raw": "ff",
    }
    assert "signature" in payload
    assert "pubkey" in payload

    json.loads(json.dumps(payload))
