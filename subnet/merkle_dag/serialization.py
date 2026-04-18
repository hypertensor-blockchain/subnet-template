"""Deterministic serialization for Merkle DAG hashing and signatures."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from typing import Any

from subnet.merkle_dag.exceptions import SerializationError
from subnet.merkle_dag.types import JSONValue


class CanonicalJSONSerializer:
    """
    Canonical JSON using sorted keys and whitespace-free encoding.

    Canonical JSON is chosen here because it is human-readable, already available in the
    standard library, and deterministic enough for hashing/signing when paired with strict
    normalization and `allow_nan=False`.
    """

    def normalize(self, value: Any) -> JSONValue:
        """Normalize a JSON-compatible value into a stable primitive form."""
        if value is None or isinstance(value, bool | int | float | str):
            return value
        if isinstance(value, Mapping):
            normalized: dict[str, JSONValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise SerializationError("Canonical JSON only supports string object keys")
                normalized[key] = self.normalize(item)
            return normalized
        if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
            return [self.normalize(item) for item in value]
        raise SerializationError(f"Unsupported canonical JSON value: {type(value)!r}")

    def serialize(self, value: Any) -> bytes:
        """Serialize a value into canonical JSON bytes."""
        try:
            normalized = self.normalize(value)
            return json.dumps(
                normalized,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except SerializationError:
            raise
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise SerializationError(str(exc)) from exc

    def deserialize(self, payload: bytes) -> JSONValue:
        """Deserialize canonical JSON bytes."""
        try:
            value = json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise SerializationError(str(exc)) from exc
        return self.normalize(value)
