"""Common type aliases for the Merkle DAG subsystem."""

from __future__ import annotations

from typing import TypeAlias

JSONScalar: TypeAlias = None | bool | int | float | str
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
