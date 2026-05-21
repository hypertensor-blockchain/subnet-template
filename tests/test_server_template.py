from __future__ import annotations

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair

from subnet.server.server_template import ServerBase


class FakeProviderStore:
    def __init__(self) -> None:
        self.provided: list[bytes] = []

    async def provide(self, key: bytes) -> bool:
        self.provided.append(key)
        return True


class FakeDHT:
    def __init__(self) -> None:
        self.provided: list[str] = []
        self.provider_store = FakeProviderStore()

    async def provide(self, key: str) -> bool:
        key.encode("utf-8")
        self.provided.append(key)
        return True


def _server(**kwargs) -> ServerBase:
    return ServerBase(
        port=0,
        key_pair=create_new_key_pair(bytes([1]) * 32),
        apply_libp2p_patches=False,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_server_template_provides_configured_dht_keys() -> None:
    server = _server(dht_provide_keys=["text-key", b"\x00content-key"])
    dht = FakeDHT()

    await server._provide_dht_keys(dht)  # type: ignore[arg-type]

    assert dht.provided == ["text-key"]
    assert dht.provider_store.provided == [b"\x00content-key"]


def test_server_template_treats_single_dht_provide_key_as_one_key() -> None:
    assert _server(dht_provide_keys="content-key").dht_provide_keys == ("content-key",)
    assert _server(dht_provide_keys=b"content-key").dht_provide_keys == (b"content-key",)
