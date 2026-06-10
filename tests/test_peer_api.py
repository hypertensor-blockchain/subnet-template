import pytest
from libp2p import create_yamux_muxer_option, new_host
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.peer.peerinfo import info_from_p2p_addr
from libp2p.security.insecure.transport import PLAINTEXT_PROTOCOL_ID, InsecureTransport
from multiaddr import Multiaddr
import trio

from subnet.protocols.api_protocol import ApiProtocol, ApiProtocolConfig


async def create_tcp_host_pair():
    """Create a pair of hosts configured for TCP communication."""
    # Create key pairs
    key_pair_a = create_new_key_pair()
    key_pair_b = create_new_key_pair()

    # Create security options (using plaintext for simplicity)
    def security_options(kp):
        return {PLAINTEXT_PROTOCOL_ID: InsecureTransport(local_key_pair=kp, secure_bytes_provider=None, peerstore=None)}

    # Host A (listener) - TCP transport (default)
    host_a = new_host(
        key_pair=key_pair_a,
        sec_opt=security_options(key_pair_a),
        muxer_opt=create_yamux_muxer_option(),
        listen_addrs=[Multiaddr("/ip4/127.0.0.1/tcp/0")],
    )

    # Host B (dialer) - TCP transport (default)
    host_b = new_host(
        key_pair=key_pair_b,
        sec_opt=security_options(key_pair_b),
        muxer_opt=create_yamux_muxer_option(),
        listen_addrs=[Multiaddr("/ip4/127.0.0.1/tcp/0")],
    )

    return host_a, host_b


# python -m pytest tests/test_peer_api.py::test_api_protocol_unary_and_stream_v1 -rP


@pytest.mark.trio
async def test_api_protocol_unary_and_stream_v1():
    host_a, host_b = await create_tcp_host_pair()

    # Node 2 API Routes
    api_routes = {
        "unary_test": {
            "url": "http://localhost:8080/unary",
            "stream": False,
        },
        "stream_test": {
            "url": "http://localhost:8080/stream",
            "stream": True,
        },
    }

    # Set up API protocols
    proto1 = ApiProtocol(host_a, config=ApiProtocolConfig(routes=api_routes))
    ApiProtocol(host_b)

    async with (
        host_a.run(listen_addrs=[Multiaddr("/ip4/127.0.0.1/tcp/0")]),
        host_b.run(listen_addrs=[]),
    ):
        listen_addrs = host_a.get_addrs()
        assert listen_addrs, "Host A should have listen addresses"

        tcp_addr_a = None
        for addr in listen_addrs:
            if "/tcp/" in str(addr) and "/ws" not in str(addr):
                tcp_addr_a = addr
                break

        assert tcp_addr_a, f"No TCP address found in {listen_addrs}"

        # Create peer info for host A
        peer_info = info_from_p2p_addr(tcp_addr_a)

        # Host B connects to host A
        await host_b.connect(peer_info)

        # Allow some time for connection to propagate
        await trio.sleep(1.0)

        # Define the proper mock structure for httpx overrides inside pytest
        class MockResp:
            def __init__(self, method, url, content_data):
                self.content_data = content_data

            async def aread(self):
                return (
                    f"Mock Unary Response to: {self.content_data.decode('utf-8') if self.content_data else ''}".encode(
                        "utf-8"
                    )
                )

            async def aiter_bytes(self):
                for i in range(5):
                    yield f"Mock Chunk {i}\n".encode("utf-8")
                    await trio.sleep(0.01)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

        class MockSession:
            def stream(self, method, url, headers=None, content=None):
                return MockResp(method, url, content)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

        from unittest.mock import patch

        with patch("subnet.protocols.api_protocol.httpx.AsyncClient", return_value=MockSession()):
            response_bytes = await proto1.call_remote(
                destination=tcp_addr_a,
                route="unary_test",
                method="POST",
                body=b"Hello API",
            )

            print(f"Response: {response_bytes}")

            stream_chunks = []
            async for chunk in proto1.stream_remote(
                destination=tcp_addr_a,
                route="stream_test",
                method="GET",
            ):
                stream_chunks.append(chunk)

            assert len(stream_chunks) > 0
            assert b"Mock Chunk" in stream_chunks[0]

        print("\nCleaning up...")
        await host_a.get_network().close()
        await host_b.get_network().close()
