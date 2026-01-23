import logging
import sys

from libp2p.abc import (
    IHost,
)
from libp2p.custom_types import (
    TProtocol,
)
from libp2p.host.exceptions import (
    ConnectionFailure,
)
from libp2p.kad_dht.kad_dht import KadDHT
from libp2p.network.stream.net_stream import (
    INetStream,
)
from libp2p.peer.id import ID as PeerID
import trio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("ping/1.0.0")

PING_PROTOCOL_ID = TProtocol("/ipfs/ping/1.0.0")
PING_LENGTH = 32
RESP_TIMEOUT = 60


async def handle_ping(stream: INetStream) -> None:
    while True:
        try:
            payload = await stream.read(PING_LENGTH)
            peer_id = stream.muxed_conn.peer_id
            if payload is not None:
                print(f"received ping from {peer_id}")

                await stream.write(payload)
                print(f"responded with pong to {peer_id}")

        except Exception:
            await stream.reset()
            break


async def send_ping(stream: INetStream) -> bool:
    try:
        payload = b"\x01" * PING_LENGTH
        logger.info(f"sending ping to {stream.muxed_conn.peer_id}")

        await stream.write(payload)

        with trio.fail_after(RESP_TIMEOUT):
            response = await stream.read(PING_LENGTH)

        if response == payload:
            logger.info(f"received pong from {stream.muxed_conn.peer_id}")
            return True

    except Exception as e:
        logger.info(f"error occurred : {e}")
    return False


class PingProtocol:
    def __init__(self, host: IHost, dht: KadDHT):
        self.host = host
        self.dht = dht
        self.lock = trio.Lock()

    async def ping(self, peer_id: PeerID, close_stream: bool = True) -> bool:
        try:
            logger.info(f"ping, Pinging peer ID {peer_id}")
            if self.dht:
                try:
                    peer_info = await self.dht.find_peer(peer_id)
                    if peer_info is None:
                        raise ConnectionFailure("Unable to find Peer address")
                    logger.debug(f"ping, Found peer ID {peer_id}: {peer_info}")
                except Exception as e:
                    logger.debug(f"ping, Error finding peer ID {peer_id}: {e}")

            stream = await self._create_stream_with_retry(peer_id)

            success = await send_ping(stream)

            if close_stream:
                await stream.close()
                logger.debug("ping, closing stream")

            return success
        except Exception as e:
            logger.debug(f"error occurred : {e}")
            return False

    async def _create_stream_with_retry(
        self, peer_id: PeerID, max_retries: int = 3, retry_delay: float = 0.5
    ) -> INetStream:
        """Create ping stream with retry mechanism for connection readiness."""
        logger.info(f"About to create stream for protocol {PING_PROTOCOL_ID}")

        for attempt in range(max_retries):
            try:
                stream = await self.host.new_stream(peer_id, [PING_PROTOCOL_ID])
                logger.info(f"Ping stream created successfully for peer {peer_id.to_base58()}")
                return stream
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.info(
                        f"Stream creation attempt {attempt + 1} for peer {peer_id.to_base58()} failed: {e}, retrying..."
                    )
                    await trio.sleep(retry_delay)
                else:
                    logger.info(
                        f"Stream creation failed after {max_retries} attempts for peer {peer_id.to_base58()}: {e}"
                    )
                    raise
        raise RuntimeError("Failed to create ping stream after retries")
