from libp2p.network.stream.net_stream_interface import INetStream
from libp2p.typing import TStreamHandler
from subnet.network.pos.proof_of_stake import ProofOfStake
from libp2p.host.basic_host import BasicHost
import logging

logger = logging.getLogger("pos_middleware")


class PoSMiddleware:
    def __init__(self, pos: ProofOfStake):
        self.pos = pos

    async def _middleware(
        self, stream: INetStream, original_handler: TStreamHandler
    ) -> None:
        remote_peer_id = stream.muxed_conn.peer_id

        logger.debug(f"Checking PoS for stream from {remote_peer_id}")

        if not self.pos.proof_of_stake(remote_peer_id):
            logger.warning(
                f"PoS verification failed for {remote_peer_id}. Resetting stream."
            )
            await stream.reset()
            return

        logger.debug(f"PoS verified for {remote_peer_id}. Proceeding to handler.")
        await original_handler(stream)

    def middleware(self, handler: TStreamHandler) -> TStreamHandler:
        async def wrapper(stream: INetStream) -> None:
            await self._middleware(stream, handler)

        return wrapper


def apply_pos_middleware(host: BasicHost, pos: ProofOfStake) -> None:
    """
    Wraps the host's set_stream_handler method to automatically apply
    PoS middleware to all registered handlers.
    """
    middleware = PoSMiddleware(pos)
    original_set_stream_handler = host.set_stream_handler

    def set_stream_handler_wrapper(
        protocol_id: str, stream_handler: TStreamHandler
    ) -> None:
        wrapped_handler = middleware.middleware(stream_handler)
        original_set_stream_handler(protocol_id, wrapped_handler)

    host.set_stream_handler = set_stream_handler_wrapper
    logger.info("PoS Middleware applied to host.set_stream_handler")
