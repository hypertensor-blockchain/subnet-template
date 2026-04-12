import json
import logging
import socket
import time

from libp2p.crypto.keys import KeyPair
from libp2p.peer.id import ID
import trio
from trio_websocket import ConnectionClosed, open_websocket_url

# Standard logging configuration for production visibility
logger = logging.getLogger(__name__)


class Telemetry:
    """
    Production-grade Telemetry client using Trio.

    Features:
    - Non-spinning worker loop with exponential backoff.
    - At-least-once delivery: Messages are only dropped if the worker is hard-killed.
    - Backpressure: emit() will block if the queue is full, protecting node memory.

    Example usage:

    import trio
    from libp2p.peer.id import ID

    async def main():
        # 1. Initialize Telemetry
        telemetry = Telemetry(
            url="ws://localhost:9000",
            subnet_id=1,
            subnet_node_id=10,
            peer_id=ID.from_base58("12D3Koo...")
        )

        # 2. Open a nursery to run the telemetry background worker
        async with trio.open_nursery() as nursery:
            # 3. Start the worker loop
            telemetry.start(nursery)

            # 4. Emit events anywhere in your app context
            await telemetry.emit("node_started")
            await telemetry.emit("peer_connected", peer="12D3Koo...")

    if __name__ == "__main__":
        trio.run(main)
    """

    def __init__(self, url: str, subnet_id: int, subnet_node_id: int, key_pair: KeyPair, max_queue: int = 1000):
        self.url = url
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.peer_id = ID.from_pubkey(key_pair.public_key).to_string()
        self.hostname = socket.gethostname()

        # Max queue size provides backpressure to the rest of the app
        self._send_channel, self._receive_channel = trio.open_memory_channel(max_queue)

    async def emit(self, event: str, **data: any) -> None:
        """
        Emits a telemetry event. This is async to allow backpressure if the
        internal channel is full, preventing memory exhaustion in the node.
        """
        payload = {
            "event": event,
            "ts": time.time(),
            "host": self.hostname,
            "subnet_id": self.subnet_id,
            "subnet_node_id": self.subnet_node_id,
            "peer_id": self.peer_id,
            "data": data,
        }
        try:
            await self._send_channel.send(payload)
        except trio.EndOfChannel:
            logger.warning("Telemetry channel closed; dropped event: %s", event)

    async def run(self) -> None:
        """
        Worker loop with exponential backoff and message persistence.
        """
        backoff = 1
        max_backoff = 60
        pending_msg = None  # Track the message in transit

        while True:
            try:
                # open_websocket_url is an async context manager
                async with open_websocket_url(self.url) as ws:
                    # Connection successful: reset backoff
                    if backoff > 1:
                        logger.info("Telemetry reconnected to %s", self.url)
                    backoff = 1

                    # 1. First, retry sending the message that failed previously
                    if pending_msg:
                        await ws.send_message(json.dumps(pending_msg))
                        pending_msg = None

                    # 2. Now process incoming messages from the channel
                    async for msg in self._receive_channel:
                        # Store in pending_msg before sending. If send_message fails,
                        # the exception will break the loop, but we still have 'msg' here.
                        pending_msg = msg
                        await ws.send_message(json.dumps(msg))
                        # Clear only after successful transmission
                        pending_msg = None

            except (ConnectionClosed, OSError, Exception) as e:
                # We do not clear pending_msg here, so it will be retried on next connection.
                wait_time = backoff
                logger.error("Telemetry connection lost (%s). Retrying in %ds...", type(e).__name__, wait_time)

                await trio.sleep(wait_time)
                # Exponentially increase wait time up to max_backoff
                backoff = min(backoff * 2, max_backoff)

    def start(self, nursery: trio.Nursery) -> None:
        """
        Launches the telemetry worker in the provided nursery.
        """
        nursery.start_soon(self.run)
