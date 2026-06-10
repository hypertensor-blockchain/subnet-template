import dataclasses
import json
import logging
import socket
import time
from typing import Any

from libp2p.crypto.keys import KeyPair
from libp2p.peer.id import ID
import trio
from trio_websocket import ConnectionClosed, open_websocket_url

# Standard logging configuration for production visibility
logger = logging.getLogger(__name__)


class Telemetry:
    """
    elemetry client

    Features:
    - Non-spinning worker loop with exponential backoff.
    - At-least-once delivery: Messages are only dropped if the worker is hard-killed.
    - Bounded queueing: emit_async() applies backpressure.

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
            await telemetry.emit_async("node_started")
            await telemetry.emit_async("peer_connected", peer="12D3Koo...")

    if __name__ == "__main__":
        trio.run(main)

    Receiver-side Verification:

    To verify these events on your telemetry endpoint (Python example):

    import json
    from libp2p.crypto.keys import unmarshal_public_key

    def verify_event(json_payload):
        # 1. Parse and extract security fields
        data = json.loads(json_payload)
        signature = bytes.fromhex(data.pop("signature"))
        pubkey_hex = data.pop("pubkey")

        # 2. Re-create the canonical signed data (must use sort_keys=True)
        canonical_data = json.dumps(data, sort_keys=True).encode()

        # 3. Unmarshal the public key and verify
        public_key = unmarshal_public_key(bytes.fromhex(pubkey_hex))
        return public_key.verify(canonical_data, signature)
    """

    def __init__(self, url: str, subnet_id: int, subnet_node_id: int, key_pair: KeyPair, max_queue: int = 1000):
        self.url = url
        self.subnet_id = subnet_id
        self.subnet_node_id = subnet_node_id
        self.key_pair = key_pair
        self.peer_id = ID.from_pubkey(key_pair.public_key).to_string()
        self.hostname = socket.gethostname()
        self.max_queue = max_queue

        # Max queue size provides backpressure to the rest of the app
        self._send_channel, self._receive_channel = trio.open_memory_channel(max_queue)

    async def emit_async(self, event: str, **data: Any) -> None:
        """
        Emits a telemetry event. This is async to allow backpressure if the
        internal channel is full, preventing memory exhaustion in the node.
        """
        payload = self._build_payload(event, data)
        try:
            await self._send_channel.send(payload)
        except trio.EndOfChannel:
            logger.warning("Telemetry channel closed; dropped event: %s", event)

    async def run(self) -> None:
        """
        Worker loop with exponential backoff and retry of the in-flight message.
        """
        logger.info("Starting Telemetry")
        backoff = 1
        max_backoff = 120
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
                        logger.info("Retrying pending telemetry event %s", pending_msg.get("event"))
                        await ws.send_message(self._dump_json(pending_msg))
                        logger.info("Telemetry message sent: %s", pending_msg.get("event"))
                        pending_msg = None

                    # 2. Now process incoming messages from the channel
                    async for msg in self._receive_channel:
                        pending_msg = msg
                        logger.info("Attempting telemetry send: %s", msg.get("event"))
                        await ws.send_message(self._dump_json(msg))
                        logger.info("Telemetry message sent: %s", msg.get("event"))

                        # Clear only after successful transmission
                        pending_msg = None

            except (ConnectionClosed, OSError) as e:
                # We do not clear pending_msg here, so it will be retried on next connection.
                wait_time = backoff
                logger.error("Telemetry connection lost (%s). Retrying in %ds...", type(e).__name__, wait_time)

                await trio.sleep(wait_time)
                # Exponentially increase wait time up to max_backoff
                backoff = min(backoff * 2, max_backoff)
            except Exception:
                wait_time = backoff
                logger.exception("Telemetry worker failed while processing an event. Retrying in %ds...", wait_time)

                await trio.sleep(wait_time)
                backoff = min(backoff * 2, max_backoff)

    def _sign_payload(self, payload: dict) -> dict:
        """
        Adds a cryptographic signature and public key to the payload.
        Ensures the telemetry endpoint can verify the sender's identity.
        """
        # 1. Create a canonical JSON string (keys sorted) for deterministic signing
        canonical_data = self._dump_json(payload, sort_keys=True).encode()

        # 2. Sign with the private key
        signature = self.key_pair.private_key.sign(canonical_data)

        # 3. Add verification data to the payload
        signed_payload = dict(payload)
        signed_payload["signature"] = signature.hex()
        signed_payload["pubkey"] = self.key_pair.public_key.serialize().hex()

        return signed_payload

    def _build_payload(self, event: str, data: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "event": event,
            "timestamp": time.time(),
            "host": self.hostname,
            "subnet_id": self.subnet_id,
            "subnet_node_id": self.subnet_node_id,
            "peer_id": self.peer_id,
            "data": self._normalize_value(data),
        }
        return self._sign_payload(payload)

    def _normalize_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (bool, float, int, str)):
            return value

        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return value.hex()

        if isinstance(value, dict):
            return {str(key): self._normalize_value(item) for key, item in value.items()}

        if isinstance(value, (list, tuple, set)):
            return [self._normalize_value(item) for item in value]

        if dataclasses.is_dataclass(value):
            return self._normalize_value(dataclasses.asdict(value))

        if hasattr(value, "to_string") and callable(value.to_string):
            return value.to_string()

        if hasattr(value, "to_primitive") and callable(value.to_primitive):
            return self._normalize_value(value.to_primitive())

        if hasattr(value, "model_dump") and callable(value.model_dump):
            try:
                dumped = value.model_dump(mode="json")
            except TypeError:
                dumped = value.model_dump()
            return self._normalize_value(dumped)

        if hasattr(value, "dict") and callable(value.dict):
            return self._normalize_value(value.dict())

        return str(value)

    def _dump_json(self, payload: dict[str, Any], *, sort_keys: bool = False) -> str:
        return json.dumps(payload, sort_keys=sort_keys, separators=(",", ":"))

    def start(self, nursery: trio.Nursery) -> None:
        """
        Launches the telemetry worker in the provided nursery.
        """
        nursery.start_soon(self.run)
