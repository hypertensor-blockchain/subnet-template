"""
Syncs arbitrary data between peers.
"""

import logging

from libp2p.abc import IHost, INetStream

from subnet.utils.db.database import RocksDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("sync_protocol/1.0.0")

PROTOCOL_ID = "/subnet/sync_protocol/1.0.0"


class SyncProtocol:
    """
    An API protocol for communication between peers. Remote peers call this protocol which
    sends out an API request to an API. This API can be another server or a local API that
    is running the logic, such as inference, training, or any other logic.

    Peers register a single api_respond handler that processes incoming requests.
    """

    def __init__(self, host: IHost, db: RocksDB):
        """
        Initialize the SyncProtocol.

        Args:
            host: The libp2p host instance
            db: The RocksDB instance

        """
        self.host = host

        # Register the protocol with the host
        self.host.set_stream_handler(PROTOCOL_ID, self._handle_incoming_stream)
        logger.info(f"SyncProtocol initialized with protocol ID: {PROTOCOL_ID}")

    async def _handle_incoming_stream(self, stream: INetStream) -> None:
        pass
