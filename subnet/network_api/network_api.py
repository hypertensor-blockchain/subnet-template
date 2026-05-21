import asyncio
import json
import logging
from pathlib import Path
from typing import Callable, List, Optional, Union

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from libp2p.abc import IHost
from libp2p.kad_dht.kad_dht import KadDHT
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from multiaddr import Multiaddr
from pydantic import BaseModel
import trio
import trio_asyncio
import uvicorn

from subnet.consensus.consensus import Consensus
from subnet.hypertensor.chain_functions import Hypertensor
from subnet.hypertensor.mock.local_chain_functions import LocalMockHypertensor
from subnet.network_api.config import ApiConfig
from subnet.protocols.api_protocol import ApiProtocol
from subnet.protocols.mock_protocol import MockProtocol
from subnet.utils.db.database import RocksDB
from subnet.utils.pos.proof_of_stake import ProofOfStake

logger = logging.getLogger("network_api")


class NetworkApi:
    """
    An API to interact with the P2P network.
    """

    def __init__(
        self,
        *,
        db: RocksDB | None = None,
        host: IHost | None = None,
        dht: KadDHT | None = None,
        gossipsub: GossipSub | None = None,
        pubsub: Pubsub | None = None,
        consensus: Consensus | None = None,
        hypertensor: Hypertensor | LocalMockHypertensor | None = None,
        proof_of_stake: ProofOfStake | None = None,
        api_protocol: ApiProtocol | None = None,
        mock_protocol: MockProtocol | None = None,
    ):
        self.db = db
        self.host = host
        self.dht = dht
        self.gossipsub = gossipsub
        self.pubsub = pubsub
        self.consensus = consensus
        self.hypertensor = hypertensor
        self.proof_of_stake = proof_of_stake
        self.api_protocol = api_protocol
        self.mock_protocol = mock_protocol

    async def publish_topic(self, topic: str, message: bytes):
        """
        Publish a message to a topic.
        """
        if self.pubsub is None:
            raise Exception("Pubsub not initialized")
        try:
            await self.pubsub.publish(topic, message)
        except Exception as e:
            logger.error(f"Failed to publish message to topic {topic}: {e}")
            raise

    async def call_api_protocol(
        self,
        destination: Multiaddr,
        route: str,
        method: str = "GET",
        headers: dict = None,
        body: bytes = b"",
    ) -> bytes:
        """
        Call a remote peer's API and wait for a single unary response.
        """
        if self.api_protocol is None:
            raise Exception("API protocol not initialized")
        try:
            return await self.api_protocol.call_remote(destination, route, method, headers, body)
        except Exception as e:
            logger.error(f"Failed to call API protocol on peer {destination}: {e}")
            raise

    def attest(self, subnet_id: int) -> bytes:
        """
        Call the attest extrinsic
        """
        if self.hypertensor is None:
            raise Exception("API protocol not initialized")
        try:
            return self.hypertensor.attest(subnet_id)
        except Exception as e:
            logger.error(f"Failed to attest for {subnet_id}: {e}")
            raise


class NetworkApiServer:
    """
    A template for an API server that allows external processes or separate
    servers to call into the P2P layer (via NetworkApi).
    """

    def __init__(self, network_api: NetworkApi, config: Union[ApiConfig, str, Path]):
        self.network_api = network_api
        self.app = FastAPI(title="P2P Network API Template")
        self.router = APIRouter()
        self.server: Optional[uvicorn.Server] = None
        self._finished = trio.Event()

        # Check if config is a path
        self._config_file = Path(config) if isinstance(config, (str, Path)) else None
        self.config = self._load_config() if self._config_file else config

        # Basic IP Whitelisting Middleware
        @self.app.middleware("http")
        async def ip_whitelist_middleware(request: Request, call_next):
            client_ip = request.client.host if request.client else None

            # Dynamically reload config to check whitelist if file was provided
            current_config = self._load_config() if self._config_file else self.config

            # If whitelist_ips is empty, allow all. Otherwise, check.
            if current_config.whitelist_ips and client_ip not in current_config.whitelist_ips:
                return JSONResponse(status_code=403, content={"detail": "Forbidden IP"})
            response = await call_next(request)
            return response

        # Setup standard routes
        self._setup_template_routes()
        self.app.include_router(self.router)

    def _load_config(self) -> ApiConfig:
        """Helper to load config from a file if provided. Returns a default/current ApiConfig otherwise."""
        if self._config_file and self._config_file.exists():
            try:
                with open(self._config_file, "r") as f:
                    return ApiConfig(**json.load(f))
            except Exception as e:
                print(f"Error loading ApiConfig from {self._config_file}: {e}")
        return getattr(self, "config", ApiConfig())

    def _setup_template_routes(self):
        """
        Setup default template routes here.
        Users can modify or add new routes to interact with P2P protocols.
        """

        class PublishRequest(BaseModel):
            topic: str
            message: str

        @self.router.post("/v1/publish")
        async def publish_message(request: PublishRequest):
            """
            Template route for publishing a gossip message to the network.
            """
            await self.network_api.publish_topic(request.topic, request.message.encode("utf-8"))
            return {"status": "success", "topic": request.topic}

        @self.router.post("/attest")
        def attest(subnet_id: int):
            """
            Template route for publishing a gossip message to the network.
            """
            self.network_api.attest(subnet_id)
            return {"status": "success"}

    def register_route(self, path: str, endpoint: Callable, methods: List[str] = ["POST"]):
        """
        Allows builders to dynamically inject routes without altering this class directly.
        """
        self.app.add_api_route(path, endpoint, methods=methods)

    async def serve(self, *, task_status=trio.TASK_STATUS_IGNORED):
        """
        Run the API server. This is a blocking call.
        Highly recommended to run this inside a trio nursery:
        nursery.start_soon(api_server.serve)
        """
        if not self.config.enable_api:
            task_status.started()
            return

        uvicorn_config = uvicorn.Config(
            app=self.app,
            host=self.config.listen_host,
            port=self.config.port,
            log_level="info",
            loop="asyncio",
            timeout_graceful_shutdown=5,
        )
        self.server = uvicorn.Server(uvicorn_config)

        # Disable uvicorn's signal handling by making it a no-op
        import contextlib

        @contextlib.contextmanager
        def _no_capture():
            yield

        self.server.capture_signals = _no_capture

        # Ensure the trio-asyncio loop is recognized as the current loop for the asyncio side
        # This is critical for bridge calls (trio_as_aio / run_trio) to work inside FastAPI routes.
        # Store the loop for use in routes
        self.loop = trio_asyncio.current_loop.get()
        asyncio.set_event_loop(self.loop)

        task_status.started()
        try:
            await trio_asyncio.aio_as_trio(self.server.serve())
        finally:
            self._finished.set()
            logger.info("NetworkApiServer has finished serving.")

    async def stop(self):
        """
        Gracefully stop the API server.
        """
        if self.server:
            logger.info("Signaling uvicorn server to exit...")
            self.server.should_exit = True
            await self._finished.wait()
            logger.info("NetworkApiServer stop complete.")
