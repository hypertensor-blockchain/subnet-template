from typing import List

from pydantic import BaseModel


class ApiConfig(BaseModel):
    """
    Configuration for the API server.

    Attributes:
        listen_host: Interface address the server binds to, such as `127.0.0.1` or `0.0.0.0`.
        port: TCP port the server listens on.
        whitelist_ips: Client IPs allowed to call the API; an empty list allows all IPs.
        enable_api: Whether the API server should start.
    """

    listen_host: str = "127.0.0.1"
    port: int = 8000
    whitelist_ips: List[str] = ["127.0.0.1"]
    enable_api: bool = True
