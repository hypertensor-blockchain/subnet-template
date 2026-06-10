import pytest
from fastapi.testclient import TestClient

from subnet.network_api.config import ApiConfig
from subnet.network_api.network_api import NetworkApiServer


class MockNetworkApi:
    """
    A standalone mock for NetworkApi to prevent connecting to
    actual P2P services during API testing.
    """

    def __init__(self):
        self.published_messages = []

    async def publish_topic(self, topic: str, message: bytes):
        self.published_messages.append((topic, message))


@pytest.fixture
def mock_api():
    return MockNetworkApi()


@pytest.fixture
def api_config():
    # FastAPI TestClient sets the request IP to 'testclient' by default.
    return ApiConfig(whitelist_ips=["127.0.0.1", "testclient", "testserver"], enable_api=True)


@pytest.fixture
def api_server(mock_api, api_config):
    """Initializes the API server template using mocked underlying P2P classes."""
    return NetworkApiServer(network_api=mock_api, config=api_config)


@pytest.fixture
def client(api_server):
    return TestClient(api_server.app)


# python -m pytest tests/test_network_api.py::test_api_whitelist_allowed -rP


def test_api_whitelist_allowed(client):
    """Test that requests from a whitelisted IP are processed through the middleware."""
    # We expect 422 Unprocessable Entity because we sent an empty body.
    # If it was blocked by the IP middleware, it would be 403 Forbidden.
    response = client.post("/v1/publish", json={})
    print("response", response)
    if response.status_code == 403:
        assert False, f"IP was unexpectedly blocked! Response: {response.text}"
    assert response.status_code == 422


def test_api_config_accepts_legacy_host():
    """Older config files that use `host` should still configure the bind address."""
    config = ApiConfig(host="0.0.0.0")

    assert config.listen_host == "0.0.0.0"


def test_api_whitelist_blocked(api_server, mock_api):
    """Test that requests from non-whitelisted IPs are blocked by the IP middleware."""
    # Overwrite config to block "testclient"
    api_server.config.whitelist_ips = ["192.168.1.1"]

    blocked_client = TestClient(api_server.app)
    response = blocked_client.post("/v1/publish", json={"topic": "test_topic", "message": "test_message"})

    # Request should be rejected immediately
    assert response.status_code == 403
    assert response.json() == {"detail": "Forbidden IP"}
    assert len(mock_api.published_messages) == 0


def test_publish_message(client, mock_api):
    """Test the template route for successfully publishing a P2P message."""
    response = client.post("/v1/publish", json={"topic": "my_topic", "message": "hello world"})

    assert response.status_code == 200
    assert response.json() == {"status": "success", "topic": "my_topic"}

    # Validate the data propagated to the underlying mock
    assert len(mock_api.published_messages) == 1
    topic, message = mock_api.published_messages[0]
    assert topic == "my_topic"
    assert message == b"hello world"


def test_register_dynamic_route(api_server, client):
    """Test that builders can dynamically inject new routes to customize their modules."""

    async def custom_endpoint():
        return {"custom": "data"}

    api_server.register_route("/v1/custom", custom_endpoint, methods=["GET"])

    response = client.get("/v1/custom")
    assert response.status_code == 200
    assert response.json() == {"custom": "data"}
