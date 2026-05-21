# P2P Network API Template

This module provides a production-ready template for spanning a REST API that acts as a bridge between your core P2P Node and separate local or remote processes. 

It is designed for cases where you might have heavy workloads—such as AI inference tasks, external logic servers, or monitoring dashboards—that operate outside of the node but still need to invoke P2P protocols, query peer statuses, or publish consensus/gossip messages.

---

## ⚡ How it Works

The API relies on **FastAPI** running asynchronously via **Uvicorn** alongside your node's primary loops. It is passed an instance of `NetworkApi`, giving it full access to publish messages, access the DHT, and trigger protocols gracefully. 

### Key Features

- **Non-blocking Execution:** The server runs as an `asyncio.Task` asynchronously alongside your node, meaning it never blocks your P2P tasks or block generation processes.
- **Dynamic Configuration:** Supports loading setup configurations remotely using a generic `JSON` format. 
- **Live Security Loading:** Ships with a base IP Whitelist Middleware to restrict who can trigger the API. If `config.json` changes, the whitelisted limits update automatically without requiring a restart!
- **Extensible Router:** You can cleanly modularize your custom endpoints inside `NetworkApiServer` or dynamically mount hooks via `.register_route()` from outside the class.

---

## 🚀 Getting Started

Follow the steps below to integrate, configure, and customize this layout for your own Node.

### 1. Create your Configuration

Begin by passing dynamic parameters to tailor your API. Create a `config.json` (such as `subnet/network_api/config.json`) containing standard listen host and port information.

```json
{
  "listen_host": "127.0.0.1",
  "port": 8000,
  "whitelist_ips": [
    "127.0.0.1",
    "192.168.1.100"
  ],
  "enable_api": true
}
```

> **Note:** Whenever you modify the `whitelist_ips` array in `config.json`, the API instantly enforces the new addresses on all subsequent incoming HTTP calls without requiring the network node to restart.

### 2. Initiate the Server

Inside your node builder or bootstrapping layer, hook up the actual API class `NetworkApiServer` to your main `NetworkApi` instance and provide your configuration parameters.

```python
from subnet.network_api.network_api import NetworkApi, NetworkApiServer

# (1) Construct your typical Network API
network_api = NetworkApi(...)

# (2) Bridge your P2P node with the web API
api_server = NetworkApiServer(network_api, config="subnet/network_api/config.json")

# Start your network and servers asynchronously in an event loop
await api_server.start()

# ...
# Do some P2P stuff and keep the loop alive
# ...

# Gracefully bring it down when closing
await api_server.stop()
```

### 3. Customize Endpoints

By default, the template includes a single demonstration route: `[POST] /v1/publish` that expects a topic and message payload.

To expand this to handle custom requirements (like pinging specific tasks or triggering DHT lookups), modify the `_setup_template_routes()` method in `network_api.py`.

```python
def _setup_template_routes(self):

    @self.router.post("/v1/my-custom-task")
    async def run_my_task(request: CustomRequest):
        # 1. Use self.network_api for external capabilities 
        # 2. Add node logic...
        return {"status": "success"}
```

Alternatively, inject routes dynamically from another location dynamically:

```python
async def my_external_handler():
    return "Handled!"

api_server.register_route(
    path="/v1/external", 
    endpoint=my_external_handler, 
    methods=["GET"]
)
```

## Stopping Operations

Always ensure `await api_server.stop()` is captured on process teardown to prevent Uvicorn from abruptly terminating long-running external hook executions.
