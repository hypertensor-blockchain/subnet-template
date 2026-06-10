from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from examples.server import server_dag


class DummyOfflinePublisher:
    def __init__(self) -> None:
        self.called = False

    async def publish_offline_state(self):
        self.called = True
        return SimpleNamespace(node_id="offline-node")


@pytest.mark.asyncio
async def test_server_cleanup_announces_offline_state_and_waits(monkeypatch, caplog) -> None:
    publisher = DummyOfflinePublisher()
    app = server_dag.SubnetApplication.__new__(server_dag.SubnetApplication)
    app.peer_state_publisher = publisher
    app.offline_gossip_settle_seconds = 1.25
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(server_dag.trio, "sleep", fake_sleep)

    with caplog.at_level(logging.INFO, logger="server_example/1.0.0"):
        await app.cleanup(SimpleNamespace())

    assert publisher.called
    assert sleeps == [1.25]
    assert "Announcing that we're going offline before shutting down" in caplog.text
    assert "Offline peer-state DAG node offline-node gossiped" in caplog.text
