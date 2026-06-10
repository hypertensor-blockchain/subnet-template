import pytest

from subnet.utils.pubsub.peer_state import (
    PeerRole,
    PeerStateData,
    PeerStatePublisher,
    ServerState,
)


class DummyPubsub:
    def __init__(self):
        self.messages: list[tuple[str, bytes]] = []

    async def publish(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class DummyEpochData:
    def __init__(self, epoch: int):
        self.epoch = epoch


class DummyHypertensor:
    def __init__(self, epoch: int = 42):
        self._epoch = epoch

    def get_subnet_slot(self, subnet_id: int) -> int:
        return subnet_id

    def get_subnet_epoch_data(self, subnet_slot: int) -> DummyEpochData:
        return DummyEpochData(self._epoch)


def _build_publisher(pubsub: DummyPubsub | None = None) -> PeerStatePublisher:
    return PeerStatePublisher(
        pubsub=pubsub or DummyPubsub(),
        topic="peer-state",
        start_state=ServerState.JOINING,
        start_role=PeerRole.VALIDATOR,
        subnet_id=1,
        subnet_node_id=2,
        hypertensor=DummyHypertensor(epoch=7),
    )


def test_update_state_sets_current_peer_state():
    publisher = _build_publisher()

    publisher.update_state(ServerState.ONLINE)

    assert publisher.state is ServerState.ONLINE


def test_state_assignment_uses_validated_update_path():
    publisher = _build_publisher()

    publisher.state = ServerState.OFFLINE

    assert publisher.state is ServerState.OFFLINE
    with pytest.raises(TypeError, match="Peer state must be a ServerState"):
        publisher.state = "online"  # type: ignore[assignment]
    assert publisher.state is ServerState.OFFLINE


@pytest.mark.asyncio
async def test_publish_uses_updated_peer_state():
    pubsub = DummyPubsub()
    publisher = _build_publisher(pubsub)

    publisher.update_state(ServerState.ONLINE)
    await publisher.publish()

    assert len(pubsub.messages) == 1
    _topic, payload = pubsub.messages[0]
    published = PeerStateData.from_json(payload.decode("utf-8"))
    assert published.state is ServerState.ONLINE
