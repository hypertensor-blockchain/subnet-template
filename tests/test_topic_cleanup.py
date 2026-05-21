from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

import pytest
from libp2p.peer.id import ID

from subnet.utils.pubsub.topic_membership_template import (
    TopicMembershipConfig,
    TopicMembershipTemplate,
    TopicPeerValidation,
)


class FakeScorer:
    def __init__(self) -> None:
        self.left_mesh: list[tuple[ID, str]] = []

    def on_leave_mesh(self, peer_id: ID, topic: str) -> None:
        self.left_mesh.append((peer_id, topic))


class FakeTaskManager:
    def __init__(self) -> None:
        self.tasks: list[tuple[Callable[..., Awaitable[None]], tuple[object, ...]]] = []

    def run_task(self, fn: Callable[..., Awaitable[None]], *args: object) -> None:
        self.tasks.append((fn, args))


class FakePubsub:
    def __init__(self, peer_topics: dict[str, set[ID]] | None = None) -> None:
        self.peer_topics = peer_topics or {}
        self.manager = FakeTaskManager()
        self.pushed: list[tuple[ID, object]] = []

    def handle_subscription(self, origin_id: ID, sub_message: object) -> None:
        topic = getattr(sub_message, "topicid")
        if getattr(sub_message, "subscribe"):
            self.peer_topics.setdefault(topic, set()).add(origin_id)
        elif topic in self.peer_topics:
            self.peer_topics[topic].discard(origin_id)

    async def push_msg(self, msg_forwarder: ID, msg: object) -> None:
        self.pushed.append((msg_forwarder, msg))


class FakeGossipsub:
    def __init__(
        self,
        mesh: dict[str, set[ID]] | None = None,
        fanout: dict[str, set[ID]] | None = None,
    ) -> None:
        self.mesh = mesh or {}
        self.fanout = fanout or {}
        self.scorer = FakeScorer()
        self.backoffs: list[tuple[ID, str, bool]] = []
        self.prunes: list[tuple[str, ID, bool, bool]] = []
        self.grafts: list[tuple[str, ID]] = []
        self.send_peers: list[ID] = []
        self.selection_peers: list[ID] = []

    def _add_back_off(self, peer_id: ID, topic: str, is_unsubscribe: bool) -> None:
        self.backoffs.append((peer_id, topic, is_unsubscribe))

    async def emit_prune(self, topic: str, peer_id: ID, do_px: bool, is_unsubscribe: bool) -> None:
        self.prunes.append((topic, peer_id, do_px, is_unsubscribe))

    async def handle_graft(self, graft_msg: object, peer_id: ID) -> None:
        topic = getattr(graft_msg, "topicID")
        self.grafts.append((topic, peer_id))
        self.mesh.setdefault(topic, set()).add(peer_id)

    def _get_peers_to_send(
        self,
        _topic_ids: Iterable[str],
        _msg_forwarder: ID,
        _origin: ID,
        _msg_id: bytes | None = None,
    ) -> Iterable[ID]:
        yield from self.send_peers

    def _get_in_topic_gossipsub_peers_from_minus(
        self,
        _topic: str,
        _num_to_select: int,
        _minus: Iterable[ID],
        _backoff_check: bool = False,
    ) -> list[ID]:
        return list(self.selection_peers)


@dataclass
class FakeSubMessage:
    subscribe: bool
    topicid: str


@dataclass
class FakeGraftMessage:
    topicID: str


@dataclass
class FakePubsubMessage:
    topicIDs: tuple[str, ...]


def _peer(seed: bytes) -> ID:
    return ID(seed)


@pytest.mark.trio
async def test_cleanup_removes_disallowed_peer_from_topic_only() -> None:
    allowed_peer = _peer(b"allowed-peer")
    disallowed_peer = _peer(b"wrong-role-peer")
    pubsub = FakePubsub(
        {
            "restricted": {allowed_peer, disallowed_peer},
            "open": {disallowed_peer},
        }
    )
    gossipsub = FakeGossipsub(
        mesh={
            "restricted": {allowed_peer, disallowed_peer},
            "open": {disallowed_peer},
        },
        fanout={
            "restricted": {disallowed_peer},
            "open": {disallowed_peer},
        },
    )

    def validate_role(_topic: str, peer_id: ID) -> TopicPeerValidation:
        return TopicPeerValidation(peer_id == allowed_peer, "wrong-role")

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[TopicMembershipConfig(topic="restricted", validator=validate_role)],
    )

    result = await cleaner.cleanup_topic("restricted")

    assert pubsub.peer_topics["restricted"] == {allowed_peer}
    assert gossipsub.mesh["restricted"] == {allowed_peer}
    assert gossipsub.fanout["restricted"] == set()
    assert pubsub.peer_topics["open"] == {disallowed_peer}
    assert gossipsub.mesh["open"] == {disallowed_peer}
    assert gossipsub.fanout["open"] == {disallowed_peer}
    assert [(removal.peer_id, removal.reason, removal.prune_sent) for removal in result.removed_peers] == [
        (disallowed_peer, "wrong-role", True)
    ]
    assert gossipsub.backoffs == [(disallowed_peer, "restricted", False)]
    assert gossipsub.prunes == [("restricted", disallowed_peer, False, False)]
    assert gossipsub.scorer.left_mesh == [(disallowed_peer, "restricted")]


@pytest.mark.trio
async def test_cleanup_does_not_prune_peers_outside_mesh() -> None:
    disallowed_peer = _peer(b"fanout-peer")
    pubsub = FakePubsub({"restricted": {disallowed_peer}})
    gossipsub = FakeGossipsub(fanout={"restricted": {disallowed_peer}})

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[TopicMembershipConfig(topic="restricted", validator=lambda _topic, _peer_id: False)],
    )

    result = await cleaner.cleanup_topic("restricted")

    removal = result.removed_peers[0]
    assert pubsub.peer_topics["restricted"] == set()
    assert gossipsub.fanout["restricted"] == set()
    assert removal.was_in_pubsub_topic is True
    assert removal.was_in_mesh is False
    assert removal.was_in_fanout is True
    assert removal.prune_sent is False
    assert gossipsub.backoffs == []
    assert gossipsub.prunes == []


@pytest.mark.trio
async def test_cleanup_supports_async_validator_callback() -> None:
    disallowed_peer = _peer(b"async-peer")
    pubsub = FakePubsub({"restricted": {disallowed_peer}})
    gossipsub = FakeGossipsub(mesh={"restricted": {disallowed_peer}})

    async def validate_async(_topic: str, _peer_id: ID) -> TopicPeerValidation:
        return TopicPeerValidation(False, "async-denied")

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[TopicMembershipConfig(topic="restricted", validator=validate_async)],
    )

    result = await cleaner.cleanup_topic("restricted")

    assert result.removed_peers[0].reason == "async-denied"
    assert pubsub.peer_topics["restricted"] == set()
    assert gossipsub.mesh["restricted"] == set()


@pytest.mark.trio
async def test_cleanup_once_handles_multiple_topic_configs() -> None:
    topic_a_peer = _peer(b"topic-a-peer")
    topic_b_peer = _peer(b"topic-b-peer")
    pubsub = FakePubsub(
        {
            "topic-a": {topic_a_peer},
            "topic-b": {topic_b_peer},
        }
    )
    gossipsub = FakeGossipsub(
        mesh={
            "topic-a": {topic_a_peer},
            "topic-b": {topic_b_peer},
        }
    )

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[
            TopicMembershipConfig(
                topic="topic-a",
                validator=lambda _topic, _peer_id: TopicPeerValidation(False, "not-topic-a-role"),
            ),
            TopicMembershipConfig(
                topic="topic-b",
                validator=lambda _topic, _peer_id: TopicPeerValidation(False, "not-topic-b-role"),
                emit_prune=False,
                add_backoff=False,
            ),
        ],
    )

    results = await cleaner.cleanup()

    assert [result.topic for result in results] == ["topic-a", "topic-b"]
    assert [result.removed_peers[0].reason for result in results] == ["not-topic-a-role", "not-topic-b-role"]
    assert pubsub.peer_topics["topic-a"] == set()
    assert pubsub.peer_topics["topic-b"] == set()
    assert gossipsub.mesh["topic-a"] == set()
    assert gossipsub.mesh["topic-b"] == set()
    assert gossipsub.prunes == [("topic-a", topic_a_peer, False, False)]
    assert gossipsub.backoffs == [(topic_a_peer, "topic-a", False)]


@pytest.mark.trio
async def test_cleanup_supports_subclass_validation_hook() -> None:
    allowed_peer = _peer(b"subclass-allowed")
    pubsub = FakePubsub({"restricted": {allowed_peer}})
    gossipsub = FakeGossipsub(mesh={"restricted": {allowed_peer}})

    class RoleCleaner(TopicMembershipTemplate):
        async def validate_peer(self, config: TopicMembershipConfig, peer_id: ID) -> bool:
            assert config.topic == "restricted"
            return peer_id == allowed_peer

    cleaner = RoleCleaner(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[TopicMembershipConfig(topic="restricted")],
    )

    result = await cleaner.cleanup_topic("restricted")

    assert result.allowed_peers == (allowed_peer,)
    assert result.removed_peers == ()
    assert pubsub.peer_topics["restricted"] == {allowed_peer}


@pytest.mark.trio
async def test_subscription_hook_enforces_restricted_topic_membership() -> None:
    allowed_peer = _peer(b"hook-allowed")
    disallowed_peer = _peer(b"hook-denied")
    pubsub = FakePubsub()
    gossipsub = FakeGossipsub(
        mesh={"restricted": {disallowed_peer}},
        fanout={"restricted": {disallowed_peer}},
    )

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[
            TopicMembershipConfig(
                topic="restricted",
                validator=lambda _topic, peer_id: TopicPeerValidation(peer_id == allowed_peer, "denied"),
            )
        ],
    )
    cleaner.install_hooks()

    pubsub.handle_subscription(disallowed_peer, FakeSubMessage(subscribe=True, topicid="restricted"))

    assert pubsub.peer_topics["restricted"] == {disallowed_peer}
    assert len(pubsub.manager.tasks) == 1

    fn, args = pubsub.manager.tasks.pop()
    await fn(*args)

    assert pubsub.peer_topics["restricted"] == set()
    assert gossipsub.mesh["restricted"] == set()
    assert gossipsub.fanout["restricted"] == set()
    assert gossipsub.prunes == [("restricted", disallowed_peer, False, False)]
    assert gossipsub.backoffs == [(disallowed_peer, "restricted", False)]


@pytest.mark.trio
async def test_graft_hook_rejects_denied_peer_before_mesh_join() -> None:
    disallowed_peer = _peer(b"graft-denied")
    pubsub = FakePubsub({"restricted": {disallowed_peer}})
    gossipsub = FakeGossipsub()

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[TopicMembershipConfig(topic="restricted", validator=lambda _topic, _peer_id: False)],
    )
    cleaner.install_hooks()

    await gossipsub.handle_graft(FakeGraftMessage(topicID="restricted"), disallowed_peer)

    assert gossipsub.grafts == []
    assert gossipsub.mesh.get("restricted", set()) == set()
    assert pubsub.peer_topics["restricted"] == set()
    assert gossipsub.prunes == [("restricted", disallowed_peer, False, False)]
    assert gossipsub.backoffs == [(disallowed_peer, "restricted", False)]
    assert gossipsub.scorer.left_mesh == []


@pytest.mark.trio
async def test_gossip_selection_hooks_only_allow_validated_topic_peers() -> None:
    allowed_peer = _peer(b"select-allowed")
    denied_peer = _peer(b"select-denied")
    unknown_peer = _peer(b"select-unknown")
    pubsub = FakePubsub({"restricted": {allowed_peer, denied_peer, unknown_peer}})
    gossipsub = FakeGossipsub()
    gossipsub.send_peers = [allowed_peer, denied_peer, unknown_peer]
    gossipsub.selection_peers = [allowed_peer, denied_peer, unknown_peer]

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[
            TopicMembershipConfig(
                topic="restricted",
                validator=lambda _topic, peer_id: peer_id == allowed_peer,
            )
        ],
    )
    cleaner.install_hooks()

    await cleaner.enforce_peer("restricted", allowed_peer)
    await cleaner.enforce_peer("restricted", denied_peer)

    assert list(gossipsub._get_peers_to_send(("restricted",), _peer(b"forwarder"), _peer(b"origin"))) == [
        allowed_peer
    ]
    assert gossipsub._get_in_topic_gossipsub_peers_from_minus("restricted", 3, set()) == [allowed_peer]


@pytest.mark.trio
async def test_publish_hook_drops_denied_restricted_topic_messages() -> None:
    denied_peer = _peer(b"publish-denied")
    allowed_peer = _peer(b"publish-allowed")
    pubsub = FakePubsub({"restricted": {denied_peer, allowed_peer}})
    gossipsub = FakeGossipsub()

    cleaner = TopicMembershipTemplate(
        pubsub=pubsub,  # type: ignore[arg-type]
        gossipsub=gossipsub,  # type: ignore[arg-type]
        configs=[
            TopicMembershipConfig(
                topic="restricted",
                validator=lambda _topic, peer_id: peer_id == allowed_peer,
            )
        ],
    )
    cleaner.install_hooks()

    msg = FakePubsubMessage(topicIDs=("restricted",))
    await pubsub.push_msg(denied_peer, msg)
    await pubsub.push_msg(allowed_peer, msg)

    assert pubsub.pushed == [(allowed_peer, msg)]
    assert pubsub.peer_topics["restricted"] == {allowed_peer}


def test_cleanup_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        TopicMembershipConfig(topic="", validator=lambda _topic, _peer_id: True)

    with pytest.raises(ValueError, match="at least one"):
        TopicMembershipTemplate(
            pubsub=FakePubsub(),  # type: ignore[arg-type]
            gossipsub=FakeGossipsub(),  # type: ignore[arg-type]
            configs=[],
        )

    duplicate_config = TopicMembershipConfig(topic="restricted", validator=lambda _topic, _peer_id: True)
    with pytest.raises(ValueError, match="Duplicate topic membership configuration: restricted"):
        TopicMembershipTemplate(
            pubsub=FakePubsub(),  # type: ignore[arg-type]
            gossipsub=FakeGossipsub(),  # type: ignore[arg-type]
            configs=[duplicate_config, duplicate_config],
        )
