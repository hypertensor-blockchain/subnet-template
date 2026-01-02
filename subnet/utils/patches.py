"""
Monkey patches for libp2p to improve stability.

These patches fix race conditions and unhandled exceptions in the upstream library.
Import this module early in your application to apply all patches.
"""

import logging
from typing import List

from libp2p.abc import INetStream
from libp2p.network.stream.exceptions import StreamReset
from libp2p.peer.id import ID
from libp2p.pubsub.exceptions import NoPubsubAttached
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.stream_muxer.exceptions import MuxedStreamError, MuxedStreamReset

logger = logging.getLogger(__name__)


def patch_get_in_topic_gossipsub_peers_from_minus():
    """
    Fix KeyError in GossipSub._get_in_topic_gossipsub_peers_from_minus.

    Race condition: peer is in peer_topics but removed from peer_protocol
    during disconnect. Use .get() for safe access.
    """
    _orig_get_peers = GossipSub._get_in_topic_gossipsub_peers_from_minus

    def safe_get_in_topic_gossipsub_peers_from_minus(
        self: GossipSub,
        topic: str,
        num_to_select: int,
        minus: List[ID],
        backoff_check: bool = False,
    ) -> List[ID]:
        if self.pubsub is None:
            raise NoPubsubAttached

        # Use .get() to safely check protocol participation
        from libp2p.pubsub.gossipsub import PROTOCOL_ID, PROTOCOL_ID_V11, PROTOCOL_ID_V12

        gossipsub_peers_in_topic = {
            peer_id
            for peer_id in self.pubsub.peer_topics[topic]
            if self.peer_protocol.get(peer_id) in (PROTOCOL_ID, PROTOCOL_ID_V11, PROTOCOL_ID_V12)
        }
        if backoff_check:
            gossipsub_peers_in_topic = {
                peer_id for peer_id in gossipsub_peers_in_topic if self._check_back_off(peer_id, topic) is False
            }
        return self.select_from_minus(num_to_select, list(gossipsub_peers_in_topic), minus)

    GossipSub._get_in_topic_gossipsub_peers_from_minus = safe_get_in_topic_gossipsub_peers_from_minus


def patch_write_msg():
    _orig_write_msg = Pubsub.write_msg

    async def safe_write_msg(self: Pubsub, stream: INetStream, rpc_msg) -> bool:
        try:
            return await _orig_write_msg(self, stream, rpc_msg)
        except (StreamReset, MuxedStreamReset, MuxedStreamError):
            try:
                peer_id = stream.muxed_conn.peer_id
            except Exception:
                # If we can't even get the peer_id, just return False
                return False

            self._handle_dead_peer(peer_id)
            return False

    Pubsub.write_msg = safe_write_msg


def apply_all_patches():
    """Apply all libp2p stability patches."""
    patch_get_in_topic_gossipsub_peers_from_minus()
    patch_write_msg()
