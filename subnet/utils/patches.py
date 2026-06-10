"""
Monkey patches for libp2p to improve stability.

These patches fix race conditions and unhandled exceptions in the upstream library.

This is a temporary solution until the upstream library issues are fixed.
"""

import logging
from typing import List

from libp2p.abc import (
    INetStream,
)
from libp2p.network.stream.exceptions import StreamReset
from libp2p.peer.id import ID
from libp2p.peer.peerstore import PeerStore
from libp2p.pubsub.exceptions import NoPubsubAttached
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
import trio

logger = logging.getLogger(__name__)


def patch_get_in_topic_gossipsub_peers_from_minus():
    """
    Fix KeyError in GossipSub._get_in_topic_gossipsub_peers_from_minus.

    Race condition: peer is in peer_topics but removed from peer_protocol
    during disconnect. Use .get() for safe access.
    """

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
    """
    Patch fixes an issue in Pubsub.write_msg where it crashes a peer with StreamReset when another peer is disconnected.
    """
    _orig_write_msg = Pubsub.write_msg

    async def safe_write_msg(self: Pubsub, stream: INetStream, rpc_msg) -> bool:
        try:
            return await _orig_write_msg(self, stream, rpc_msg)
        except StreamReset:
            try:
                peer_id = stream.muxed_conn.peer_id
            except Exception:
                # If we can't even get the peer_id, just return False
                return False

            self._handle_dead_peer(peer_id)
            return False

    Pubsub.write_msg = safe_write_msg


def patch_maybe_delete_peer_record():
    """
    Patch fixes an issue in PeerStore.maybe_delete_peer_record where it crashes a peer with StreamReset when another
    peer is disconnected.
    """

    def safe_maybe_delete_peer_record(self: PeerStore, peer_id: ID) -> bool:
        if peer_id in self.peer_record_map:
            try:
                if not self.addrs(peer_id):
                    self.peer_record_map.pop(peer_id, None)
            except Exception as e:
                logger.error(f"Failed to maybe delete peer record for {peer_id}: {e}")

    PeerStore.maybe_delete_peer_record = safe_maybe_delete_peer_record


def patch_pubsub_run():
    """
    Patch fixes an issue in Pubsub where it doesn't actively graft peers and add peers to its mesh (without new peers
    entering) when below the target degree (or adaptive degree).

    This will actively try to graft peers the peer is already connected to and add peers to its mesh when below the
    target degree (or adaptive degree).
    """

    async def patch_run(self: Pubsub) -> None:
        self.manager.run_daemon_task(self.handle_peer_queue)
        self.manager.run_daemon_task(self.handle_dead_peer_queue)
        self.manager.run_daemon_task(self._validation_cache_cleanup)
        self.manager.run_daemon_task(_periodic_connection_sweep, self)
        await self.manager.wait_finished()

    async def _periodic_connection_sweep(self: Pubsub) -> None:
        """
        Periodically sweep existing host connections to ensure transient stream
        negotiation failures don't permanently exclude peers from pubsub.
        """
        while True:
            # Sleep first to give initial handlers time to fire
            await trio.sleep(15)
            network = self.host.get_network()
            if hasattr(network, "connections"):
                for peer_id in network.connections.keys():
                    if peer_id not in self.peers and not self.is_peer_blacklisted(peer_id):
                        print(
                            "Periodic sweep: retrying connection to host peer %s",
                            peer_id,
                        )
                        self.manager.run_task(self._handle_new_peer_safe, peer_id)

    Pubsub.run = patch_run


def apply_all_patches():
    """Apply all libp2p stability patches."""
    # patch_get_in_topic_gossipsub_peers_from_minus() # Fixed in PR #1116 https://github.com/libp2p/py-libp2p/pull/1116
    # patch_write_msg() # Fixed in PR #1117 https://github.com/libp2p/py-libp2p/pull/1117
    patch_maybe_delete_peer_record()
    # patch_pubsub_run()  # Only use this if the subnet will have <6 peers
