import logging

from libp2p.abc import IRawConnection, ISecureConn, ISecureTransport, TProtocol
from libp2p.peer.id import ID

from subnet.utils.pos.exceptions import InvalidProofOfStake
from subnet.utils.pos.proof_of_stake import ProofOfStake

logger = logging.getLogger("pos_transport/1.0.0")

PROTOCOL_ID = TProtocol("/pos-transport/1.0.0")


class POSTransport:
    """
    POSTransport is a wrapper around a secure transport that implements proof of stake.

    POS triggers on inbound and outbound connections.

    NOTE: For PoS on the stream level, implement directly where the stream is created.
    """

    def __init__(
        self,
        transport: ISecureTransport,
        pos: ProofOfStake | None = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.transport = transport
        self.pos = pos
        self.log_level = log_level

    async def secure_inbound(self, conn: IRawConnection) -> ISecureConn:
        """
        Secure an inbound connection (when another peer connects to you).
        Implement your authentication/validation logic here.

        Returns:
            ISecureConn

            Example return:
                return SecureSession(
                    local_peer=self.local_peer,
                    local_private_key=self.libp2p_privkey,
                    remote_peer=remote_peer_id_from_pubkey,
                    remote_permanent_pubkey=remote_pubkey,
                    is_initiator=False,
                    conn=transport_read_writer,
                )

        """
        noise_secure_inbound = await self.transport.secure_inbound(conn)

        if self.pos is not None:
            if not self.proof_of_stake(
                peer_id=noise_secure_inbound.remote_peer,
            ):
                raise InvalidProofOfStake

        return noise_secure_inbound

    async def secure_outbound(self, conn: IRawConnection, peer_id: ID) -> ISecureConn:
        """
        Secure an outbound connection (when you connect to another peer).
        Implement your request signing/authentication logic here.

        Returns:
            ISecureConn

            Example return:
                return SecureSession(
                    local_peer=self.local_peer,
                    local_private_key=self.libp2p_privkey,
                    remote_peer=remote_peer_id_from_pubkey,
                    remote_permanent_pubkey=remote_pubkey,
                    is_initiator=True,
                    conn=transport_read_writer,
                )

        """
        noise_secure_outbound = await self.transport.secure_outbound(conn, peer_id)

        if self.pos is not None:
            if not self.proof_of_stake(
                peer_id=noise_secure_outbound.remote_peer,
            ):
                raise InvalidProofOfStake

        return noise_secure_outbound

    def proof_of_stake(self, peer_id: ID) -> bool:
        try:
            pos = self.pos.proof_of_stake(
                peer_id=peer_id,
            )
            logger.log(self.log_level, f"Proof of stake from {peer_id}: {pos}")
            return pos
        except Exception as e:
            logger.warning(f"Proof of stake failed: {e}", exc_info=True)
            # If error with RPC, allow connection
            return True
