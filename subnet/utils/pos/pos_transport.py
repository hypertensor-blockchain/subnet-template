from libp2p.abc import IRawConnection, ISecureConn, ISecureTransport, TProtocol
from libp2p.peer.id import ID

from subnet.utils.pos.exceptions import InvalidProofOfStake
from subnet.utils.pos.proof_of_stake import ProofOfStake

PROTOCOL_ID = TProtocol("/pos-transport/1.0.0")


class POSTransport:
    """
    POSTransport is a wrapper around a secure transport that implements proof of stake.
    """

    def __init__(
        self,
        transport: ISecureTransport,
        pos: ProofOfStake | None = None,
    ) -> None:
        self.transport = transport
        self.pos = pos
        print("POSTransport init")

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
        print("POSTransport secure_inbound")
        noise_secure_inbound = await self.transport.secure_inbound(conn)
        print("POSTransport noise_secure_inbound", noise_secure_inbound)
        print(
            "POSTransport noise_secure_inbound remote_peer",
            noise_secure_inbound.remote_peer,
        )

        if self.pos is not None:
            if not self.proof_of_stake(
                peer_id=noise_secure_inbound.remote_peer,
            ):
                raise InvalidProofOfStake

        print(f"POSTransport inbound pos successful for {noise_secure_inbound.remote_peer}")

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
        print("POSTransport secure_outbound")
        noise_secure_outbound = await self.transport.secure_outbound(conn, peer_id)
        print("POSTransport noise_secure_outbound", noise_secure_outbound)
        print(
            "POSTransport noise_secure_outbound remote_peer",
            noise_secure_outbound.remote_peer,
        )

        if self.pos is not None:
            if not self.proof_of_stake(
                peer_id=noise_secure_outbound.remote_peer,
            ):
                raise InvalidProofOfStake

        print(f"POSTransport outbound pos successful for {noise_secure_outbound.remote_peer}")

        return noise_secure_outbound

    def proof_of_stake(self, peer_id: ID) -> bool:
        try:
            pos = self.pos.proof_of_stake(
                peer_id=peer_id,
            )
            print(f"Proof of stake for {peer_id} is {pos}")
            return pos
        except Exception as e:
            print(f"Proof of stake failed: {e}", exc_info=True)
            return False
