from libp2p.abc import IRawConnection, ISecureConn, TProtocol
from libp2p.peer.id import ID
from libp2p.security.secio.transport import Transport as SecioTransport

from subnet.utils.pos.exceptions import InvalidProofOfStake
from subnet.utils.pos.proof_of_stake import ProofOfStake

PROTOCOL_ID = TProtocol("/pos/1.0.0")


class POSNoiseTransport:
    def __init__(
        self,
        secio_transport: SecioTransport,
        pos: ProofOfStake | None = None,
    ) -> None:
        self.secio_transport = secio_transport
        self.pos = pos
        print("POSNoiseTransport init")

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
        print("POSNoiseTransport secure_inbound")
        noise_secure_inbound = await self.secio_transport.secure_inbound(conn)
        print("POSNoiseTransport noise_secure_inbound", noise_secure_inbound)
        print(
            "POSNoiseTransport noise_secure_inbound remote_peer",
            noise_secure_inbound.remote_peer,
        )

        if self.pos is not None:
            if not self.proof_of_stake(
                peer_id=noise_secure_inbound.remote_peer,
            ):
                raise InvalidProofOfStake

        print(f"POSNoiseTransport inbound pos successful for {noise_secure_inbound.remote_peer}")

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
        print("POSNoiseTransport secure_outbound")
        noise_secure_outbound = await self.secio_transport.secure_outbound(conn, peer_id)
        print("POSPOSNoiseTransportTransport noise_secure_outbound", noise_secure_outbound)
        print(
            "POSNoiseTransport noise_secure_outbound remote_peer",
            noise_secure_outbound.remote_peer,
        )

        if self.pos is not None:
            if not self.proof_of_stake(
                peer_id=noise_secure_outbound.remote_peer,
            ):
                raise InvalidProofOfStake

        print(f"POSNoiseTransport outbound pos successful for {noise_secure_outbound.remote_peer}")

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
