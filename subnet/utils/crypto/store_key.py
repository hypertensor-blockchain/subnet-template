import secrets

from libp2p.crypto.ed25519 import (
    Ed25519PrivateKey,
    create_new_key_pair as create_new_ed25519_key_pair,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.secp256k1 import Secp256k1PrivateKey
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2


def store_key(path: str):
    key_pair = create_new_ed25519_key_pair(secrets.token_bytes(32))

    peer_id = PeerID.from_pubkey(key_pair.public_key)
    print(f"Peer ID: {peer_id}")

    protobuf = crypto_pb2.PrivateKey(Type=crypto_pb2.KeyType.Ed25519, Data=key_pair.private_key.to_bytes())

    # Store main private key
    with open(path, "wb") as f:
        f.write(protobuf.SerializeToString())


def get_key_pair(
    path: str,
) -> KeyPair:
    """
    Get a keypair if it exists in path.
    """
    try:
        with open(f"{path}", "rb") as f:
            data = f.read()
        private_key = crypto_pb2.PrivateKey.FromString(data)
        if private_key.Type == crypto_pb2.KeyType.Ed25519:
            private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
        elif private_key.Type == crypto_pb2.KeyType.Secp256k1:
            private_key = Secp256k1PrivateKey.from_bytes(private_key.Data)
        else:
            raise ValueError("Unsupported key type")

        return KeyPair(private_key, private_key.get_public_key())
    except FileNotFoundError:
        raise ValueError("Private key not found")


def get_peer_id(
    path: str,
) -> PeerID:
    """
    Get a peer ID if it exists in path.
    """
    try:
        with open(f"{path}", "rb") as f:
            data = f.read()
        private_key = crypto_pb2.PrivateKey.FromString(data)
        if private_key.Type == crypto_pb2.KeyType.Ed25519:
            private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
        elif private_key.Type == crypto_pb2.KeyType.Secp256k1:
            private_key = Secp256k1PrivateKey.from_bytes(private_key.Data)
        else:
            raise ValueError("Unsupported key type")

        return PeerID.from_pubkey(private_key.get_public_key())
    except FileNotFoundError:
        raise ValueError("Private key not found")
