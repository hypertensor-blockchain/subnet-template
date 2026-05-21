import pytest
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2

from subnet.utils.crypto.store_key import get_key_pair, get_peer_id, store_private_key


@pytest.mark.parametrize(
    ("key_type", "protobuf_key_type"),
    [
        ("ecc", crypto_pb2.KeyType.ECDSA),
        ("ed25519", crypto_pb2.KeyType.Ed25519),
        ("rsa", crypto_pb2.KeyType.RSA),
        ("secp256k", crypto_pb2.KeyType.Secp256k1),
    ],
)
def test_store_private_key_round_trips_supported_key_types(tmp_path, key_type, protobuf_key_type):
    key_path = tmp_path / f"{key_type}.key"

    store_private_key(str(key_path), key_type=key_type)

    protobuf = crypto_pb2.PrivateKey.FromString(key_path.read_bytes())
    assert protobuf.Type == protobuf_key_type

    key_pair = get_key_pair(str(key_path))
    assert key_pair.public_key.to_bytes() == key_pair.private_key.get_public_key().to_bytes()
    assert str(get_peer_id(str(key_path))) == str(PeerID.from_pubkey(key_pair.public_key))


def test_store_private_key_rejects_unknown_key_type(tmp_path):
    with pytest.raises(ValueError, match="Unsupported key type"):
        store_private_key(str(tmp_path / "unsupported.key"), key_type="x25519")
