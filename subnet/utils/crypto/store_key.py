import secrets
import time

from Crypto.PublicKey import RSA
from fastecdsa.encoding.pem import PEMEncoder
from libp2p.crypto.ecc import (
    ECCPrivateKey,
    create_new_key_pair as create_new_ecc_key_pair,
    infer_local_type,
)
from libp2p.crypto.ed25519 import (
    Ed25519PrivateKey,
    create_new_key_pair as create_new_ed25519_key_pair,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.rsa import (
    RSAPrivateKey,
    create_new_key_pair as create_new_rsa_key_pair,
)
from libp2p.crypto.secp256k1 import (
    Secp256k1PrivateKey,
    create_new_key_pair as create_new_secp256k1_key_pair,
)
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2

SUPPORTED_KEY_TYPES = ("ecc", "ed25519", "rsa", "secp256k", "secp256k1")

_KEY_TYPE_ALIASES = {
    "ecc": "ecc",
    "ecdsa": "ecc",
    "p256": "ecc",
    "eccp256": "ecc",
    "ed25519": "ed25519",
    "rsa": "rsa",
    "secp256k": "secp256k1",
    "secp256k1": "secp256k1",
}

_PROTOBUF_KEY_TYPES = {
    "ecc": crypto_pb2.KeyType.ECDSA,
    "ed25519": crypto_pb2.KeyType.Ed25519,
    "rsa": crypto_pb2.KeyType.RSA,
    "secp256k1": crypto_pb2.KeyType.Secp256k1,
}


def _normalize_key_type(key_type: str) -> str:
    normalized = key_type.lower().replace("-", "").replace("_", "")
    try:
        return _KEY_TYPE_ALIASES[normalized]
    except KeyError as e:
        supported_key_types = ", ".join(SUPPORTED_KEY_TYPES)
        raise ValueError(f"Unsupported key type '{key_type}'. Supported key types: {supported_key_types}") from e


def _create_key_pair(key_type: str) -> KeyPair:
    key_type = _normalize_key_type(key_type)
    if key_type == "ecc":
        return create_new_ecc_key_pair("P-256")
    if key_type == "ed25519":
        return create_new_ed25519_key_pair(secrets.token_bytes(32))
    if key_type == "rsa":
        return create_new_rsa_key_pair()
    if key_type == "secp256k1":
        return create_new_secp256k1_key_pair()

    raise ValueError(f"Unsupported key type '{key_type}'. Supported key types: {', '.join(SUPPORTED_KEY_TYPES)}")


def _rsa_private_key_from_bytes(data: bytes) -> RSAPrivateKey:
    return RSAPrivateKey(RSA.import_key(data))


def _ecc_private_key_from_bytes(data: bytes) -> ECCPrivateKey:
    private_key_impl, public_key_impl = PEMEncoder.decode_private_key(data.decode())
    curve = public_key_impl.curve if public_key_impl is not None else infer_local_type("P-256")
    return ECCPrivateKey(private_key_impl, curve)


_PRIVATE_KEY_DESERIALIZERS = {
    crypto_pb2.KeyType.ECDSA: _ecc_private_key_from_bytes,
    crypto_pb2.KeyType.Ed25519: Ed25519PrivateKey.from_bytes,
    crypto_pb2.KeyType.RSA: _rsa_private_key_from_bytes,
    crypto_pb2.KeyType.Secp256k1: Secp256k1PrivateKey.from_bytes,
}


def _deserialize_private_key(protobuf: crypto_pb2.PrivateKey):
    try:
        deserializer = _PRIVATE_KEY_DESERIALIZERS[protobuf.Type]
    except KeyError as e:
        if protobuf.Type in crypto_pb2.KeyType.values():
            key_type = crypto_pb2.KeyType.Name(protobuf.Type)
        else:
            key_type = protobuf.Type
        raise ValueError(f"Unsupported key type: {key_type}") from e

    return deserializer(protobuf.Data)


def _load_private_key(path: str):
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        raise ValueError("Private key not found")

    protobuf = crypto_pb2.PrivateKey.FromString(data)
    return _deserialize_private_key(protobuf)


def store_private_key(path: str, key_type: str = "ed25519"):
    normalized_key_type = _normalize_key_type(key_type)
    key_pair = _create_key_pair(normalized_key_type)

    peer_id = PeerID.from_pubkey(key_pair.public_key)
    print(f"Peer ID: {peer_id}")

    protobuf = crypto_pb2.PrivateKey(
        Type=_PROTOBUF_KEY_TYPES[normalized_key_type],
        Data=key_pair.private_key.to_bytes(),
    )

    # Store main private key
    with open(path, "wb") as f:
        f.write(protobuf.SerializeToString())

    time.sleep(0.5)

    try:
        key_pair = get_key_pair(path)
        print("✅ Success")
    except Exception as e:
        print(f"❌ Error getting key pair: {e}")


def get_key_pair(
    path: str,
) -> KeyPair:
    """
    Get a keypair if it exists in path.
    """
    private_key = _load_private_key(path)
    return KeyPair(private_key, private_key.get_public_key())


def get_peer_id(
    path: str,
) -> PeerID:
    """
    Get a peer ID if it exists in path.
    """
    private_key = _load_private_key(path)
    return PeerID.from_pubkey(private_key.get_public_key())
