import argparse
import secrets

from libp2p.crypto.ed25519 import (
    Ed25519PrivateKey,
    create_new_key_pair as create_new_ed25519_key_pair,
)
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.secp256k1 import Secp256k1PrivateKey
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2

# keyview --path alith.key


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--path",
        type=str,
        required=False,
        default="private_key.key",
        help="File location of private key. ",
    )

    args = parser.parse_args()

    with open(f"{args.path}", "rb") as f:
        data = f.read()
    private_key = crypto_pb2.PrivateKey.FromString(data)
    if private_key.Type == crypto_pb2.KeyType.Ed25519:
        private_key = Ed25519PrivateKey.from_bytes(private_key.Data)
    elif private_key.Type == crypto_pb2.KeyType.Secp256k1:
        private_key = Secp256k1PrivateKey.from_bytes(private_key.Data)
    else:
        raise ValueError("Unsupported key type")

    public_key = private_key.get_public_key()
    key_pair = KeyPair(private_key, public_key)

    peer_id = PeerID.from_pubkey(key_pair.public_key)
    print(f"Peer ID: {peer_id}")


if __name__ == "__main__":
    main()
