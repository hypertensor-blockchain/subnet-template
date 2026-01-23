import argparse
import secrets

from libp2p.crypto.ed25519 import create_new_key_pair as create_new_ed25519_key_pair
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2

# keygen --path test.key


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--path",
        type=str,
        required=False,
        default="private_key.key",
        help="File location of private key. ",
    )
    # parser.add_argument(
    #     "--bootstrap_path",
    #     type=str,
    #     required=False,
    #     default="bootstrap_private_key.key",
    #     help="File location of bootstrap private key. ",
    # )
    # parser.add_argument(
    #     "--client_path",
    #     type=str,
    #     required=False,
    #     default="client_private_key.key",
    #     help="File location of bootstrap private key. ",
    # )
    # parser.add_argument(
    #     "--key_type",
    #     type=str,
    #     required=False,
    #     default="ed25519",
    #     help="Key type used in subnet. ed25519, rsa",
    # )

    args = parser.parse_args()

    path = args.path
    # bootstrap_path = args.bootstrap_path
    # client_path = args.client_path
    # key_type = args.key_type.lower()

    key_pair = create_new_ed25519_key_pair(secrets.token_bytes(32))

    peer_id = PeerID.from_pubkey(key_pair.public_key)
    print(f"Peer ID: {peer_id}")

    protobuf = crypto_pb2.PrivateKey(Type=crypto_pb2.KeyType.Ed25519, Data=key_pair.private_key.to_bytes())

    # Store main private key
    with open(path, "wb") as f:
        f.write(protobuf.SerializeToString())


if __name__ == "__main__":
    main()
