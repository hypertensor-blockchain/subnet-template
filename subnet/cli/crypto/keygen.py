import argparse
import secrets
import time

from libp2p.crypto.ed25519 import create_new_key_pair as create_new_ed25519_key_pair
from libp2p.peer.id import ID as PeerID
from libp2p.peer.pb import crypto_pb2

from subnet.utils.crypto.store_key import get_key_pair

# python -m subnet.cli.crypto.keygen --path test.key


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

    path = args.path

    key_pair = create_new_ed25519_key_pair(secrets.token_bytes(32))

    peer_id = PeerID.from_pubkey(key_pair.public_key)
    print(f"Peer ID: {peer_id}")

    protobuf = crypto_pb2.PrivateKey(Type=crypto_pb2.KeyType.Ed25519, Data=key_pair.private_key.to_bytes())

    # Store main private key
    with open(path, "wb") as f:
        f.write(protobuf.SerializeToString())

    time.sleep(0.5)

    try:
        key_pair = get_key_pair(path)
        print("✅ Success")
    except Exception as e:
        print(f"❌ Error getting key pair: {e}")
        print(f"Run `rm -rf {path}` and try again.")


if __name__ == "__main__":
    main()
