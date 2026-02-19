import argparse

from subnet.utils.crypto.store_key import get_peer_id

# python -m subnet.cli.crypto.keyview --path alith.key
# python -m subnet.cli.crypto.keyview --path main-node01.key


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

    try:
        peer_id = get_peer_id(args.path)
        print(f"Peer ID: {peer_id}")
    except Exception as e:
        print(f"{e}")


if __name__ == "__main__":
    main()
