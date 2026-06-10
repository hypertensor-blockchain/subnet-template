import argparse

from subnet.utils.crypto.store_key import SUPPORTED_KEY_TYPES, store_private_key

# python -m subnet.cli.crypto.keygen --path test.key --key-type ed25519


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--path",
        type=str,
        required=False,
        default="private_key.key",
        help="File location of private key. ",
    )
    parser.add_argument(
        "--key-type",
        "--key_type",
        type=str,
        required=False,
        default="ed25519",
        choices=SUPPORTED_KEY_TYPES,
        help="Type of libp2p private key to generate. ",
    )

    args = parser.parse_args()

    try:
        store_private_key(args.path, key_type=args.key_type)
    except Exception as e:
        print(f"❌ Error getting key pair: {e}")
        print(f"Run `rm -rf {args.path}` and try again.")


if __name__ == "__main__":
    main()
