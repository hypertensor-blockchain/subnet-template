import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from substrateinterface import Keypair, KeypairType
from substrateinterface.utils.ecdsa_helpers import mnemonic_to_ecdsa_private_key

load_dotenv(os.path.join(Path.cwd(), ".env"))

PHRASE = os.getenv("PHRASE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


"""
multi-key-view --phrase "mnemonic phrase here"
"""


def main():
    # fmt:off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--phrase", type=str, required=False, default=12, help="The mnemonic phrase")

    args = parser.parse_args()
    phrase = args.phrase

    try:
      mnemonic = phrase
      ecdsa_keypair = Keypair.create_from_mnemonic(mnemonic, crypto_type=KeypairType.ECDSA)
      sr25519_keypair = Keypair.create_from_mnemonic(mnemonic, crypto_type=KeypairType.SR25519)
      private_key = mnemonic_to_ecdsa_private_key(mnemonic).hex()

      print("\n")
      print("SR25519 Private Key: ", sr25519_keypair.private_key.hex())
      print("SR25519 Address: ", sr25519_keypair.ss58_address)
      print("SR25519 Public Key: ", sr25519_keypair.public_key)
      print("SR25519 Seed Hex: ", sr25519_keypair.seed_hex)
      print("SR25519 SS58 Format: ", sr25519_keypair.ss58_format)
      print("SR25519 Mnemonic: ", sr25519_keypair.mnemonic)
      print("SR25519 Crypto Type: ", sr25519_keypair.crypto_type)
      print("SR25519 Derive Path: ", sr25519_keypair.derive_path)

      print("\n")
      print("ECDSA Private Key: ", ecdsa_keypair.private_key.hex())
      print("ECDSA Private Key: ", private_key)
      print("ECDSA Address: ", ecdsa_keypair.ss58_address)
      print("ECDSA Mnemonic: ", ecdsa_keypair.mnemonic)
      print("ECDSA Crypto Type: ", ecdsa_keypair.crypto_type)
      print("ECDSA Derive Path: ", ecdsa_keypair.derive_path)

      print("\n")
    except Exception as e:
        logger.error("Error: ", e, exc_info=True)


if __name__ == "__main__":
    main()
