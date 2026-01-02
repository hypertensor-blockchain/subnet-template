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
generate-key --words 12
"""


def main():
    # fmt:off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--words", type=int, required=False, default=12, help="The amount of words to generate, valid values are 12, 15, 18, 21 and 24. ")

    args = parser.parse_args()
    words = args.words
    assert (
      words == 12 or
      words == 15 or
      words == 18 or
      words == 21 or
      words == 24
    ), "words valid values are 12, 15, 18, 21 and 24."

    try:
      mnemonic = Keypair.generate_mnemonic(words)
      keypair = Keypair.create_from_mnemonic(mnemonic, crypto_type=KeypairType.ECDSA)
      private_key = mnemonic_to_ecdsa_private_key(mnemonic).hex()
      hotkey = keypair.ss58_address

      print("\n")
      print(
        "Store the following mnemonic phrase in a safe place: \n \n"
        f"{mnemonic}"
      )
      print("\n\n")
      print(
        "Private key: \n \n"
        f"0x{private_key}"
      )
      print("\n\n")
      print(
        "Your account_id (hotkey or coldkey) is: \n \n"
        f"{hotkey}"
      )

      print("\n")
    except Exception as e:
        logger.error("Error: ", e, exc_info=True)


if __name__ == "__main__":
    main()
