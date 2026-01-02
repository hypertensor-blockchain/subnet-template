import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom

load_dotenv(os.path.join(Path.cwd(), ".env"))

PHRASE = os.getenv("PHRASE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

"""
activate-subnet \
--subnet_id 1 \
--phrase "craft squirrel soap letter garment unfair meat slide swift miss forest wide" \
--local_rpc

[Local]

activate-subnet \
--subnet_id 1 \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc

"""


def main():
    # fmt:off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--subnet_id", type=int, required=True, help="Subnet UID")
    parser.add_argument("--local_rpc", action="store_true", help="[Testing] Run in local RPC mode, uses LOCAL_RPC")
    parser.add_argument("--phrase", type=str, required=False, help="[Testing] Coldkey phrase that controls actions which include funds, such as registering, and staking")
    parser.add_argument("--private_key", type=str, required=False, help="[Testing] Hypertensor blockchain private key")

    args = parser.parse_args()
    local_rpc = args.local_rpc
    phrase = args.phrase
    private_key = args.private_key

    if local_rpc:
        rpc = os.getenv('LOCAL_RPC')
    else:
        rpc = os.getenv('DEV_RPC')

    if phrase is not None:
        hypertensor = Hypertensor(rpc, phrase)
    elif private_key is not None:
        hypertensor = Hypertensor(rpc, private_key, KeypairFrom.PRIVATE_KEY)
    else:
        hypertensor = Hypertensor(rpc, PHRASE)

    subnet_id = args.subnet_id
    assert subnet_id > 0, "Invalid subnet ID, must be greater than zero"

    try:
        receipt = hypertensor.activate_subnet(
            subnet_id
        )
        if receipt.is_success:
            logger.info('✅ Success, triggered events:')
            for event in receipt.triggered_events:
                print(f'* {event.value}')
        else:
            logger.error(f'⚠️ Extrinsic Failed: {receipt.error_message}')
    except Exception as e:
        logger.error("Error: ", e, exc_info=True)


if __name__ == "__main__":
    main()
