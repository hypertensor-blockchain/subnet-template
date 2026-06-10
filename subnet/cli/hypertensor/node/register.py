import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from libp2p.utils.address_validation import (
    get_optimal_binding_address,
)

from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom
from subnet.hypertensor.helpers import multiaddr_to_bytes
from subnet.utils.logging_config import configure_logging

load_dotenv(os.path.join(Path.cwd(), ".env"))

PHRASE = os.getenv("PHRASE")

configure_logging()
logger = logging.getLogger(__name__)

"""
python -m subnet.cli.hypertensor.node.register \
--subnet_id 1 \
--hotkey 0x317D7a5a2ba5787A99BE4693Eb340a10C71d680b \
--peer_id 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cb \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00

# Local

[Alith]

python -m subnet.cli.hypertensor.node.register \
--subnet_id 1 \
--hotkey 0x317D7a5a2ba5787A99BE4693Eb340a10C71d680b \
--peer_id 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cb \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc

[Baltathar]

python -m subnet.cli.hypertensor.node.register \
--subnet_id 1 \
--hotkey 0xc30fE91DE91a3FA79E42Dfe7a01917d0D92D99D7 \
--peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsV \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x8075991ce870b93a8870eca0c0f91913d12f47948ca0fd25b49c6fa7cdbeee8b" \
--local_rpc

[Charleth]

python -m subnet.cli.hypertensor.node.register \
--subnet_id 1 \
--hotkey 0x2f7703Ba9953d422294079A1CB32f5d2B60E38EB \
--peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEt \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x0b6e18cafb6ed99687ec547bd28139cafdd2bffe70e6b688025de6b445aa5c5b" \
--local_rpc

[Dorothy]

python -m subnet.cli.hypertensor.node.register \
--subnet_id 1 \
--hotkey 0x294BFfC18b5321264f55c517Aca2963bEF9D29EA \
--peer_id 12D3KooWD1BgwEJGUXz3DsKVXGFq3VcmHRjeX56NKpyEa1QAP6uV \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x39539ab1876910bbf3a223d84a29e28f1cb4e2e456503e7e91ed39b2e7223d68" \
--local_rpc

"""


def main():
    # fmt:off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--subnet_id", type=int, required=True, help="Subnet ID stored on blockchain. ")
    parser.add_argument("--hotkey", type=str, required=True, help="Hotkey responsible for subnet features. ")
    parser.add_argument("--peer_id", type=str, required=True, help="Peer ID generated using `keygen`")
    parser.add_argument("--port", type=int, required=False, default=None, help="Port to listen on when running node, used to generate peer info multiaddr")
    parser.add_argument("--bootnode_peer_id", type=str, required=False, default=None, help="Bootnode Peer ID generated using `keygen`")
    parser.add_argument("--client_peer_id", type=str, required=False, default=None, help="Bootstrap Peer ID generated using `keygen`")
    parser.add_argument("--delegate_reward_rate", type=float, required=False, default=0.0, help="Reward weight for your delegate stakers")
    parser.add_argument("--stake_to_be_added", type=float, required=True, help="Amount of stake to be added as float")
    parser.add_argument("--unique", type=str, required=False, default=None, help="Non-unique value for subnet node")
    parser.add_argument("--non_unique", type=str, required=False, default=None, help="Non-unique value for subnet node")
    parser.add_argument("--max_burn_amount", type=float, required=False, default=None, help="Non-unique value for subnet node")
    parser.add_argument("--local_rpc", action="store_true", help="[Testing] Run in local RPC mode, uses LOCAL_RPC")
    parser.add_argument("--phrase", type=str, required=False, help="[Testing] Coldkey phrase that controls actions which include funds, such as registering, and staking")
    parser.add_argument("--private_key", type=str, required=False, help="[Testing] Hypertensor blockchain private key")

    args = parser.parse_args()
    local_rpc = args.local_rpc
    phrase = args.phrase
    private_key = args.private_key

    hotkey = args.hotkey

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

    if hotkey is None:
        hotkey = hypertensor.keypair.ss58_address

    subnet_id = args.subnet_id

    if subnet_id < 128000:
        real_subnet_id = hypertensor.get_subnet_id_from_friendly_id(subnet_id)
        logger.info(
            f"Subnet ID {subnet_id} is less than 128000 and likely a friendly ID, using real subnet ID {real_subnet_id}"
        )
        subnet_id = int(str(real_subnet_id))

    # Peer info
    if args.port is None:
        peer_info = {
            "peer_id": args.peer_id,
            "multiaddr": None,
        }
    else:
        optimal_addr = get_optimal_binding_address(args.port)
        optimal_addr_with_peer = f"{optimal_addr}/p2p/{args.peer_id}"

        peer_info = {
            "peer_id": args.peer_id,
            "multiaddr": multiaddr_to_bytes(optimal_addr_with_peer),
        }

    # Bootnode peer info
    if args.bootnode_peer_id is None:
        bootnode_peer_info = None
    else:
        bootnode_peer_info = {
            "peer_id": args.bootnode_peer_id,
            "multiaddr": None,
        }

    # Client peer info
    if args.client_peer_id is None:
        client_peer_info = None
    else:
        client_peer_info = {
            "peer_id": args.client_peer_id,
            "multiaddr": None,
        }

    delegate_reward_rate = int(args.delegate_reward_rate * 1e18)
    stake_to_be_added = int(args.stake_to_be_added * 1e18)
    unique = args.unique
    non_unique = args.non_unique
    max_burn_amount = int(args.max_burn_amount * 1e18)

    try:
        receipt = hypertensor.register_subnet_node(
            subnet_id,
            hotkey,
            peer_info,
            delegate_reward_rate,
            stake_to_be_added,
            max_burn_amount,
            bootnode_peer_info=bootnode_peer_info,
            client_peer_info=client_peer_info,
            unique=unique,
            non_unique=non_unique,
            delegate_account=None
        )
        if receipt.is_success:
            logger.info('✅ Success, triggered events:')
            for event in receipt.triggered_events:
                attributes = event.value.get('attributes')
                if not attributes and 'event' in event.value:
                    attributes = event.value['event'].get('attributes')

                if attributes and 'subnet_node_id' in attributes:
                    logger.info(f"Subnet Node ID: {attributes['subnet_node_id']}")
        else:
            logger.error(f'⚠️ Extrinsic Failed: {receipt.error_message}')
    except Exception as e:
        logger.error("Error: ", e, exc_info=True)


if __name__ == "__main__":
    main()
