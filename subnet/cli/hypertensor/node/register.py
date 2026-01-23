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
register-node \
--subnet_id 1 \
--hotkey 0x317D7a5a2ba5787A99BE4693Eb340a10C71d680b \
--peer_id QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JB \
--bootnode_peer_id QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JC \
--bootnode /ip4/127.00.1/tcp/31330/p2p/QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JC \
--client_peer_id QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JD \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \

# Local

[Alith]

# NOTE: We register the bootnode.id peer ID as Aliths peer ID so the bootnode passes PoS when connecting to bootnode

register-node \
--subnet_id 1 \
--hotkey 0x317D7a5a2ba5787A99BE4693Eb340a10C71d680b \
--peer_id QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JB \
--bootnode_peer_id QmSjcNmhbRvek3YDQAAQ3rV8GKR8WByfW8LC4aMxk6gj7v \
--bootnode /ip4/127.00.1/tcp/31330/p2p/QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JC \
--client_peer_id QmShJYgxNoKn7xqdRQj5PBcNfPSsbWkgFBPA4mK5PH73JD \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc

[Baltathar]

register-node \
--subnet_id 1 \
--hotkey 0xc30fE91DE91a3FA79E42Dfe7a01917d0D92D99D7 \
--peer_id QmbRz8Bt1pMcVnUzVQpL2icveZz2MF7VtELC44v8kVNwiG \
--bootnode_peer_id QmbRz8Bt1pMcVnUzVQpL2icveZz2MF7VtELC44v8kVNwiH \
--bootnode /ip4/127.00.1/tcp/31330/p2p/QmbRz8Bt1pMcVnUzVQpL2icveZz2MF7VtELC44v8kVNwiH \
--client_peer_id QmbRz8Bt1pMcVnUzVQpL2icveZz2MF7VtELC44v8kVNwiI \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x8075991ce870b93a8870eca0c0f91913d12f47948ca0fd25b49c6fa7cdbeee8b" \
--local_rpc

[Charleth]

register-node \
--subnet_id 1 \
--hotkey 0x2f7703Ba9953d422294079A1CB32f5d2B60E38EB \
--peer_id QmTJ8uyLJBwVprejUQfYFAywdXWfdnUQbC1Xif6QiTNta9 \
--bootnode_peer_id QmTJ8uyLJBwVprejUQfYFAywdXWfdnUQbC1Xif6QiTNta1 \
--bootnode /ip4/127.00.1/tcp/31330/p2p/QmTJ8uyLJBwVprejUQfYFAywdXWfdnUQbC1Xif6QiTNta1 \
--client_peer_id QmTJ8uyLJBwVprejUQfYFAywdXWfdnUQbC1Xif6QiTNta2 \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x0b6e18cafb6ed99687ec547bd28139cafdd2bffe70e6b688025de6b445aa5c5b" \
--local_rpc

[Dorothy]

register-node \
--subnet_id 1 \
--hotkey 0x294BFfC18b5321264f55c517Aca2963bEF9D29EA \
--peer_id QmPpeHpL6R4aXeBxRqqvA78mNW9QjM1ZiFrS3n2MdMtPKJ \
--bootnode_peer_id QmPpeHpL6R4aXeBxRqqvA78mNW9QjM1ZiFrS3n2MdMtPKK \
--bootnode /ip4/127.00.1/tcp/31330/p2p/QmPpeHpL6R4aXeBxRqqvA78mNW9QjM1ZiFrS3n2MdMtPKK \
--client_peer_id QmPpeHpL6R4aXeBxRqqvA78mNW9QjM1ZiFrS3n2MdMtPKL \
--delegate_reward_rate 0.125 \
--stake_to_be_added 100.00 \
--max_burn_amount 100.00 \
--private_key "0x39539ab1876910bbf3a223d84a29e28f1cb4e2e456503e7e91ed39b2e7223d68" \
--local_rpc

"""


def main():
    # fmt:off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--subnet_id", type=str, required=True, help="Subnet ID stored on blockchain. ")
    parser.add_argument("--hotkey", type=str, required=True, help="Hotkey responsible for subnet features. ")
    parser.add_argument("--peer_id", type=str, required=True, help="Peer ID generated using `keygen`")
    parser.add_argument("--bootnode_peer_id", type=str, required=True, help="Bootnode Peer ID generated using `keygen`")
    parser.add_argument("--client_peer_id", type=str, required=True, help="Bootstrap Peer ID generated using `keygen`")
    parser.add_argument("--bootnode", type=str, required=False, default=None, help="Bootnode URL/MultiAddr")
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
    bootnode = args.bootnode

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
    peer_id = args.peer_id
    bootnode_peer_id = args.bootnode_peer_id
    client_peer_id = args.client_peer_id
    delegate_reward_rate = int(args.delegate_reward_rate * 1e18)
    stake_to_be_added = int(args.stake_to_be_added * 1e18)
    unique = args.unique
    non_unique = args.non_unique
    max_burn_amount = int(args.max_burn_amount * 1e18)

    try:
        receipt = hypertensor.register_subnet_node(
            subnet_id,
            hotkey,
            peer_id,
            bootnode_peer_id,
            client_peer_id,
            delegate_reward_rate,
            stake_to_be_added,
            max_burn_amount,
            bootnode=bootnode,
            unique=unique,
            non_unique=non_unique
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
