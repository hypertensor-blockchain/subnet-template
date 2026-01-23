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
[Local RPC Testing]

Alith (register with Alith as owner of subnet):

Initial coldkeys (See hypertensor/README.md): Alith, Baltathar, Charleth, Dorothy

python -m subnet.cli.hypertensor.subnet.register \
--max_cost 100.00 \
--name subnet-1 \
--repo github.com/subnet-1 \
--description "artificial intelligence" \
--misc "cool subnet" \
--min_stake 100.00 \
--max_stake  1000.00 \
--delegate_stake_percentage 0.1 \
--initial_coldkey 0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac 1 \
--initial_coldkey 0x3Cd0A705a2DC65e5b1E1205896BaA2be8A07c6e0 1 \
--initial_coldkey 0x798d4Ba9baf0064Ec19eB4F0a1a45785ae9D6DFc 1 \
--initial_coldkey 0x773539d4Ac0e786233D90A233654ccEE26a613D9 1 \
--initial_coldkey 0xFf64d3F6efE2317EE2807d223a0Bdc4c0c49dfDB 1 \
--key_types "Rsa" \
--bootnode 12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc

register_subnet \
--max_cost 100.00 \
--name subnet-1 \
--repo github.com/subnet-1 \
--description "artificial intelligence" \
--misc "cool subnet" \
--min_stake 100.00 \
--max_stake  1000.00 \
--delegate_stake_percentage 0.1 \
--initial_coldkey 0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac 1 \
--initial_coldkey 0x3Cd0A705a2DC65e5b1E1205896BaA2be8A07c6e0 1 \
--initial_coldkey 0x798d4Ba9baf0064Ec19eB4F0a1a45785ae9D6DFc 1 \
--initial_coldkey 0x773539d4Ac0e786233D90A233654ccEE26a613D9 1 \
--initial_coldkey 0xFf64d3F6efE2317EE2807d223a0Bdc4c0c49dfDB 1 \
--key_types "Rsa" \
--bootnode 12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc

"""


def main():
    # fmt:off
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--max_cost", type=float, required=True, help="Max cost you want to pay to register the subnet")
    parser.add_argument("--name", type=str, required=True, help="Subnet name (unique)")
    parser.add_argument("--repo", type=str, required=True, help="Subnet repository (unique)")
    parser.add_argument("--description", type=str, required=True, help="A short description of what the subnet does")
    parser.add_argument("--misc", type=str, required=True, help="Misc information about the subnet")
    parser.add_argument("--min_stake", type=float, required=True, help="Minimum stake balance to register a Subnet Node in the subnet")
    parser.add_argument("--max_stake", type=float, required=True, help="Maximum stake balance to register a Subnet Node in the subnet")
    parser.add_argument("--delegate_stake_percentage", type=float, required=True, help="Percentage of emissions that are allocated to delegate stakers as decimal (100% == 1.0)")
    parser.add_argument(
        "--initial_coldkey",
        action="append",
        nargs=2,
        metavar=("COLDKEY", "COUNT"),
        help="Specify a coldkey and count pair",
        required=True
    )
    parser.add_argument("--key_types", type=str, nargs='+', required=True, help="Key type of subnet signature system")
    parser.add_argument(
        "--bootnode",
        action="append",
        nargs=2,
        metavar=("PEER_ID", "MULTIADDRESS"),
        help="Specify a bootnode peer ID and multiaddress pair",
        required=True
    )
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

    hotkey = hypertensor.keypair.ss58_address
    max_cost = int(args.max_cost * 1e18)
    assert max_cost > 0, "Max cost must be greater than zero"
    name = args.name
    repo = args.repo
    description = args.description
    misc = args.misc
    min_stake = int(args.min_stake * 1e18)
    max_stake = int(args.max_stake * 1e18)
    assert min_stake <= max_stake, "min_stake must be less than or equal to max_stake"
    delegate_stake_percentage = 1e18 if args.delegate_stake_percentage > 1.0 else int(args.delegate_stake_percentage * 1e18)
    assert delegate_stake_percentage <= 0.95e18, "delegate_stake_percentage must be less than or equal to 95%"
    initial_coldkeys = [(ck, int(num)) for ck, num in args.initial_coldkey]
    initial_coldkeys = sorted(set(initial_coldkeys), key=lambda x: int(x[0], 16))

    assert len(initial_coldkeys) >= 3, "At least 3 initial coldkeys must be provided, See MinSubnetNodes in the Hypertensor Network pallet"
    key_types = args.key_types
    bootnodes = [(peer_id, multiaddr) for peer_id, multiaddr in args.bootnode]

    try:
        receipt = hypertensor.register_subnet(
            max_cost,
            name,
            repo,
            description,
            misc,
            min_stake,
            max_stake,
            delegate_stake_percentage,
            initial_coldkeys,
            key_types,
            bootnodes,
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
