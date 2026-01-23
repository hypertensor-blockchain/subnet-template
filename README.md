# py-libp2p-subnet

A subnet template framework implementation.

⚠️ This is a work in progress and is not yet ready for production use.

## Installation

### From Source

Clone the repository and install:

```bash
git clone https://github.com/hayotensor/py-libp2p-subnet.git
cd py-libp2p-subnet
python -m venv .venv
source .venv/bin/activate
pip install .
touch .env
```

### Development Installation

For development, install with dev dependencies:

```bash
pip install -e ".[dev]"
```

```python
# Import your package
import subnet

# Add usage examples here
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=subnet --cov-report=html

# Run specific test file
pytest tests/test_example.py
```

### Running Locally

The the subnetwork locally with no blockchain integration for testing purposes.

#### Start Bootnode

Start the bootnode that doesn't participate in consensus

```bash
python -m subnet.cli.run_node \
--private_key_path bootnode.key \
--port 38960 \
--subnet_id 1 \
--no_blockchain_rpc \
--is_bootstrap \
--no_blockchain_rpc
```

#### Start Peers (Nodes)

##### Start Node 1 (Alith)

```bash
python -m subnet.cli.run_node \
--private_key_path alith.key \
--port 38961 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 1 \
--no_blockchain_rpc
```

##### Start Node 2 (Baltathar)

```bash
python -m subnet.cli.run_node \
--private_key_path baltathar.key \
--port 38962 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 2 \
--no_blockchain_rpc
```

##### Start Node 3 (Charleth)

```bash
python -m subnet.cli.run_node \
--private_key_path charleth.key \
--port 38963 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 3 \
--no_blockchain_rpc
```

##### Start Node 4 (Dorothy) (Optional)

```bash
python -m subnet.cli.run_node \
--private_key_path dorothy.key \
--port 38964 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 4 \
--no_blockchain_rpc
```

### Running Local RPC (Local BLockchain)

Start the blockchain (See [GitHub](https://github.com/hypertensor-blockchain/hypertensor-blockchain))

#### Register Subnet

Register with Alith as the owner

```bash
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
```

#### Register Nodes

##### Register Node ID 1 (Alith, alith.key)

We use bootnode.key as the `bootnode_peer_id` of node ID to pass validation mechanisms like proof-of-stake and connection maintenance when connecting to the bootnode. Otherwise the bootnode will not allow the node to connect since it will query the chain to ensure whoever connects to it is a valid peer ID (`peer_id`, `bootnode_peer_id`, `client_peer_id`).

```bash
register_node \
--subnet_id 1 \
--hotkey 0x317D7a5a2ba5787A99BE4693Eb340a10C71d680b \
--peer_id 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cb \
--bootnode_peer_id 12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--bootnode /ip4/127.00.1/tcp/38961/p2p/12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cb \
--client_peer_id 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cd \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc
```

##### Register Node ID 2 (Baltathar, baltathar.key)

```bash
register_node \
--subnet_id 1 \
--hotkey 0xc30fE91DE91a3FA79E42Dfe7a01917d0D92D99D7 \
--peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsV \
--bootnode_peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsw \
--bootnode /ip4/127.00.1/tcp/38962/p2p/12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsV \
--client_peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsx \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x8075991ce870b93a8870eca0c0f91913d12f47948ca0fd25b49c6fa7cdbeee8b" \
--local_rpc
```

##### Register Node ID 3 (Charleth, charleth.key)

```bash
register_node \
--subnet_id 1 \
--hotkey 0x2f7703Ba9953d422294079A1CB32f5d2B60E38EB \
--peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEt \
--bootnode_peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEu \
--bootnode /ip4/127.00.1/tcp/38963/p2p/12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEt \
--client_peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEv \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x0b6e18cafb6ed99687ec547bd28139cafdd2bffe70e6b688025de6b445aa5c5b" \
--local_rpc
```

##### Register Node ID 4 (Dorothy, dorothy.key)

```bash
register_node \
--subnet_id 1 \
--hotkey 0x294BFfC18b5321264f55c517Aca2963bEF9D29EA \
--peer_id 12D3KooWD1BgwEJGUXz3DsKVXGFq3VcmHRjeX56NKpyEa1QAP6uV \
--bootnode_peer_id 12D3KooWD1BgwEJGUXz3DsKVXGFq3VcmHRjeX56NKpyEa1QAP6uW \
--bootnode /ip4/127.00.1/tcp/38964/p2p/12D3KooWD1BgwEJGUXz3DsKVXGFq3VcmHRjeX56NKpyEa1QAP6uV \
--client_peer_id 12D3KooWD1BgwEJGUXz3DsKVXGFq3VcmHRjeX56NKpyEa1QAP6uX \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x39539ab1876910bbf3a223d84a29e28f1cb4e2e456503e7e91ed39b2e7223d68" \
--local_rpc
```

##### Optional Node ID 5 (Ethan, ethan.key)

```bash
register_node \
--subnet_id 1 \
--hotkey 0x919a696741e5bEe48538D43CB8A34a95261E62fc \
--peer_id 12D3KooWMGKEpzz3EWGU2ayhwFriRh23QnQ479Ctfj8xSmDRirde \
--bootnode_peer_id 12D3KooWMGKEpzz3EWGU2ayhwFriRh23QnQ479Ctfj8xSmDRirdf \
--bootnode /ip4/127.00.1/tcp/38965/p2p/12D3KooWMGKEpzz3EWGU2ayhwFriRh23QnQ479Ctfj8xSmDRirde \
--client_peer_id 12D3KooWMGKEpzz3EWGU2ayhwFriRh23QnQ479Ctfj8xSmDRirdg \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x7dce9bc8babb68fec1409be38c8e1a52650206a7ed90ff956ae8a6d15eeaaef4" \
--local_rpc
```

#### Run Nodes

##### Start Bootnode

```bash
python -m subnet.cli.run_node \
--private_key_path bootnode.key \
--port 38960 \
--subnet_id 1 \
--is_bootstrap \
--local_rpc
```

##### Start Node ID 1 (Alith)

We use the bootnodes peer id (alith.key) in node01's bootnode so Alith can connect (subnet requires on-chain proof-of-stake for connection).

```bash
python -m subnet.cli.run_node \
--private_key_path alith.key \
--port 38961 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 1 \
--local_rpc \
--tensor_private_key "0x883189525adc71f940606d02671bd8b7dfe3b2f75e2a6ed1f5179ac794566b40"
```

##### Start Node ID 2 (Baltathar)

We use the bootnodes peer id (alith.key) in node01's bootnode so Alith can connect (subnet requires on-chain proof-of-stake for connection).

```bash
python -m subnet.cli.run_node \
--private_key_path baltathar.key \
--port 38962 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 2 \
--local_rpc \
--tensor_private_key "0x6cbf451fc5850e75cd78055363725dcf8c80b3f1dfb9c29d131fece6dfb72490"
```

##### Start Node ID 3 (Charleth)

```bash
python -m subnet.cli.run_node \
--private_key_path charleth.key \
--port 38963 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 3 \
--local_rpc \
--tensor_private_key "0x51b7c50c1cd27de89a361210431e8f03a7ddda1a0c8c5ff4e4658ca81ac02720"
```

##### Optional Start Node ID 4 (Dorothy)

```bash
python -m subnet.cli.run_node \
--private_key_path dorothy.key \
--port 38964 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 4 \
--local_rpc \
--tensor_private_key "0xa1983be71acf4b323612067ac9ae91308da19c2956b227618e8c611bd4746056"
```

##### Optional Start Node ID 5 (Ethan)

```bash
python -m subnet.cli.run_node \
--private_key_path ethan.key \
--port 38965 \
--bootstrap /ip4/127.0.0.1/tcp/38960/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 5 \
--local_rpc \
--tensor_private_key "0xcde1e97047f6cc83c0b3b4b795f45427857dee65e5348d39d08cf79840105882"
```

#### Delegate Stake To Subnet

This requires a minimum delegate stake of 0.01% of the total supply.

#### Activate Subnet

Activate the subnet from the owners coldkey (Alith).

```bash
python -m subnet.cli.activate_subnet \
--subnet_id 1 \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133"
```

### Code Quality

This project uses several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing

Run all quality checks:

```bash
make lint
make test
```

### Pre-commit Hooks

Install pre-commit hooks:

```bash
pre-commit install
```

## Documentation

Coming soon...

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
