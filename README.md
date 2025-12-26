# py-libp2p-subnet

A Python libp2p subnet template framework implementation.

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

#### Start Bootnode

```bash
python -m subnet.cli.run_bootnode_v2 \
--identity_path bootnode-ed25519.key \
--port 38959
```

#### Start Peers (Nodes)

##### Start Node 1 (Alith)

```bash
python -m subnet.cli.run_node_v2 \
--identity_path alith-ed25519.key \
--port 38960 \
--bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF
```

##### Start Node 2 (Baltathar)

```bash
python -m subnet.cli.run_node_v2 \
--identity_path baltathar-ed25519.key \
--port 38961 \
--bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF
```

##### Start Node 3 (Charleth)

```bash
python -m subnet.cli.run_node_v2 \
--identity_path charleth-ed25519.key \
--port 38962 \
--bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF
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
--key_types "Rsa" \
--bootnodes "p2p/127.0.0.1/tcp" \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc
```

#### Register Nodes

##### Register Node01 (Alith)

```bash
register_node \
--subnet_id 1 \
--hotkey 0x317D7a5a2ba5787A99BE4693Eb340a10C71d680b \
--peer_id 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cb \
--bootnode_peer_id 12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--bootnode /ip4/127.00.1/tcp/31331/p2p/12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cc \
--client_peer_id 12D3KooWMwW1VqH7uWtUc5UGoyMJp1dG26Nkosc6RkRJ7RNiW6Cd \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133" \
--local_rpc
```

##### Register Node02 (Baltathar)

```bash
register_node \
--subnet_id 1 \
--hotkey 0xc30fE91DE91a3FA79E42Dfe7a01917d0D92D99D7 \
--peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsV \
--bootnode_peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsW \
--bootnode /ip4/127.00.1/tcp/31332/p2p/12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsW \
--client_peer_id 12D3KooWM5J4zS17XR2LHGZgRpmzbeqg4Eibyq8sbRLwRuWxJqsX \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x8075991ce870b93a8870eca0c0f91913d12f47948ca0fd25b49c6fa7cdbeee8b" \
--local_rpc
```

##### Register Node03 (Charleth)

```bash
register_node \
--subnet_id 1 \
--hotkey 0x2f7703Ba9953d422294079A1CB32f5d2B60E38EB \
--peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEt \
--bootnode_peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEu \
--bootnode /ip4/127.00.1/tcp/31332/p2p/12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEu \
--client_peer_id 12D3KooWKxAhu5U8SreDZpokVkN6ciTBbsHxteo3Vmq6Cpuf8KEv \
--delegate_reward_rate 0.125 \
--stake_to_be_added 200.00 \
--max_burn_amount 100.00 \
--private_key "0x0b6e18cafb6ed99687ec547bd28139cafdd2bffe70e6b688025de6b445aa5c5b" \
--local_rpc
```

#### Run Nodes

##### Start Bootnode

```bash
python -m subnet.cli.run_bootnode_v2 \
--identity_path bootnode-ed25519.key \
--port 38959 \
--local_rpc
```

##### Start Node01 (Alith)

We use the bootnodes peer id (bootnode-ed25519.key) in node01's bootnode so Alith can connect (subnet requires on-chain proof-of-stake for connection).

```bash
python -m subnet.cli.run_node \
--identity_path alith.key \
--port 31331 \
--bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 1 \
--local_rpc \
--tensor_private_key "0x883189525adc71f940606d02671bd8b7dfe3b2f75e2a6ed1f5179ac794566b40"
```

##### Start Node02 (Baltathar)

```bash
python -m subnet.cli.run_node \
--identity_path baltathar.key \
--port 31332 \
--bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 2 \
--local_rpc \
--tensor_private_key "0x6cbf451fc5850e75cd78055363725dcf8c80b3f1dfb9c29d131fece6dfb72490"
```

##### Start Node03 (Charleth)

```bash
python -m subnet.cli.run_node \
--identity_path charleth.key \
--port 31333 \
--bootstrap /ip4/127.0.0.1/tcp/38959/p2p/12D3KooWLGmub3LXuKQixBD5XwNW4PtSfnrysYzqs1oj19HxMUCF \
--subnet_id 1 \
--subnet_node_id 3 \
--local_rpc \
--tensor_private_key "0x51b7c50c1cd27de89a361210431e8f03a7ddda1a0c8c5ff4e4658ca81ac02720"
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
