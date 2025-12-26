
# Local Mock Hypertensor

## Overview

This is a simulated blockchain environment for local testing. It is an sqlite database to simulate extrinsics and RPC methods.

## Usage

### When starting a bootnode or node

If no initial peers are specified in the arguments via `--bootstrap` it will delete the database and reinitialize a new one. Otherwise, it will not remove the database.

**Ensure** when starting nodes, they all use **unique** `--subnet_node_id` arguments because the consensus logic depends on this.

### Contributing

File a new issue on [GitHub](https://github.com/hypertensor-blockchain/py-libp2p-subnet-template/issues)