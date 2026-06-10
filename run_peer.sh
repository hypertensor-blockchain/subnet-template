#!/bin/bash

# Run the docker container, passing all arguments to the run_node.py script
# We mount the current directory to access the .key files
docker run -it --rm \
  --network host \
  -v $(pwd):/app/keys \
  rocksdb-api \
  subnet.cli.run_node "$@"
