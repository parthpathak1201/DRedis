#!/bin/bash
set -euo pipefail

docker run --rm --network=dredis-demo redis:alpine redis-benchmark --cluster \
    -h node0 -p 6380 -c 10 -n 5000 -t set,get -q "$@"
