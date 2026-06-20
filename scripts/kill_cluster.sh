#!/usr/bin/env bash
set -e

NODES=${1:-3}

echo "Stopping cluster..."
for i in $(seq 0 $((NODES - 1))); do
    echo "  Stopping node $i..."
    docker rm -f "dredis-node-$i" 2>/dev/null || true
done

echo "All nodes stopped."
