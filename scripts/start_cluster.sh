#!/usr/bin/env bash
set -e

NODES=${1:-3}
BASE_PORT=${2:-6380}

echo "Starting ${NODES}-node DRedis cluster on ports ${BASE_PORT}-$((BASE_PORT + NODES - 1))"

NET_NAME="dredis-net"
docker network create "$NET_NAME" 2>/dev/null || true

for i in $(seq 0 $((NODES - 1))); do
    PORT=$((BASE_PORT + i))
    CLUSTER_PORT=$((PORT + 10000))
    SEEDS=""
    for j in $(seq 0 $((NODES - 1))); do
        if [ "$j" -eq "$i" ]; then continue; fi
        if [ -n "$SEEDS" ]; then SEEDS="${SEEDS},"; fi
        SEEDS="${SEEDS}dredis-node-$j:$PORT"
    done

    echo "Starting node $i on port $PORT..."
    docker rm -f "dredis-node-$i" 2>/dev/null || true
    docker run -d \
        --name "dredis-node-$i" \
        --network "$NET_NAME" \
        -p "$PORT:$PORT" \
        -p "$CLUSTER_PORT:$CLUSTER_PORT" \
        -e "DREDIS_PORT=$PORT" \
        -e "DREDIS_IP=dredis-node-$i" \
        -e "DREDIS_SEEDS=$SEEDS" \
        -e "DREDIS_REPL_FACTOR=3" \
        -e "DREDIS_WRITE_QUORUM=2" \
        -e "DREDIS_READ_QUORUM=2" \
        dredis > /dev/null

    echo "  Node $i: 127.0.0.1:$PORT (cluster port $CLUSTER_PORT)"
done

echo ""
echo "All nodes started. Test with:"
echo "  redis-cli -p $BASE_PORT PING"
echo "  redis-cli -p $BASE_PORT CLUSTER INFO"
echo "  redis-cli -p $BASE_PORT CLUSTER NODES"
