#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-6380}"
COUNT="${2:-100000}"
CLIENTS="${3:-50}"
KEYSPACE="${4:-10000}"

echo "[Bench] XADD — $COUNT requests, $CLIENTS clients, port $PORT"

redis-benchmark \
    -p "$PORT" \
    -n "$COUNT" \
    -c "$CLIENTS" \
    -r "$KEYSPACE" \
    -q \
    XADD __rand_int__ \* field value
