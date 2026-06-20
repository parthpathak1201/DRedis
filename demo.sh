#!/usr/bin/env bash
# ==============================================================================
#  DRedis — 10-Node Cluster Demo
#  ------------------------------
#  This script:
#    1. Builds the DRedis Docker image (if not already built)
#    2. Spins up a 10-node DRedis cluster on a dedicated Docker network
#    3. Exposes node0's dashboard at http://localhost:6381/
#    4. Exposes node0's Redis port at localhost:6380
#
#  All 10 nodes discover each other via gossip, form a consistent-hash ring,
#  and replicate data with replication-factor 3.
#
#  Hit Ctrl-C when done, then run the printed cleanup command.
# ==============================================================================

set -e

# ---- Configuration ----------------------------------------------------------
N=10                    # Number of cluster nodes
NET=dredis-demo         # Docker network name
IMG=dredis              # Docker image name
HOST_CLIENT=6380        # Host port mapped to node0's Redis port
HOST_DASHBOARD=6381     # Host port mapped to node0's HTTP dashboard

# ---- Helper functions -------------------------------------------------------
banner() {
  echo ""
  echo "================================================================"
  echo "  $1"
  echo "================================================================"
}

info()  { echo "  [INFO]  $1"; }
ok()    { echo "  [OK]    $1"; }
warn()  { echo "  [WARN]  $1"; }
fail()  { echo "  [FAIL]  $1"; exit 1; }

cleanup() {
  local ids
  ids=$(docker ps -a --filter "label=demo=$NET" --format "{{.ID}}")
  if [ -n "$ids" ]; then
    docker rm -f $ids > /dev/null 2>&1
  fi
  docker network rm "$NET" > /dev/null 2>&1 || true
  ok "Previous demo cleaned up"
}

# ==============================================================================
#  STEP 1 — Build Docker image
# ==============================================================================
banner "STEP 1/5 : Build DRedis Docker image"

if docker image inspect "$IMG" > /dev/null 2>&1; then
  info "Image '$IMG' already exists. Rebuilding to pick up latest changes..."
  docker build -t "$IMG" . > /dev/null 2>&1
else
  info "Building DRedis from source (this takes ~2 minutes the first time)..."
  docker build -t "$IMG" . > /dev/null 2>&1
fi
ok "Image '$IMG' is ready"

# ==============================================================================
#  STEP 2 — Clean up any previous demo
# ==============================================================================
banner "STEP 2/5 : Clean up previous demo (if any)"
cleanup

# ==============================================================================
#  STEP 3 — Create Docker network
# ==============================================================================
banner "STEP 3/5 : Create isolated Docker network"
docker network create "$NET" > /dev/null
ok "Network '$NET' created (bridge, 172.x.x.x)"

# ==============================================================================
#  STEP 4 — Start 10 DRedis nodes
# ==============================================================================
banner "STEP 4/5 : Start $N DRedis cluster nodes"

# Build seed list — every node gets the full list
SEEDS=""
for i in $(seq 0 $((N-1))); do
  [ -n "$SEEDS" ] && SEEDS+=","
  SEEDS+="node${i}:6380"
done

for i in $(seq 0 $((N-1))); do
  # Only node0 exposes ports to the host
  PORTS=""
  [ "$i" = "0" ] && PORTS="-p ${HOST_CLIENT}:6380 -p ${HOST_DASHBOARD}:6381"

  docker run -d \
    --label "demo=$NET" \
    --name "node${i}" \
    --network "$NET" \
    $PORTS \
    -e DREDIS_PORT=6380 \
    -e DREDIS_IP="node${i}" \
    -e DREDIS_SEEDS="$SEEDS" \
    "$IMG" > /dev/null

  info "node${i} started (internal: node${i}:6380)"
done
ok "All $N nodes launched"

# Give gossip time to propagate
info "Waiting 15s for cluster gossip to stabilize..."
sleep 15
ok "Cluster should be fully formed"

# Quick sanity check via node0
ACTIVE=$(docker exec node0 redis-cli -p 6380 DASHBOARDSTATS 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d['cluster']['active_node_count'])" 2>/dev/null || echo "0")
if [ "$ACTIVE" -ge "$N" ]; then
  ok "$ACTIVE / $N nodes have joined the cluster (ring formed)"
else
  warn "Only $ACTIVE / $N nodes visible so far (gossip may still be converging)"
fi

# ==============================================================================
#  STEP 5 — Print summary & node/port mapping
# ==============================================================================
banner "STEP 5/5 : DRedis 10-Node Cluster — READY!"

echo ""
echo "  NODE / PORT MAPPING"
echo "  ───────────────────"
echo "   Node       Internal Address      Host Address"
echo "   ─────────────────────────────────────────────"
printf "   %-10s %-21s %s\n" "node0" "node0:6380" "localhost:6380 (redis)"
printf "   %-10s %-21s %s\n" "node0" "node0:6381" "localhost:6381 (dashboard)"
for i in $(seq 1 $((N-1))); do
  printf "   %-10s %-21s %s\n" "node${i}" "node${i}:6380" "(container only)"
done
echo ""
echo "  HOW TO INTERACT"
echo "  ───────────────"
echo "   Dashboard  →  http://localhost:$HOST_DASHBOARD/"
echo "   CLI        →  redis-cli -p $HOST_CLIENT"
echo ""
echo "  DEMO COMMANDS (copy-paste into another terminal)"
echo "  ─────────────────────────────────────────────────"
echo "  # Write a key on node0, read it back:"
echo "    redis-cli -p $HOST_CLIENT SET mykey myvalue"
echo "  "
echo "  # Read it from a different node (direct via container):"
echo "    docker exec node1 redis-cli -p 6380 GET mykey"
echo "  "
echo "  # Cluster status:"
echo "    redis-cli -p $HOST_CLIENT CLUSTER NODES"
echo "  "
echo "  # Kill a node and watch the dashboard react:"
echo "    docker stop node5"
echo "    # Refresh dashboard → node5 goes DEAD, ring rebuilds"
echo "    docker start node5"
echo "    # Node5 rejoins within seconds"
echo "  "
echo "  # Raw cluster metrics (JSON):"
echo "    curl -s http://localhost:$HOST_DASHBOARD/metrics.json | jq ."
echo ""
echo "  CLEANUP"
echo "  ────────"
echo "   docker rm -f \$(docker ps -a --filter \"label=demo=$NET\" --format \"{{.ID}}\") && docker network rm $NET"
echo ""
info "Happy demoing!"
