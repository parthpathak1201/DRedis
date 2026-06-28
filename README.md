# DRedis

A distributed, leaderless key-value store inspired by Redis and Amazon
Dynamo, built from scratch in C++. Built as a learning project to go
deep on distributed systems internals — consistent hashing, gossip-based
failure detection, quorum reads/writes, vector clocks, and Merkle-tree
anti-entropy repair.

It speaks RESP2, so it works directly with `redis-cli` and `redis-benchmark` —
no custom client needed.

## Quick start

```bash
./demo.sh
```

This builds the image, spins up a 10-node cluster on a dedicated Docker
network, and prints connection info. You get:

- A live dashboard at `http://localhost:6381/` — cluster membership,
  ring state, replication queue depth, AOF size, all refreshed every
  2 seconds
- A Redis-compatible CLI at `localhost:6380`

```bash
redis-cli -p 6380 SET mykey myvalue
docker exec node1 redis-cli -p 6380 GET mykey   # written on node0, read from node1
```

Kill a node and watch the dashboard react:

```bash
docker stop node5
# refresh dashboard → node5 goes DEAD, ring rebuilds around it
docker start node5
# node5 rejoins and anti-entropy heals any missed writes within ~30s
# we can also simulate such scenarios in docker integrated IDEs like CLion (which I used)
```

Cleanup:

```bash
docker rm -f $(docker ps -a --filter "label=demo=dredis-demo" --format "{{.ID}}")
docker network rm dredis-demo
```

## What this demonstrates

- **Consistent hashing** with virtual nodes (150 per physical node) for
  even key distribution and minimal reshuffling on membership change
- **SWIM-inspired failure detection** — gossip-based membership with
  suspicion before death, generation numbers to distinguish a restarted
  node from a stale one, and periodic heartbeat broadcast to all peers
  (full mesh transport, with state-change information piggybacked on
  each heartbeat)
- **Leaderless replication** with configurable write/read quorum
  (W + R > N enforced at startup)
- **Vector clocks** for conflict resolution, with last-write-wins
  tiebreak on concurrent writes
- **Merkle-tree anti-entropy** — nodes periodically diff a 1024-leaf
  hash tree with their peers and repair only the buckets that actually
  diverged, instead of comparing every key
- **Tombstone-based deletes** so a stale replica can't resurrect a key
  that was legitimately deleted elsewhere
- **Quorum reads with read-repair** — when enabled, a read fans out to
  multiple replicas, returns the freshest value by vector clock, and
  asynchronously patches any replica that was behind
- **AOF persistence** per node, with background rewrite/compaction
- Five Redis data types — String, List, Hash, Set, Sorted Set — plus
  basic Stream support

## Architecture

Every node runs the same binary. There's no special coordinator process —
whichever node a client connects to acts as the coordinator for that
request: serving it directly if it owns the key, or proxying it
internally to the correct owner if not.

```
src/
├── network.cpp      epoll event loop, client + peer connections, dashboard HTTP
├── dispatcher.cpp   routes commands to local store or proxies to the owning node
├── cmd.cpp          command handlers (GET/SET/LPUSH/HSET/ZADD/...)
├── cluster.cpp       gossip, membership, consistent hash ring
├── merkle.cpp        Merkle tree construction + diffing for anti-entropy
├── store.cpp         in-memory KV store, AOF read/write, vector clocks
├── parser.cpp         RESP2 parser/serializer + internal binary RPC frame format
├── config.cpp         config file + env var parsing
└── dashboard.cpp      JSON metrics endpoint + embedded HTML dashboard
```

## Benchmarks

Tested locally — M-5 MacBook, Docker Desktop, 10 containers sharing one host.
These numbers reflect that shared-host setup, not isolated hardware per node.

### Single node (isolated, no contention)

| Command | Req/s   |
|---|---------|
| SET | 307,692 |
| GET | 301,205 |
| INCR | 306,748 |
| LPUSH | 307,692 |
| HSET | 311,526 |
| ZADD | 311,526 |
| MSET (10 keys) | 133,156 |

### 10-node cluster (concurrent load, shared host)

| Metric | SET        | GET        |
|---|------------|------------|
| Aggregate (sum across 10 nodes) | ~353k req/s | ~710k req/s |
| Average per node | ~35k req/s | ~71k req/s |

Each node was benchmarked directly and concurrently (one `redis-benchmark`
process per node, run in parallel). Aggregate throughput roughly matches
or exceeds the single-node ceiling, which suggests the cluster's internal
coordination (gossip, replication, anti-entropy) isn't the bottleneck at
this scale — the limiting factor is host CPU shared across 10 single-threaded
processes, not the distributed system overhead itself.

**Note:** this is 10 nodes on one machine, not 10 independent hosts.
On real separate hardware I'd expect each node closer to its solo ~300k
ceiling, and a correspondingly higher real aggregate. I haven't tested
that yet.

I also tried routing all traffic through a single entrypoint node with a
large randomized keyspace, to measure proxy-hop overhead specifically
(i.e. client → one node → internally routed to the correct owner). That
test exposed a real bottleneck: a high volume of concurrent proxied
writes through one node degrades sharply, most likely because proxy
requests queue up faster than they drain under that specific load shape.
I haven't root-caused it yet — flagging it here rather than hiding it,
since it's a genuine limitation worth knowing about.

### Fault tolerance (manually verified)

- **Node killed mid-traffic** → reads/writes for its keys continue via
  replicas, no downtime for unaffected keys
- **Node restarted after being down** → anti-entropy heals it within
  ~30 seconds, including keys written entirely during its downtime
- **Key deleted while a replica was offline** → tombstone correctly
  prevents the key from reappearing when that replica rejoins

## Configuration

Nodes are configured via environment variables (used by `demo.sh`) or a
`dredis.conf` file:

| Variable | Default | Description |
|---|---|---|
| `DREDIS_PORT` | 6380 | Client + cluster port |
| `DREDIS_IP` | 127.0.0.1 | This node's advertised address |
| `DREDIS_SEEDS` | — | Comma-separated `host:port` list for gossip bootstrap |
| `DREDIS_REPL_FACTOR` | 3 | Keys are stored on this many nodes |
| `DREDIS_WRITE_QUORUM` | 2 | Writes ack after this many confirmations |
| `DREDIS_READ_QUORUM` | 2 | Reads wait for this many responses (if quorum reads enabled) |

`write_quorum + read_quorum > replication_factor` is enforced at startup —
the node refuses to boot if the invariant is violated.

## Status & scope

This is a learning project, not a production system.

Known gaps:
- No TLS on client or inter-node connections
- No formal chaos-engineering test suite (fault tolerance above was
  verified manually, not via automated partition/kill scripts)
- Single-AZ / single-host assumptions throughout — not tested across
  real separate machines
- The proxy-overhead bottleneck noted in the benchmarks section above
  is unresolved

## Testing

Manual cluster tests (replication, node failure, anti-entropy, tombstone
correctness) were run by hand against a live `demo.sh` cluster using
`redis-cli` and `docker stop`/`start`/`network disconnect`. See the
Fault tolerance section above for what was verified.
