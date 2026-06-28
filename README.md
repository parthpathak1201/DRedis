# DRedis

A distributed, leaderless Redis-compatible key-value store written in C++17/20. Implements gossip-based membership, consistent hashing, vector clock CRDTs, Merkle tree anti-entropy, and a custom binary protocol — all in a single-threaded epoll event loop.

## Architecture

Every node is identical — no leader, no replicaset roles. Each node owns a subset of the 16384 hash slots (via CRC32C consistent hashing with 150 virtual nodes per physical node) and stores replicas for slots owned by other nodes.

```
Client (redis-cli)
  │  RESP2 (TCP :6380)
  ▼
Dispatcher ──► Router ──► Command Handler
  │                          ▲
  ▼                          │
Proxy Layer ─────────────────┘   (if key not local)
  │
  ▼
Replication Queue ──► Peers (binary protocol, TCP :client_port+10000)
  │
  ▼
Store (vector clocks, tombstones, AOF)
```

Inter-node traffic uses a custom binary frame: 30-byte header (magic, type, sender, length, CRC32C checksum) + variable payload. Gossip, replication, proxy requests, and anti-entropy all share this framing.

## Quick Start

```bash
./cluster_init.sh                     # 10-node Docker cluster
sleep 15                              # wait for gossip convergence
redis-cli -p 6380 SET mykey myvalue
docker exec node3 redis-cli -p 6380 GET mykey   # replicated
open http://localhost:6381/            # dashboard
```

## Configuration

| Env Var | Config Key | Default | Description |
|---------|------------|---------|-------------|
| `DREDIS_PORT` | `client_port` | 6380 | RESP2 port |
| `DREDIS_IP` | `ip` | auto | Advertised IP |
| `DREDIS_SEEDS` | `seed` | — | Comma-separated `host:port` seeds |
| `DREDIS_REPL_FACTOR` | `replication_factor` | 3 | Replicas per key |
| `DREDIS_WRITE_QUORUM` | `write_quorum` | 2 | Acks needed before responding to client |
| `DREDIS_READ_QUORUM` | `read_quorum` | 2 | (not enforced — reads hit one node) |
| `DREDIS_STRICT_QUORUM` | `strict_quorum` | no | Fail writes if zero replicas connected |

Also available via `dredis.conf`. The server refuses to start if `write_quorum + read_quorum <= replication_factor`.

## Supported Commands

| Category | Commands |
|----------|----------|
| Strings | `GET`, `SET`, `MGET`, `MSET`, `GETSET`, `SETNX`, `SETEX`, `DEL`, `EXISTS`, `KEYS`, `RENAME`, `APPEND`, `STRLEN`, `INCR`, `INCRBY`, `DECR`, `DECRBY` |
| Hashes | `HSET`, `HMSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LINSERT`, `LREM`, `LSET` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SPOP` |
| Sorted Sets | `ZADD`, `ZREM`, `ZRANGE`, `ZREVRANGE`, `ZRANK`, `ZSCORE`, `ZCARD`, `ZPOPMIN`, `ZINCRBY`, `ZRANGEBYSCORE` |
| Streams | `XADD` |
| Keys | `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST`, `TYPE`, `DBSIZE`, `FLUSHDB` |
| Cluster | `CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER KEYSLOT`, `CLUSTER SLOTS`, `CLUSTER MEET`, `CLUSTER FORGET`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT` |
| Admin | `PING`, `INFO`, `CONFIG GET`, `HELLO`, `QUIT`, `AUTH` (no-op), `SELECT` (no-op), `CLIENT` (no-op), `BGREWRITEAOF`, `DASHBOARDSTATS` |

## How It Works

### Consistent Hashing

A key's owner is `slot_owners[CRC32C(key) % 16384]`, where each slot is assigned to a node via a consistent hash ring with 150 virtual nodes per physical node. Membership changes rebuild the ring and slot table. Lookup is O(1) via the cached array.

### Gossip Membership

Every 200ms, each node sends a PING with gossip payload to 3 random peers. The payload carries up to 4 node entries (self + 3 shuffled) with ID, IP, port, status, generation, and version. Recipients merge via generation (always wins) then version (higher wins within same generation).

Failure detection: 10s no contact → SUSPECT. 30s in SUSPECT → DEAD. DEAD nodes leave the ring. Resurrection from DEAD requires 3 gossip confirmations + a live TCP socket.

### Vector Clocks

Each value carries `node_id → counter`. Local writes bump self. Replication embeds the vector clock via a `VCLOCK` token appended to commands. `store_set` (`src/store.cpp:163`) compares:

- **Newer/Equal**: accepts
- **Older**: rejects (stale replication)
- **Concurrent**: accepts (last-writer-wins by arrival order)

A global flag `g_replication_mode` suppresses local clock bumps during replay from peers.

### Write Path

1. Client sends SET via RESP2
2. Dispatcher computes slot → owner via `get_owner()` (`src/cluster.cpp:181`)
3. **Local**: execute, embed vclock, append to AOF, queue replication to `get_replicas()` (walks the ring for N-1 other nodes)
4. **Remote**: wrap as `PROXY_REQUEST` frame → owner node → executes → responds with `PROXY_RESPONSE`

The initiating node waits for acks from `min(write_quorum, connected_replicas)` replicas. If connected_replicas < write_quorum, the effective quorum degrades — the write still goes through but with fewer confirmations. A 5s timeout returns an error to the client, though the write may have already been applied locally and on some replicas. Non-idempotent commands (INCR, APPEND, LPUSH) cannot be safely retried.

### Read Path

Reads hit exactly one node — either the local store (if the key's owner) or the remote owner via `PROXY_REQUEST`/`PROXY_RESPONSE`. There is no read quorum or read-repair in the current implementation. The `READ_REQUEST`/`READ_RESPONSE` frame types exist in the protocol but are not wired into the client request path.

### Anti-Entropy (Merkle Trees)

Every 30s, each node computes a 1024-leaf Merkle tree over all keys (including tombstones). Leaf hashes use CRC32C XOR-combined — fast, not cryptographic. Nodes exchange tree hashes; differing leaves trigger a full slot sync (`FULL_SYNC_CHUNK`) with LWW reconciliation on the receiver.

### Tombstones

Deletes place a tombstone (`Type::TOMBSTONE`) with the current vector clock rather than removing the entry. This prevents key resurrection during anti-entropy. Tombstones expire after `tombstone_ttl_ms` (default 60s) and are garbage-collected by `expire_sweep` (`src/store.cpp:336`).

### Full Sync

When a peer reconnects, it sends `FULL_SYNC_REQUEST`. The responder serializes the store as RESP2 commands in chunks of 50 keys. The receiver applies them with `g_replication_mode = true` so vector clocks are preserved and stale entries are rejected.

### Persistence (AOF)

Append-only file at `data/append_only.aof`. Every write command is appended before the response is sent. AOF rewrite (`rewriteAOF`, `src/store.cpp:451`) re-serializes current state — note this runs inside the epoll event loop and blocks all I/O while in progress. Fsync: `everysec` (default), `always`, or `no`.

## Benchmarks

Tested on an M5 MacBook with Docker Desktop — 10 containers sharing one host. These are not isolated hardware measurements.

### Single Node (no contention)

| Command | Req/s |
|---------|-------|
| SET | 307,692 |
| GET | 301,205 |
| INCR | 306,748 |
| LPUSH | 307,692 |
| HSET | 311,526 |
| ZADD | 311,526 |
| MSET (10 keys) | 133,156 |

### 10-Node Cluster (concurrent load)

| Metric | SET | GET |
|--------|-----|-----|
| Aggregate across 10 nodes | ~353k req/s | ~710k req/s |
| Average per node | ~35k req/s | ~71k req/s |

Individual nodes saturate below their solo ceiling due to shared host CPU across 10 single-threaded processes.

### Proxy-Hop Overhead

Routing all traffic through a single node with random keyspace degrades write throughput sharply under high concurrency. Not yet root-caused.

### Fault Tolerance (manually verified)

| Scenario | Observed behavior |
|----------|-------------------|
| Node killed mid-traffic | Keys served via replicas; other keys unaffected |
| Node restarted after downtime | Anti-entropy heals within ~30s |
| Key deleted while replica offline | Tombstone prevents resurrection on rejoin |

## Known Limitations

- **No read quorum**: Reads hit one node. A stale replica can serve stale data until anti-entropy converges.
- **No retry safety**: Non-idempotent commands (INCR, APPEND) can be double-applied if the client retries a timed-out write.
- **Eventually consistent**: No read-your-writes, monotonic read, or strong consistency guarantee. Vector clocks detect but don't resolve concurrent writes — last-writer-wins by arrival order.
- **Quorum degrades under partition**: If fewer than `write_quorum` replicas are reachable, the effective quorum silently shrinks to however many are connected. The write still executes locally before the quorum check, so a timeout error does not mean the write was rolled back.
- **No cross-slot atomicity**: Multi-key operations (MGET, MSET, DEL, EXISTS) scatter by owner and gather partial results. A timeout returns partial results with nulls.
- **Blocking operations in event loop**: AOF rewrite and Merkle tree computation iterate the entire store inside the main epoll loop, blocking all I/O for the duration.
- **Memory estimate is approximate**: `entry_memory_estimate` (`src/store.cpp:143`) does not account for container heap overhead (`unordered_map`, `deque`). `maxmemory` is a rough guard, not a hard limit.
- **No TLS, no auth** (AUTH accepted but ignored), **no RDB snapshots**.

## Binary Frame Protocol

```
┌──────────────────────────────────────────────┐
│ Magic:      0xDDEE1234   (4 bytes)           │
│ Version:    1            (1 byte)            │
│ MsgType:    see below    (1 byte)            │
│ MsgID:      uint64_t     (8 bytes)           │
│ SenderID:   uint64_t     (8 bytes)           │
│ PayloadLen: uint32_t     (4 bytes)           │
│ Checksum:   CRC32C       (4 bytes)           │
├──────────────────────────────────────────────┤
│ Payload (≤10 MB)                             │
└──────────────────────────────────────────────┘
Total header: 30 bytes (packed).
```

| Type | Code | Direction | Purpose |
|------|------|-----------|---------|
| `PING` | 0x0F | both | Heartbeat + gossip payload |
| `CLUSTER_JOIN` | 0x0B | outbound | Node introduction |
| `CLUSTER_JOIN_ACK` | 0x0C | inbound | Acknowledge join |
| `REPLICATE_PUT` | 0x04 | outbound | Replicate a write |
| `REPLICATE_ACK` | 0x06 | inbound | Replication confirmation |
| `PROXY_REQUEST` | 0x0D | both | Forward client command |
| `PROXY_RESPONSE` | 0x0E | both | Proxy result |
| `READ_REQUEST` | 0x07 | — | Defined but not wired |
| `READ_RESPONSE` | 0x08 | — | Defined but not wired |
| `ANTIENTROPY_HASH` | 0x09 | both | Merkle tree exchange |
| `ANTIENTROPY_SYNC` | 0x0A | both | Request slot entries |
| `FULL_SYNC_REQUEST` | 0x11 | outbound | Request full state |
| `FULL_SYNC_CHUNK` | 0x12 | inbound | Full state chunk |
