# Benchmarks

## XADD

Uses `redis-benchmark` (custom command mode). The server auto-generates
entry IDs via `*` and the key is randomised with `__rand_int__`.

### Usage

```bash
# Default: port 6380, 100k requests, 50 clients
./tests/bench_xadd.sh [port] [count] [clients] [keyspace]
```

Examples:

```bash
./tests/bench_xadd.sh                         # port=6380, 100k req, 50 clients
./tests/bench_xadd.sh 6379 50000 20 500        # custom args
```

### Lua alternative

If luasocket is installed:

```bash
lua tests/bench_xadd.lua 127.0.0.1 6380 100000 10
```

Without luasocket, pipe RESP through redis-cli:

```bash
lua tests/bench_xadd.lua | redis-cli -p 6380 --pipe
```
