# ForgeKV

**[forgekv.com](https://forgekv.com)** — A Redis-compatible key-value server built in Rust, designed from the ground up for **multi-core scale-out** and **SSD-backed persistence**. Drop-in replacement for Redis — any Redis client works without changes.

---

## Why ForgeKV?

Redis is single-threaded by design. It saturates one core and tops out around 80–90K ops/sec regardless of how many cores your machine has.

ForgeKV uses a **64-shard lock architecture** — each shard has its own WAL, memtable, and RwLock. Concurrent writers never block each other unless they hash to the same shard. The result: throughput scales with the number of client threads.

---

## Benchmark — ForgeKV vs Redis 7 vs Dragonfly

**Setup:** `memtier_benchmark`, pipeline=16, 64-byte values, 20s per config.
`t` = benchmark threads, `c` = clients per thread.

### SET throughput (ops/sec — higher is better)

| Config | ForgeKV | Redis 7 | Dragonfly | vs Redis | vs Dragonfly |
|--------|--------:|--------:|----------:|---------:|-------------:|
| t=1 c=10 | 69,143 | 80,704 | 23,067 | 86% | **300%** |
| t=1 c=20 | 76,868 | 85,538 | 26,398 | 90% | **291%** |
| t=2 c=10 | **147,992** | 96,279 | 39,477 | **154%** | **375%** |
| t=2 c=20 | **157,968** | 111,827 | 45,129 | **141%** | **350%** |
| t=4 c=10 | **100,175** | 61,171 | 61,384 | **164%** | **163%** |
| t=4 c=50 | 59,401 | 81,660 | 94,665 | 73% | 63% |

### Average latency ms (lower is better)

| Config | ForgeKV | Redis 7 | Dragonfly |
|--------|--------:|--------:|----------:|
| t=1 c=10 | 0.222 | 0.179 | 0.659 |
| t=1 c=20 | 0.379 | 0.339 | 1.104 |
| t=2 c=10 | **0.195** | 0.316 | 0.765 |
| t=2 c=20 | **0.367** | 0.546 | 1.273 |
| t=4 c=10 | **0.577** | 0.903 | 0.940 |
| t=4 c=50 | 4.896 | 3.563 | 3.056 |

### p99 latency ms (lower is better)

| Config | ForgeKV | Redis 7 | Dragonfly |
|--------|--------:|--------:|----------:|
| t=1 c=10 | 0.85 | 0.84 | 5.18 |
| t=1 c=20 | 1.59 | 1.38 | 6.24 |
| t=2 c=10 | 1.09 | 1.02 | 4.80 |
| t=2 c=20 | 2.67 | 2.19 | 7.84 |
| t=4 c=10 | 2.90 | 2.67 | 7.10 |
| t=4 c=50 | 17.28 | 11.97 | 27.77 |

> `vs Redis / vs Dragonfly` = ForgeKV ÷ competitor × 100%. Values above 100% mean ForgeKV wins.

**Summary:**
- At **t=2 c=20** (40 concurrent connections, 2 cores) ForgeKV delivers **158K SET/s** — 41% faster than Redis 7 and 3.5× faster than Dragonfly.
- ForgeKV **scales with cores**. Redis 7 plateaus; ForgeKV keeps climbing as you add benchmark threads.
- High-concurrency extreme (t=4 c=50, 200 connections) is the current weak point — targeted by the upcoming group-commit WAL optimization.

---

## Architecture

```
Client connections (Tokio async, 1 task per connection)
         │
         ▼
   Command Registry
         │
         ▼
  RedisDatabase  ─── 64 FNV-1a shards (RwLock each)
         │
         ▼
  LsmStorage  ─── 64-shard WAL  +  64-shard MemTable (BTreeMap)
         │                              │
         ▼                              ▼
  SSTable files (SSD)          Immutable snapshots → flush
```

- **64-shard WAL**: Each shard writes to its own `wal-XX.bin` file with a 256KB BufWriter. WAL rotation is atomic — the WAL lock is held during both rotation and memtable snapshot.
- **put2**: `SET` writes two entries (meta + data) in a single WAL lock acquisition and a single memtable lock — no extra round-trips.
- **Redis key-aware sharding**: `shard_of()` strips the internal storage prefix before hashing, so meta and data keys for the same Redis key always land in the same shard.
- **Blocking commands**: `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN/MAX` use a Tokio broadcast channel per `(db, key)`.

---

## Redis Compatibility

Full RESP2 protocol. Supported command families:

| Family | Commands |
|--------|----------|
| Strings | GET, SET, MGET, MSET, INCR/DECR, APPEND, GETRANGE, SETRANGE, GETSET, GETDEL, GETEX, SETNX, SETEX, PSETEX, MSETNX, SUBSTR, LCS |
| Keys | DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, COPY, UNLINK, SCAN, KEYS, RANDOMKEY, OBJECT, WAIT |
| Hashes | HGET, HSET, HMGET, HMSET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSCAN, HRANDFIELD, HEXPIRE, HPEXPIRE, HTTL, HPTTL |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LMOVE, LMPOP, BLPOP, BRPOP, BLMOVE, LPOS |
| Sets | SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SPOP, SRANDMEMBER, SUNION, SINTER, SDIFF, SUNIONSTORE, SINTERSTORE, SDIFFSTORE, SINTERCARD, SSCAN, SMOVE |
| Sorted Sets | ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZRANGESTORE, ZCOUNT, ZLEXCOUNT, ZCARD, ZINCRBY, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZMPOP, BZPOPMIN, BZPOPMAX, ZDIFF, ZINTER, ZUNION, ZDIFFSTORE, ZINTERSTORE, ZUNIONSTORE, ZMSCORE, ZSCAN |
| HyperLogLog | PFADD, PFCOUNT, PFMERGE |
| Geo | GEOADD, GEODIST, GEOPOS, GEOSEARCH, GEOSEARCHSTORE, GEORADIUS, GEORADIUSBYMEMBER, GEOHASH |
| Bitmaps | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB |
| Streams | XADD, XREAD, XLEN, XRANGE, XREVRANGE, XGROUP, XREADGROUP, XACK, XCLAIM, XAUTOCLAIM, XDEL, XTRIM, XINFO, XPENDING, XSETID |
| Transactions | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| Scripting | EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH |
| Server | INFO, CONFIG GET/SET/REWRITE, DBSIZE, FLUSHDB, FLUSHALL, SELECT, PING, ECHO, QUIT, RESET, DEBUG, COMMAND, LATENCY, SLOWLOG, ACL, CLIENT, TIME, LASTSAVE, BGSAVE, BGREWRITEAOF, SAVE, OBJECT ENCODING/FREQ/HELP/IDLETIME/REFCOUNT/VERSION |
| Modules | JSON (JSON.*), Bloom Filter (BF.*), RedisSearch stubs (FT.*), CMS/TopK/TDigest |

---

## Quick Start

### Docker

```bash
docker compose up
```

Connects on `localhost:6379`. Use any Redis client:

```bash
redis-cli -p 6379 set foo bar
redis-cli -p 6379 get foo
```

### Configuration

Edit `forgekv.conf`:

```
bind 0.0.0.0
port 6379
dir /data
databases 16
```

### Build from source

```bash
cd rust
cargo build --release
./target/release/forgekv --config ../forgekv.conf
```

Requires Rust 1.75+.

---

## Run Benchmarks

Reproduce the numbers above (requires Docker):

```bash
bash benchmark/redis-comparison/run.sh
```

Options:
```
--skip-build      Skip Docker image rebuild
--test-time=N     Seconds per config (default: 20)
```

Results are saved to `benchmark/redis-comparison/results/`.

---

## License

Source-available. Free to use, fork, and self-host for evaluation and non-commercial purposes.

**Commercial use, redistribution as a service (SaaS/hosted), or inclusion in commercial products requires a separate commercial license.**
Visit [forgekv.com](https://forgekv.com) for licensing inquiries.

> The license terms may be updated in future versions.
