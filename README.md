# ForgeKV

**[forgekv.com](https://forgekv.com)** — A Redis-compatible key-value server built in Rust, designed from the ground up for **multi-core scale-out** and **SSD-backed persistence**. Drop-in replacement for Redis — any Redis client works without changes.

```bash
docker pull forgekv/forgekv
```

---

## Why ForgeKV?

Redis is single-threaded by design. It saturates one core and tops out around 80–90K ops/sec regardless of how many cores your machine has.

ForgeKV uses a **256-shard lock architecture** — each shard has its own WAL, memtable, and RwLock. Concurrent writers never block each other unless they hash to the same shard. The result: throughput scales with the number of client threads.

---

## Benchmark v2 — ForgeKV vs Redis 7 vs Dragonfly

**Setup:** `memtier_benchmark`, `--network host`, pipeline=16, 64-byte values, 20s per config.
`t` = benchmark threads, `c` = clients per thread.

### SET throughput (ops/sec — higher is better)

| Config | ForgeKV v2 | Redis 7 | Dragonfly | vs Redis | vs Dragonfly |
|--------|----------:|--------:|----------:|---------:|-------------:|
| t=1 c=10 | 452,721 | 451,023 | 144,726 | **100%** | **313%** |
| t=1 c=20 | 476,677 | 527,199 | 207,407 | 90% | **230%** |
| t=2 c=10 | **908,408** | 693,045 | 354,091 | **131%** | **257%** |
| t=2 c=20 | **955,010** | 756,600 | 408,084 | **126%** | **234%** |
| t=4 c=10 | **1,542,005** | 525,048 | 592,571 | **294%** | **260%** |
| t=4 c=50 | **1,452,569** | 618,241 | 760,924 | **235%** | **191%** |

### GET throughput (ops/sec — higher is better)

| Config | ForgeKV v2 | Redis 7 | Dragonfly | vs Redis | vs Dragonfly |
|--------|----------:|--------:|----------:|---------:|-------------:|
| t=1 c=10 | 524,635 | 762,926 | 133,016 | 69% | **394%** |
| t=1 c=20 | 570,173 | 730,880 | 232,198 | 78% | **246%** |
| t=2 c=10 | **1,108,087** | 736,096 | 381,827 | **150%** | **290%** |
| t=2 c=20 | **1,226,648** | 837,285 | 484,340 | **147%** | **253%** |
| t=4 c=10 | **1,962,310** | 548,393 | 668,289 | **358%** | **294%** |
| t=4 c=50 | **1,171,594** | 627,456 | 887,687 | **187%** | **132%** |

### SET p50 latency ms (lower is better)

| Config | ForgeKV v2 | Redis 7 | Dragonfly |
|--------|----------:|--------:|----------:|
| t=1 c=10 | 0.343 | 0.303 | 0.703 |
| t=1 c=20 | 0.663 | 0.567 | 1.263 |
| t=2 c=10 | **0.367** | 0.503 | 0.911 |
| t=2 c=20 | **0.727** | 0.863 | 1.767 |
| t=4 c=10 | **0.439** | 1.295 | 1.295 |
| t=4 c=50 | **2.271** | 5.311 | 4.127 |

### SET p99 latency ms (lower is better)

| Config | ForgeKV v2 | Redis 7 | Dragonfly |
|--------|----------:|--------:|----------:|
| t=1 c=10 | 0.84 | 0.61 | 4.13 |
| t=1 c=20 | 1.23 | 1.01 | 4.70 |
| t=2 c=10 | 1.06 | 0.82 | 6.27 |
| t=2 c=20 | **1.50** | 1.53 | 9.02 |
| t=4 c=10 | **1.14** | 2.43 | 10.43 |
| t=4 c=50 | **4.77** | 9.22 | 28.03 |

> `vs Redis / vs Dragonfly` = ForgeKV ÷ competitor × 100%. For throughput: above 100% = ForgeKV wins. For latency: below 100% = ForgeKV wins.

### What changed in v2

1. **256-shard architecture** (was 64) — reduced lock contention at high concurrency
2. **Eliminated redundant metadata reads** — `save_meta_inner` no longer re-reads from storage; new `get_live_meta_inner` replaces the double-lookup pattern across 70+ call sites
3. **Batched writes for LPUSH/RPUSH/HSET/SADD** — single WAL+memtable lock acquisition instead of N separate locks per element

### Summary

- **SET single-thread** (t=1): ForgeKV matches Redis 7 within 10% and is **2.3–3.1x faster** than Dragonfly.
- **SET multi-thread** (t=4 c=10): **1.54M SET/s** — 2.9x Redis 7, 2.6x Dragonfly.
- **GET multi-thread** (t=4 c=10): **1.96M GET/s** — 3.6x Redis 7, 2.9x Dragonfly.
- **High concurrency** (t=4 c=50): **1.45M SET/s** and **1.17M GET/s** — both faster than Redis 7 and Dragonfly. In v1 this config was the weak point (73% of Redis SET); now it's a strength.
- **Tail latency**: SET p99 at t=4 c=50 is **4.8ms** — 1.9x better than Redis 7 (9.2ms), 5.9x better than Dragonfly (28.0ms).

---

### Benchmark v1 (historical) — ForgeKV vs Redis 7 vs Dragonfly

<details>
<summary>Click to expand v1 results</summary>

**Setup:** `memtier_benchmark`, pipeline=16, 64-byte values, 20s per config.

#### SET throughput (ops/sec)

| Config | ForgeKV v1 | Redis 7 | Dragonfly | vs Redis | vs Dragonfly |
|--------|----------:|--------:|----------:|---------:|-------------:|
| t=1 c=10 | 69,143 | 80,704 | 23,067 | 86% | **300%** |
| t=1 c=20 | 76,868 | 85,538 | 26,398 | 90% | **291%** |
| t=2 c=10 | **147,992** | 96,279 | 39,477 | **154%** | **375%** |
| t=2 c=20 | **157,968** | 111,827 | 45,129 | **141%** | **350%** |
| t=4 c=10 | **100,175** | 61,171 | 61,384 | **164%** | **163%** |
| t=4 c=50 | 59,401 | 81,660 | 94,665 | 73% | 63% |

#### p99 latency ms

| Config | ForgeKV v1 | Redis 7 | Dragonfly |
|--------|----------:|--------:|----------:|
| t=1 c=10 | 0.85 | 0.84 | 5.18 |
| t=1 c=20 | 1.59 | 1.38 | 6.24 |
| t=2 c=10 | 1.09 | 1.02 | 4.80 |
| t=2 c=20 | 2.67 | 2.19 | 7.84 |
| t=4 c=10 | 2.90 | 2.67 | 7.10 |
| t=4 c=50 | 17.28 | 11.97 | 27.77 |

</details>

---

## Architecture

```
Client connections (Tokio async, 1 task per connection)
         │
         ▼
   Command Registry
         │
         ▼
  RedisDatabase  ─── 256 FNV-1a shards (RwLock each)
         │
         ▼
  LsmStorage  ─── 256-shard WAL  +  256-shard MemTable (BTreeMap)
         │                              │
         ▼                              ▼
  SSTable files (SSD)          Immutable snapshots → flush
```

- **256-shard WAL**: Each shard writes to its own WAL file with a 256KB BufWriter. WAL rotation is atomic — the WAL lock is held during both rotation and memtable snapshot.
- **put2 / put_batch**: `SET` writes two entries (meta + data) in a single lock. `LPUSH`, `HSET`, `SADD` batch all elements + meta into one `put_batch` call — single WAL+memtable lock acquisition regardless of element count.
- **Redis key-aware sharding**: `shard_of()` strips the internal storage prefix before hashing, so meta and data keys for the same Redis key always land in the same shard.
- **Zero redundant reads**: `get_live_meta_inner` combines existence check + metadata fetch in a single storage read, eliminating the double-lookup pattern across all commands.
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
