# Optional string value compression (zstd)

ForgeKV can transparently compress **string values** with [zstd](https://facebook.github.io/zstd/)
before they are written to the LSM store, and decompress them on read. It is
**off by default**. Clients see no difference: `GET`, `STRLEN`, `APPEND`,
`SETRANGE`, `INCR`, `GETEX`, `GETDEL`, `SET ... GET`, `MSET`, `MSETNX`,
`RENAME`, `COPY`, ... all operate on the logical (decompressed) value.

Scope: only values of the **string** type (what a typical cache stores).
Hashes, lists, sets, sorted sets, streams, JSON and the probabilistic types
are not compressed (out of scope for this release).

## Configuration

In `forgekv.conf` and via `CONFIG GET` / `CONFIG SET` (runtime, no restart):

| Option | Default | Meaning |
|---|---|---|
| `value-compression` | `off` | `off` or `zstd`. Only affects **writes**; reads always decode every format. |
| `value-compression-level` | `3` | zstd level, 1–19. 1–3 is the usual cache choice. |
| `value-compression-min-size` | `512` | Values shorter than this many bytes are stored raw. Accepts `512`, `1kb`, ... |

A compressed form is kept only if it is at least **10 % smaller** (including the
8-byte header); otherwise the raw value is stored. Incompressible data
(already compressed, encrypted, images) therefore costs one failed compression
attempt per write and no space/latency penalty on reads.

### Environment variables (Docker / compose)

The same settings can be given as environment variables. **Env overrides `forgekv.conf`**; an unset or
empty variable leaves the file value untouched. An **invalid value is a fatal startup error** (exit code 2,
message on stderr) so a typo can never silently run with the wrong mode.

| Variable | Default | Meaning |
|---|---|---|
| `FORGEKV_VALUE_COMPRESSION` | `off` | `off` or `zstd` |
| `FORGEKV_VALUE_COMPRESSION_LEVEL` | `3` | integer 1–19 |
| `FORGEKV_VALUE_COMPRESSION_MIN_SIZE` | `512` | bytes (`512`, `1kb`, ...) |
| `FORGEKV_COMPRESS_ON_START` | `off` | `on` starts a throttled `FORGEKV.COMPRESS` ~5 s after startup (requires `zstd`; otherwise a startup error) |
| `FORGEKV_COMPRESS_RATE` | `200` | keys/sec for the start-up job (`0` = unlimited) |

The repo `docker-compose.yml` exposes them as `${FORGEKV_VALUE_COMPRESSION:-off}` etc., so
`FORGEKV_VALUE_COMPRESSION=zstd docker compose up -d` (or a `.env` file) turns it on.
The start-up job is idempotent (converted keys are skipped), so a restart simply continues it, but its
progress is only visible through `FORGEKV.COMPRESS STATUS` / `INFO compression`. For a large production data
set prefer running `FORGEKV.COMPRESS DRYRUN` and then `FORGEKV.COMPRESS RATE <n>` manually and watching it.

`CONFIG SET value-compression <bad value>` is rejected with an error (it never
silently flips the mode). Invalid values in `forgekv.conf` print a warning and
fall back to the defaults.

## On-disk format

Each string value in the store is one of:

| Stored bytes | Meaning |
|---|---|
| anything **not** starting with `FF 5A 4B` (`\xffZK`) | plain raw value, verbatim (identical to the pre-compression format) |
| `FF 5A 4B 01` + payload | *escaped raw*: payload is the value |
| `FF 5A 4B 02` + `orig_len` (u32 LE) + zstd frame | compressed value |

Rules for values written by this version (whether compression is on or off):

* A value that does not start with the 3-byte magic is stored verbatim, or compressed if eligible.
* A value that **does** start with `FF 5A 4B` and is stored raw is always **escaped** (`FF 5A 4B 01` header),
  so decoding is unambiguous for everything this version writes.
* `orig_len` is validated (max 512 MiB, must match the decoded length) before decoding.

Reads **always** decode all three formats, independent of the `value-compression` setting.
So switching the flag off is a safe rollback for *this* binary: already-compressed values stay readable.

### The one residual limit

A **legacy** raw value (written by a binary without this feature) that already begins with
the bytes `FF 5A 4B 01` or `FF 5A 4B 02` cannot be told apart from the formats above:

* `FF 5A 4B 01 ...` is returned with the first 4 bytes stripped;
* `FF 5A 4B 02 ...` is returned verbatim if it is not a valid zstd frame with matching declared length
  (counted in `compression_decode_fallbacks`), otherwise decompressed.

`0xFF` is not valid UTF-8 and never appears in JSON/text, so this can only affect binary values that
start with exactly those bytes. If your data may contain such values, do not enable the feature.
(Other `FF 5A 4B <kind>` prefixes with a different kind byte are treated as plain raw values.)

## Compatibility / rollback binaries

* A binary **without** this feature (e.g. `0.2.x`) does not understand headers. It would return the
  compressed bytes as the value. **Never run an old binary against a data directory that contains compressed values.**
* Therefore the **read-support release must be deployed everywhere** (all instances, replicas and every
  rollback image you might fall back to) *before* `value-compression` is turned on anywhere that shares data.
* The `0.3.0` default (`off`) writes the same bytes as `0.2.x` for all values except the exotic `FF 5A 4B`-prefixed
  ones (escaped). So deploying `0.3.0` with the flag off is compatible with rolling back to `0.2.x` as long as no
  compressed value and no `FF 5A 4B`-prefixed value has been written.
* To go back to an old binary after enabling compression: set the flag off, run `FORGEKV.DECOMPRESS`
  until `state:completed` with `keys_error:0`, and only then roll back.

## Migrating existing data in place

Existing values stay raw until rewritten. To compress them without a re-warm:

```
FORGEKV.COMPRESS [DRYRUN] [RATE keys/sec] [DB n] [LIMIT n] [RESUME]
FORGEKV.COMPRESS STATUS | STOP | RESET
FORGEKV.DECOMPRESS [RATE keys/sec] [DB n] [LIMIT n] [RESUME]      (same sub-commands)
```

* Runs as one background thread; **one job at a time**. The command returns immediately; poll `STATUS`
  (or `INFO compression`).
* `DRYRUN` reads values, compresses them in memory and reports projected savings; it writes nothing and
  works with `value-compression off`. Use `LIMIT n` to sample the first *n* keys quickly.
* A real `FORGEKV.COMPRESS` requires `value-compression zstd` (it uses the configured level and min size).
  `FORGEKV.DECOMPRESS` works with the flag in any state.
* **Concurrency safety:** each key is re-read and rewritten under that key's own lock, only the value
  bytes change (TTL/metadata untouched). A client write to the same key is either applied before (then the newer
  value is converted) or after (it overwrites the converted value). A client write is never lost.
  Keys that expire/vanish/change type meanwhile are skipped.
* **Throttling:** `RATE` is keys/second (default 500, `0` = unlimited). Pick it from the average value size to
  bound write MB/s and CPU.
* **Resumable:** the job tracks a cursor (last processed key). `STOP` (or `LIMIT`) leaves state `stopped`;
  `RESUME` continues from the cursor with the same direction/`DRYRUN` setting and keeps the counters.
  `RESET` forgets cursor and counters. A restart of the server forgets the job (just run it again; converted keys are skipped).
* Only the metadata entries of the string keys in the database being processed are held in memory
  (like `SCAN`), never the values.
* Only values stored as plain raw are compressed. Values already compressed, below `min-size`,
  incompressible, or escaped magic-prefixed values are left unchanged.
* **Disk space:** rewriting adds new data to the WAL/SSTs; old versions are reclaimed only by LSM compaction
  (run `COMPACT` after the job). Make sure there is free disk for the transient overhead before running it on a
  large data set, and take a backup/snapshot first.

## Observability (`INFO compression`, also part of `INFO`)

```
value_compression, value_compression_level, value_compression_min_size
compression_writes_compressed / _raw / _below_min_size / _incompressible / _escaped
compression_write_bytes_in / compression_write_bytes_stored / compression_write_ratio   (since start)
compression_reads_decompressed, compression_decode_fallbacks
compression_job_*   state, direction, dry_run, keys_examined/converted/..., compressed_keys, raw_keys,
                     bytes_before, bytes_after, ratio   (of the last/current migration job)
```

`compressed_keys` / `raw_keys` / `bytes_before` / `bytes_after` / `ratio` describe what the migration job saw
(a full `DRYRUN` gives the inventory and projected ratio for the whole data set). The write counters cover
traffic since process start; ForgeKV does not maintain a live per-key inventory.

## Recommended rollout

1. **Deploy read-support, flag off.** Build `0.3.0`, deploy to DEV, then prod via blue/green with explicit
   approval, `value-compression off`. Verify normal operation. Ensure *every* instance/replica/rollback image that
   shares the data runs `>= 0.3.0` before continuing.
2. **Measure on a copy.** On DEV, load a copy/sample of real data, run `FORGEKV.COMPRESS DRYRUN` and a real run, and
   benchmark read/write latency and CPU with the real values (`benchmark/compression_bench.py` has a
   synthetic-data harness). Decide the level and `min-size` from real numbers.
3. **Enable writes.** `CONFIG SET value-compression zstd` (and put it in `forgekv.conf`). New and refreshed values
   are compressed. Watch `INFO compression`, latency and CPU.
4. **Migrate existing data off-peak**, never inside the web/app container: `FORGEKV.COMPRESS DRYRUN` first,
   then `FORGEKV.COMPRESS RATE <n>`, watching `STATUS`, disk and latency; `STOP`/`RESUME` as needed; `COMPACT` afterwards.
5. **Rollback:** `CONFIG SET value-compression off` (stops new compressed writes; compressed values stay
   readable). To fully revert (or before rolling back to a pre-0.3 binary): `FORGEKV.DECOMPRESS`, wait for
   `state:completed`, `keys_error:0`.

## Risks / limits

* CPU: compression costs CPU on every eligible write (mostly cache refresh/warm-up); decompression costs CPU on every read.
  Higher levels cost much more CPU for small gains; start with 1–3.
* A pre-0.3 binary cannot read compressed values (see above).
* Residual legacy-value ambiguity (see above).
* Single-key operations decompress/recompress the whole value (`APPEND`/`SETRANGE`/`GETRANGE` on very large compressed values are O(size)).
* The migration job is not crash-persistent (cursor is in memory) and its LSM write volume is real: use `RATE`.
* `DUMP`/`RESTORE` use the logical (decoded) value, so their payload is format independent.
* No trained zstd dictionary in this release (planned: dictionary id in a new `kind` byte).
* Hashes/lists/sets/zsets/streams/JSON are not compressed.
