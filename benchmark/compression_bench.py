#!/usr/bin/env python3
"""Value-compression benchmark harness (SYNTHETIC data only).

Generates synthetic "hotel-like" JSON documents (~100 KB each). The documents
are made up for benchmarking; they are NOT real data and real compression
ratios/latencies depend entirely on your real values. Re-run it against a real
sample (--dir DIR with one value per file) on a DEV copy before deciding.

Part 1 (library level): zstd levels 1/3/9 (+ lz4 for reference) -> ratio,
         compress/decompress latency per value, throughput.
Part 2 (end to end, optional --server /path/to/forgekv): starts a local
         ForgeKV on a temp dir for each mode (off, zstd-1/3/9), does SET/GET of the values
         via RESP, reports p50/p99 latency, server CPU time and peak RSS.

pip install zstandard lz4 redis
"""
import argparse, json, os, random, resource, shutil, signal, socket, statistics, subprocess, sys, tempfile, time

import zstandard as zstd

try:
    import lz4.block as lz4b
except ImportError:
    lz4b = None

WORDS = ("sea view pool spa wifi breakfast parking airport shuttle family room suite king twin double "
         "balcony garden terrace bar restaurant gym sauna beach city centre quiet luxury budget boutique "
         "resort family friendly pets allowed concierge laundry minibar safe kitchenette lounge rooftop "
         "historic modern renovated cosy spacious bright central station metro museum market old town "
         "harbour mountain lake river golf ski tennis diving yoga massage cocktail buffet vegan halal "
         "kosher gluten free early late check cancel refund deposit tax fee included extra night stay").split()
AMEN = ["wifi", "pool", "spa", "parking", "gym", "bar", "restaurant", "pets", "ac", "tv", "kettle", "safe"]


def synth_doc(rnd: random.Random, target=100_000) -> bytes:
    rooms = []
    size = 0
    hotel = {"hotelId": rnd.randrange(10**6), "name": " ".join(rnd.choice(WORDS).title() for _ in range(3)),
             "stars": rnd.randint(1, 5), "lat": round(rnd.uniform(-60, 60), 6), "lon": round(rnd.uniform(-170, 170), 6),
             "currency": "USD", "description": " ".join(rnd.choice(WORDS) for _ in range(120)), "rooms": rooms}
    while size < target:
        r = {"roomId": rnd.randrange(10**8), "code": "".join(rnd.choice("ABCDEFGH") for _ in range(4)),
             "name": " ".join(rnd.choice(WORDS).title() for _ in range(3)),
             "board": rnd.choice(["RO", "BB", "HB", "FB", "AI"]),
             "price": {"net": round(rnd.uniform(40, 900), 2), "gross": round(rnd.uniform(45, 1000), 2), "tax": round(rnd.uniform(0, 90), 2)},
             "cancellation": {"free_until": f"2026-{rnd.randint(1,12):02d}-{rnd.randint(1,28):02d}", "penalty": round(rnd.uniform(0, 300), 2)},
             "amenities": rnd.sample(AMEN, rnd.randint(3, 9)),
             "description": " ".join(rnd.choice(WORDS) for _ in range(rnd.randint(20, 60))),
             "image": f"https://img.example.invalid/{rnd.getrandbits(64):016x}/{rnd.getrandbits(48):012x}.jpg",
             "token": "".join(rnd.choice("0123456789abcdef") for _ in range(48))}
        rooms.append(r)
        size += len(json.dumps(r, separators=(",", ":")))
    return json.dumps(hotel, separators=(",", ":")).encode()


def load_values(args):
    if args.dir:
        return [open(os.path.join(args.dir, f), "rb").read() for f in sorted(os.listdir(args.dir))][: args.n]
    rnd = random.Random(1234)
    return [synth_doc(rnd, args.size) for _ in range(args.n)]


def lib_bench(vals, reps):
    raw = sum(map(len, vals))
    print(f"\n[library level] n={len(vals)} avg_value={raw // len(vals)} bytes  total={raw / 1e6:.1f} MB")
    print(f"{'codec':10s} {'ratio':>6s} {'comp us/val':>12s} {'decomp us/val':>14s} {'comp MB/s':>10s} {'decomp MB/s':>12s}")
    cases = [(f"zstd-{lv}", (lambda c: c.compress)(zstd.ZstdCompressor(level=lv)), zstd.ZstdDecompressor().decompress) for lv in (1, 3, 9)]
    if lz4b:
        cases.append(("lz4", lz4b.compress, lz4b.decompress))
    for name, comp, dec in cases:
        t = time.perf_counter()
        for _ in range(reps):
            out = [comp(v) for v in vals]
        ct = (time.perf_counter() - t) / reps
        t = time.perf_counter()
        for _ in range(reps):
            [dec(o) for o in out]
        dt = (time.perf_counter() - t) / reps
        cs = sum(map(len, out))
        print(f"{name:10s} {raw / cs:6.2f} {ct / len(vals) * 1e6:12.0f} {dt / len(vals) * 1e6:14.0f} {raw / ct / 1e6:10.0f} {raw / dt / 1e6:12.0f}")


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def proc_stat(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    cpu = (int(parts[11]) + int(parts[12])) / ticks
    with open(f"/proc/{pid}/status") as f:
        hwm = next(int(l.split()[1]) for l in f if l.startswith("VmHWM"))
    return cpu, hwm / 1024


def e2e(vals, binary, ops):
    import redis
    print(f"\n[end to end, local ForgeKV, single client, {len(vals)} distinct values x {ops} ops]")
    print(f"{'mode':8s} {'SET p50/p99 ms':>16s} {'GET p50/p99 ms':>16s} {'written MB':>10s} {'srv CPU s':>10s} {'peak RSS MB':>12s}")
    for label, mode, level in [("off", "off", 3), ("zstd-1", "zstd", 1), ("zstd-3", "zstd", 3), ("zstd-9", "zstd", 9)]:
        d = tempfile.mkdtemp(prefix="fkvbench-")
        port = free_port()
        conf = os.path.join(d, "f.conf")
        open(conf, "w").write(f"bind 127.0.0.1\nport {port}\ndir {d}/data\ndatabases 2\nmemtable-size-mb 256\nvalue-compression {mode}\nvalue-compression-level {level}\n")
        p = subprocess.Popen([binary, "--config", conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            r = None
            for _ in range(100):
                try:
                    r = redis.Redis(port=port, protocol=2); r.ping(); break
                except Exception:
                    time.sleep(0.1)
            cpu0, _ = proc_stat(p.pid)
            st, gt = [], []
            for i in range(ops):
                k = f"k{i % len(vals)}"; v = vals[i % len(vals)]
                t = time.perf_counter(); r.set(k, v); st.append((time.perf_counter() - t) * 1000)
            for i in range(ops):
                k = f"k{i % len(vals)}"
                t = time.perf_counter(); got = r.get(k); gt.append((time.perf_counter() - t) * 1000)
                assert got == vals[i % len(vals)]
            info = r.info("compression")
            stored = int(info.get("compression_write_bytes_stored", 0)) / 1e6 if mode == "zstd" else sum(len(vals[i % len(vals)]) for i in range(ops)) / 1e6
            cpu1, rss = proc_stat(p.pid)
            q = lambda a, f: statistics.quantiles(a, n=100)[f - 1]
            print(f"{label:8s} {q(st,50):7.2f}/{q(st,99):7.2f}   {q(gt,50):7.2f}/{q(gt,99):7.2f}   {stored:10.1f} {cpu1 - cpu0:10.2f} {rss:12.0f}")
        finally:
            p.send_signal(signal.SIGTERM)
            try: p.wait(10)
            except Exception: p.kill()
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--size", type=int, default=100_000)
    ap.add_argument("--dir", help="directory with real sample values (one per file) instead of synthetic data")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--server", help="path to forgekv binary for the end-to-end part")
    ap.add_argument("--ops", type=int, default=600)
    a = ap.parse_args()
    vals = load_values(a)
    print("DATA: " + ("real sample from --dir" if a.dir else "SYNTHETIC hotel-like JSON (generated, not real data)"))
    lib_bench(vals, a.reps)
    print(f"python peak RSS (incl. generated data) {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f} MB")
    if a.server:
        e2e(vals, a.server, a.ops)
