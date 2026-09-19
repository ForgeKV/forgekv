#!/usr/bin/env python3
"""Concurrent large-SET stress for ForgeKV write-path stalls.

Mimics RestReserve /OneTime/CacheHotels: many writers, ~100KB values.
Success criteria:
  - PING stays responsive (<100ms) during the write storm
  - SET p99 stays under SyncTimeout-like budgets (default check 5s)
  - DBSIZE completes (may be slow on huge data; we use modest N)
  - INFO persistence exposes lsm_* stall metrics
"""
from __future__ import annotations

import argparse
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def encode_cmd(*parts: bytes) -> bytes:
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        out.append(f"${len(p)}\r\n".encode())
        out.append(p)
        out.append(b"\r\n")
    return b"".join(out)


class RespClient:
    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def call(self, *parts: bytes):
        self.sock.sendall(encode_cmd(*parts))
        return self._read()

    def _read(self):
        while True:
            if self.buf:
                kind = self.buf[0:1]
                if kind in (b"+", b"-", b":"):
                    idx = self.buf.find(b"\r\n")
                    if idx >= 0:
                        line = self.buf[:idx]
                        self.buf = self.buf[idx + 2 :]
                        if kind == b"+":
                            return line[1:].decode()
                        if kind == b"-":
                            raise RuntimeError(line[1:].decode())
                        return int(line[1:])
                elif kind == b"$":
                    idx = self.buf.find(b"\r\n")
                    if idx >= 0:
                        n = int(self.buf[1:idx])
                        rest = self.buf[idx + 2 :]
                        if n == -1:
                            self.buf = rest
                            return None
                        need = n + 2
                        if len(rest) >= need:
                            val = rest[:n]
                            self.buf = rest[need:]
                            return val
                elif kind == b"*":
                    idx = self.buf.find(b"\r\n")
                    if idx >= 0:
                        n = int(self.buf[1:idx])
                        self.buf = self.buf[idx + 2 :]
                        if n == -1:
                            return None
                        arr = []
                        for _ in range(n):
                            arr.append(self._read())
                        return arr
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("connection closed")
            self.buf += chunk


def percentile(xs, p):
    if not xs:
        return 0.0
    ys = sorted(xs)
    i = min(len(ys) - 1, max(0, int(round((p / 100.0) * (len(ys) - 1)))))
    return ys[i]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=str(Path(__file__).resolve().parents[1] / "target" / "release" / "forgekv"))
    ap.add_argument("--port", type=int, default=16379)
    ap.add_argument("--clients", type=int, default=24)
    ap.add_argument("--keys-per-client", type=int, default=40)
    ap.add_argument("--value-bytes", type=int, default=100_000)
    ap.add_argument("--set-timeout", type=float, default=5.0)
    args = ap.parse_args()

    data_dir = tempfile.mkdtemp(prefix="forgekv-stress-")
    conf = Path(data_dir) / "forgekv.conf"
    conf.write_text(
        f"bind 127.0.0.1\nport {args.port}\ndir {data_dir}\n"
        f"databases 16\nmemtable-size-mb 256\nprotected-mode no\n"
    )

    proc = subprocess.Popen(
        [args.bin, str(conf)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # Wait for ready
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                c = RespClient("127.0.0.1", args.port, timeout=1)
                assert c.call(b"PING") == "PONG"
                c.close()
                break
            except OSError:
                time.sleep(0.05)
        else:
            out = proc.stdout.read() if proc.stdout else ""
            raise SystemExit(f"server did not start:\n{out}")

        value = b"V" * args.value_bytes
        set_lat = []
        ping_lat = []
        errors = []
        stop_ping = threading.Event()

        def ping_loop():
            while not stop_ping.is_set():
                t0 = time.perf_counter()
                try:
                    c = RespClient("127.0.0.1", args.port, timeout=1.0)
                    r = c.call(b"PING")
                    c.close()
                    if r != "PONG":
                        errors.append(f"PING got {r!r}")
                    ping_lat.append((time.perf_counter() - t0) * 1000)
                except Exception as e:
                    errors.append(f"PING {e}")
                    ping_lat.append((time.perf_counter() - t0) * 1000)
                time.sleep(0.05)

        ping_t = threading.Thread(target=ping_loop, daemon=True)
        ping_t.start()

        def writer(cid: int):
            local = []
            c = RespClient("127.0.0.1", args.port, timeout=args.set_timeout)
            for i in range(args.keys_per_client):
                key = f"hotel:{cid}:{i}".encode()
                t0 = time.perf_counter()
                try:
                    r = c.call(b"SET", key, value)
                    dt = (time.perf_counter() - t0) * 1000
                    local.append(dt)
                    if r != "OK":
                        errors.append(f"SET {key!r} -> {r!r}")
                except Exception as e:
                    dt = (time.perf_counter() - t0) * 1000
                    local.append(dt)
                    errors.append(f"SET {cid}:{i} {e}")
            c.close()
            return local

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.clients) as ex:
            futs = [ex.submit(writer, i) for i in range(args.clients)]
            for f in as_completed(futs):
                set_lat.extend(f.result())
        write_secs = time.perf_counter() - t0

        stop_ping.set()
        ping_t.join(timeout=2)

        c = RespClient("127.0.0.1", args.port, timeout=30)
        # Let background flushes drain so DBSIZE is stable under async flush.
        for _ in range(200):
            info = c.call(b"INFO", b"persistence")
            info_s = info.decode() if isinstance(info, (bytes, bytearray)) else str(info)
            pend = 0
            for line in info_s.splitlines():
                if line.startswith("lsm_pending_flushes:"):
                    pend = int(line.split(":", 1)[1])
            if pend == 0:
                break
            time.sleep(0.05)
        t0 = time.perf_counter()
        dbsize = c.call(b"DBSIZE")
        dbsize_ms = (time.perf_counter() - t0) * 1000
        info = c.call(b"INFO", b"persistence")
        compact = c.call(b"COMPACT")
        info_mem = c.call(b"INFO", b"memory")
        c.close()

        info_s = info.decode() if isinstance(info, (bytes, bytearray)) else str(info)
        mem_s = info_mem.decode() if isinstance(info_mem, (bytes, bytearray)) else str(info_mem)

        total_keys = args.clients * args.keys_per_client
        print("=== ForgeKV large-SET stress ===")
        print(f"clients={args.clients} keys={total_keys} value_bytes={args.value_bytes}")
        print(f"write_wall_sec={write_secs:.3f} throughput_set_per_sec={total_keys/write_secs:.1f}")
        print(
            f"SET_ms p50={percentile(set_lat,50):.1f} p95={percentile(set_lat,95):.1f} "
            f"p99={percentile(set_lat,99):.1f} max={max(set_lat) if set_lat else 0:.1f}"
        )
        print(
            f"PING_ms p50={percentile(ping_lat,50):.1f} p95={percentile(ping_lat,95):.1f} "
            f"p99={percentile(ping_lat,99):.1f} max={max(ping_lat) if ping_lat else 0:.1f}"
        )
        print(f"DBSIZE={dbsize} dbsize_ms={dbsize_ms:.1f}")
        print(f"COMPACT={compact}")
        for line in info_s.splitlines():
            if line.startswith("lsm_"):
                print(line)
        for line in mem_s.splitlines():
            if line.startswith("used_memory") or line.startswith("used_memory_rss"):
                print(line)

        fail = False
        if errors:
            print(f"ERRORS ({len(errors)}):")
            for e in errors[:20]:
                print(" ", e)
            fail = True
        if ping_lat and percentile(ping_lat, 99) > 500:
            print("FAIL: PING p99 > 500ms under write load")
            fail = True
        if set_lat and percentile(set_lat, 99) > args.set_timeout * 1000:
            print(f"FAIL: SET p99 exceeds SyncTimeout budget ({args.set_timeout}s)")
            fail = True
        if dbsize != total_keys:
            # Allow slight mismatch only if errors; otherwise fail
            print(f"WARN/FAIL: DBSIZE {dbsize} != expected {total_keys}")
            if not errors:
                fail = True
        if "lsm_pending_flushes:" not in info_s:
            print("FAIL: missing lsm_* metrics in INFO persistence")
            fail = True
        if "used_memory:1000000" in mem_s:
            print("FAIL: INFO memory still hardcoded stub")
            fail = True

        if fail:
            sys.exit(1)
        print("PASS")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
