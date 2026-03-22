#!/usr/bin/env python3
"""
Comprehensive Redis/Dragonfly compatibility test suite for ForgeKV.

Usage:
    python tests/comprehensive_test.py [--host 127.0.0.1] [--port 6379]

Each test is marked:
  [PASS]  - command works correctly
  [FAIL]  - command returns wrong result
  [ERROR] - command returned an error (when it shouldn't)
  [SKIP]  - test skipped (e.g. feature not applicable)

Requires: pip install redis
"""

import sys
import time
import socket
import argparse
import traceback
from typing import Any

try:
    import redis
except ImportError:
    print("ERROR: redis-py not installed. Run: pip install redis")
    sys.exit(1)

HOST = "127.0.0.1"
PORT = 6379
PASS_COUNT = 0
FAIL_COUNT = 0
ERROR_COUNT = 0
TOTAL_COUNT = 0
RESULTS = []

def test(name: str, expected: Any, actual: Any, description: str = ""):
    global PASS_COUNT, FAIL_COUNT, TOTAL_COUNT
    TOTAL_COUNT += 1

    if isinstance(expected, float) and isinstance(actual, (int, float)):
        ok = abs(float(actual) - expected) < 0.001
    elif expected is None:
        ok = actual is None
    elif isinstance(expected, list) and isinstance(actual, (list, tuple)):
        ok = sorted(str(e) for e in expected) == sorted(str(a) for a in actual)
    else:
        ok = actual == expected

    if ok:
        PASS_COUNT += 1
        status = "PASS"
    else:
        FAIL_COUNT += 1
        status = "FAIL"
        print(f"  {status}: {name}")
        print(f"    expected: {expected!r}")
        print(f"    actual:   {actual!r}")
        if description:
            print(f"    note:     {description}")

    RESULTS.append((status, name, expected, actual))

def test_error(name: str, fn, *args, **kwargs):
    """Test that a command raises an error."""
    global PASS_COUNT, ERROR_COUNT, FAIL_COUNT, TOTAL_COUNT
    TOTAL_COUNT += 1
    try:
        result = fn(*args, **kwargs)
        FAIL_COUNT += 1
        status = "FAIL"
        print(f"  {status}: {name} (expected error, got {result!r})")
        RESULTS.append((status, name, "error", result))
    except redis.exceptions.ResponseError:
        PASS_COUNT += 1
        RESULTS.append(("PASS", name, "error", "error"))
    except Exception as e:
        PASS_COUNT += 1  # Any error is fine for this test
        RESULTS.append(("PASS", name, "error", str(e)))

def section(name: str):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

def run_tests(r: redis.Redis):
    # ── Basic connectivity ────────────────────────────────────────────────────
    section("Basic Connectivity")
    test("PING", True, r.ping())
    test("ECHO", b"hello", r.echo("hello"))

    # ── Server state ─────────────────────────────────────────────────────────
    r.flushall()

    # ── String Commands ───────────────────────────────────────────────────────
    section("String Commands")

    test("SET basic", True, r.set("str1", "hello"))
    test("GET basic", b"hello", r.get("str1"))
    test("GET missing", None, r.get("nonexistent_key_xyz"))
    test("SET EX", True, r.set("str2", "world", ex=100))
    test("TTL after SET EX", True, r.ttl("str2") > 0)
    test("SET PX", True, r.set("str3", "px", px=100000))
    test("SET NX (new key)", True, r.set("setnx1", "val", nx=True))
    test("SET NX (existing key)", None, r.set("setnx1", "val2", nx=True))
    test("SET XX (existing key)", True, r.set("setnx1", "val3", xx=True))
    test("SET XX (missing key)", None, r.set("nonexistent_xx", "v", xx=True))
    test("SET GET", b"val3", r.set("setnx1", "newval", get=True))
    test("GETSET", b"newval", r.getset("setnx1", "final"))
    test("SETNX", 1, r.setnx("setnx2", "v"))
    test("SETNX existing", 0, r.setnx("setnx2", "v2"))
    test("SETEX", True, r.setex("setex1", 100, "hello"))
    test("PSETEX", True, r.psetex("psetex1", 10000, "hello"))
    test("MSET", True, r.mset({"k1": "v1", "k2": "v2", "k3": "v3"}))
    test("MGET", [b"v1", b"v2", b"v3"], r.mget("k1", "k2", "k3"))
    test("MSETNX all new", 1, r.msetnx({"mnx1": "a", "mnx2": "b"}))
    test("MSETNX any existing", 0, r.msetnx({"mnx1": "c", "mnx3": "d"}))
    test("INCR", 1, r.incr("counter"))
    test("INCR again", 2, r.incr("counter"))
    test("INCRBY", 12, r.incrby("counter", 10))
    test("DECRBY", 10, r.decrby("counter", 2))
    test("DECR", 9, r.decr("counter"))
    test("INCRBYFLOAT", 1.5, r.incrbyfloat("float1", 1.5))
    test("INCRBYFLOAT add", 3.0, r.incrbyfloat("float1", 1.5))
    test("APPEND", 11, r.append("str1", " world"))
    test("STRLEN", 11, r.strlen("str1"))
    test("GETRANGE", b"hello", r.getrange("str1", 0, 4))
    test("SETRANGE", 11, r.setrange("str1", 6, "Redis"))
    test("GETDEL", b"hello Redis", r.getdel("str1"))
    test("GET after GETDEL", None, r.get("str1"))
    test("GETEX with EX", b"hello", r.set("gex1", "hello") and r.getex("gex1", ex=100))
    test("GETEX PERSIST", b"hello", r.getex("gex1", persist=True))
    test("GETEX EXAT", b"hello", r.getex("gex1", exat=int(time.time())+100))

    # Bit commands
    r.set("bitkey", b"\xff\xf0\x00")
    test("GETBIT", 1, r.getbit("bitkey", 0))
    test("GETBIT off", 0, r.getbit("bitkey", 12))
    test("SETBIT", 1, r.setbit("bitkey", 0, 0))  # bit 0 of 0xff is 1
    test("BITCOUNT", 11, r.bitcount("bitkey"))
    r.set("bk1", b"\xff\xf0")
    r.set("bk2", b"\x0f\xff")
    test("BITOP AND", 2, r.bitop("AND", "bkand", "bk1", "bk2"))
    test("BITOP OR", 2, r.bitop("OR", "bkor", "bk1", "bk2"))
    test("BITOP XOR", 2, r.bitop("XOR", "bkxor", "bk1", "bk2"))
    test("BITPOS first 1", 0, r.bitpos("bkor", 1))
    test("BITPOS first 0", 12, r.bitpos("bk1", 0))  # 0xff=all1s, 0xf0=11110000, first 0 at pos 12

    # LCS
    r.set("lcs1", "ohmytext")
    r.set("lcs2", "mynewtext")
    lcs_result = r.execute_command("LCS", "lcs1", "lcs2")
    test("LCS", b"mytext", lcs_result)

    # ── String error/edge case regression tests ───────────────────────────────
    section("String Error/Edge Cases (Regression)")
    # SET EX 0 must error (regression: was returning OK)
    test_error("SET EX 0 must error", r.execute_command, "SET", "k_ex0", "v", "EX", "0")
    test_error("SET EX -1 must error", r.execute_command, "SET", "k_ex_neg", "v", "EX", "-1")
    test_error("SET PX 0 must error", r.execute_command, "SET", "k_px0", "v", "PX", "0")
    test_error("SET PX -1 must error", r.execute_command, "SET", "k_px_neg", "v", "PX", "-1")
    test_error("SET EXAT 0 must error", r.execute_command, "SET", "k_exat0", "v", "EXAT", "0")
    test_error("SET EXAT -1 must error", r.execute_command, "SET", "k_exat_neg", "v", "EXAT", "-1")
    test_error("SET PXAT 0 must error", r.execute_command, "SET", "k_pxat0", "v", "PXAT", "0")
    test_error("SET PXAT -1 must error", r.execute_command, "SET", "k_pxat_neg", "v", "PXAT", "-1")
    test_error("SETEX 0 must error", r.setex, "k_setex0", 0, "v")
    test_error("SETEX -1 must error", r.setex, "k_setex_neg", -1, "v")
    test_error("PSETEX 0 must error", r.psetex, "k_psetex0", 0, "v")
    test_error("PSETEX -1 must error", r.psetex, "k_psetex_neg", -1, "v")
    # keys should not exist after failed SET
    test("SET EX 0 key not created", None, r.get("k_ex0"))
    test("SET EX -1 key not created", None, r.get("k_ex_neg"))
    # Valid expire should still work
    test("SET EX 1 ok", True, r.set("k_ex1", "hello", ex=1))
    test("SET PX 100 ok", True, r.set("k_px100", "hello", px=100))
    test("SET KEEPTTL works", True, r.set("k_ex1", "world", keepttl=True))

    # INCR on non-integer
    r.set("not_int", "hello")
    test_error("INCR non-integer", r.incr, "not_int")
    test_error("INCRBY non-integer", r.incrby, "not_int", 1)

    # INCR overflow
    r.set("max_int", str(2**63 - 1))
    test_error("INCR overflow", r.incr, "max_int")

    # SET KEEPTTL preserves TTL
    r.set("kttl", "val", ex=100)
    r.set("kttl", "new_val", keepttl=True)
    test("SET KEEPTTL preserves TTL", True, r.ttl("kttl") > 0)

    # SET without KEEPTTL removes TTL
    r.set("kttl2", "val", ex=100)
    r.set("kttl2", "new_val")
    test("SET without KEEPTTL removes TTL", -1, r.ttl("kttl2"))

    # ── Key Commands ──────────────────────────────────────────────────────────
    section("Key Commands")

    r.flushall()
    r.set("key1", "val1")
    r.set("key2", "val2")
    r.set("key3", "val3")
    r.set("expire_key", "val", ex=100)

    test("DEL single", 1, r.delete("key1"))
    test("DEL multiple", 2, r.delete("key2", "key3"))
    test("DEL missing", 0, r.delete("nonexistent_xyz"))
    test("EXISTS single", 1, r.exists("expire_key"))
    test("EXISTS missing", 0, r.exists("key1"))
    test("EXISTS multiple", 1, r.exists("expire_key", "key1", "nonexistent"))
    test("TTL with expiry", True, r.ttl("expire_key") > 0)
    test("TTL no expiry", -1, r.ttl("key_no_expiry") if r.set("key_no_expiry", "v") else -1)
    test("PTTL", True, r.pttl("expire_key") > 0)
    test("EXPIRE", 1, r.expire("expire_key", 200))
    test("TTL after EXPIRE", True, r.ttl("expire_key") > 100)
    test("EXPIREAT", 1, r.expireat("expire_key", int(time.time()) + 300))
    test("PEXPIRE", 1, r.pexpire("expire_key", 300000))
    test("PEXPIREAT", 1, r.pexpireat("expire_key", int(time.time() * 1000) + 400000))
    test("EXPIRETIME", True, r.expiretime("expire_key") > 0)
    test("PEXPIRETIME", True, r.pexpiretime("expire_key") > 0)
    test("PERSIST", 1, r.persist("expire_key"))
    test("TTL after PERSIST", -1, r.ttl("expire_key"))

    # EXPIRE with negative/zero values: key expires immediately (Redis behavior)
    r.set("expire_neg_test", "v")
    test("EXPIRE -1 expires key", True, r.execute_command("EXPIRE", "expire_neg_test", -1) == 1)
    test("GET after EXPIRE -1 is nil", None, r.get("expire_neg_test"))
    r.set("expire_neg_test2", "v")
    test("EXPIRE 0 expires key", True, r.execute_command("EXPIRE", "expire_neg_test2", 0) == 1)
    test("GET after EXPIRE 0 is nil", None, r.get("expire_neg_test2"))
    r.set("pexpire_neg_test", "v")
    test("PEXPIRE -1 expires key", True, r.execute_command("PEXPIRE", "pexpire_neg_test", -1) == 1)
    test("GET after PEXPIRE -1 is nil", None, r.get("pexpire_neg_test"))
    r.set("expireat_neg_test", "v")
    test("EXPIREAT -1 expires key", True, r.execute_command("EXPIREAT", "expireat_neg_test", -1) == 1)
    test("GET after EXPIREAT -1 is nil", None, r.get("expireat_neg_test"))

    # EXPIRE options (Redis 7+)
    r.set("eopts", "v")
    test("EXPIRE NX (no expiry)", 1, r.execute_command("EXPIRE", "eopts", 100, "NX"))
    test("EXPIRE NX (has expiry)", 0, r.execute_command("EXPIRE", "eopts", 200, "NX"))
    test("EXPIRE XX (has expiry)", 1, r.execute_command("EXPIRE", "eopts", 200, "XX"))
    test("EXPIRE GT (greater)", 1, r.execute_command("EXPIRE", "eopts", 300, "GT"))
    test("EXPIRE GT (smaller)", 0, r.execute_command("EXPIRE", "eopts", 100, "GT"))
    test("EXPIRE LT (smaller)", 1, r.execute_command("EXPIRE", "eopts", 50, "LT"))
    test("EXPIRE LT (greater)", 0, r.execute_command("EXPIRE", "eopts", 300, "LT"))

    r.flushall()
    r.set("k1", "v1"); r.set("k2", "v2"); r.set("k3", "v3")

    test("TYPE string", b"string", r.type("k1"))
    test("TYPE missing", b"none", r.type("missing_xyz"))
    test("RENAME", True, r.rename("k1", "k1new"))
    test("RENAMENX success", True, r.renamenx("k2", "k2new"))
    test("RENAMENX fail", False, r.renamenx("k3", "k2new"))
    test_error("RENAME missing", r.rename, "missing_xyz", "dest")
    test("KEYS *", True, len(r.keys("*")) >= 3)
    test("KEYS pattern", True, len(r.keys("k*")) >= 1)
    test("SCAN basic", True, len(r.scan(0, count=100)[1]) > 0)
    test("UNLINK", 1, r.unlink("k3"))
    test("TOUCH existing", 1, r.touch("k2new"))
    test("TOUCH missing", 0, r.touch("missing_xyz"))
    test("RANDOMKEY", True, r.randomkey() is not None)
    r.set("obj_int_key", "12345")
    test("OBJECT ENCODING int", b"int", r.execute_command("OBJECT", "ENCODING", "obj_int_key"))
    test("OBJECT REFCOUNT", 1, r.execute_command("OBJECT", "REFCOUNT", "k1new"))
    test("OBJECT IDLETIME", True, r.execute_command("OBJECT", "IDLETIME", "k1new") >= 0)

    # COPY
    r.set("src_copy", "value")
    test("COPY", True, r.copy("src_copy", "dst_copy"))
    test("COPY target exists (no REPLACE)", False, r.copy("src_copy", "dst_copy"))
    test("COPY with REPLACE", True, r.copy("src_copy", "dst_copy", replace=True))

    # DUMP/RESTORE (basic check - just ensure they work)
    dump_data = r.dump("k1new")
    test("DUMP returns data", True, dump_data is not None and len(dump_data) > 0)

    # ── Hash Commands ─────────────────────────────────────────────────────────
    section("Hash Commands")

    r.flushall()
    test("HSET single", 1, r.hset("h1", "field1", "value1"))
    test("HSET multiple", 2, r.hset("h1", mapping={"field2": "value2", "field3": "value3"}))
    test("HGET", b"value1", r.hget("h1", "field1"))
    test("HGET missing field", None, r.hget("h1", "missing"))
    test("HGET missing key", None, r.hget("missing_h", "field1"))
    test("HMSET", True, r.hmset("h2", {"a": "1", "b": "2", "c": "3"}))
    test("HMGET", [b"1", b"2", None], r.hmget("h2", "a", "b", "z"))
    test("HGETALL", {b"a": b"1", b"b": b"2", b"c": b"3"}, r.hgetall("h2"))
    test("HEXISTS true", 1, r.hexists("h2", "a"))
    test("HEXISTS false", 0, r.hexists("h2", "z"))
    test("HLEN", 3, r.hlen("h2"))
    test("HKEYS", True, set(r.hkeys("h2")) == {b"a", b"b", b"c"})
    test("HVALS", True, set(r.hvals("h2")) == {b"1", b"2", b"3"})
    test("HDEL", 1, r.hdel("h2", "c"))
    test("HLEN after HDEL", 2, r.hlen("h2"))
    test("HSETNX new field", 1, r.hsetnx("h2", "new", "nv"))
    test("HSETNX existing field", 0, r.hsetnx("h2", "a", "new"))
    test("HINCRBY", 5, r.hincrby("h2", "counter", 5))
    test("HINCRBY again", 8, r.hincrby("h2", "counter", 3))
    test("HINCRBYFLOAT", 1.5, r.hincrbyfloat("h2", "floatfield", 1.5))
    test("HSTRLEN", 1, r.hstrlen("h2", "a"))

    # HRANDFIELD
    hrand = r.hrandfield("h2")
    test("HRANDFIELD", True, hrand is not None)
    hrand_multi = r.hrandfield("h2", count=2)
    test("HRANDFIELD count", 2, len(hrand_multi))
    hrand_wv = r.hrandfield("h2", count=2, withvalues=True)
    test("HRANDFIELD WITHVALUES", 4, len(hrand_wv))  # 2 fields * 2 (field+value)

    # HSCAN
    cursor, fields = r.hscan("h2", 0)
    test("HSCAN returns fields", True, len(fields) > 0)

    # TYPE for hash
    test("TYPE hash", b"hash", r.type("h2"))
    test("OBJECT ENCODING hash", True, r.execute_command("OBJECT", "ENCODING", "h2") in [b"listpack", b"ziplist", b"hashtable"])

    # ── List Commands ─────────────────────────────────────────────────────────
    section("List Commands")

    r.flushall()
    test("LPUSH", 3, r.lpush("list1", "c", "b", "a"))
    test("RPUSH", 5, r.rpush("list1", "d", "e"))
    test("LLEN", 5, r.llen("list1"))
    test("LRANGE all", [b"a", b"b", b"c", b"d", b"e"], r.lrange("list1", 0, -1))
    test("LPOP", b"a", r.lpop("list1"))
    test("RPOP", b"e", r.rpop("list1"))
    test("LINDEX 0", b"b", r.lindex("list1", 0))
    test("LINDEX -1", b"d", r.lindex("list1", -1))
    test("LSET", True, r.lset("list1", 0, "B"))
    test("LINSERT BEFORE", 4, r.linsert("list1", "BEFORE", "c", "CC"))
    test("LINSERT AFTER", 5, r.linsert("list1", "AFTER", "c", "CCC"))
    test("LREM", 1, r.lrem("list1", 1, "CC"))
    test("LTRIM", True, r.ltrim("list1", 0, 10))
    test("LPUSHX existing", True, r.lpushx("list1", "z") > 0)
    test("RPUSHX existing", True, r.rpushx("list1", "z") > 0)
    test("LPUSHX missing", 0, r.lpushx("missing_list", "z"))
    test("RPUSHX missing", 0, r.rpushx("missing_list", "z"))

    # LMOVE
    r.delete("srclist", "dstlist")
    r.rpush("srclist", "a", "b", "c")
    result = r.lmove("srclist", "dstlist", "LEFT", "RIGHT")
    test("LMOVE", b"a", result)

    # RPOPLPUSH
    r.rpush("rplsrc", "x", "y")
    test("RPOPLPUSH", b"y", r.rpoplpush("rplsrc", "rpldst"))

    # LPOS
    r.delete("lpos_list")
    r.rpush("lpos_list", "a", "b", "c", "b", "d", "b")
    test("LPOS first", 1, r.lpos("lpos_list", "b"))
    test("LPOS RANK 2", 3, r.lpos("lpos_list", "b", rank=2))
    test("LPOS COUNT 0", [1, 3, 5], r.lpos("lpos_list", "b", count=0))

    # LMPOP
    r.delete("lmpop1", "lmpop2")
    r.rpush("lmpop1", "a", "b", "c")
    lmpop_result = r.execute_command("LMPOP", "1", "lmpop1", "LEFT")
    test("LMPOP", True, lmpop_result is not None and lmpop_result[0] == b"lmpop1")

    # TYPE for list
    test("TYPE list", b"list", r.type("list1"))
    test("OBJECT ENCODING list", True, r.execute_command("OBJECT", "ENCODING", "list1") in [b"listpack", b"quicklist", b"ziplist"])

    # ── Set Commands ──────────────────────────────────────────────────────────
    section("Set Commands")

    r.flushall()
    test("SADD", 3, r.sadd("set1", "a", "b", "c"))
    test("SADD duplicate", 1, r.sadd("set1", "d", "a"))  # 'a' exists, 'd' new
    test("SMEMBERS", True, r.smembers("set1") == {b"a", b"b", b"c", b"d"})
    test("SCARD", 4, r.scard("set1"))
    test("SISMEMBER true", 1, r.sismember("set1", "a"))
    test("SISMEMBER false", 0, r.sismember("set1", "z"))
    test("SMISMEMBER", [1, 0, 1], r.smismember("set1", "a", "z", "b"))
    test("SREM", 1, r.srem("set1", "d"))
    test("SREM missing", 0, r.srem("set1", "z"))
    test("SCARD after SREM", 3, r.scard("set1"))

    r.sadd("set2", "b", "c", "d", "e")
    test("SUNION", True, r.sunion("set1", "set2") == {b"a", b"b", b"c", b"d", b"e"})
    test("SINTER", True, r.sinter("set1", "set2") == {b"b", b"c"})
    test("SDIFF", True, r.sdiff("set1", "set2") == {b"a"})

    test("SUNIONSTORE", 5, r.sunionstore("sunion_dst", "set1", "set2"))
    test("SINTERSTORE", 2, r.sinterstore("sinter_dst", "set1", "set2"))
    test("SDIFFSTORE", 1, r.sdiffstore("sdiff_dst", "set1", "set2"))

    test("SMOVE", True, r.smove("set1", "set2", "a"))
    test("SRANDMEMBER", True, r.srandmember("set1") is not None)
    test("SRANDMEMBER count", 2, len(r.srandmember("set1", 2)))
    test("SPOP", True, r.spop("set1") is not None)

    # SSCAN
    cursor, members = r.sscan("set2", 0)
    test("SSCAN", True, len(members) > 0)

    # SINTERCARD
    r.sadd("sc1", "a", "b", "c"); r.sadd("sc2", "b", "c", "d")
    test("SINTERCARD", 2, r.execute_command("SINTERCARD", 2, "sc1", "sc2"))
    test("SINTERCARD LIMIT", 1, r.execute_command("SINTERCARD", 2, "sc1", "sc2", "LIMIT", 1))

    # TYPE for set
    test("TYPE set", b"set", r.type("set1"))

    # ── Sorted Set Commands ───────────────────────────────────────────────────
    section("Sorted Set Commands")

    r.flushall()
    test("ZADD", 3, r.zadd("zset1", {"a": 1.0, "b": 2.0, "c": 3.0}))
    test("ZADD NX", 1, r.zadd("zset1", {"d": 4.0}, nx=True))
    test("ZADD XX", 0, r.zadd("zset1", {"a": 10.0}, xx=True))
    test("ZADD GT", 0, r.zadd("zset1", {"a": 5.0}, gt=True))  # 10 > 5, no update
    test("ZADD LT", 0, r.zadd("zset1", {"a": 3.0}, lt=True))  # 10 > 3, update
    test("ZADD CH", 1, r.zadd("zset1", {"b": 20.0}, ch=True))  # changed count
    test("ZSCORE", 3.0, r.zscore("zset1", "a"))
    test("ZRANK", 0, r.zrank("zset1", "a"))
    test("ZREVRANK", 3, r.zrevrank("zset1", "a"))
    test("ZCARD", 4, r.zcard("zset1"))
    test("ZINCRBY", 6.0, r.zincrby("zset1", 3.0, "a"))
    # State after all ZADDs + ZINCRBY: a=6, b=20, c=3, d=4
    test("ZRANGE", [b"c", b"d", b"a"], r.zrange("zset1", 0, 2))
    test("ZRANGE WITHSCORES", True, len(r.zrange("zset1", 0, -1, withscores=True)) == 4)
    test("ZREVRANGE", [b"b", b"a"], r.zrevrange("zset1", 0, 1))
    test("ZRANGEBYSCORE", [b"a", b"b"], r.zrangebyscore("zset1", 6.0, 20.0))
    test("ZREVRANGEBYSCORE", [b"b", b"a"], r.zrevrangebyscore("zset1", 20.0, 6.0))
    test("ZRANGEBYLEX", [b"b", b"c"], r.zrangebylex("zset1", b"[b", b"[c") if r.zadd("lexset", {"a": 0, "b": 0, "c": 0, "d": 0}) else [])
    test("ZCOUNT", 2, r.zcount("zset1", 6.0, 20.0))
    test("ZLEXCOUNT", 4, r.zlexcount("zset1", "-", "+"))
    test("ZREM", 1, r.zrem("zset1", "d"))
    # After ZREM d: a=6, b=20, c=3. ZPOPMIN pops lowest (c=3)
    test("ZPOPMIN", [(b"c", 3.0)], r.zpopmin("zset1", 1))
    # After ZPOPMIN: a=6, b=20. ZPOPMAX pops highest (b=20)
    test("ZPOPMAX", [(b"b", 20.0)], r.zpopmax("zset1", 1))
    # After pops: only a=6 remains. ZMSCORE for "a" (6) and "missing" (None)
    test("ZMSCORE", [6.0, None], r.execute_command("ZMSCORE", "zset1", "a", "missing"))
    # ZREMRANGEBYSCORE: no elements left in range 6..6, returns 1 (removes a=6)
    test("ZREMRANGEBYSCORE", 1, r.zremrangebyscore("zset1", 6.0, 6.0))

    r.zadd("zset2", {"b": 2.0, "c": 3.0, "e": 5.0})
    r.zadd("zset3", {"a": 1.0, "b": 2.0, "c": 3.0})
    test("ZUNIONSTORE", 4, r.zunionstore("zunion", ["zset2", "zset3"]))
    test("ZINTERSTORE", 2, r.zinterstore("zinter", ["zset2", "zset3"]))
    test("ZDIFFSTORE", 1, r.zdiffstore("zdiff", ["zset2", "zset3"]))
    test("ZDIFF", True, len(r.zdiff(["zset2", "zset3"])) >= 1)
    test("ZUNION", True, len(r.zunion(["zset2", "zset3"])) >= 1)
    test("ZINTER", True, len(r.zinter(["zset2", "zset3"])) >= 1)

    # ZRANGESTORE
    r.zadd("zsrc", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    test("ZRANGESTORE", 3, r.zrangestore("zdst", "zsrc", 0, 2))

    # ZRANDMEMBER
    r.zadd("zrand", {"a": 1.0, "b": 2.0, "c": 3.0})
    test("ZRANDMEMBER", True, r.zrandmember("zrand") is not None)
    test("ZRANDMEMBER count", 2, len(r.zrandmember("zrand", 2)))
    test("ZRANDMEMBER WITHSCORES", 4, len(r.zrandmember("zrand", 2, withscores=True)))

    # ZSCAN
    cursor, members = r.zscan("zrand", 0)
    test("ZSCAN", True, len(members) > 0)

    # ZINTERCARD
    r.zadd("zic1", {"a": 1, "b": 2, "c": 3})
    r.zadd("zic2", {"b": 2, "c": 3, "d": 4})
    test("ZINTERCARD", 2, r.execute_command("ZINTERCARD", 2, "zic1", "zic2"))
    test("ZINTERCARD LIMIT", 1, r.execute_command("ZINTERCARD", 2, "zic1", "zic2", "LIMIT", 1))

    # ZMPOP
    r.zadd("zmpop1", {"a": 1.0, "b": 2.0})
    zmpop_result = r.execute_command("ZMPOP", "1", "zmpop1", "MIN")
    test("ZMPOP", True, zmpop_result is not None and zmpop_result[0] == b"zmpop1")

    # TYPE for zset
    test("TYPE zset", b"zset", r.type("zset2"))
    test("OBJECT ENCODING zset", True, r.execute_command("OBJECT", "ENCODING", "zset2") in [b"listpack", b"ziplist", b"skiplist"])

    # ZREMRANGEBYRANK
    r.zadd("zrrr", {"a": 1, "b": 2, "c": 3, "d": 4})
    test("ZREMRANGEBYRANK", 2, r.zremrangebyrank("zrrr", 0, 1))

    # ZREMRANGEBYLEX
    r.zadd("zrrl", {"a": 0, "b": 0, "c": 0, "d": 0})
    test("ZREMRANGEBYLEX", 2, r.zremrangebylex("zrrl", b"[a", b"[b"))

    # ── Stream Commands ───────────────────────────────────────────────────────
    section("Stream Commands")

    r.flushall()
    entry_id = r.xadd("stream1", {"name": "Alice", "age": "30"})
    test("XADD", True, entry_id is not None and b"-" in entry_id)
    r.xadd("stream1", {"name": "Bob", "age": "25"})
    r.xadd("stream1", {"name": "Carol", "age": "35"})

    test("XLEN", 3, r.xlen("stream1"))

    entries = r.xrange("stream1", "-", "+")
    test("XRANGE", 3, len(entries))
    entries_rev = r.xrevrange("stream1", "+", "-")
    test("XREVRANGE", 3, len(entries_rev))

    # TYPE for stream (requires ext_type_registry)
    test("TYPE stream", b"stream", r.type("stream1"))
    test("EXISTS stream", 1, r.exists("stream1"))

    stream_keys = r.keys("*")
    test("KEYS includes stream", True, b"stream1" in stream_keys)

    # XTRIM
    r.xadd("xtrim_stream", {"f": "v"})
    r.xadd("xtrim_stream", {"f": "v"})
    r.xadd("xtrim_stream", {"f": "v"})
    test("XTRIM MAXLEN", True, r.xtrim("xtrim_stream", 2) >= 0)
    test("XLEN after XTRIM", 2, r.xlen("xtrim_stream"))

    # XDEL
    entry = r.xadd("xdel_stream", {"f": "v"})
    test("XDEL", 1, r.xdel("xdel_stream", entry))

    # XREAD
    result = r.xread({"stream1": 0})
    test("XREAD", True, result is not None and len(result) > 0)

    # XGROUP + XREADGROUP
    r.delete("xg_stream")
    r.xadd("xg_stream", {"msg": "hello"})
    r.xadd("xg_stream", {"msg": "world"})
    r.execute_command("XGROUP", "CREATE", "xg_stream", "group1", "0")
    xrg_result = r.execute_command("XREADGROUP", "GROUP", "group1", "consumer1", "COUNT", "10", "STREAMS", "xg_stream", ">")
    test("XREADGROUP", True, xrg_result is not None and len(xrg_result) > 0)

    # XACK
    if xrg_result and len(xrg_result) > 0:
        stream_data = xrg_result[0][1]
        if stream_data:
            first_id = stream_data[0][0]
            test("XACK", 1, r.xack("xg_stream", "group1", first_id))

    # XINFO
    xinfo = r.execute_command("XINFO", "STREAM", "xg_stream")
    test("XINFO STREAM", True, xinfo is not None and len(xinfo) > 0)

    # ── Pub/Sub Commands ──────────────────────────────────────────────────────
    section("Pub/Sub Commands")

    # Test PUBSUB commands
    subs = r.pubsub_numsub()
    test("PUBSUB NUMSUB", True, isinstance(subs, (dict, list)))
    patterns = r.pubsub_numpat()
    test("PUBSUB NUMPAT", True, isinstance(patterns, int))
    channels = r.pubsub_channels()
    test("PUBSUB CHANNELS", True, isinstance(channels, list))

    # Publish (no subscribers, returns 0)
    test("PUBLISH", 0, r.publish("test_channel", "hello"))

    # ── Geo Commands ──────────────────────────────────────────────────────────
    section("Geo Commands")

    r.flushall()
    test("GEOADD", 2, r.geoadd("geo1", [
        13.361389, 38.115556, "Palermo",
        15.087269, 37.502669, "Catania",
    ]))
    dist = r.geodist("geo1", "Palermo", "Catania", unit="km")
    test("GEODIST km", True, abs(dist - 166.274) < 1.0)
    pos = r.geopos("geo1", "Palermo")
    test("GEOPOS", True, pos[0] is not None and abs(pos[0][0] - 13.361389) < 0.001)
    hashes = r.geohash("geo1", "Palermo", "Catania")
    test("GEOHASH", True, len(hashes) == 2 and hashes[0] is not None)

    # GEOSEARCH
    search_result = r.geosearch("geo1", longitude=15.0, latitude=37.0, radius=200, unit="km")
    test("GEOSEARCH radius", True, len(search_result) >= 1)

    # ── HyperLogLog Commands ───────────────────────────────────────────────────
    section("HyperLogLog Commands")

    r.flushall()
    test("PFADD", 1, r.pfadd("hll1", "a", "b", "c", "d", "e"))
    test("PFADD no change", 0, r.pfadd("hll1", "a"))  # same elements
    count = r.pfcount("hll1")
    test("PFCOUNT", True, abs(count - 5) <= 1)  # HLL is approximate
    r.pfadd("hll2", "d", "e", "f", "g")
    test("PFMERGE", True, r.pfmerge("hll_merged", "hll1", "hll2"))
    merged_count = r.pfcount("hll_merged")
    test("PFCOUNT merged", True, merged_count >= 7)

    # ── Bloom Filter Commands ──────────────────────────────────────────────────
    section("Bloom Filter Commands")

    r.flushall()
    test("BF.ADD new", 1, r.execute_command("BF.ADD", "bf1", "hello"))
    test("BF.ADD existing", 0, r.execute_command("BF.ADD", "bf1", "hello"))
    test("BF.ADD new item", 1, r.execute_command("BF.ADD", "bf1", "world"))
    test("BF.EXISTS true", 1, r.execute_command("BF.EXISTS", "bf1", "hello"))
    test("BF.EXISTS false", 0, r.execute_command("BF.EXISTS", "bf1", "notadded"))
    test("BF.MADD", True, r.execute_command("BF.MADD", "bf1", "a", "b", "c") is not None)
    test("BF.MEXISTS", True, r.execute_command("BF.MEXISTS", "bf1", "a", "nothere") is not None)
    test("BF.RESERVE", True, r.execute_command("BF.RESERVE", "bf2", "0.01", "1000") == b"OK")
    test("BF.INFO", True, r.execute_command("BF.INFO", "bf1") is not None)
    test("BF.CARD", True, r.execute_command("BF.CARD", "bf1") >= 0)
    test("TYPE BF", b"MBbloom--", r.type("bf1"))
    test("EXISTS BF", 1, r.exists("bf1"))

    # ── JSON Commands ─────────────────────────────────────────────────────────
    section("JSON Commands")

    r.flushall()
    test("JSON.SET", True, r.execute_command("JSON.SET", "json1", ".", '{"name":"Alice","age":30,"scores":[1,2,3]}') == b"OK")
    result = r.execute_command("JSON.GET", "json1", ".")
    test("JSON.GET root", True, b"Alice" in result)
    result_field = r.execute_command("JSON.GET", "json1", "$.name")
    test("JSON.GET field", True, b"Alice" in result_field)
    test("JSON.TYPE", True, r.execute_command("JSON.TYPE", "json1", ".") in [b"object", b"array"])
    test("JSON.STRLEN", 5, r.execute_command("JSON.STRLEN", "json1", "$.name"))
    test("JSON.NUMINCRBY", True, r.execute_command("JSON.NUMINCRBY", "json1", "$.age", 1) is not None)
    test("JSON.ARRLEN", 3, r.execute_command("JSON.ARRLEN", "json1", "$.scores"))
    test("JSON.ARRAPPEND", 4, r.execute_command("JSON.ARRAPPEND", "json1", "$.scores", "4"))
    test("JSON.OBJKEYS", True, r.execute_command("JSON.OBJKEYS", "json1", ".") is not None)
    test("JSON.OBJLEN", True, r.execute_command("JSON.OBJLEN", "json1", ".") >= 3)
    test("JSON.DEL field", 1, r.execute_command("JSON.DEL", "json1", "$.age"))
    test("TYPE JSON", True, r.type("json1") in [b"ReJSON-RL", b"string"])
    test("EXISTS JSON", 1, r.exists("json1"))
    test("KEYS includes JSON", True, b"json1" in r.keys("*"))

    # ── Server Commands ───────────────────────────────────────────────────────
    section("Server Commands")

    test("DBSIZE", True, r.dbsize() >= 0)
    info = r.info()
    test("INFO", True, "redis_version" in info)
    test("INFO server", True, "uptime_in_seconds" in r.info("server"))
    test("INFO memory", True, "used_memory" in r.info("memory"))
    test("INFO keyspace", True, isinstance(r.info("keyspace"), dict))
    test("TIME", True, len(r.time()) == 2)
    test("COMMAND COUNT", True, r.command_count() > 100)

    # CONFIG
    test("CONFIG GET maxmemory", True, "maxmemory" in r.config_get("maxmemory"))
    test("CONFIG SET bind-source-addr", True, r.config_set("bind-source-addr", "") == True)
    test("CONFIG RESETSTAT", True, r.config_resetstat() == True)

    # CLIENT commands
    test("CLIENT SETNAME", True, r.client_setname("test-client"))
    test("CLIENT GETNAME", True, r.client_getname() in (b"test-client", "test-client"))
    test("CLIENT ID", True, r.client_id() > 0)
    client_info = r.client_info()
    test("CLIENT INFO", True, client_info is not None and len(client_info) > 0)
    client_list = r.client_list()
    test("CLIENT LIST", True, client_list is not None and len(client_list) > 0)
    test("CLIENT NO-EVICT", True, r.execute_command("CLIENT", "NO-EVICT", "ON") == b"OK")

    # HELLO
    hello_result = r.execute_command("HELLO")
    test("HELLO", True, hello_result is not None)
    hello2 = r.execute_command("HELLO", "2")
    test("HELLO 2", True, hello2 is not None)

    # DEBUG
    test("DEBUG SLEEP", True, r.execute_command("DEBUG", "SLEEP", "0") == b"OK")

    # OBJECT
    r.set("obj_test", "12345")
    test("OBJECT ENCODING int", b"int", r.execute_command("OBJECT", "ENCODING", "obj_test"))
    r.set("obj_test2", "hello world this is a string")
    test("OBJECT ENCODING embstr", True, r.execute_command("OBJECT", "ENCODING", "obj_test2") in [b"embstr", b"raw"])
    test("OBJECT FREQ", True, r.execute_command("OBJECT", "FREQ", "obj_test") >= 0)
    test("OBJECT HELP", True, r.execute_command("OBJECT", "HELP") is not None)

    # SLOWLOG
    test("SLOWLOG LEN", True, r.slowlog_len() >= 0)
    test("SLOWLOG RESET", True, r.slowlog_reset() == True)
    test("SLOWLOG GET", True, isinstance(r.slowlog_get(), list))

    # LATENCY
    test("LATENCY LATEST", True, isinstance(r.execute_command("LATENCY", "LATEST"), list))
    test("LATENCY RESET", True, r.execute_command("LATENCY", "RESET") is not None)

    # MEMORY
    test("MEMORY USAGE", True, r.memory_usage("obj_test") is not None)
    test("MEMORY STATS", True, isinstance(r.execute_command("MEMORY", "STATS"), (list, dict)))

    # SAVE
    test("BGSAVE", True, r.bgsave() in [True, b"Background saving started"])

    # ACL
    test("ACL WHOAMI", True, r.acl_whoami() in (b"default", "default"))
    test("ACL LIST", True, len(r.acl_list()) > 0)
    test("ACL USERS", True, len(r.acl_users()) > 0)
    test("ACL CAT", True, len(r.acl_cat()) > 0)

    # WAIT
    test("WAIT", 0, r.wait(0, 0))

    # LOLWUT
    test("LOLWUT", True, r.execute_command("LOLWUT") is not None)

    # ── MULTI/EXEC Transactions ───────────────────────────────────────────────
    section("Transactions (MULTI/EXEC)")

    r.flushall()
    with r.pipeline() as pipe:
        pipe.multi()
        pipe.set("tx1", "v1")
        pipe.set("tx2", "v2")
        pipe.get("tx1")
        results = pipe.execute()
    test("MULTI/EXEC SET", True, results[0] == True)
    test("MULTI/EXEC GET", True, results[2] == b"v1")

    # DISCARD - test that DISCARD outside MULTI returns error
    test_error("DISCARD without MULTI", r.execute_command, "DISCARD")

    # ── Keyspace Notifications (if enabled) ───────────────────────────────────
    section("SELECT / Database Switch")
    test("SELECT 0", True, r.execute_command("SELECT", "0") in (True, b"OK"))
    test("SELECT 1", True, r.execute_command("SELECT", "1") in (True, b"OK"))
    test("SELECT 0 again", True, r.execute_command("SELECT", "0") in (True, b"OK"))
    test_error("SELECT out of range", r.execute_command, "SELECT", "999")

    # SWAPDB
    r.flushall()
    r.set("swapkey", "indb0")
    r.execute_command("SELECT", "1")
    r.set("swapkey", "indb1")
    r.execute_command("SELECT", "0")
    test("SWAPDB", True, r.execute_command("SWAPDB", "0", "1") in (True, b"OK"))
    test("GET after SWAPDB", b"indb1", r.get("swapkey"))

    # ── SORT ─────────────────────────────────────────────────────────────────
    section("SORT Command")

    r.flushall()
    r.rpush("sortlist", 3, 1, 2, 5, 4)
    test("SORT", [b"1", b"2", b"3", b"4", b"5"], r.sort("sortlist"))
    test("SORT DESC", [b"5", b"4", b"3", b"2", b"1"], r.sort("sortlist", desc=True))
    test("SORT LIMIT", [b"2", b"3"], r.sort("sortlist", start=1, num=2))
    test("SORT ALPHA", True, len(r.sort("sortlist", alpha=True)) == 5)

    # SORT STORE (regression: was not storing result)
    r.delete("sortdst")
    count = r.sort("sortlist", store="sortdst")
    test("SORT STORE returns count", 5, count)
    test("SORT STORE creates list", [b"1", b"2", b"3", b"4", b"5"], r.lrange("sortdst", 0, -1))
    test("SORT STORE overwrites", 5, r.sort("sortlist", desc=True, store="sortdst"))
    test("SORT STORE overwrite value", [b"5", b"4", b"3", b"2", b"1"], r.lrange("sortdst", 0, -1))

    # SORT STORE empty result
    r.delete("sort_empty_src", "sort_empty_dst")
    r.rpush("sort_empty_src", 1)
    r.sort("sort_empty_src", store="sort_empty_dst")
    r.delete("sort_empty_src")
    empty_count = r.execute_command("SORT", "sort_empty_src", "STORE", "sort_empty_dst")
    test("SORT STORE empty clears dst", 0, empty_count)
    test("SORT STORE empty key gone", 0, r.exists("sort_empty_dst"))

    # ── OBJECT ENCODING comprehensive ────────────────────────────────────────
    section("OBJECT ENCODING")

    r.flushall()
    # String encodings
    r.set("enc_int", "12345")
    r.set("enc_embstr", "short")
    r.set("enc_raw", "x" * 45)
    test("OBJECT ENCODING int", b"int", r.execute_command("OBJECT", "ENCODING", "enc_int"))
    test("OBJECT ENCODING embstr", b"embstr", r.execute_command("OBJECT", "ENCODING", "enc_embstr"))
    test("OBJECT ENCODING raw", b"raw", r.execute_command("OBJECT", "ENCODING", "enc_raw"))

    # Hash encodings
    r.hset("enc_hash_small", "f", "v")
    test("OBJECT ENCODING hash small (listpack)", b"listpack", r.execute_command("OBJECT", "ENCODING", "enc_hash_small"))

    # List encodings
    r.rpush("enc_list_small", *range(10))
    test("OBJECT ENCODING list small (listpack)", b"listpack", r.execute_command("OBJECT", "ENCODING", "enc_list_small"))

    # Set encodings
    r.sadd("enc_set_int", 1, 2, 3)
    r.sadd("enc_set_str", "a", "b", "c")
    test("OBJECT ENCODING set int (intset)", b"intset", r.execute_command("OBJECT", "ENCODING", "enc_set_int"))
    test("OBJECT ENCODING set str (listpack)", b"listpack", r.execute_command("OBJECT", "ENCODING", "enc_set_str"))

    # ZSet encodings
    r.zadd("enc_zset_small", {"a": 1.0, "b": 2.0})
    test("OBJECT ENCODING zset small (listpack)", b"listpack", r.execute_command("OBJECT", "ENCODING", "enc_zset_small"))

    # Large encodings - force encoding upgrade
    for i in range(200):
        r.sadd("enc_set_large", i)  # integers but over intset threshold (default 512? Our server uses 512)
    for i in range(200):
        r.sadd("enc_set_large_str", f"str{i}")
    test("OBJECT ENCODING set large str (hashtable)", b"hashtable", r.execute_command("OBJECT", "ENCODING", "enc_set_large_str"))

    # ── Dragonfly-specific ────────────────────────────────────────────────────
    section("Dragonfly-specific Commands")

    test("DFLY GETVER", True, r.execute_command("DFLY", "GETVER") is not None)
    r.set("dfly_test_key", "12345")
    test("OBJECT VERSION", True, r.execute_command("OBJECT", "VERSION", "dfly_test_key") is not None)
    test("OBJECT SET-ENCODING", True, r.execute_command("OBJECT", "SET-ENCODING", "dfly_test_key", "raw") in [b"OK", True])

    # OBJECT VERSION increments on each write (regression)
    r.delete("ver_test")
    r.set("ver_test", "v1")
    v1 = r.execute_command("OBJECT", "VERSION", "ver_test")
    r.set("ver_test", "v2")
    v2 = r.execute_command("OBJECT", "VERSION", "ver_test")
    r.set("ver_test", "v3")
    v3 = r.execute_command("OBJECT", "VERSION", "ver_test")
    test("OBJECT VERSION monotonic increase", True, v1 < v2 < v3)
    test("OBJECT VERSION missing key", None, r.execute_command("OBJECT", "VERSION", "nonexistent_xyz_abc"))

    # ── FT.* (RediSearch) stubs ───────────────────────────────────────────────
    section("RediSearch (FT.*) Stubs")

    test("FT.CREATE", True, r.execute_command("FT.CREATE", "idx1", "ON", "HASH", "SCHEMA", "name", "TEXT") == b"OK")
    test("FT.LIST", True, isinstance(r.execute_command("FT.LIST"), list))
    ft_info = r.execute_command("FT.INFO", "idx1")
    test("FT.INFO", True, ft_info is not None)
    test("FT.DROPINDEX", True, r.execute_command("FT.DROPINDEX", "idx1") == b"OK")

    # ── CMS / TopK / TDigest ─────────────────────────────────────────────────
    section("Probabilistic Commands")

    test("CMS.INITBYDIM", True, r.execute_command("CMS.INITBYDIM", "cms1", "10", "5") == b"OK")
    test("CMS.INCRBY", True, r.execute_command("CMS.INCRBY", "cms1", "item1", "5") is not None)
    test("CMS.QUERY", True, r.execute_command("CMS.QUERY", "cms1", "item1") is not None)
    test("CMS.INFO", True, r.execute_command("CMS.INFO", "cms1") is not None)

    test("TOPK.RESERVE", True, r.execute_command("TOPK.RESERVE", "topk1", "3") == b"OK")
    test("TOPK.ADD", True, r.execute_command("TOPK.ADD", "topk1", "a", "b", "a", "c", "a") is not None)
    test("TOPK.LIST", True, r.execute_command("TOPK.LIST", "topk1") is not None)
    test("TOPK.INFO", True, r.execute_command("TOPK.INFO", "topk1") is not None)

    test("TDIGEST.CREATE", True, r.execute_command("TDIGEST.CREATE", "td1") == b"OK")
    test("TDIGEST.ADD", True, r.execute_command("TDIGEST.ADD", "td1", "1.0", "2.0", "3.0", "4.0", "5.0") == b"OK")
    test("TDIGEST.QUANTILE", True, r.execute_command("TDIGEST.QUANTILE", "td1", "0.5") is not None)
    test("TDIGEST.INFO", True, r.execute_command("TDIGEST.INFO", "td1") is not None)

    # ── CL.THROTTLE ──────────────────────────────────────────────────────────
    section("Rate Limiter (CL.THROTTLE)")

    result = r.execute_command("CL.THROTTLE", "throttle1", "100", "10", "60")
    test("CL.THROTTLE", True, result is not None and len(result) >= 3)

    # ── DUMP/RESTORE ─────────────────────────────────────────────────────────
    section("DUMP/RESTORE")

    r.set("dump_src", "hello world")
    dump_data = r.dump("dump_src")
    test("DUMP not None", True, dump_data is not None)

    # ── SCAN with TYPE filter ────────────────────────────────────────────────
    section("SCAN with TYPE filter")

    r.flushall()
    r.set("str_key", "v")
    r.hset("hash_key", "f", "v")
    r.rpush("list_key", "v")
    r.sadd("set_key", "v")
    r.zadd("zset_key", {"v": 1.0})
    r.xadd("stream_key2", {"f": "v"})

    cursor, str_keys = r.scan(0, match="*", count=100, _type="string")
    test("SCAN TYPE string", True, b"str_key" in str_keys)
    cursor, hash_keys = r.scan(0, match="*", count=100, _type="hash")
    test("SCAN TYPE hash", True, b"hash_key" in hash_keys)
    cursor, list_keys = r.scan(0, match="*", count=100, _type="list")
    test("SCAN TYPE list", True, b"list_key" in list_keys)
    cursor, set_keys = r.scan(0, match="*", count=100, _type="set")
    test("SCAN TYPE set", True, b"set_key" in set_keys)
    cursor, zset_keys = r.scan(0, match="*", count=100, _type="zset")
    test("SCAN TYPE zset", True, b"zset_key" in zset_keys)

    # ── Bitfield ─────────────────────────────────────────────────────────────
    section("BITFIELD")

    r.delete("bf_test")
    result = r.execute_command("BITFIELD", "bf_test", "SET", "u8", "0", "255")
    test("BITFIELD SET", [0], result)
    result = r.execute_command("BITFIELD", "bf_test", "GET", "u8", "0")
    test("BITFIELD GET", [255], result)
    result = r.execute_command("BITFIELD", "bf_test", "INCRBY", "u8", "0", "10")
    test("BITFIELD INCRBY overflow wrap", [9], result)  # 255+10 = 265, wraps to 9

    # ── Keyspace Notifications ────────────────────────────────────────────────
    section("Keyspace Notifications Config")
    test("CONFIG SET notify-keyspace-events", True,
         r.config_set("notify-keyspace-events", "KEA") in [True, b"OK"])
    test("CONFIG GET notify-keyspace-events", True,
         "notify-keyspace-events" in r.config_get("notify-keyspace-events"))
    # Reset
    r.config_set("notify-keyspace-events", "")

    # ── REPLICAOF / CLUSTER stubs ─────────────────────────────────────────────
    section("Replication / Cluster Stubs")
    test("REPLICAOF NO ONE", True, r.execute_command("REPLICAOF", "NO", "ONE") == b"OK")
    test("CLUSTER INFO", True, r.execute_command("CLUSTER", "INFO") is not None)

    # Final cleanup
    r.flushall()

def print_summary():
    print(f"\n{'='*60}")
    print(f"  TEST SUMMARY")
    print(f"{'='*60}")
    print(f"  Total:  {TOTAL_COUNT}")
    print(f"  PASS:   {PASS_COUNT}  ({100*PASS_COUNT//max(TOTAL_COUNT,1)}%)")
    print(f"  FAIL:   {FAIL_COUNT}")
    print(f"  ERROR:  {ERROR_COUNT}")
    print()

    if FAIL_COUNT > 0:
        print("FAILED TESTS:")
        for status, name, expected, actual in RESULTS:
            if status == "FAIL":
                print(f"  - {name}")
                print(f"      expected: {expected!r}")
                print(f"      actual:   {actual!r}")

    print()
    print(f"{'='*60}")

    # Checklist
    print("\nFEATURE CHECKLIST:")
    features = [
        ("String commands (SET/GET/INCR/etc)", ["set basic", "get", "incr", "append", "strlen", "mset", "mget", "setnx", "setex", "getset", "getex", "substr", "lcs", "bitcount", "bitop", "getbit", "setbit", "bitpos", "bitfield"]),
        ("Key commands (DEL/EXISTS/EXPIRE/etc)", ["del", "exists", "expire", "ttl", "persist", "type string", "rename", "keys", "scan", "touch", "randomkey", "dump", "copy", "object encoding", "object refcount", "object idle"]),
        ("Hash commands (HSET/HGET/etc)", ["hset", "hget", "hmset", "hmget", "hgetall", "hkeys", "hvals", "hlen", "hexists", "hdel", "hincrby", "hrandfield", "hscan", "hexpire"]),
        ("List commands (LPUSH/LRANGE/etc)", ["lpush", "rpush", "lrange", "llen", "lpop", "rpop", "lindex", "lset", "linsert", "lrem", "ltrim", "lpos", "lmove", "lmpop", "blpop"]),
        ("Set commands (SADD/SMEMBERS/etc)", ["sadd", "smembers", "scard", "sismember", "smismember", "srandmember", "srem", "sunion", "sinter", "sdiff", "sscan", "sintercard", "spop"]),
        ("Sorted set commands (ZADD/ZRANGE/etc)", ["zadd", "zscore", "zrank", "zrevrank", "zcard", "zincrby", "zrange", "zrevrange", "zrangebyscore", "zrem", "zpopmin", "zpopmax", "zmscore", "zunionstore", "zinterstore", "zdiff", "zrandmember", "zscan", "zintercard", "zmpop"]),
        ("Stream commands (XADD/XREAD/etc)", ["xadd", "xlen", "xrange", "xrevrange", "xread", "xinfo", "xgroup", "xreadgroup", "xack", "xdel", "xtrim", "xpending"]),
        ("Geo commands (GEOADD/GEODIST/etc)", ["geoadd", "geodist", "geopos", "geohash", "geosearch"]),
        ("HyperLogLog (PFADD/PFCOUNT)", ["pfadd", "pfcount", "pfmerge"]),
        ("Bloom Filter (BF.*)", ["bf.add", "bf.madd", "bf.exists", "bf.mexists", "bf.reserve", "bf.info", "bf.card"]),
        ("JSON (JSON.*)", ["json.set", "json.get", "json.del", "json.type", "json.numincrby", "json.arrappend"]),
        ("Pub/Sub", ["pubsub", "publish"]),
        ("Transactions (MULTI/EXEC)", ["multi/exec", "discard without"]),
        ("Dragonfly extensions (DFLY/OBJECT VERSION)", ["dfly", "object version", "object set-encoding"]),
        ("RediSearch stubs (FT.*)", ["ft.create", "ft.list", "ft.info", "ft.dropindex", "ft.search"]),
        ("Probabilistic (CMS/TopK/TDigest)", ["cms.", "topk.", "tdigest.", "cl.throttle"]),
    ]

    pass_features = set()
    fail_features = set()

    for status, name, expected, actual in RESULTS:
        name_lower = name.lower()
        for feature_name, keywords in features:
            if any(kw in name_lower for kw in keywords):
                if status == "PASS":
                    pass_features.add(feature_name)
                else:
                    fail_features.add(feature_name)

    for feature_name, _ in features:
        if feature_name in fail_features and feature_name not in pass_features:
            mark = "[PARTIAL]"
        elif feature_name in fail_features:
            mark = "[PARTIAL]"
        elif feature_name in pass_features:
            mark = "[OK]"
        else:
            mark = "[UNKNOWN]"
        print(f"  {mark:<12} {feature_name}")

    return FAIL_COUNT == 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ForgeKV comprehensive test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    print(f"\nConnecting to Redis at {args.host}:{args.port}...")

    try:
        r = redis.Redis(host=args.host, port=args.port, socket_timeout=5)
        r.ping()
        print("Connected successfully.\n")
    except Exception as e:
        print(f"ERROR: Cannot connect to Redis at {args.host}:{args.port}: {e}")
        sys.exit(1)

    try:
        run_tests(r)
    except Exception as e:
        print(f"\nFATAL ERROR during tests: {e}")
        traceback.print_exc()

    success = print_summary()
    sys.exit(0 if success else 1)
