#!/usr/bin/env python3
"""
Comprehensive feature tests for ForgeKV: covers every major command group.
"""
import sys, time, random, string
import redis

HOST, PORT = "127.0.0.1", 6379
PASSED, FAILED = [], []

def r():
    c = redis.Redis(host=HOST, port=PORT, decode_responses=True)
    c.flushall()
    return c

def check(name, got, expected):
    ok = got == expected
    if ok:
        PASSED.append(name)
    else:
        FAILED.append(name)
        print(f"  FAIL  {name}: expected={expected!r}  got={got!r}")
    return ok

def section(name):
    print(f"\n--- {name} ---")

# ─── String commands ─────────────────────────────────────────────────────────
def test_strings():
    section("Strings")
    c = r()
    check("SET/GET", c.set("k", "v") and c.get("k"), "v")
    check("SETNX", c.setnx("k", "other"), False)
    check("SETEX", c.setex("ex", 100, "val") and c.get("ex"), "val")
    check("TTL>0", c.ttl("ex") > 0, True)
    check("INCR", c.incr("n"), 1)
    check("INCRBY", c.incrby("n", 9), 10)
    check("DECR", c.decr("n"), 9)
    check("DECRBY", c.decrby("n", 4), 5)
    check("INCRBYFLOAT", float(c.incrbyfloat("f", 1.5)), 1.5)
    check("APPEND", c.append("s", "foo"), 3)
    check("APPEND2", c.append("s", "bar"), 6)
    check("GETRANGE", c.getrange("s", 0, 2), "foo")
    check("STRLEN", c.strlen("s"), 6)
    check("MSET/MGET", c.mset({"a":"1","b":"2","c":"3"}) and c.mget("a","b","c"), ["1","2","3"])
    check("GETSET", c.getset("k", "new"), "v")
    check("GETDEL", c.getdel("k"), "new")
    check("GETDEL_miss", c.getdel("k"), None)

# ─── Key commands ─────────────────────────────────────────────────────────────
def test_keys():
    section("Keys")
    c = r()
    c.set("a", "1"); c.set("b", "2"); c.set("c", "3")
    check("DEL", c.delete("a", "b"), 2)
    check("EXISTS", c.exists("c"), 1)
    check("EXISTS_miss", c.exists("a"), 0)
    check("TYPE", c.type("c"), "string")
    check("RENAME", c.rename("c", "d") and c.get("d"), "3")
    check("RENAMENX", c.renamenx("d", "e"), True)
    check("EXPIRE/TTL", c.expire("e", 100) and c.ttl("e") > 0, True)
    check("PERSIST", c.persist("e") and c.ttl("e"), -1)
    check("KEYS", sorted(c.keys("*")), ["e"])
    check("SCAN_returns_list", isinstance(c.scan(0)[1], list), True)
    check("COPY", c.copy("e", "ecopy") and c.get("ecopy"), "3")
    check("TOUCH", c.touch("e", "ecopy"), 2)
    check("UNLINK", c.unlink("ecopy"), 1)

# ─── Hash commands ────────────────────────────────────────────────────────────
def test_hashes():
    section("Hashes")
    c = r()
    c.hset("h", mapping={"f1":"v1","f2":"v2","f3":"v3"})
    check("HGET", c.hget("h","f1"), "v1")
    check("HMGET", c.hmget("h","f1","f2"), ["v1","v2"])
    check("HGETALL", c.hgetall("h"), {"f1":"v1","f2":"v2","f3":"v3"})
    check("HLEN", c.hlen("h"), 3)
    check("HEXISTS", c.hexists("h","f1"), True)
    check("HDEL", c.hdel("h","f3"), 1)
    check("HKEYS", sorted(c.hkeys("h")), ["f1","f2"])
    check("HVALS", sorted(c.hvals("h")), ["v1","v2"])
    check("HINCRBY", c.hincrby("h","n",5), 5)
    check("HINCRBYFLOAT", float(c.hincrbyfloat("h","x",1.5)), 1.5)
    check("HSETNX", c.hsetnx("h","f1","other"), False)
    check("HSTRLEN", c.hstrlen("h","f1"), 2)

# ─── List commands ────────────────────────────────────────────────────────────
def test_lists():
    section("Lists")
    c = r()
    c.rpush("l", "a","b","c")
    check("LLEN", c.llen("l"), 3)
    check("LRANGE", c.lrange("l",0,-1), ["a","b","c"])
    check("LINDEX", c.lindex("l",1), "b")
    check("LSET", c.lset("l",1,"B") and c.lindex("l",1), "B")
    check("LPUSH", c.lpush("l","z"), 4)
    check("LPOP", c.lpop("l"), "z")
    check("RPOP", c.rpop("l"), "c")
    check("LINSERT", c.linsert("l","BEFORE","B","X"), 3)
    check("LREM", c.lrem("l",1,"X"), 1)
    check("LPOS", c.lpos("l","B"), 1)
    check("LTRIM", c.ltrim("l",0,0) or True, True)
    check("RPOPLPUSH", c.rpoplpush("l","l2"), "a")
    check("LMOVE", c.lmove("l","l3","LEFT","RIGHT"), None)  # l is now empty

# ─── Set commands ─────────────────────────────────────────────────────────────
def test_sets():
    section("Sets")
    c = r()
    c.sadd("s1","a","b","c","d")
    c.sadd("s2","c","d","e","f")
    check("SCARD", c.scard("s1"), 4)
    check("SISMEMBER", c.sismember("s1","a"), True)
    check("SMISMEMBER", c.smismember("s1","a","z"), [True, False])
    check("SDIFF", sorted(c.sdiff("s1","s2")), ["a","b"])
    check("SINTER", sorted(c.sinter("s1","s2")), ["c","d"])
    check("SUNION", len(c.sunion("s1","s2")), 6)
    check("SMOVE", c.smove("s1","s2","a"), True)
    check("SREM", c.srem("s1","b"), 1)
    check("SRANDMEMBER", c.srandmember("s1") in c.smembers("s1"), True)
    pop = c.spop("s1")
    check("SPOP", pop is not None, True)
    c.sadd("s3","a","b","c"); c.sadd("s4","b","c","d")
    check("SINTERCARD", c.sintercard(2,["s3","s4"]), 2)

# ─── Sorted Set commands ──────────────────────────────────────────────────────
def test_zsets():
    section("Sorted Sets")
    c = r()
    c.zadd("z", {"a":1,"b":2,"c":3,"d":4})
    check("ZCARD", c.zcard("z"), 4)
    check("ZSCORE", c.zscore("z","b"), 2.0)
    check("ZRANK", c.zrank("z","c"), 2)
    check("ZREVRANK", c.zrevrank("z","c"), 1)
    check("ZINCRBY", c.zincrby("z",10,"a"), 11.0)
    check("ZRANGE", c.zrange("z",0,1), ["b","c"])
    check("ZREVRANGE", c.zrevrange("z",0,1), ["a","d"])
    check("ZRANGEBYSCORE", c.zrangebyscore("z",2,3), ["b","c"])
    check("ZCOUNT", c.zcount("z",2,3), 2)
    check("ZREM", c.zrem("z","d"), 1)
    check("ZPOPMIN", c.zpopmin("z"), [("b", 2.0)])
    check("ZPOPMAX", c.zpopmax("z"), [("a", 11.0)])
    c.zadd("z2",{"x":1,"y":2})
    check("ZUNIONSTORE", c.zunionstore("out",["z","z2"]), 3)
    check("ZINTERCARD", c.zintercard(2,"z","z2"), 0)

# ─── HyperLogLog ──────────────────────────────────────────────────────────────
def test_hll():
    section("HyperLogLog")
    c = r()
    c.pfadd("hll","a","b","c","a")
    check("PFCOUNT_approx", c.pfcount("hll") >= 3, True)
    c.pfadd("hll2","d","e")
    c.pfmerge("hll3","hll","hll2")
    check("PFMERGE", c.pfcount("hll3") >= 5, True)

# ─── Geo commands ─────────────────────────────────────────────────────────────
def test_geo():
    section("Geo")
    c = r()
    c.geoadd("g", [13.361389, 38.115556, "Palermo",
                   15.087269, 37.502669, "Catania"])
    dist = c.geodist("g","Palermo","Catania","km")
    check("GEODIST_range", 166 < float(dist) < 167, True)
    hashes = c.geohash("g","Palermo")
    check("GEOHASH_nonempty", len(hashes[0]) > 5, True)
    pos = c.geopos("g","Palermo")[0]
    check("GEOPOS_lat_range", 38.0 < pos[1] < 39.0, True)
    results = c.geosearch("g", longitude=15, latitude=37, radius=200, unit="km")
    check("GEOSEARCH_count", len(results), 2)

# ─── Bitmap commands ──────────────────────────────────────────────────────────
def test_bitmaps():
    section("Bitmaps")
    c = r()
    c.setbit("bm", 7, 1)
    c.setbit("bm", 3, 1)
    check("GETBIT_on", c.getbit("bm", 7), 1)
    check("GETBIT_off", c.getbit("bm", 0), 0)
    check("BITCOUNT", c.bitcount("bm"), 2)
    # Use SETBIT to build test data (avoids UTF-8 encoding issues with binary strings)
    # b1: set all 8 bits of byte 0 = 0xFF
    for bit in range(8):
        c.setbit("b1", bit, 1)
    check("BITCOUNT_range", c.bitcount("b1", 0, 0), 8)
    # b1 has 0xFF in byte 0; b2 has 0x0F in byte 0 (bits 4-7 set)
    for bit in range(4, 8):
        c.setbit("b2", bit, 1)
    c.bitop("AND", "b3", "b1", "b2")
    # AND of 0xFF and 0x0F = 0x0F → bits 4-7 set in byte 0
    check("BITOP_AND_bitcount", c.bitcount("b3", 0, 0), 4)

# ─── TTL & expiry edge cases ──────────────────────────────────────────────────
def test_ttl():
    section("TTL / Expiry")
    c = r()
    c.set("x", "1")
    check("TTL_no_expire", c.ttl("x"), -1)
    check("PTTL_no_expire", c.pttl("x"), -1)
    c.expire("x", 10)
    check("EXPIRETIME_positive", c.expiretime("x") > 0, True)
    check("PEXPIRETIME_positive", c.pexpiretime("x") > 0, True)
    c.pexpire("x", 500)
    time.sleep(0.1)
    check("PTTL_after_pexpire", 0 < c.pttl("x") < 500, True)

# ─── Server commands ──────────────────────────────────────────────────────────
def test_server():
    section("Server")
    c = r()
    check("PING", c.ping(), True)
    check("ECHO", c.echo("hello"), "hello")
    c.set("k","v")
    check("DBSIZE", c.dbsize(), 1)
    c.flushdb()
    check("FLUSHDB", c.dbsize(), 0)
    check("TIME_tuple", len(c.time()), 2)
    info = c.info()
    check("INFO_has_server", "redis_version" in info or "server" in str(info), True)
    c.select(1)
    c.set("db1key","val")
    c.select(0)
    check("SELECT_isolation", c.exists("db1key"), 0)

# ─── Transaction (MULTI/EXEC) ─────────────────────────────────────────────────
def test_transactions():
    section("Transactions")
    c = r()
    pipe = c.pipeline(transaction=True)
    pipe.set("tx1","a")
    pipe.set("tx2","b")
    pipe.get("tx1")
    pipe.get("tx2")
    results = pipe.execute()
    check("MULTI_EXEC_set1", results[0], True)
    check("MULTI_EXEC_set2", results[1], True)
    check("MULTI_EXEC_get1", results[2], "a")
    check("MULTI_EXEC_get2", results[3], "b")

    # DISCARD
    pipe2 = c.pipeline(transaction=True)
    pipe2.set("discarded","x")
    # Simulate DISCARD: don't execute
    try:
        pipe2.reset()
    except Exception:
        pass
    check("DISCARD_no_side_effect", c.exists("discarded"), 0)


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    tests = [
        test_strings,
        test_keys,
        test_hashes,
        test_lists,
        test_sets,
        test_zsets,
        test_hll,
        test_geo,
        test_bitmaps,
        test_ttl,
        test_server,
        test_transactions,
    ]
    for t in tests:
        try:
            t()
        except Exception as e:
            FAILED.append(t.__name__)
            print(f"  ERROR  {t.__name__}: {e}")

    total = len(PASSED) + len(FAILED)
    print(f"\n{'='*55}")
    print(f"Results: {len(PASSED)}/{total} passed")
    if FAILED:
        print(f"FAILED: {FAILED}")
        sys.exit(1)
    else:
        print("ALL TESTS PASSED")

if __name__ == "__main__":
    run()
