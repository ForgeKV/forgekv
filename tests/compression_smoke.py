# End-to-end smoke test of value compression over RESP (needs: pip install redis). Not collected by pytest.
import os, subprocess, time, shutil, socket, redis, json, random
BIN=os.environ.get("FORGEKV_BIN","./rust/target/release/forgekv")  # usage: FORGEKV_BIN=/path/to/forgekv python tests/compression_smoke.py
d="/tmp/forgekv-compression-smoke"; shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
s=socket.socket(); s.bind(("127.0.0.1",0)); port=s.getsockname()[1]; s.close()
def start(mode):
    open(d+"/f.conf","w").write(f"bind 127.0.0.1\nport {port}\ndir {d}/data\ndatabases 2\nmemtable-size-mb 256\nvalue-compression {mode}\n")
    p=subprocess.Popen([BIN,"--config",d+"/f.conf"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            r=redis.Redis(port=port, protocol=2); r.ping(); return p,r
        except Exception as e:
            err=e; time.sleep(0.1)
    raise SystemExit("no start: "+repr(err))
def stop(p):
    p.terminate(); p.wait(15)
def doc(i): return json.dumps({"i":i,"name":"Example "*200,"r":[random.random() for _ in range(50)]}).encode()
random.seed(1)
vals={f"k{i}":doc(i) for i in range(300)}
vals["magic"]=b"\xffZK\x02abcdefgh"
vals["small"]=b"hi"
p,r=start("off")
for k,v in vals.items(): r.set(k,v)
r.setex("ttl",3600,vals["k1"]); vals["ttl"]=vals["k1"]
r.hset("h","f","v")
print("cfg:",r.config_get("value-compression*"))
try: r.config_set("value-compression","lz4"); print("BAD: accepted")
except redis.ResponseError as e: print("lz4 rejected:",e)
try: r.execute_command("FORGEKV.COMPRESS","RATE","0"); print("BAD: allowed with flag off")
except redis.ResponseError as e: print("compress w/ flag off rejected:",e)
print(r.execute_command("FORGEKV.COMPRESS","DRYRUN","RATE","0"))
time.sleep(1)
st=dict(zip(*[iter([x.decode() for x in r.execute_command("FORGEKV.COMPRESS","STATUS")])]*2))
print("dryrun:",{k:st[k] for k in ("state","keys_examined","keys_converted","bytes_before","bytes_after","ratio","compressed_keys","raw_keys")})
r.config_set("value-compression","zstd")
print(r.execute_command("FORGEKV.COMPRESS","RATE","0"))
time.sleep(1)
st=dict(zip(*[iter([x.decode() for x in r.execute_command("FORGEKV.COMPRESS","STATUS")])]*2))
print("compress:",{k:st[k] for k in ("state","keys_examined","keys_converted","keys_error","bytes_before","bytes_after","ratio")})
for k,v in vals.items(): assert r.get(k)==v,k
assert r.strlen("k5")==len(vals["k5"]); assert r.getrange("k5",2,10)==vals["k5"][2:11]
assert r.append("k5",b"XX")==len(vals["k5"])+2; vals["k5"]+=b"XX"
assert r.get("k5")==vals["k5"]
assert 3000<r.ttl("ttl")<=3600
assert r.dump("k7") is not None
info=r.info("compression"); print("info keys:",{k:info[k] for k in ("value_compression","compression_writes_compressed","compression_reads_decompressed","compression_job_state","compression_job_ratio")})
print("dbsize",r.dbsize(), "type h",r.type("h"))
stop(p)
# restart with flag OFF: still readable
p,r=start("off")
for k,v in vals.items(): assert r.get(k)==v,k
print("after restart w/ flag off: all",len(vals),"values equal")
print(r.execute_command("FORGEKV.DECOMPRESS","RATE","0")); time.sleep(1)
st=dict(zip(*[iter([x.decode() for x in r.execute_command("FORGEKV.DECOMPRESS","STATUS")])]*2))
print("decompress:",{k:st[k] for k in ("state","keys_converted","keys_error")})
for k,v in vals.items(): assert r.get(k)==v,k
print("OK")
stop(p)
