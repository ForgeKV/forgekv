/// Cuckoo Filter commands — compatible with RedisBloom / Dragonfly.
///
/// Commands: CF.RESERVE CF.ADD CF.ADDNX CF.INSERT CF.INSERTNX
///           CF.EXISTS CF.MEXISTS CF.DEL CF.COUNT CF.COMPACT
///           CF.INFO CF.SCANDUMP CF.LOADCHUNK
///
/// A Cuckoo filter supports deletion (unlike Bloom filters) at the cost of
/// slightly higher false-positive rate. This implementation uses a simplified
/// 2-way cuckoo scheme with 4-slot buckets and 8-bit fingerprints.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::resp::RespValue;
use super::CommandHandler;

// ── Cuckoo Filter ─────────────────────────────────────────────────────────────

const BUCKET_SIZE: usize = 4;
const MAX_KICKS: usize = 500;

#[derive(Clone)]
struct CuckooFilter {
    buckets:   Vec<[u8; BUCKET_SIZE]>,  // fingerprints (0 = empty)
    num_items: usize,
    capacity:  usize, // total slots = buckets.len() * BUCKET_SIZE
}

impl CuckooFilter {
    fn new(capacity: usize) -> Self {
        let num_buckets = ((capacity + BUCKET_SIZE - 1) / BUCKET_SIZE).next_power_of_two().max(1);
        CuckooFilter {
            buckets: vec![[0u8; BUCKET_SIZE]; num_buckets],
            num_items: 0,
            capacity: num_buckets * BUCKET_SIZE,
        }
    }

    fn fingerprint(key: &[u8]) -> u8 {
        let h = fnv1a(key);
        let fp = (h & 0xFF) as u8;
        if fp == 0 { 1 } else { fp }
    }

    fn index1(key: &[u8], num_buckets: usize) -> usize {
        (fnv1a(key) as usize) % num_buckets
    }

    fn index2(idx1: usize, fp: u8, num_buckets: usize) -> usize {
        let h = fnv1a(&[fp]);
        (idx1 ^ (h as usize)) % num_buckets
    }

    fn find_slot(bucket: &[u8; BUCKET_SIZE]) -> Option<usize> {
        bucket.iter().position(|&s| s == 0)
    }

    fn remove_from_bucket(bucket: &mut [u8; BUCKET_SIZE], fp: u8) -> bool {
        for slot in bucket.iter_mut() {
            if *slot == fp {
                *slot = 0;
                return true;
            }
        }
        false
    }

    fn contains_in_bucket(bucket: &[u8; BUCKET_SIZE], fp: u8) -> bool {
        bucket.contains(&fp)
    }

    fn add(&mut self, key: &[u8]) -> bool {
        let fp = Self::fingerprint(key);
        let nb = self.buckets.len();
        let i1 = Self::index1(key, nb);
        let i2 = Self::index2(i1, fp, nb);

        // Try i1
        if let Some(slot) = Self::find_slot(&self.buckets[i1]) {
            self.buckets[i1][slot] = fp;
            self.num_items += 1;
            return true;
        }
        // Try i2
        if let Some(slot) = Self::find_slot(&self.buckets[i2]) {
            self.buckets[i2][slot] = fp;
            self.num_items += 1;
            return true;
        }
        // Kick
        let mut cur_idx = i1;
        let mut cur_fp = fp;
        for _ in 0..MAX_KICKS {
            // Pick a random slot to evict (use slot 0 for determinism)
            let evicted_fp = self.buckets[cur_idx][0];
            self.buckets[cur_idx][0] = cur_fp;
            cur_fp = evicted_fp;
            cur_idx = Self::index2(cur_idx, cur_fp, nb);
            if let Some(slot) = Self::find_slot(&self.buckets[cur_idx]) {
                self.buckets[cur_idx][slot] = cur_fp;
                self.num_items += 1;
                return true;
            }
        }
        false // filter is full
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        let fp = Self::fingerprint(key);
        let nb = self.buckets.len();
        let i1 = Self::index1(key, nb);
        let i2 = Self::index2(i1, fp, nb);
        Self::contains_in_bucket(&self.buckets[i1], fp)
            || Self::contains_in_bucket(&self.buckets[i2], fp)
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        let fp = Self::fingerprint(key);
        let nb = self.buckets.len();
        let i1 = Self::index1(key, nb);
        let i2 = Self::index2(i1, fp, nb);
        if Self::remove_from_bucket(&mut self.buckets[i1], fp) {
            self.num_items -= 1;
            return true;
        }
        if Self::remove_from_bucket(&mut self.buckets[i2], fp) {
            self.num_items -= 1;
            return true;
        }
        false
    }

    fn count(&self, key: &[u8]) -> usize {
        let fp = Self::fingerprint(key);
        let nb = self.buckets.len();
        let i1 = Self::index1(key, nb);
        let i2 = Self::index2(i1, fp, nb);
        let c1 = self.buckets[i1].iter().filter(|&&s| s == fp).count();
        let c2 = self.buckets[i2].iter().filter(|&&s| s == fp).count();
        c1 + c2
    }

    fn load_factor(&self) -> f64 {
        self.num_items as f64 / self.capacity as f64
    }
}

fn fnv1a(data: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

lazy_static::lazy_static! {
    static ref CF_STORE: Mutex<HashMap<Vec<u8>, CuckooFilter>> = Mutex::new(HashMap::new());
}

// ── CF.RESERVE ────────────────────────────────────────────────────────────────

pub struct CfReserveCommand;
impl CommandHandler for CfReserveCommand {
    fn name(&self) -> &str { "CF.RESERVE" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.RESERVE'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let capacity: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) if n > 0 => n,
            _ => return RespValue::error("ERR capacity must be a positive integer"),
        };
        if CF_STORE.lock().contains_key(&key) {
            return RespValue::error("ERR item exists");
        }
        CF_STORE.lock().insert(key, CuckooFilter::new(capacity));
        RespValue::ok()
    }
}

// ── CF.ADD ────────────────────────────────────────────────────────────────────

pub struct CfAddCommand;
impl CommandHandler for CfAddCommand {
    fn name(&self) -> &str { "CF.ADD" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.ADD'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let item = match args[2].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR item must be a string") };
        let mut store = CF_STORE.lock();
        let cf = store.entry(key).or_insert_with(|| CuckooFilter::new(1024));
        if cf.add(&item) {
            RespValue::Integer(1)
        } else {
            RespValue::error("ERR filter is full")
        }
    }
}

// ── CF.ADDNX ─────────────────────────────────────────────────────────────────

pub struct CfAddNxCommand;
impl CommandHandler for CfAddNxCommand {
    fn name(&self) -> &str { "CF.ADDNX" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.ADDNX'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let item = match args[2].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR item must be a string") };
        let mut store = CF_STORE.lock();
        let cf = store.entry(key).or_insert_with(|| CuckooFilter::new(1024));
        if cf.may_contain(&item) {
            return RespValue::Integer(0); // already exists (maybe)
        }
        if cf.add(&item) {
            RespValue::Integer(1)
        } else {
            RespValue::error("ERR filter is full")
        }
    }
}

// ── CF.INSERT / CF.INSERTNX ───────────────────────────────────────────────────

pub struct CfInsertCommand { pub nx: bool }
impl CommandHandler for CfInsertCommand {
    fn name(&self) -> &str { if self.nx { "CF.INSERTNX" } else { "CF.INSERT" } }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // CF.INSERT key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let mut capacity = 1024usize;
        let mut nocreate = false;
        let mut items_start = 2;

        let mut i = 2;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("CAPACITY") => { i += 1; capacity = args.get(i).and_then(|a| a.as_str()).and_then(|s| s.parse().ok()).unwrap_or(1024); }
                Some("NOCREATE") => { nocreate = true; }
                Some("ITEMS") => { items_start = i + 1; break; }
                _ => {}
            }
            i += 1;
        }

        let mut store = CF_STORE.lock();
        if nocreate && !store.contains_key(&key) {
            return RespValue::error("ERR not found");
        }
        let cf = store.entry(key).or_insert_with(|| CuckooFilter::new(capacity));

        let results: Vec<RespValue> = args[items_start..].iter().map(|a| {
            if let Some(item) = a.as_bytes() {
                if self.nx && cf.may_contain(item) {
                    return RespValue::Integer(0);
                }
                if cf.add(item) { RespValue::Integer(1) } else { RespValue::Integer(-1) }
            } else {
                RespValue::Integer(-1)
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

// ── CF.EXISTS ─────────────────────────────────────────────────────────────────

pub struct CfExistsCommand;
impl CommandHandler for CfExistsCommand {
    fn name(&self) -> &str { "CF.EXISTS" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.EXISTS'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let item = match args[2].as_bytes() { Some(b) => b, None => return RespValue::error("ERR item must be a string") };
        let store = CF_STORE.lock();
        match store.get(&key) {
            Some(cf) => RespValue::Integer(if cf.may_contain(item) { 1 } else { 0 }),
            None => RespValue::Integer(0),
        }
    }
}

// ── CF.MEXISTS ────────────────────────────────────────────────────────────────

pub struct CfMExistsCommand;
impl CommandHandler for CfMExistsCommand {
    fn name(&self) -> &str { "CF.MEXISTS" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.MEXISTS'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = CF_STORE.lock();
        let cf = store.get(&key);
        let results: Vec<RespValue> = args[2..].iter().map(|a| {
            if let Some(item) = a.as_bytes() {
                RespValue::Integer(match cf { Some(f) => if f.may_contain(item) { 1 } else { 0 }, None => 0 })
            } else {
                RespValue::Integer(0)
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

// ── CF.DEL ────────────────────────────────────────────────────────────────────

pub struct CfDelCommand;
impl CommandHandler for CfDelCommand {
    fn name(&self) -> &str { "CF.DEL" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.DEL'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let item = match args[2].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR item must be a string") };
        let mut store = CF_STORE.lock();
        match store.get_mut(&key) {
            Some(cf) => RespValue::Integer(if cf.delete(&item) { 1 } else { 0 }),
            None => RespValue::error("ERR not found"),
        }
    }
}

// ── CF.COUNT ──────────────────────────────────────────────────────────────────

pub struct CfCountCommand;
impl CommandHandler for CfCountCommand {
    fn name(&self) -> &str { "CF.COUNT" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.COUNT'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let item = match args[2].as_bytes() { Some(b) => b, None => return RespValue::error("ERR item must be a string") };
        let store = CF_STORE.lock();
        match store.get(&key) {
            Some(cf) => RespValue::Integer(cf.count(item) as i64),
            None => RespValue::Integer(0),
        }
    }
}

// ── CF.INFO ───────────────────────────────────────────────────────────────────

pub struct CfInfoCommand;
impl CommandHandler for CfInfoCommand {
    fn name(&self) -> &str { "CF.INFO" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'CF.INFO'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = CF_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR not found"),
            Some(cf) => RespValue::Array(Some(vec![
                RespValue::bulk_str("Size"),
                RespValue::Integer((cf.buckets.len() * BUCKET_SIZE) as i64),
                RespValue::bulk_str("Number of buckets"),
                RespValue::Integer(cf.buckets.len() as i64),
                RespValue::bulk_str("Number of filters"),
                RespValue::Integer(1),
                RespValue::bulk_str("Number of items inserted"),
                RespValue::Integer(cf.num_items as i64),
                RespValue::bulk_str("Number of items deleted"),
                RespValue::Integer(0),
                RespValue::bulk_str("Bucket size"),
                RespValue::Integer(BUCKET_SIZE as i64),
                RespValue::bulk_str("Expansion rate"),
                RespValue::Integer(1),
                RespValue::bulk_str("Max iterations"),
                RespValue::Integer(MAX_KICKS as i64),
                RespValue::bulk_str("Load factor"),
                RespValue::bulk_str(&format!("{:.4}", cf.load_factor())),
            ])),
        }
    }
}

// ── CF.COMPACT ────────────────────────────────────────────────────────────────

pub struct CfCompactCommand;
impl CommandHandler for CfCompactCommand {
    fn name(&self) -> &str { "CF.COMPACT" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'CF.COMPACT'");
        }
        // No-op compaction for this implementation
        RespValue::ok()
    }
}

// ── CF.SCANDUMP / CF.LOADCHUNK ────────────────────────────────────────────────

pub struct CfScanDumpCommand;
impl CommandHandler for CfScanDumpCommand {
    fn name(&self) -> &str { "CF.SCANDUMP" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CF.SCANDUMP'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let iter: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
        let store = CF_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR not found"),
            Some(cf) => {
                if iter == 0 {
                    // Serialize: [num_buckets:8LE][num_items:8LE][buckets...]
                    let nb = cf.buckets.len() as u64;
                    let ni = cf.num_items as u64;
                    let mut data = Vec::with_capacity(16 + cf.buckets.len() * BUCKET_SIZE);
                    data.extend_from_slice(&nb.to_le_bytes());
                    data.extend_from_slice(&ni.to_le_bytes());
                    for bucket in &cf.buckets {
                        data.extend_from_slice(bucket);
                    }
                    RespValue::Array(Some(vec![
                        RespValue::Integer(1),
                        RespValue::BulkString(Some(data)),
                    ]))
                } else {
                    RespValue::Array(Some(vec![
                        RespValue::Integer(0),
                        RespValue::BulkString(None),
                    ]))
                }
            }
        }
    }
}

pub struct CfLoadChunkCommand;
impl CommandHandler for CfLoadChunkCommand {
    fn name(&self) -> &str { "CF.LOADCHUNK" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'CF.LOADCHUNK'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let data = match args[3].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR data must be a string") };
        if data.len() < 16 {
            return RespValue::error("ERR invalid cuckoo filter data");
        }
        let num_buckets = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let num_items = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        let bucket_data = &data[16..];
        let mut buckets: Vec<[u8; BUCKET_SIZE]> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let offset = i * BUCKET_SIZE;
            if offset + BUCKET_SIZE <= bucket_data.len() {
                let mut b = [0u8; BUCKET_SIZE];
                b.copy_from_slice(&bucket_data[offset..offset + BUCKET_SIZE]);
                buckets.push(b);
            } else {
                buckets.push([0u8; BUCKET_SIZE]);
            }
        }
        let cf = CuckooFilter {
            buckets,
            num_items,
            capacity: num_buckets * BUCKET_SIZE,
        };
        CF_STORE.lock().insert(key, cf);
        RespValue::ok()
    }
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

pub fn flush_all() {
    CF_STORE.lock().clear();
}

pub fn flush_db(_db_index: usize) {
    // CF_STORE keys are currently not db-isolated; flush_all clears everything.
    // A full fix would require changing CF_STORE to use (usize, Vec<u8>) keys.
}
