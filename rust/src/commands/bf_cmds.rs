/// Bloom Filter commands — compatible with RedisBloom / Dragonfly.
///
/// Commands: BF.ADD BF.MADD BF.EXISTS BF.MEXISTS BF.RESERVE BF.CARD BF.INFO BF.INSERT BF.LOADCHUNK BF.SCANDUMP

use parking_lot::Mutex;
use std::collections::HashMap;

use crate::ext_type_registry;
use crate::resp::RespValue;
use crate::storage::bloom::BloomFilter;
use super::CommandHandler;

/// In-memory bloom filter store, keyed by (db_index, key).
lazy_static::lazy_static! {
    static ref BF_STORE: Mutex<HashMap<(usize, Vec<u8>), BloomFilter>> = Mutex::new(HashMap::new());
}

fn bf_register(db: usize, key: &[u8]) {
    ext_type_registry::register(db, key, "MBbloom--");
}

// ── BF.RESERVE ────────────────────────────────────────────────────────────────

pub struct BfReserveCommand;
impl CommandHandler for BfReserveCommand {
    fn name(&self) -> &str { "BF.RESERVE" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'BF.RESERVE'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let capacity: usize = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) if n > 0 => n,
            _ => return RespValue::error("ERR capacity must be a positive integer"),
        };
        let db = *db_index;
        let mut store = BF_STORE.lock();
        if store.contains_key(&(db, key.clone())) {
            return RespValue::error("ERR item exists");
        }
        store.insert((db, key.clone()), BloomFilter::new(capacity));
        bf_register(db, &key);
        RespValue::ok()
    }
}

// ── BF.ADD ────────────────────────────────────────────────────────────────────

pub struct BfAddCommand;
impl CommandHandler for BfAddCommand {
    fn name(&self) -> &str { "BF.ADD" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'BF.ADD'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let item = match args[2].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR item must be a string"),
        };
        let db = *db_index;
        let mut store = BF_STORE.lock();
        let is_new = !store.contains_key(&(db, key.clone()));
        let bf = store.entry((db, key.clone())).or_insert_with(|| BloomFilter::new(100));
        let existed = bf.may_contain(&item);
        bf.add(&item);
        if is_new { bf_register(db, &key); }
        RespValue::Integer(if existed { 0 } else { 1 })
    }
}

// ── BF.MADD ───────────────────────────────────────────────────────────────────

pub struct BfMAddCommand;
impl CommandHandler for BfMAddCommand {
    fn name(&self) -> &str { "BF.MADD" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'BF.MADD'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let db = *db_index;
        let mut store = BF_STORE.lock();
        let is_new = !store.contains_key(&(db, key.clone()));
        let bf = store.entry((db, key.clone())).or_insert_with(|| BloomFilter::new(100));
        if is_new { bf_register(db, &key); }
        let results: Vec<RespValue> = args[2..].iter().map(|a| {
            if let Some(item) = a.as_bytes() {
                let existed = bf.may_contain(item);
                bf.add(item);
                RespValue::Integer(if existed { 0 } else { 1 })
            } else {
                RespValue::error("ERR item must be a string")
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

// ── BF.EXISTS ─────────────────────────────────────────────────────────────────

pub struct BfExistsCommand;
impl CommandHandler for BfExistsCommand {
    fn name(&self) -> &str { "BF.EXISTS" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'BF.EXISTS'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let item = match args[2].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR item must be a string"),
        };
        let db = *db_index;
        let store = BF_STORE.lock();
        match store.get(&(db, key)) {
            Some(bf) => RespValue::Integer(if bf.may_contain(&item) { 1 } else { 0 }),
            None => RespValue::Integer(0),
        }
    }
}

// ── BF.MEXISTS ────────────────────────────────────────────────────────────────

pub struct BfMExistsCommand;
impl CommandHandler for BfMExistsCommand {
    fn name(&self) -> &str { "BF.MEXISTS" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'BF.MEXISTS'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let db = *db_index;
        let store = BF_STORE.lock();
        let bf = store.get(&(db, key));
        let results: Vec<RespValue> = args[2..].iter().map(|a| {
            if let Some(item) = a.as_bytes() {
                RespValue::Integer(match bf {
                    Some(b) => if b.may_contain(item) { 1 } else { 0 },
                    None => 0,
                })
            } else {
                RespValue::Integer(0)
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

// ── BF.CARD ───────────────────────────────────────────────────────────────────

pub struct BfCardCommand;
impl CommandHandler for BfCardCommand {
    fn name(&self) -> &str { "BF.CARD" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'BF.CARD'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let db = *db_index;
        let store = BF_STORE.lock();
        match store.get(&(db, key)) {
            Some(bf) => RespValue::Integer(bf.items_inserted() as i64),
            None => RespValue::Integer(0),
        }
    }
}

// ── BF.INFO ───────────────────────────────────────────────────────────────────

pub struct BfInfoCommand;
impl CommandHandler for BfInfoCommand {
    fn name(&self) -> &str { "BF.INFO" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'BF.INFO'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let db = *db_index;
        let store = BF_STORE.lock();
        match store.get(&(db, key)) {
            None => RespValue::error("ERR not found"),
            Some(bf) => {
                let capacity = bf.capacity() as i64;
                let items_inserted = bf.items_inserted() as i64;
                let serialized = bf.serialize();
                let hash_count = u32::from_le_bytes(serialized[4..8].try_into().unwrap()) as i64;
                let byte_count = serialized.len() as i64;
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("Capacity"),
                    RespValue::Integer(capacity),
                    RespValue::bulk_str("Size"),
                    RespValue::Integer(byte_count),
                    RespValue::bulk_str("Number of filters"),
                    RespValue::Integer(1),
                    RespValue::bulk_str("Number of items inserted"),
                    RespValue::Integer(items_inserted),
                    RespValue::bulk_str("Expansion rate"),
                    RespValue::Integer(2),
                    RespValue::bulk_str("Number of hash functions"),
                    RespValue::Integer(hash_count),
                ]))
            }
        }
    }
}

// ── BF.INSERT ─────────────────────────────────────────────────────────────────

pub struct BfInsertCommand;
impl CommandHandler for BfInsertCommand {
    fn name(&self) -> &str { "BF.INSERT" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'BF.INSERT'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let mut capacity = 100usize;
        let mut nocreate = false;
        let mut items_start = 2;

        let mut i = 2;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("CAPACITY") => {
                    i += 1;
                    capacity = args.get(i)
                        .and_then(|a| a.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(100);
                }
                Some("ERROR") | Some("EXPANSION") | Some("NONSCALING") => {
                    if args.get(i + 1).is_some() { i += 1; }
                }
                Some("NOCREATE") => { nocreate = true; }
                Some("ITEMS") => { items_start = i + 1; break; }
                _ => {}
            }
            i += 1;
        }

        let db = *db_index;
        let mut store = BF_STORE.lock();
        if nocreate && !store.contains_key(&(db, key.clone())) {
            return RespValue::error("ERR not found");
        }
        let is_new = !store.contains_key(&(db, key.clone()));
        let bf = store.entry((db, key.clone())).or_insert_with(|| BloomFilter::new(capacity));
        if is_new { bf_register(db, &key); }

        let results: Vec<RespValue> = args[items_start..].iter().map(|a| {
            if let Some(item) = a.as_bytes() {
                let existed = bf.may_contain(item);
                bf.add(item);
                RespValue::Integer(if existed { 0 } else { 1 })
            } else {
                RespValue::Integer(0)
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

// ── BF.SCANDUMP / BF.LOADCHUNK ────────────────────────────────────────────────

pub struct BfScanDumpCommand;
impl CommandHandler for BfScanDumpCommand {
    fn name(&self) -> &str { "BF.SCANDUMP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'BF.SCANDUMP'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let iter: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
        let db = *db_index;
        let store = BF_STORE.lock();
        match store.get(&(db, key)) {
            None => RespValue::error("ERR not found"),
            Some(bf) => {
                if iter == 0 {
                    let data = bf.serialize();
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

pub struct BfLoadChunkCommand;
impl CommandHandler for BfLoadChunkCommand {
    fn name(&self) -> &str { "BF.LOADCHUNK" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'BF.LOADCHUNK'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let data = match args[3].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR data must be a string"),
        };
        if data.len() < 8 {
            return RespValue::error("ERR invalid bloom filter data");
        }
        let db = *db_index;
        let bf = BloomFilter::deserialize(&data);
        let is_new = !BF_STORE.lock().contains_key(&(db, key.clone()));
        BF_STORE.lock().insert((db, key.clone()), bf);
        if is_new { bf_register(db, &key); }
        RespValue::ok()
    }
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

pub fn flush_all() {
    BF_STORE.lock().clear();
    // ext_type_registry cleared by caller
}

pub fn flush_db(db_index: usize) {
    BF_STORE.lock().retain(|(db, _), _| *db != db_index);
    ext_type_registry::flush_db(db_index);
}
