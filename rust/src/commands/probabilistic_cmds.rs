/// Probabilistic data structure commands — compatible with Dragonfly / RedisBloom.
///
/// Count-Min Sketch:   CMS.INITBYDIM  CMS.INITBYPROB  CMS.INCRBY  CMS.QUERY  CMS.MERGE  CMS.INFO
/// Top-K:              TOPK.RESERVE   TOPK.ADD         TOPK.INCRBY TOPK.QUERY TOPK.COUNT TOPK.LIST TOPK.INFO
/// Rate limiting:      CL.THROTTLE

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::resp::RespValue;
use super::CommandHandler;

// ── Count-Min Sketch ──────────────────────────────────────────────────────────

struct CountMinSketch {
    width:  usize,
    depth:  usize,
    matrix: Vec<Vec<i64>>,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        CountMinSketch {
            width,
            depth,
            matrix: vec![vec![0i64; width]; depth],
        }
    }

    fn from_error_prob(error: f64, prob: f64) -> Self {
        let width = (2.0 / error).ceil() as usize;
        let depth = (-prob.ln() / 2f64.ln()).ceil() as usize;
        Self::new(width.max(1), depth.max(1))
    }

    fn hash_row(&self, key: &[u8], row: usize) -> usize {
        let seed: u64 = (row as u64).wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(0x6c62272e07bb0142);
        let h = fnv1a_seeded(key, seed);
        (h as usize) % self.width
    }

    fn add(&mut self, key: &[u8], count: i64) {
        for row in 0..self.depth {
            let col = self.hash_row(key, row);
            self.matrix[row][col] += count;
        }
    }

    fn query(&self, key: &[u8]) -> i64 {
        (0..self.depth)
            .map(|row| self.matrix[row][self.hash_row(key, row)])
            .min()
            .unwrap_or(0)
    }

    fn total_count(&self) -> i64 {
        self.matrix.first().map(|row| row.iter().sum()).unwrap_or(0)
    }
}

fn fnv1a_seeded(data: &[u8], seed: u64) -> u64 {
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = seed ^ 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

lazy_static::lazy_static! {
    static ref CMS_STORE: Mutex<HashMap<Vec<u8>, CountMinSketch>> = Mutex::new(HashMap::new());
}

pub struct CmsInitByDimCommand;
impl CommandHandler for CmsInitByDimCommand {
    fn name(&self) -> &str { "CMS.INITBYDIM" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'CMS.INITBYDIM'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let width: usize = match args[2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR width must be a positive integer") };
        let depth: usize = match args[3].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR depth must be a positive integer") };
        if CMS_STORE.lock().contains_key(&key) {
            return RespValue::error("ERR CMS: key already exists");
        }
        CMS_STORE.lock().insert(key, CountMinSketch::new(width, depth));
        RespValue::ok()
    }
}

pub struct CmsInitByProbCommand;
impl CommandHandler for CmsInitByProbCommand {
    fn name(&self) -> &str { "CMS.INITBYPROB" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'CMS.INITBYPROB'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let error: f64 = match args[2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR error must be a valid float") };
        let prob: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR prob must be a valid float") };
        if error <= 0.0 || error >= 1.0 { return RespValue::error("ERR error must be between 0 and 1"); }
        if prob <= 0.0 || prob >= 1.0 { return RespValue::error("ERR probability must be between 0 and 1"); }
        if CMS_STORE.lock().contains_key(&key) {
            return RespValue::error("ERR CMS: key already exists");
        }
        CMS_STORE.lock().insert(key, CountMinSketch::from_error_prob(error, prob));
        RespValue::ok()
    }
}

pub struct CmsIncrByCommand;
impl CommandHandler for CmsIncrByCommand {
    fn name(&self) -> &str { "CMS.INCRBY" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // CMS.INCRBY key item increment [item increment ...]
        if args.len() < 4 || (args.len() - 2) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'CMS.INCRBY'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let mut store = CMS_STORE.lock();
        let cms = match store.get_mut(&key) {
            Some(c) => c,
            None => return RespValue::error("ERR CMS: key does not exist"),
        };
        let mut results = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() {
            let item = match args[i].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR item must be a string") };
            let incr: i64 = match args[i+1].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR increment must be an integer") };
            cms.add(&item, incr);
            results.push(RespValue::Integer(cms.query(&item)));
            i += 2;
        }
        RespValue::Array(Some(results))
    }
}

pub struct CmsQueryCommand;
impl CommandHandler for CmsQueryCommand {
    fn name(&self) -> &str { "CMS.QUERY" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'CMS.QUERY'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = CMS_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR CMS: key does not exist"),
            Some(cms) => {
                let results: Vec<RespValue> = args[2..].iter().map(|a| {
                    match a.as_bytes() {
                        Some(item) => RespValue::Integer(cms.query(item)),
                        None => RespValue::Integer(0),
                    }
                }).collect();
                RespValue::Array(Some(results))
            }
        }
    }
}

pub struct CmsInfoCommand;
impl CommandHandler for CmsInfoCommand {
    fn name(&self) -> &str { "CMS.INFO" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'CMS.INFO'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = CMS_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR CMS: key does not exist"),
            Some(cms) => RespValue::Array(Some(vec![
                RespValue::bulk_str("width"),
                RespValue::Integer(cms.width as i64),
                RespValue::bulk_str("depth"),
                RespValue::Integer(cms.depth as i64),
                RespValue::bulk_str("count"),
                RespValue::Integer(cms.total_count()),
            ])),
        }
    }
}

pub struct CmsMergeCommand;
impl CommandHandler for CmsMergeCommand {
    fn name(&self) -> &str { "CMS.MERGE" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // CMS.MERGE destination numkeys source [source ...] [WEIGHTS weight [weight ...]]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'CMS.MERGE'");
        }
        let dest_key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR numkeys must be an integer") };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR wrong number of arguments");
        }
        let mut store = CMS_STORE.lock();
        // Get source dimensions
        let first_src = match args[3].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR source key must be a string"),
        };
        let (width, depth) = match store.get(&first_src) {
            Some(c) => (c.width, c.depth),
            None => return RespValue::error("ERR CMS: key does not exist"),
        };
        let mut merged = CountMinSketch::new(width, depth);
        for i in 3..3 + numkeys {
            let src_key = match args[i].as_bytes() { Some(b) => b.to_vec(), None => continue };
            if let Some(src) = store.get(&src_key) {
                if src.width == width && src.depth == depth {
                    for row in 0..depth {
                        for col in 0..width {
                            merged.matrix[row][col] += src.matrix[row][col];
                        }
                    }
                }
            }
        }
        store.insert(dest_key, merged);
        RespValue::ok()
    }
}

// ── Top-K ─────────────────────────────────────────────────────────────────────

struct TopK {
    k: usize,
    /// (item, count) heap — always sorted by count desc
    heap: Vec<(Vec<u8>, i64)>,
    /// Count-Min Sketch for frequency estimation
    cms: CountMinSketch,
}

impl TopK {
    fn new(k: usize, width: usize, depth: usize) -> Self {
        TopK {
            k,
            heap: Vec::new(),
            cms: CountMinSketch::new(width, depth),
        }
    }

    /// Add item, return (dropped_item_if_any)
    fn add(&mut self, item: &[u8], count: i64) -> Option<Vec<u8>> {
        self.cms.add(item, count);
        let freq = self.cms.query(item);

        // Update or insert in heap
        if let Some(pos) = self.heap.iter().position(|(k, _)| k == item) {
            self.heap[pos].1 = freq;
        } else {
            self.heap.push((item.to_vec(), freq));
        }
        // Keep sorted
        self.heap.sort_by(|a, b| b.1.cmp(&a.1));

        // Evict if over k
        if self.heap.len() > self.k {
            let dropped = self.heap.pop().map(|(k, _)| k);
            return dropped;
        }
        None
    }

    fn query(&self, item: &[u8]) -> bool {
        self.heap.iter().any(|(k, _)| k == item)
    }

    fn count(&self, item: &[u8]) -> i64 {
        self.cms.query(item)
    }

    fn list(&self) -> Vec<(&[u8], i64)> {
        self.heap.iter().map(|(k, c)| (k.as_slice(), *c)).collect()
    }
}

lazy_static::lazy_static! {
    static ref TOPK_STORE: Mutex<HashMap<Vec<u8>, TopK>> = Mutex::new(HashMap::new());
}

pub struct TopKReserveCommand;
impl CommandHandler for TopKReserveCommand {
    fn name(&self) -> &str { "TOPK.RESERVE" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // TOPK.RESERVE key topk [width depth decay]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.RESERVE'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let k: usize = match args[2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR topk must be a positive integer") };
        let width: usize = args.get(3).and_then(|a| a.as_str()).and_then(|s| s.parse().ok()).unwrap_or(2000);
        let depth: usize = args.get(4).and_then(|a| a.as_str()).and_then(|s| s.parse().ok()).unwrap_or(7);
        if TOPK_STORE.lock().contains_key(&key) {
            return RespValue::error("ERR TopK: key already exists");
        }
        TOPK_STORE.lock().insert(key, TopK::new(k, width, depth));
        RespValue::ok()
    }
}

pub struct TopKAddCommand;
impl CommandHandler for TopKAddCommand {
    fn name(&self) -> &str { "TOPK.ADD" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.ADD'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let mut store = TOPK_STORE.lock();
        let topk = match store.get_mut(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR TopK: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..].iter().map(|a| {
            if let Some(item) = a.as_bytes() {
                match topk.add(item, 1) {
                    Some(dropped) => RespValue::BulkString(Some(dropped)),
                    None => RespValue::null_bulk(),
                }
            } else {
                RespValue::null_bulk()
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

pub struct TopKIncrByCommand;
impl CommandHandler for TopKIncrByCommand {
    fn name(&self) -> &str { "TOPK.INCRBY" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // TOPK.INCRBY key item increment [item increment ...]
        if args.len() < 4 || (args.len() - 2) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.INCRBY'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let mut store = TOPK_STORE.lock();
        let topk = match store.get_mut(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR TopK: key does not exist"),
        };
        let mut results = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() {
            let item = match args[i].as_bytes() { Some(b) => b, None => { results.push(RespValue::null_bulk()); i += 2; continue; } };
            let incr: i64 = args[i+1].as_str().and_then(|s| s.parse().ok()).unwrap_or(1);
            results.push(match topk.add(item, incr) {
                Some(dropped) => RespValue::BulkString(Some(dropped)),
                None => RespValue::null_bulk(),
            });
            i += 2;
        }
        RespValue::Array(Some(results))
    }
}

pub struct TopKQueryCommand;
impl CommandHandler for TopKQueryCommand {
    fn name(&self) -> &str { "TOPK.QUERY" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.QUERY'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = TOPK_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR TopK: key does not exist"),
            Some(topk) => {
                let results: Vec<RespValue> = args[2..].iter().map(|a| {
                    match a.as_bytes() {
                        Some(item) => RespValue::Integer(if topk.query(item) { 1 } else { 0 }),
                        None => RespValue::Integer(0),
                    }
                }).collect();
                RespValue::Array(Some(results))
            }
        }
    }
}

pub struct TopKCountCommand;
impl CommandHandler for TopKCountCommand {
    fn name(&self) -> &str { "TOPK.COUNT" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.COUNT'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = TOPK_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR TopK: key does not exist"),
            Some(topk) => {
                let results: Vec<RespValue> = args[2..].iter().map(|a| {
                    match a.as_bytes() {
                        Some(item) => RespValue::Integer(topk.count(item)),
                        None => RespValue::Integer(0),
                    }
                }).collect();
                RespValue::Array(Some(results))
            }
        }
    }
}

pub struct TopKListCommand;
impl CommandHandler for TopKListCommand {
    fn name(&self) -> &str { "TOPK.LIST" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.LIST'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let withcount = args.get(2)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase() == "WITHCOUNT")
            .unwrap_or(false);
        let store = TOPK_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR TopK: key does not exist"),
            Some(topk) => {
                let list = topk.list();
                if withcount {
                    let mut results = Vec::new();
                    for (item, count) in &list {
                        results.push(RespValue::BulkString(Some(item.to_vec())));
                        results.push(RespValue::Integer(*count));
                    }
                    RespValue::Array(Some(results))
                } else {
                    RespValue::Array(Some(list.iter().map(|(item, _)| {
                        RespValue::BulkString(Some(item.to_vec()))
                    }).collect()))
                }
            }
        }
    }
}

pub struct TopKInfoCommand;
impl CommandHandler for TopKInfoCommand {
    fn name(&self) -> &str { "TOPK.INFO" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'TOPK.INFO'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let store = TOPK_STORE.lock();
        match store.get(&key) {
            None => RespValue::error("ERR TopK: key does not exist"),
            Some(topk) => RespValue::Array(Some(vec![
                RespValue::bulk_str("k"),
                RespValue::Integer(topk.k as i64),
                RespValue::bulk_str("width"),
                RespValue::Integer(topk.cms.width as i64),
                RespValue::bulk_str("depth"),
                RespValue::Integer(topk.cms.depth as i64),
                RespValue::bulk_str("decay"),
                RespValue::bulk_str("0.9"),
            ])),
        }
    }
}

// ── CL.THROTTLE ───────────────────────────────────────────────────────────────

/// Token-bucket rate limiter compatible with Redis-Cell / Dragonfly.
///
/// CL.THROTTLE key max_burst count_per_period period [quantity]
///   → [allowed, limit, remaining, retry_after, reset_after]
///      allowed:      0 = allowed, 1 = denied
///      limit:        max_burst + 1
///      remaining:    tokens remaining after this request
///      retry_after:  seconds until next allowed (-1 if allowed)
///      reset_after:  seconds until bucket is full

use std::time::{SystemTime, UNIX_EPOCH};

struct TokenBucket {
    tokens:     f64,
    max_tokens: f64,
    fill_rate:  f64,  // tokens per second
    last_refill: f64, // unix timestamp
}

impl TokenBucket {
    fn new(max_burst: f64, count_per_period: f64, period_secs: f64) -> Self {
        let fill_rate = count_per_period / period_secs;
        let now = unix_ts();
        TokenBucket {
            tokens: max_burst,
            max_tokens: max_burst,
            fill_rate,
            last_refill: now,
        }
    }

    fn refill(&mut self) {
        let now = unix_ts();
        let elapsed = now - self.last_refill;
        self.tokens = (self.tokens + elapsed * self.fill_rate).min(self.max_tokens);
        self.last_refill = now;
    }

    /// Try to consume `quantity` tokens. Returns (allowed, remaining, retry_after, reset_after).
    fn consume(&mut self, quantity: f64) -> (bool, f64, f64, f64) {
        self.refill();
        let remaining = self.tokens - quantity;
        let allowed = remaining >= 0.0;
        let retry_after = if allowed { -1.0 } else { (-remaining) / self.fill_rate };
        let reset_after = (self.max_tokens - self.tokens.max(0.0)) / self.fill_rate;
        if allowed {
            self.tokens = remaining;
        }
        (allowed, self.tokens.max(0.0), retry_after, reset_after)
    }
}

fn unix_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

lazy_static::lazy_static! {
    static ref THROTTLE_STORE: Mutex<HashMap<Vec<u8>, TokenBucket>> = Mutex::new(HashMap::new());
}

pub struct ClThrottleCommand;
impl CommandHandler for ClThrottleCommand {
    fn name(&self) -> &str { "CL.THROTTLE" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // CL.THROTTLE key max_burst count_per_period period [quantity]
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'CL.THROTTLE'");
        }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR key must be a string") };
        let max_burst: f64 = match args[2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR max_burst must be a number") };
        let count_per_period: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR count_per_period must be a number") };
        let period_secs: f64 = match args[4].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => return RespValue::error("ERR period must be a number") };
        let quantity: f64 = args.get(5).and_then(|a| a.as_str()).and_then(|s| s.parse().ok()).unwrap_or(1.0);

        let mut store = THROTTLE_STORE.lock();
        let bucket = store.entry(key).or_insert_with(|| {
            TokenBucket::new(max_burst, count_per_period, period_secs)
        });
        // Update parameters if they changed
        bucket.max_tokens = max_burst;
        bucket.fill_rate = count_per_period / period_secs.max(1.0);

        let (allowed, remaining, retry_after, reset_after) = bucket.consume(quantity);
        let limit = (max_burst + 1.0) as i64;

        RespValue::Array(Some(vec![
            RespValue::Integer(if allowed { 0 } else { 1 }),
            RespValue::Integer(limit),
            RespValue::Integer(remaining as i64),
            RespValue::Integer(if retry_after < 0.0 { -1 } else { retry_after.ceil() as i64 }),
            RespValue::Integer(reset_after.ceil() as i64),
        ]))
    }
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

pub fn flush_all() {
    CMS_STORE.lock().clear();
    TOPK_STORE.lock().clear();
    THROTTLE_STORE.lock().clear();
}

pub fn flush_db(_db_index: usize) {
    // Stores are currently not db-isolated; full fix requires (usize, Vec<u8>) keys.
}
