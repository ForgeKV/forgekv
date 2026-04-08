/// T-Digest sketch commands — compatible with RedisBloom / Dragonfly.
///
/// Commands: TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.MERGE
///           TDIGEST.QUANTILE TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK
///           TDIGEST.BYREVRANK TDIGEST.CDF TDIGEST.MIN TDIGEST.MAX
///           TDIGEST.TRIMMED_MEAN TDIGEST.INFO
///
/// Implementation: A simplified T-Digest using a sorted vector of (value, count)
/// centroids. Suitable for approximate quantile estimation.
use std::collections::HashMap;

use parking_lot::Mutex;

use super::CommandHandler;
use crate::resp::RespValue;

// ── T-Digest data structure ────────────────────────────────────────────────────

#[derive(Clone)]
struct Centroid {
    mean: f64,
    count: f64,
}

#[derive(Clone)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    compression: f64,
    total_count: f64,
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        TDigest {
            centroids: Vec::new(),
            compression: compression.max(10.0),
            total_count: 0.0,
        }
    }

    pub fn add(&mut self, value: f64, count: f64) {
        self.centroids.push(Centroid { mean: value, count });
        self.total_count += count;
        // Compress if too many centroids
        if self.centroids.len() > (self.compression as usize * 10) {
            self.compress();
        }
    }

    fn compress(&mut self) {
        self.centroids.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let max_centroids = self.compression as usize;
        if self.centroids.len() <= max_centroids {
            return;
        }

        let old = std::mem::take(&mut self.centroids);
        let mut merged: Vec<Centroid> = Vec::new();
        let mut iter = old.into_iter();
        if let Some(first) = iter.next() {
            let mut current = first;
            for next in iter {
                if merged.len() < max_centroids - 1 {
                    let total = current.count + next.count;
                    current.mean = (current.mean * current.count + next.mean * next.count) / total;
                    current.count = total;
                } else {
                    merged.push(current);
                    current = next;
                }
            }
            merged.push(current);
        }
        self.centroids = merged;
    }

    pub fn min(&self) -> Option<f64> {
        self.centroids.iter().map(|c| c.mean).reduce(f64::min)
    }

    pub fn max(&self) -> Option<f64> {
        self.centroids.iter().map(|c| c.mean).reduce(f64::max)
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.centroids.is_empty() || self.total_count == 0.0 {
            return None;
        }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let target = q * self.total_count;
        let mut cumulative = 0.0;
        for c in &sorted {
            cumulative += c.count;
            if cumulative >= target {
                return Some(c.mean);
            }
        }
        sorted.last().map(|c| c.mean)
    }

    pub fn cdf(&self, value: f64) -> f64 {
        if self.centroids.is_empty() || self.total_count == 0.0 {
            return 0.0;
        }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut cumulative = 0.0;
        for c in &sorted {
            if c.mean > value {
                break;
            }
            cumulative += c.count;
        }
        cumulative / self.total_count
    }

    pub fn rank(&self, value: f64) -> i64 {
        if self.centroids.is_empty() {
            return -1;
        }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut rank: i64 = 0;
        for c in &sorted {
            if c.mean < value {
                rank += c.count as i64;
            } else {
                break;
            }
        }
        rank
    }

    pub fn trimmed_mean(&self, low_cut: f64, high_cut: f64) -> Option<f64> {
        if self.centroids.is_empty() {
            return None;
        }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let low = low_cut * self.total_count;
        let high = (1.0 - high_cut) * self.total_count;
        let mut cumulative = 0.0;
        let mut sum = 0.0;
        let mut count = 0.0;
        for c in &sorted {
            let prev = cumulative;
            cumulative += c.count;
            if cumulative > low && prev < high {
                sum += c.mean * c.count;
                count += c.count;
            }
        }
        if count == 0.0 {
            None
        } else {
            Some(sum / count)
        }
    }

    pub fn merge_from(&mut self, other: &TDigest) {
        for c in &other.centroids {
            self.add(c.mean, c.count);
        }
        self.compress();
    }

    pub fn reset(&mut self) {
        self.centroids.clear();
        self.total_count = 0.0;
    }

    pub fn size(&self) -> usize {
        self.centroids.len()
    }
}

// ── Global store ──────────────────────────────────────────────────────────────

lazy_static::lazy_static! {
    static ref TD_STORE: Mutex<HashMap<Vec<u8>, TDigest>> = Mutex::new(HashMap::new());
}

// ── TDIGEST.CREATE ────────────────────────────────────────────────────────────

pub struct TDigestCreateCommand;
impl CommandHandler for TDigestCreateCommand {
    fn name(&self) -> &str {
        "TDIGEST.CREATE"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'TDIGEST.CREATE'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let compression: f64 = if args.len() >= 4
            && args[2].as_str().map(|s| s.to_uppercase()) == Some("COMPRESSION".to_string())
        {
            args[3]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100.0)
        } else {
            100.0
        };
        let mut store = TD_STORE.lock();
        if store.contains_key(&key) {
            return RespValue::error("ERR T-Digest: key already exists");
        }
        store.insert(key, TDigest::new(compression));
        RespValue::ok()
    }
}

// ── TDIGEST.ADD ───────────────────────────────────────────────────────────────

pub struct TDigestAddCommand;
impl CommandHandler for TDigestAddCommand {
    fn name(&self) -> &str {
        "TDIGEST.ADD"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'TDIGEST.ADD'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let values: Vec<f64> = args[2..]
            .iter()
            .filter_map(|a| a.as_str().and_then(|s| s.parse().ok()))
            .collect();
        if values.is_empty() {
            return RespValue::error("ERR no valid values");
        }
        let mut store = TD_STORE.lock();
        let td = store.entry(key).or_insert_with(|| TDigest::new(100.0));
        for v in values {
            td.add(v, 1.0);
        }
        RespValue::ok()
    }
}

// ── TDIGEST.RESET ─────────────────────────────────────────────────────────────

pub struct TDigestResetCommand;
impl CommandHandler for TDigestResetCommand {
    fn name(&self) -> &str {
        "TDIGEST.RESET"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let mut store = TD_STORE.lock();
        if let Some(td) = store.get_mut(&key) {
            td.reset();
            RespValue::ok()
        } else {
            RespValue::error("ERR T-Digest: key does not exist")
        }
    }
}

// ── TDIGEST.MERGE ─────────────────────────────────────────────────────────────

pub struct TDigestMergeCommand;
impl CommandHandler for TDigestMergeCommand {
    fn name(&self) -> &str {
        "TDIGEST.MERGE"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // TDIGEST.MERGE destKey numkeys sourceKey [sourceKey ...] [COMPRESSION compression] [OVERRIDE]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let dest = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let numkeys: usize = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR not enough source keys");
        }
        let sources: Vec<Vec<u8>> = args[3..3 + numkeys]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();
        let mut store = TD_STORE.lock();
        let merged: Vec<TDigest> = sources
            .iter()
            .filter_map(|k| store.get(k).cloned())
            .collect();
        let mut dest_td = store
            .get(&dest)
            .cloned()
            .unwrap_or_else(|| TDigest::new(100.0));
        for src in &merged {
            dest_td.merge_from(src);
        }
        store.insert(dest, dest_td);
        RespValue::ok()
    }
}

// ── TDIGEST.QUANTILE ──────────────────────────────────────────────────────────

pub struct TDigestQuantileCommand;
impl CommandHandler for TDigestQuantileCommand {
    fn name(&self) -> &str {
        "TDIGEST.QUANTILE"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        let td = match store.get(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR T-Digest: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..]
            .iter()
            .map(|a| {
                let q: f64 = a.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                match td.quantile(q) {
                    Some(v) => RespValue::bulk_str(&format!("{}", v)),
                    None => RespValue::null_bulk(),
                }
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── TDIGEST.CDF ───────────────────────────────────────────────────────────────

pub struct TDigestCdfCommand;
impl CommandHandler for TDigestCdfCommand {
    fn name(&self) -> &str {
        "TDIGEST.CDF"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        let td = match store.get(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR T-Digest: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..]
            .iter()
            .map(|a| {
                let v: f64 = a.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                RespValue::bulk_str(&format!("{}", td.cdf(v)))
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── TDIGEST.RANK ──────────────────────────────────────────────────────────────

pub struct TDigestRankCommand;
impl CommandHandler for TDigestRankCommand {
    fn name(&self) -> &str {
        "TDIGEST.RANK"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        let td = match store.get(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR T-Digest: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..]
            .iter()
            .map(|a| {
                let v: f64 = a.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                RespValue::integer(td.rank(v))
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── TDIGEST.REVRANK ───────────────────────────────────────────────────────────

pub struct TDigestRevRankCommand;
impl CommandHandler for TDigestRevRankCommand {
    fn name(&self) -> &str {
        "TDIGEST.REVRANK"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        let td = match store.get(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR T-Digest: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..]
            .iter()
            .map(|a| {
                let v: f64 = a.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let rank = td.rank(v);
                let revrank = td.total_count as i64 - rank - 1;
                RespValue::integer(revrank.max(-1))
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── TDIGEST.BYRANK ────────────────────────────────────────────────────────────

pub struct TDigestByRankCommand;
impl CommandHandler for TDigestByRankCommand {
    fn name(&self) -> &str {
        "TDIGEST.BYRANK"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        let td = match store.get(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR T-Digest: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..]
            .iter()
            .map(|a| {
                let rank: f64 = a.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let q = if td.total_count > 0.0 {
                    rank / td.total_count
                } else {
                    0.0
                };
                match td.quantile(q) {
                    Some(v) => RespValue::bulk_str(&format!("{}", v)),
                    None => RespValue::bulk_str("nan"),
                }
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── TDIGEST.BYREVRANK ─────────────────────────────────────────────────────────

pub struct TDigestByRevRankCommand;
impl CommandHandler for TDigestByRevRankCommand {
    fn name(&self) -> &str {
        "TDIGEST.BYREVRANK"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        let td = match store.get(&key) {
            Some(t) => t,
            None => return RespValue::error("ERR T-Digest: key does not exist"),
        };
        let results: Vec<RespValue> = args[2..]
            .iter()
            .map(|a| {
                let revrank: f64 = a.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let rank = td.total_count - revrank - 1.0;
                let q = if td.total_count > 0.0 {
                    rank / td.total_count
                } else {
                    0.0
                };
                match td.quantile(q.max(0.0)) {
                    Some(v) => RespValue::bulk_str(&format!("{}", v)),
                    None => RespValue::bulk_str("nan"),
                }
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── TDIGEST.MIN ───────────────────────────────────────────────────────────────

pub struct TDigestMinCommand;
impl CommandHandler for TDigestMinCommand {
    fn name(&self) -> &str {
        "TDIGEST.MIN"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        match store.get(&key) {
            Some(td) => match td.min() {
                Some(v) => RespValue::bulk_str(&format!("{}", v)),
                None => RespValue::bulk_str("nan"),
            },
            None => RespValue::error("ERR T-Digest: key does not exist"),
        }
    }
}

// ── TDIGEST.MAX ───────────────────────────────────────────────────────────────

pub struct TDigestMaxCommand;
impl CommandHandler for TDigestMaxCommand {
    fn name(&self) -> &str {
        "TDIGEST.MAX"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        match store.get(&key) {
            Some(td) => match td.max() {
                Some(v) => RespValue::bulk_str(&format!("{}", v)),
                None => RespValue::bulk_str("nan"),
            },
            None => RespValue::error("ERR T-Digest: key does not exist"),
        }
    }
}

// ── TDIGEST.TRIMMED_MEAN ──────────────────────────────────────────────────────

pub struct TDigestTrimmedMeanCommand;
impl CommandHandler for TDigestTrimmedMeanCommand {
    fn name(&self) -> &str {
        "TDIGEST.TRIMMED_MEAN"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let low: f64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let high: f64 = args[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let store = TD_STORE.lock();
        match store.get(&key) {
            Some(td) => match td.trimmed_mean(low, high) {
                Some(v) => RespValue::bulk_str(&format!("{}", v)),
                None => RespValue::bulk_str("nan"),
            },
            None => RespValue::error("ERR T-Digest: key does not exist"),
        }
    }
}

// ── TDIGEST.INFO ──────────────────────────────────────────────────────────────

pub struct TDigestInfoCommand;
impl CommandHandler for TDigestInfoCommand {
    fn name(&self) -> &str {
        "TDIGEST.INFO"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let store = TD_STORE.lock();
        match store.get(&key) {
            Some(td) => RespValue::Array(Some(vec![
                RespValue::bulk_str("Compression"),
                RespValue::bulk_str(&format!("{}", td.compression as i64)),
                RespValue::bulk_str("Capacity"),
                RespValue::integer((td.compression as i64) * 6),
                RespValue::bulk_str("Merged nodes"),
                RespValue::integer(td.size() as i64),
                RespValue::bulk_str("Unmerged nodes"),
                RespValue::integer(0),
                RespValue::bulk_str("Merged weight"),
                RespValue::bulk_str(&format!("{}", td.total_count as i64)),
                RespValue::bulk_str("Unmerged weight"),
                RespValue::integer(0),
                RespValue::bulk_str("Observations"),
                RespValue::bulk_str(&format!("{}", td.total_count as i64)),
                RespValue::bulk_str("Total compressions"),
                RespValue::integer(1),
                RespValue::bulk_str("Memory usage"),
                RespValue::integer((td.size() * 16 + 64) as i64),
            ])),
            None => RespValue::error("ERR T-Digest: key does not exist"),
        }
    }
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

pub fn flush_all() {
    TD_STORE.lock().clear();
}

pub fn flush_db(_db_index: usize) {
    // TD_STORE is currently not db-isolated; full fix requires (usize, Vec<u8>) keys.
}
