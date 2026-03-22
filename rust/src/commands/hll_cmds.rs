use std::sync::Arc;

use crate::database::RedisDatabase;
use crate::resp::RespValue;

use super::CommandHandler;

fn wrong_type() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn map_err(e: crate::database::RedisError) -> RespValue {
    match e {
        crate::database::RedisError::WrongType => wrong_type(),
        other => RespValue::error(&other.to_string()),
    }
}

// ── HyperLogLog constants ──────────────────────────────────────────────────────
// Uses 2^14 = 16384 registers, 6 bits each → 12288 bytes of register storage.
// Error rate ≈ 0.81%.  Compatible with Redis HLL semantics (not binary format).

const HLL_P: usize = 14;
const HLL_M: usize = 1 << HLL_P;          // 16384
const HLL_REG_BYTES: usize = HLL_M * 6 / 8; // 12288
const HLL_MAGIC: &[u8] = b"HYLL";
const HLL_BLOB_LEN: usize = HLL_MAGIC.len() + HLL_REG_BYTES; // 12292

// ── 64-bit hash (MurmurHash64A, Austin Appleby) ────────────────────────────────
fn hll_hash(data: &[u8]) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    const SEED: u64 = 0xadc83b19;

    let len = data.len();
    let mut h: u64 = SEED ^ ((len as u64).wrapping_mul(M));

    let num_blocks = len / 8;
    for i in 0..num_blocks {
        let mut k = u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Tail bytes (little-endian accumulation, mirroring the C fallthrough)
    let tail = &data[num_blocks * 8..];
    if !tail.is_empty() {
        let mut v: u64 = 0;
        for (i, &b) in tail.iter().enumerate() {
            v |= (b as u64) << (i * 8);
        }
        h ^= v;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

// ── 6-bit register access (densely packed) ────────────────────────────────────
#[inline]
fn get_register(regs: &[u8], idx: usize) -> u8 {
    let bit_start = idx * 6;
    let byte0 = bit_start / 8;
    let shift = bit_start % 8;
    let lo = regs[byte0] as u16;
    let hi = if byte0 + 1 < regs.len() { regs[byte0 + 1] as u16 } else { 0 };
    ((lo | (hi << 8)) >> shift) as u8 & 0x3F
}

#[inline]
fn set_register(regs: &mut [u8], idx: usize, val: u8) {
    let bit_start = idx * 6;
    let byte0 = bit_start / 8;
    let shift = bit_start % 8;
    let val16 = (val & 0x3F) as u16;
    let lo = regs[byte0] as u16;
    let hi = if byte0 + 1 < regs.len() { regs[byte0 + 1] as u16 } else { 0 };
    let word = (lo | (hi << 8)) & !((0x3Fu16) << shift) | (val16 << shift);
    regs[byte0] = word as u8;
    if byte0 + 1 < regs.len() {
        regs[byte0 + 1] = (word >> 8) as u8;
    }
}

// ── Core HLL operations ────────────────────────────────────────────────────────

/// Add element to HLL registers. Returns true if any register changed.
fn hll_add_element(regs: &mut [u8], elem: &[u8]) -> bool {
    let hash = hll_hash(elem);
    // Bottom HLL_P bits → register index (same convention as Redis)
    let idx = (hash & ((1u64 << HLL_P) - 1)) as usize;
    // Shift out index bits; set sentinel at bit (64-HLL_P) so loop terminates
    let mut h = (hash >> HLL_P) | (1u64 << (64 - HLL_P));
    let mut count: u8 = 1;
    while h & 1 == 0 {
        count += 1;
        h >>= 1;
    }
    let old = get_register(regs, idx);
    if count > old {
        set_register(regs, idx, count);
        true
    } else {
        false
    }
}

/// Estimate cardinality from dense registers.
fn hll_count(regs: &[u8]) -> i64 {
    let m = HLL_M as f64;
    // alpha_m for m = 16384
    let alpha = 0.7213 / (1.0 + 1.079 / m);

    let mut sum = 0.0f64;
    let mut zeros: u32 = 0;
    for i in 0..HLL_M {
        let r = get_register(regs, i) as i32;
        if r == 0 {
            zeros += 1;
        }
        sum += 2.0f64.powi(-r);
    }

    let raw_est = alpha * m * m / sum;

    let result = if raw_est <= 2.5 * m && zeros > 0 {
        // Linear counting for small cardinalities
        (m * (m / zeros as f64).ln()).round() as i64
    } else {
        raw_est.round() as i64
    };
    result.max(0)
}

/// Union two register arrays in-place (take max of each register).
fn hll_union(dst: &mut [u8], src: &[u8]) {
    for i in 0..HLL_M {
        let a = get_register(dst, i);
        let b = get_register(src, i);
        if b > a {
            set_register(dst, i, b);
        }
    }
}

// ── Storage helpers ────────────────────────────────────────────────────────────

/// Read registers from LSM.  Returns (registers_vec, is_new).
fn hll_read(
    db: &RedisDatabase,
    db_index: usize,
    key: &[u8],
) -> Result<(Vec<u8>, bool), crate::database::RedisError> {
    match db.string_get(db_index, key)? {
        Some(v) if v.len() == HLL_BLOB_LEN && v.starts_with(HLL_MAGIC) => {
            Ok((v[HLL_MAGIC.len()..].to_vec(), false))
        }
        Some(_) => {
            // Key exists but is not a valid HLL blob — treat as empty HLL.
            // (A real WRONGTYPE would have come from string_get already.)
            Ok((vec![0u8; HLL_REG_BYTES], true))
        }
        None => Ok((vec![0u8; HLL_REG_BYTES], true)),
    }
}

fn hll_write(
    db: &RedisDatabase,
    db_index: usize,
    key: &[u8],
    regs: &[u8],
) -> Result<(), crate::database::RedisError> {
    let mut blob = Vec::with_capacity(HLL_BLOB_LEN);
    blob.extend_from_slice(HLL_MAGIC);
    blob.extend_from_slice(regs);
    db.string_set(db_index, key, &blob, 0, false, false)
}

// ── PFADD ──────────────────────────────────────────────────────────────────────

pub struct PfAddCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PfAddCommand {
    fn name(&self) -> &str { "PFADD" }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'pfadd' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };

        let (mut regs, is_new) = match hll_read(&self.db, *db_index, &key) {
            Ok(r) => r,
            Err(e) => return map_err(e),
        };

        // PFADD key (no elements): just ensure the key exists
        if args.len() == 2 {
            if is_new {
                if let Err(e) = hll_write(&self.db, *db_index, &key, &regs) {
                    return map_err(e);
                }
                return RespValue::integer(1);
            }
            return RespValue::integer(0);
        }

        let mut changed = false;
        for arg in &args[2..] {
            if let Some(elem) = arg.as_bytes() {
                if hll_add_element(&mut regs, elem) {
                    changed = true;
                }
            }
        }

        if changed || is_new {
            if let Err(e) = hll_write(&self.db, *db_index, &key, &regs) {
                return map_err(e);
            }
        }

        RespValue::integer(if changed { 1 } else { 0 })
    }
}

// ── PFCOUNT ────────────────────────────────────────────────────────────────────

pub struct PfCountCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PfCountCommand {
    fn name(&self) -> &str { "PFCOUNT" }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'pfcount' command");
        }

        // Single-key fast path
        if args.len() == 2 {
            let key = match args[1].as_bytes() {
                Some(k) => k,
                None => return RespValue::integer(0),
            };
            return match hll_read(&self.db, *db_index, key) {
                Ok((regs, _)) => RespValue::integer(hll_count(&regs)),
                Err(e) => map_err(e),
            };
        }

        // Multi-key: union all registers then count
        let mut union = vec![0u8; HLL_REG_BYTES];
        for arg in &args[1..] {
            let key = match arg.as_bytes() {
                Some(k) => k,
                None => continue,
            };
            match hll_read(&self.db, *db_index, key) {
                Ok((regs, _)) => hll_union(&mut union, &regs),
                Err(e) => return map_err(e),
            }
        }
        RespValue::integer(hll_count(&union))
    }
}

// ── PFMERGE ────────────────────────────────────────────────────────────────────

pub struct PfMergeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PfMergeCommand {
    fn name(&self) -> &str { "PFMERGE" }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'pfmerge' command");
        }
        let dst = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };

        // Start from destination's existing registers
        let (mut union, _) = match hll_read(&self.db, *db_index, &dst) {
            Ok(r) => r,
            Err(e) => return map_err(e),
        };

        // Merge in each source key
        for arg in &args[2..] {
            let key = match arg.as_bytes() {
                Some(k) => k,
                None => continue,
            };
            match hll_read(&self.db, *db_index, key) {
                Ok((regs, _)) => hll_union(&mut union, &regs),
                Err(e) => return map_err(e),
            }
        }

        match hll_write(&self.db, *db_index, &dst, &union) {
            Ok(()) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}
