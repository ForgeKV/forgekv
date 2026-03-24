pub mod metadata;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::ext_type_registry;
use crate::storage::key_encoding::*;
use crate::storage::LsmStorage;
use metadata::{RedisMetadata, RedisType, METADATA_SIZE_LEGACY};

#[derive(Debug)]
pub enum RedisError {
    WrongType,
    NotInteger,
    Overflow,
    NxFail,
    XxFail,
    Other(String),
}

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisError::WrongType => write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value"),
            RedisError::NotInteger => write!(f, "ERR value is not an integer or out of range"),
            RedisError::Overflow => write!(f, "ERR increment or decrement would overflow"),
            RedisError::NxFail => write!(f, "NX condition failed"),
            RedisError::XxFail => write!(f, "XX condition failed"),
            RedisError::Other(s) => write!(f, "{}", s),
        }
    }
}

/// Number of key-space shards. Each shard has its own RwLock, so concurrent
/// operations on different keys can proceed in parallel.
const NUM_SHARDS: usize = 64;

pub struct RedisDatabase {
    storage: Arc<LsmStorage>,
    pub num_dbs: usize,
    shards: Box<[RwLock<()>]>,
}

const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

/// FNV-1a hash → shard index. Fast, zero allocation, excellent key distribution.
#[inline(always)]
fn shard_for(key: &[u8]) -> usize {
    let mut h: u64 = FNV_OFFSET_BASIS;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    (h as usize) % NUM_SHARDS
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl RedisDatabase {
    pub fn new(storage: Arc<LsmStorage>, num_dbs: usize) -> Self {
        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(())).collect::<Vec<_>>().into_boxed_slice();
        RedisDatabase {
            storage,
            num_dbs,
            shards,
        }
    }

    // ── Shard lock helpers ────────────────────────────────────────────────────

    /// Acquire write lock for a single key's shard.
    #[inline(always)]
    fn shard_w(&self, key: &[u8]) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.shards[shard_for(key)].write()
    }

    /// Acquire read lock for a single key's shard.
    #[inline(always)]
    fn shard_r(&self, key: &[u8]) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.shards[shard_for(key)].read()
    }

    /// Acquire write locks for multiple keys' shards in ascending shard-id order
    /// (prevents deadlock). Deduplicates shard ids automatically.
    fn shard_w_multi<'a>(&'a self, keys: &[&[u8]]) -> Vec<parking_lot::RwLockWriteGuard<'a, ()>> {
        let mut ids: Vec<usize> = keys.iter().map(|k| shard_for(k)).collect();
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter().map(|i| self.shards[i].write()).collect()
    }

    /// Acquire write locks on ALL shards (for cross-keyspace ops: FLUSHDB, SCAN, etc.).
    fn shard_w_all(&self) -> Vec<parking_lot::RwLockWriteGuard<'_, ()>> {
        (0..NUM_SHARDS).map(|i| self.shards[i].write()).collect()
    }

    /// Acquire read locks on ALL shards.
    fn shard_r_all(&self) -> Vec<parking_lot::RwLockReadGuard<'_, ()>> {
        (0..NUM_SHARDS).map(|i| self.shards[i].read()).collect()
    }

    // ── Internal helpers (called while lock is held) ──────────────────────────

    fn get_meta_inner(&self, db: usize, key: &[u8]) -> Option<RedisMetadata> {
        let meta_key = encode_meta_key(db, key);
        let data = self.storage.get(&meta_key)?;
        if data.len() < METADATA_SIZE_LEGACY {
            return None;
        }
        Some(RedisMetadata::deserialize(&data))
    }

    fn save_meta_inner(&self, db: usize, key: &[u8], meta: &RedisMetadata) {
        // Auto-increment version: always use old_version+1 so OBJECT VERSION tracks writes
        let old_version = self.get_meta_inner(db, key).map(|m| m.version).unwrap_or(0);
        let new_version = old_version + 1;
        let meta_to_save = RedisMetadata {
            r#type: meta.r#type,
            count: meta.count,
            expiry_ms: meta.expiry_ms,
            list_head: meta.list_head,
            list_tail: meta.list_tail,
            version: new_version,
        };
        let meta_key = encode_meta_key(db, key);
        self.storage.put(meta_key, meta_to_save.serialize());
    }

    fn is_live_key_inner(&self, db: usize, key: &[u8]) -> bool {
        match self.get_meta_inner(db, key) {
            None => false,
            Some(meta) => !(meta.expiry_ms > 0 && meta.expiry_ms <= now_ms()),
        }
    }

    fn set_expiry_inner(&self, db: usize, key: &[u8], meta: &mut RedisMetadata, expiry_ms: i64) {
        // Remove old TTL key if any
        if meta.expiry_ms > 0 {
            let old_ttl_key = encode_ttl_key(meta.expiry_ms, db, key);
            self.storage.delete(old_ttl_key);
        }
        meta.expiry_ms = expiry_ms;
        if expiry_ms > 0 {
            let ttl_key = encode_ttl_key(expiry_ms, db, key);
            self.storage.put(ttl_key, vec![1u8]);
        }
    }

    fn delete_key_internal(&self, db: usize, key: &[u8], meta: Option<&RedisMetadata>) {
        let meta = match meta {
            Some(m) => RedisMetadata {
                r#type: m.r#type,
                count: m.count,
                expiry_ms: m.expiry_ms,
                list_head: m.list_head,
                list_tail: m.list_tail,
                version: 0,
            },
            None => match self.get_meta_inner(db, key) {
                Some(m) => m,
                None => return,
            },
        };

        // Remove TTL index if present
        if meta.expiry_ms > 0 {
            let ttl_key = encode_ttl_key(meta.expiry_ms, db, key);
            self.storage.delete(ttl_key);
        }

        // Delete metadata
        let meta_key = encode_meta_key(db, key);
        self.storage.delete(meta_key);

        // Delete type-specific data
        match meta.r#type {
            RedisType::String => {
                let sk = encode_string_key(db, key);
                self.storage.delete(sk);
            }
            RedisType::Hash => {
                // Scan and delete all hash fields
                let prefix = encode_key_prefix(TAG_HASH, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, _) in entries {
                    self.storage.delete(k);
                }
            }
            RedisType::List => {
                // Delete all list entries by sequence
                let prefix = encode_key_prefix(TAG_LIST, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, _) in entries {
                    self.storage.delete(k);
                }
            }
            RedisType::Set => {
                let prefix = encode_key_prefix(TAG_SET, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, _) in entries {
                    self.storage.delete(k);
                }
            }
            RedisType::ZSet => {
                let prefix = encode_key_prefix(TAG_ZSET, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, _) in entries {
                    self.storage.delete(k);
                }
            }
            RedisType::None => {}
        }
    }

    // ── String commands ───────────────────────────────────────────────────────

    pub fn string_set(
        &self,
        db: usize,
        key: &[u8],
        value: &[u8],
        expiry_ms: i64,
        nx: bool,
        xx: bool,
    ) -> Result<(), RedisError> {
        let _guard = self.shard_w(key);

        let existing = self.get_meta_inner(db, key);
        let is_live = match &existing {
            None => false,
            Some(m) => !(m.expiry_ms > 0 && m.expiry_ms <= now_ms()),
        };

        if nx && is_live {
            return Err(RedisError::NxFail);
        }
        if xx && !is_live {
            return Err(RedisError::XxFail);
        }

        // Delete old data if type changed
        if let Some(ref m) = existing {
            if m.r#type != RedisType::String {
                self.delete_key_internal(db, key, Some(m));
            } else if m.expiry_ms > 0 {
                let old_ttl_key = encode_ttl_key(m.expiry_ms, db, key);
                self.storage.delete(old_ttl_key);
            }
        }

        let mut meta = RedisMetadata {
            r#type: RedisType::String,
            count: 1,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        };

        if expiry_ms > 0 {
            meta.expiry_ms = expiry_ms;
            let ttl_key = encode_ttl_key(expiry_ms, db, key);
            self.storage.put(ttl_key, vec![1u8]);
        }

        // Inline save_meta_inner to reuse `existing` (avoids a second get_meta_inner call).
        let old_version = existing.as_ref().map(|m| m.version).unwrap_or(0);
        meta.version = old_version + 1;

        // One WAL lock acquisition covers both the meta key and the string data key.
        let meta_key = encode_meta_key(db, key);
        let sk = encode_string_key(db, key);
        self.storage.put2(meta_key, meta.serialize(), sk, value.to_vec());

        Ok(())
    }

    /// MSET-optimized: acquire shard locks for the keys involved (not all 64 shards),
    /// and issue a single WAL batch for all meta + data writes.
    /// Semantics: plain SET (no NX/XX, no expiry).
    pub fn string_set_batch(&self, db: usize, pairs: &[(&[u8], &[u8])]) -> Result<(), RedisError> {
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| *k).collect();
        let _guard = self.shard_w_multi(&keys);

        // Single pass: clean up conflicts and collect WAL batch entries.
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pairs.len() * 2);
        for (key, value) in pairs {
            let existing = self.get_meta_inner(db, key);
            if let Some(ref m) = existing {
                if m.r#type != RedisType::String {
                    self.delete_key_internal(db, key, Some(m));
                } else if m.expiry_ms > 0 {
                    let old_ttl_key = encode_ttl_key(m.expiry_ms, db, key);
                    self.storage.delete(old_ttl_key);
                }
            }
            let old_version = existing.as_ref().map(|m| m.version).unwrap_or(0);
            let meta = RedisMetadata {
                r#type: RedisType::String,
                count: 1,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: old_version + 1,
            };
            batch.push((encode_meta_key(db, key), meta.serialize()));
            batch.push((encode_string_key(db, key), value.to_vec()));
        }
        self.storage.put_batch(&batch);
        Ok(())
    }

    pub fn string_get(&self, db: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_r(key);

        let meta = match self.get_meta_inner(db, key) {
            None => return Ok(None),
            Some(m) => m,
        };

        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            return Ok(None);
        }

        if meta.r#type != RedisType::String {
            return Err(RedisError::WrongType);
        }

        let sk = encode_string_key(db, key);
        Ok(self.storage.get(&sk))
    }

    pub fn string_incr(&self, db: usize, key: &[u8], delta: i64) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let current: i64 = if self.is_live_key_inner(db, key) {
            let meta = self.get_meta_inner(db, key).unwrap();
            if meta.r#type != RedisType::String {
                return Err(RedisError::WrongType);
            }
            let sk = encode_string_key(db, key);
            match self.storage.get(&sk) {
                None => 0,
                Some(v) => {
                    let s = std::str::from_utf8(&v).map_err(|_| RedisError::NotInteger)?;
                    s.trim().parse::<i64>().map_err(|_| RedisError::NotInteger)?
                }
            }
        } else {
            0
        };

        let new_val = current.checked_add(delta).ok_or(RedisError::Overflow)?;

        let mut meta = self.get_meta_inner(db, key).unwrap_or(RedisMetadata {
            r#type: RedisType::String,
            count: 1,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        });
        meta.r#type = RedisType::String;
        meta.count = 1;

        self.save_meta_inner(db, key, &meta);
        let sk = encode_string_key(db, key);
        self.storage.put(sk, new_val.to_string().into_bytes());

        Ok(new_val)
    }

    pub fn string_append(&self, db: usize, key: &[u8], to_append: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let existing = if self.is_live_key_inner(db, key) {
            let meta = self.get_meta_inner(db, key).unwrap();
            if meta.r#type != RedisType::String {
                return Err(RedisError::WrongType);
            }
            let sk = encode_string_key(db, key);
            self.storage.get(&sk).unwrap_or_default()
        } else {
            vec![]
        };

        let mut new_val = existing;
        new_val.extend_from_slice(to_append);
        let new_len = new_val.len() as i64;

        let mut meta = self.get_meta_inner(db, key).unwrap_or(RedisMetadata {
            r#type: RedisType::String,
            count: 1,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        });
        meta.r#type = RedisType::String;
        meta.count = 1;

        self.save_meta_inner(db, key, &meta);
        let sk = encode_string_key(db, key);
        self.storage.put(sk, new_val);

        Ok(new_len)
    }

    // ── Key commands ──────────────────────────────────────────────────────────

    pub fn delete(&self, db: usize, keys: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w_multi(keys);
        let mut count = 0i64;

        for &key in keys {
            if self.is_live_key_inner(db, key) {
                self.delete_key_internal(db, key, None);
                count += 1;
            } else if ext_type_registry::exists(db, key) {
                // Extended type (Stream, JSON, BF, CF, etc.) — just remove from registry.
                // The in-memory store cleanup is handled by each module's flush/delete hooks.
                ext_type_registry::unregister(db, key);
                count += 1;
            }
        }

        Ok(count)
    }

    pub fn exists(&self, db: usize, keys: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w_multi(keys);
        let mut count = 0i64;

        for &key in keys {
            if self.is_live_key_inner(db, key) || ext_type_registry::exists(db, key) {
                count += 1;
            }
        }

        Ok(count)
    }

    pub fn expire(&self, db: usize, key: &[u8], expiry_ms: i64) -> Result<bool, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(false);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        self.set_expiry_inner(db, key, &mut meta, expiry_ms);
        self.save_meta_inner(db, key, &meta);

        Ok(true)
    }

    pub fn expire_cond(&self, db: usize, key: &[u8], expiry_ms: i64, cond: u8) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);
        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }
        let mut meta = self.get_meta_inner(db, key).unwrap();
        let current_expiry = meta.expiry_ms;
        let should_set = match cond {
            0 => current_expiry == 0,                                      // NX: only if no expiry
            1 => current_expiry != 0,                                      // XX: only if has expiry
            2 => current_expiry != 0 && expiry_ms > current_expiry,        // GT: only if new > current
            3 => current_expiry == 0 || expiry_ms < current_expiry,        // LT: infinite = always set
            _ => true,
        };
        if !should_set {
            return Ok(0);
        }
        self.set_expiry_inner(db, key, &mut meta, expiry_ms);
        self.save_meta_inner(db, key, &meta);
        Ok(1)
    }

    pub fn ttl_secs(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        let meta = match self.get_meta_inner(db, key) {
            None => return Ok(-2),
            Some(m) => m,
        };

        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            return Ok(-2);
        }

        if meta.expiry_ms == 0 {
            return Ok(-1);
        }

        let remaining_ms = meta.expiry_ms - now_ms();
        if remaining_ms <= 0 {
            Ok(-2)
        } else {
            Ok((remaining_ms + 999) / 1000) // ceiling division
        }
    }

    pub fn ttl_ms(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        let meta = match self.get_meta_inner(db, key) {
            None => return Ok(-2),
            Some(m) => m,
        };

        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            return Ok(-2);
        }

        if meta.expiry_ms == 0 {
            return Ok(-1);
        }

        let remaining_ms = meta.expiry_ms - now_ms();
        if remaining_ms <= 0 {
            Ok(-2)
        } else {
            Ok(remaining_ms)
        }
    }

    pub fn expiretime_secs(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);
        match self.get_meta_inner(db, key) {
            None => Ok(-2),
            Some(m) => {
                if m.expiry_ms > 0 && m.expiry_ms <= now_ms() { return Ok(-2); }
                if m.expiry_ms == 0 { return Ok(-1); }
                Ok(m.expiry_ms / 1000)
            }
        }
    }

    pub fn expiretime_ms(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);
        match self.get_meta_inner(db, key) {
            None => Ok(-2),
            Some(m) => {
                if m.expiry_ms > 0 && m.expiry_ms <= now_ms() { return Ok(-2); }
                if m.expiry_ms == 0 { return Ok(-1); }
                Ok(m.expiry_ms)
            }
        }
    }

    pub fn keys(&self, db: usize, pattern: &str) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_r_all();

        // Scan all meta keys for this db
        // Meta key format: [TAG_META:1][db:1][keyLen:2BE][key]
        let prefix = vec![TAG_META, db as u8];
        let end_prefix = vec![TAG_META, db as u8 + 1];

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let now = now_ms();
        let mut result = Vec::new();

        for (encoded_key, value) in entries {
            if value.is_none() {
                continue; // tombstone
            }
            if encoded_key.len() < 4 {
                continue;
            }
            let key_len = u16::from_be_bytes(encoded_key[2..4].try_into().unwrap()) as usize;
            if encoded_key.len() < 4 + key_len {
                continue;
            }
            let user_key = &encoded_key[4..4 + key_len];

            // Check if expired
            if let Some(v) = &value {
                if v.len() >= 33 {
                    let meta = RedisMetadata::deserialize(v);
                    if meta.expiry_ms > 0 && meta.expiry_ms <= now {
                        continue;
                    }
                }
            }

            // Apply pattern matching
            if let Ok(s) = std::str::from_utf8(user_key) {
                if Self::glob_match(pattern, s) {
                    result.push(user_key.to_vec());
                }
            }
        }

        // Include keys from extended type registry (Stream, JSON, BF, CF, etc.)
        // Use a set for O(1) dedup instead of O(N) contains check
        let existing: std::collections::HashSet<Vec<u8>> = result.iter().cloned().collect();
        for ext_key in ext_type_registry::keys_for_db(db) {
            if !existing.contains(&ext_key) {
                if let Ok(s) = std::str::from_utf8(&ext_key) {
                    if Self::glob_match(pattern, s) {
                        result.push(ext_key);
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn scan(
        &self,
        db: usize,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<Vec<u8>>), RedisError> {
        let _guard = self.shard_r_all();

        let all_keys = self.get_all_live_keys(db);
        let total = all_keys.len();

        if total == 0 {
            return Ok((0, vec![]));
        }

        let start = cursor as usize;
        if start >= total {
            return Ok((0, vec![]));
        }

        let end = (start + count).min(total);
        let next_cursor = if end >= total { 0 } else { end as u64 };

        let mut result = Vec::new();
        for key in &all_keys[start..end] {
            let key_str = std::str::from_utf8(key).unwrap_or("");
            if let Some(pat) = pattern {
                if !Self::glob_match(pat, key_str) {
                    continue;
                }
            }
            result.push(key.clone());
        }

        Ok((next_cursor, result))
    }

    fn get_all_live_keys(&self, db: usize) -> Vec<Vec<u8>> {
        let prefix = vec![TAG_META, db as u8];
        let end_prefix = vec![TAG_META, db as u8 + 1];

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let now = now_ms();
        let mut result = Vec::new();

        for (encoded_key, value) in entries {
            let value = match value {
                Some(v) => v,
                None => continue,
            };
            if encoded_key.len() < 4 {
                continue;
            }
            let key_len = u16::from_be_bytes(encoded_key[2..4].try_into().unwrap()) as usize;
            if encoded_key.len() < 4 + key_len {
                continue;
            }
            let user_key = encoded_key[4..4 + key_len].to_vec();

            if value.len() >= 33 {
                let meta = RedisMetadata::deserialize(&value);
                if meta.expiry_ms > 0 && meta.expiry_ms <= now {
                    continue;
                }
            }

            result.push(user_key);
        }

        // Include extended keys (Stream, JSON, BF, CF, etc.) with O(1) dedup
        let existing: std::collections::HashSet<Vec<u8>> = result.iter().cloned().collect();
        for ext_key in ext_type_registry::keys_for_db(db) {
            if !existing.contains(&ext_key) {
                result.push(ext_key);
            }
        }

        result
    }

    pub fn key_type(&self, db: usize, key: &[u8]) -> Result<&'static str, RedisError> {
        let _guard = self.shard_r(key);

        let meta = match self.get_meta_inner(db, key) {
            None => {
                // Check extended type registry (for JSON, BF, CF, Stream, etc.)
                if let Some(t) = ext_type_registry::get_type(db, key) {
                    return Ok(t);
                }
                return Ok("none");
            }
            Some(m) => m,
        };

        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            return Ok("none");
        }

        Ok(match meta.r#type {
            RedisType::String => "string",
            RedisType::Hash => "hash",
            RedisType::List => "list",
            RedisType::Set => "set",
            RedisType::ZSet => "zset",
            RedisType::None => "none",
        })
    }

    pub fn key_count(&self, db: usize, key: &[u8]) -> Option<i64> {
        let _guard = self.shard_r(key);
        let meta = self.get_meta_inner(db, key)?;
        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            return None;
        }
        Some(meta.count)
    }

    pub fn key_version(&self, db: usize, key: &[u8]) -> Option<u64> {
        let _guard = self.shard_r(key);
        let meta = self.get_meta_inner(db, key)?;
        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            return None;
        }
        Some(meta.version)
    }

    pub fn persist(&self, db: usize, key: &[u8]) -> Result<bool, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(false);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.expiry_ms == 0 {
            return Ok(false);
        }

        self.set_expiry_inner(db, key, &mut meta, 0);
        self.save_meta_inner(db, key, &meta);

        Ok(true)
    }

    pub fn db_info(&self, db: usize) -> Result<(i64, i64), RedisError> {
        let _guard = self.shard_r_all();

        let all_keys = self.get_all_live_keys(db);
        let now = now_ms();
        let mut expires = 0i64;

        for key in &all_keys {
            let meta_key = encode_meta_key(db, key);
            if let Some(data) = self.storage.get(&meta_key) {
                if data.len() >= 33 {
                    let meta = RedisMetadata::deserialize(&data);
                    if meta.expiry_ms > 0 && meta.expiry_ms > now {
                        expires += 1;
                    }
                }
            }
        }

        // Extended keys (Stream, JSON, BF, CF, etc.) count toward db size but have no TTL
        let ext_count = ext_type_registry::keys_for_db(db).len() as i64;
        // Avoid double-counting (get_all_live_keys already includes ext keys)
        let _ = ext_count;

        Ok((all_keys.len() as i64, expires))
    }

    pub fn flush_all(&self) -> Result<(), RedisError> {
        let _guard = self.shard_w_all();

        for db in 0..self.num_dbs {
            self.flush_db_inner(db);
        }

        Ok(())
    }

    pub fn flush_db(&self, db: usize) -> Result<(), RedisError> {
        let _guard = self.shard_w_all();
        self.flush_db_inner(db);
        Ok(())
    }

    fn flush_db_inner(&self, db: usize) {
        // Collect ALL meta keys for this db, including expired keys that
        // get_all_live_keys would skip, so nothing is left behind.
        let prefix = vec![TAG_META, db as u8];
        let end_prefix = vec![TAG_META, db as u8 + 1];
        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));

        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            if encoded_key.len() < 4 {
                continue;
            }
            let key_len = u16::from_be_bytes(encoded_key[2..4].try_into().unwrap()) as usize;
            if encoded_key.len() < 4 + key_len {
                continue;
            }
            keys_to_delete.push(encoded_key[4..4 + key_len].to_vec());
        }

        for key in &keys_to_delete {
            self.delete_key_internal(db, key, None);
        }

        // Remove any orphaned TTL index entries that may remain (e.g. from
        // previously expired keys whose metadata was already cleaned up).
        let ttl_start = vec![TAG_TTL];
        let mut ttl_end = ttl_start.clone();
        ttl_end[0] = TAG_TTL + 1;
        let ttl_entries = self.storage.scan(Some(&ttl_start), Some(&ttl_end));
        for (k, _) in ttl_entries {
            if k.len() >= 10 && k[9] as usize == db {
                self.storage.delete(k);
            }
        }
    }

    // ── Hash commands ─────────────────────────────────────────────────────────

    pub fn hset(
        &self,
        db: usize,
        key: &[u8],
        pairs: &[(&[u8], &[u8])],
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let mut meta = self.get_meta_inner(db, key).unwrap_or(RedisMetadata {
            r#type: RedisType::Hash,
            count: 0,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        });

        if self.is_live_key_inner(db, key) && meta.r#type != RedisType::Hash {
            return Err(RedisError::WrongType);
        }

        if meta.expiry_ms > 0 && meta.expiry_ms <= now_ms() {
            // Key expired, reset
            meta = RedisMetadata {
                r#type: RedisType::Hash,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            };
        }

        meta.r#type = RedisType::Hash;
        let mut added = 0i64;

        for (field, value) in pairs {
            let hk = encode_hash_key(db, key, field);
            let existing = self.storage.get(&hk);
            if existing.is_none() {
                added += 1;
                meta.count += 1;
            }
            self.storage.put(hk, value.to_vec());
        }

        self.save_meta_inner(db, key, &meta);
        Ok(added)
    }

    pub fn hget(&self, db: usize, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Hash {
            return Err(RedisError::WrongType);
        }

        let hk = encode_hash_key(db, key, field);
        Ok(self.storage.get(&hk))
    }

    pub fn hdel(&self, db: usize, key: &[u8], fields: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Hash {
            return Err(RedisError::WrongType);
        }

        let mut deleted = 0i64;
        for &field in fields {
            let hk = encode_hash_key(db, key, field);
            if self.storage.get(&hk).is_some() {
                self.storage.delete(hk);
                deleted += 1;
                meta.count -= 1;
            }
        }

        if meta.count <= 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(deleted)
    }

    pub fn hgetall(&self, db: usize, key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Hash {
            return Err(RedisError::WrongType);
        }

        let prefix = encode_key_prefix(TAG_HASH, db, key);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let mut result = Vec::new();

        for (encoded_key, value) in entries {
            let value = match value {
                Some(v) => v,
                None => continue,
            };
            // Decode field from encoded key: [TAG_HASH:1][db:1][keyLen:2BE][key][fieldLen:2BE][field]
            let header_size = 1 + 1 + 2 + key.len();
            if encoded_key.len() < header_size + 2 {
                continue;
            }
            let field_len =
                u16::from_be_bytes(encoded_key[header_size..header_size + 2].try_into().unwrap())
                    as usize;
            if encoded_key.len() < header_size + 2 + field_len {
                continue;
            }
            let field = encoded_key[header_size + 2..header_size + 2 + field_len].to_vec();
            result.push((field, value));
        }

        Ok(result)
    }

    pub fn hexists(&self, db: usize, key: &[u8], field: &[u8]) -> Result<bool, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(false);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Hash {
            return Err(RedisError::WrongType);
        }

        let hk = encode_hash_key(db, key, field);
        Ok(self.storage.get(&hk).is_some())
    }

    pub fn hlen(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Hash {
            return Err(RedisError::WrongType);
        }

        Ok(meta.count)
    }

    // ── List commands ─────────────────────────────────────────────────────────

    pub fn lpush(&self, db: usize, key: &[u8], elements: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);
        self.push_inner(db, key, elements, true)
    }

    pub fn rpush(&self, db: usize, key: &[u8], elements: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);
        self.push_inner(db, key, elements, false)
    }

    fn push_inner(
        &self,
        db: usize,
        key: &[u8],
        elements: &[&[u8]],
        left: bool,
    ) -> Result<i64, RedisError> {
        let mut meta = if self.is_live_key_inner(db, key) {
            let m = self.get_meta_inner(db, key).unwrap();
            if m.r#type != RedisType::List {
                return Err(RedisError::WrongType);
            }
            m
        } else {
            RedisMetadata {
                r#type: RedisType::List,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            }
        };

        meta.r#type = RedisType::List;

        for &elem in elements {
            if left {
                meta.list_head -= 1;
                let lk = encode_list_key(db, key, meta.list_head);
                self.storage.put(lk, elem.to_vec());
            } else {
                let lk = encode_list_key(db, key, meta.list_tail);
                self.storage.put(lk, elem.to_vec());
                meta.list_tail += 1;
            }
            meta.count += 1;
        }

        self.save_meta_inner(db, key, &meta);
        Ok(meta.count)
    }

    pub fn lpop(&self, db: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);
        self.pop_inner(db, key, true)
    }

    pub fn rpop(&self, db: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);
        self.pop_inner(db, key, false)
    }

    fn pop_inner(&self, db: usize, key: &[u8], left: bool) -> Result<Option<Vec<u8>>, RedisError> {
        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        if meta.count == 0 {
            return Ok(None);
        }

        let (seq, next_head, next_tail) = if left {
            (meta.list_head, meta.list_head + 1, meta.list_tail)
        } else {
            (meta.list_tail - 1, meta.list_head, meta.list_tail - 1)
        };

        let lk = encode_list_key(db, key, seq);
        let value = self.storage.get(&lk);
        self.storage.delete(lk);

        meta.list_head = next_head;
        meta.list_tail = next_tail;
        meta.count -= 1;

        if meta.count == 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(value)
    }

    pub fn lrange(
        &self,
        db: usize,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        let count = meta.count;
        let head = meta.list_head;
        let tail = meta.list_tail;

        if count == 0 {
            return Ok(vec![]);
        }

        // Normalize indices
        let start = if start < 0 { (count + start).max(0) } else { start.min(count) };
        let stop = if stop < 0 { (count + stop).max(-1) } else { stop.min(count - 1) };

        if start > stop {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        for i in start..=stop {
            let seq = head + i;
            if seq >= tail {
                break;
            }
            let lk = encode_list_key(db, key, seq);
            if let Some(v) = self.storage.get(&lk) {
                result.push(v);
            }
        }

        Ok(result)
    }

    pub fn llen(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        // FIXED: use meta.count directly (O(1))
        Ok(meta.count)
    }

    // ── Set commands ──────────────────────────────────────────────────────────

    pub fn sadd(&self, db: usize, key: &[u8], members: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let mut meta = if self.is_live_key_inner(db, key) {
            let m = self.get_meta_inner(db, key).unwrap();
            if m.r#type != RedisType::Set {
                return Err(RedisError::WrongType);
            }
            m
        } else {
            RedisMetadata {
                r#type: RedisType::Set,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            }
        };

        meta.r#type = RedisType::Set;
        let mut added = 0i64;

        for &member in members {
            let sk = encode_set_key(db, key, member);
            if self.storage.get(&sk).is_none() {
                added += 1;
                meta.count += 1;
            }
            self.storage.put(sk, vec![1u8]);
        }

        self.save_meta_inner(db, key, &meta);
        Ok(added)
    }

    pub fn srem(&self, db: usize, key: &[u8], members: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Set {
            return Err(RedisError::WrongType);
        }

        let mut removed = 0i64;
        for &member in members {
            let sk = encode_set_key(db, key, member);
            if self.storage.get(&sk).is_some() {
                self.storage.delete(sk);
                removed += 1;
                meta.count -= 1;
            }
        }

        if meta.count <= 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(removed)
    }

    pub fn smembers(&self, db: usize, key: &[u8]) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Set {
            return Err(RedisError::WrongType);
        }

        let prefix = encode_key_prefix(TAG_SET, db, key);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let mut result = Vec::new();

        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            // Decode member: [TAG_SET:1][db:1][keyLen:2BE][key][memberLen:2BE][member]
            let header_size = 1 + 1 + 2 + key.len();
            if encoded_key.len() < header_size + 2 {
                continue;
            }
            let member_len =
                u16::from_be_bytes(encoded_key[header_size..header_size + 2].try_into().unwrap())
                    as usize;
            if encoded_key.len() < header_size + 2 + member_len {
                continue;
            }
            let member = encoded_key[header_size + 2..header_size + 2 + member_len].to_vec();
            result.push(member);
        }

        Ok(result)
    }

    pub fn sismember(&self, db: usize, key: &[u8], member: &[u8]) -> Result<bool, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(false);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Set {
            return Err(RedisError::WrongType);
        }

        let sk = encode_set_key(db, key, member);
        Ok(self.storage.get(&sk).is_some())
    }

    pub fn scard(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Set {
            return Err(RedisError::WrongType);
        }

        // FIXED: use meta.count directly (O(1))
        Ok(meta.count)
    }

    // ── Sorted Set commands ───────────────────────────────────────────────────

    pub fn zadd(
        &self,
        db: usize,
        key: &[u8],
        pairs: &[(f64, &[u8])],
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let mut meta = if self.is_live_key_inner(db, key) {
            let m = self.get_meta_inner(db, key).unwrap();
            if m.r#type != RedisType::ZSet {
                return Err(RedisError::WrongType);
            }
            m
        } else {
            RedisMetadata {
                r#type: RedisType::ZSet,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            }
        };

        meta.r#type = RedisType::ZSet;
        let mut added = 0i64;

        for &(score, member) in pairs {
            let member_key = encode_zset_member_key(db, key, member);

            // Check if member already exists
            if let Some(old_score_bytes) = self.storage.get(&member_key) {
                // Update: remove old score key
                if old_score_bytes.len() == 8 {
                    let old_score = decode_sortable_double(&old_score_bytes);
                    let old_score_key = encode_zset_score_key(db, key, old_score, member);
                    self.storage.delete(old_score_key);
                }
            } else {
                added += 1;
                meta.count += 1;
            }

            // Write new member key (value = sortable score bytes)
            let score_bytes = encode_sortable_double(score);
            self.storage.put(member_key, score_bytes.to_vec());

            // Write score key
            let score_key = encode_zset_score_key(db, key, score, member);
            self.storage.put(score_key, vec![1u8]);
        }

        self.save_meta_inner(db, key, &meta);
        Ok(added)
    }

    pub fn zrem(&self, db: usize, key: &[u8], members: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let mut removed = 0i64;
        for &member in members {
            let member_key = encode_zset_member_key(db, key, member);
            if let Some(score_bytes) = self.storage.get(&member_key) {
                if score_bytes.len() == 8 {
                    let score = decode_sortable_double(&score_bytes);
                    let score_key = encode_zset_score_key(db, key, score, member);
                    self.storage.delete(score_key);
                }
                self.storage.delete(member_key);
                removed += 1;
                meta.count -= 1;
            }
        }

        if meta.count <= 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(removed)
    }

    /// Returns members with scores in score order
    pub fn zrange(
        &self,
        db: usize,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<(Vec<u8>, f64)>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        // Scan score-ordered keys: [TAG_ZSET:1][db:1][keyLen:2BE][key][0x02][score:8][memberLen:2BE][member]
        let mut prefix = encode_key_prefix(TAG_ZSET, db, key);
        prefix.push(0x02);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let mut all: Vec<(Vec<u8>, f64)> = Vec::new();

        let base_len = prefix.len(); // [tag][db][keyLen:2][key][0x02]
        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            // Parse: after prefix comes [score:8][memberLen:2BE][member]
            if encoded_key.len() < base_len + 8 + 2 {
                continue;
            }
            let score = decode_sortable_double(&encoded_key[base_len..base_len + 8]);
            let member_len = u16::from_be_bytes(
                encoded_key[base_len + 8..base_len + 10].try_into().unwrap(),
            ) as usize;
            if encoded_key.len() < base_len + 10 + member_len {
                continue;
            }
            let member = encoded_key[base_len + 10..base_len + 10 + member_len].to_vec();
            all.push((member, score));
        }

        let count = all.len() as i64;
        if count == 0 {
            return Ok(vec![]);
        }

        let start = if start < 0 { (count + start).max(0) } else { start.min(count) };
        let stop = if stop < 0 { (count + stop).max(-1) } else { stop.min(count - 1) };

        if start > stop {
            return Ok(vec![]);
        }

        Ok(all[start as usize..=stop as usize].to_vec())
    }

    pub fn zscore(&self, db: usize, key: &[u8], member: &[u8]) -> Result<Option<f64>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let member_key = encode_zset_member_key(db, key, member);
        match self.storage.get(&member_key) {
            None => Ok(None),
            Some(score_bytes) => {
                if score_bytes.len() == 8 {
                    Ok(Some(decode_sortable_double(&score_bytes)))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn zcard(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        // FIXED: use meta.count directly (O(1))
        Ok(meta.count)
    }

    pub fn zrank(&self, db: usize, key: &[u8], member: &[u8]) -> Result<Option<i64>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        // Get the member's score
        let member_key = encode_zset_member_key(db, key, member);
        let score_bytes = match self.storage.get(&member_key) {
            None => return Ok(None),
            Some(b) => b,
        };

        if score_bytes.len() != 8 {
            return Ok(None);
        }

        let target_score = decode_sortable_double(&score_bytes);

        // Count members with score < target, or score == target and member < this member
        let mut prefix = encode_key_prefix(TAG_ZSET, db, key);
        prefix.push(0x02);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let base_len = prefix.len();
        let mut rank = 0i64;

        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            if encoded_key.len() < base_len + 8 + 2 {
                continue;
            }
            let entry_score = decode_sortable_double(&encoded_key[base_len..base_len + 8]);
            let entry_member_len = u16::from_be_bytes(
                encoded_key[base_len + 8..base_len + 10].try_into().unwrap(),
            ) as usize;
            if encoded_key.len() < base_len + 10 + entry_member_len {
                continue;
            }
            let entry_member = &encoded_key[base_len + 10..base_len + 10 + entry_member_len];

            if entry_score < target_score {
                rank += 1;
            } else if entry_score == target_score && entry_member < member {
                rank += 1;
            } else if entry_member == member {
                break;
            }
        }

        Ok(Some(rank))
    }

    // ── String extended commands ──────────────────────────────────────────────

    pub fn string_get_raw(&self, db: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        // Same as string_get but also returns the TTL metadata
        self.string_get(db, key)
    }

    pub fn string_getdel(&self, db: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::String {
            return Err(RedisError::WrongType);
        }

        let sk = encode_string_key(db, key);
        let val = self.storage.get(&sk);
        self.delete_key_internal(db, key, Some(&meta));
        Ok(val)
    }

    /// Returns (old_value, current_expiry_ms)
    pub fn string_getex(
        &self,
        db: usize,
        key: &[u8],
        new_expiry_ms: Option<i64>, // None = keep current, Some(0) = persist, Some(x) = new expiry
    ) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::String {
            return Err(RedisError::WrongType);
        }

        let sk = encode_string_key(db, key);
        let val = self.storage.get(&sk);

        if let Some(new_exp) = new_expiry_ms {
            self.set_expiry_inner(db, key, &mut meta, new_exp);
            self.save_meta_inner(db, key, &meta);
        }

        Ok(val)
    }

    pub fn string_setrange(
        &self,
        db: usize,
        key: &[u8],
        offset: usize,
        value: &[u8],
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let existing: Vec<u8> = if self.is_live_key_inner(db, key) {
            let meta = self.get_meta_inner(db, key).unwrap();
            if meta.r#type != RedisType::String {
                return Err(RedisError::WrongType);
            }
            let sk = encode_string_key(db, key);
            self.storage.get(&sk).unwrap_or_default()
        } else {
            vec![]
        };

        let new_len = offset + value.len();
        let mut new_val = existing;
        if new_val.len() < new_len {
            new_val.resize(new_len, 0u8);
        }
        new_val[offset..offset + value.len()].copy_from_slice(value);
        let result_len = new_val.len() as i64;

        let mut meta = self.get_meta_inner(db, key).unwrap_or(RedisMetadata {
            r#type: RedisType::String,
            count: 1,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        });
        meta.r#type = RedisType::String;
        meta.count = 1;

        self.save_meta_inner(db, key, &meta);
        let sk = encode_string_key(db, key);
        self.storage.put(sk, new_val);

        Ok(result_len)
    }

    pub fn string_set_get_old(
        &self,
        db: usize,
        key: &[u8],
        value: &[u8],
        expiry_ms: i64,
        nx: bool,
        xx: bool,
        keepttl: bool,
    ) -> Result<(bool, Option<Vec<u8>>), RedisError> {
        let _guard = self.shard_w(key);

        let existing = self.get_meta_inner(db, key);
        let is_live = match &existing {
            None => false,
            Some(m) => !(m.expiry_ms > 0 && m.expiry_ms <= now_ms()),
        };

        // Get old value if string type
        let old_value: Option<Vec<u8>> = if is_live {
            if let Some(ref m) = existing {
                if m.r#type == RedisType::String {
                    let sk = encode_string_key(db, key);
                    self.storage.get(&sk)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if nx && is_live {
            return Ok((false, old_value));
        }
        if xx && !is_live {
            return Ok((false, old_value));
        }

        // Delete old data if type changed
        if let Some(ref m) = existing {
            if m.r#type != RedisType::String {
                self.delete_key_internal(db, key, Some(m));
            } else if m.expiry_ms > 0 {
                let old_ttl_key = encode_ttl_key(m.expiry_ms, db, key);
                self.storage.delete(old_ttl_key);
            }
        }

        let effective_expiry = if keepttl {
            existing.as_ref().map(|m| m.expiry_ms).unwrap_or(0)
        } else {
            expiry_ms
        };

        let mut meta = RedisMetadata {
            r#type: RedisType::String,
            count: 1,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        };

        if effective_expiry > 0 {
            meta.expiry_ms = effective_expiry;
            let ttl_key = encode_ttl_key(effective_expiry, db, key);
            self.storage.put(ttl_key, vec![1u8]);
        }

        self.save_meta_inner(db, key, &meta);
        let sk = encode_string_key(db, key);
        self.storage.put(sk, value.to_vec());

        Ok((true, old_value))
    }

    pub fn string_get_expiry_ms(&self, db: usize, key: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);
        match self.get_meta_inner(db, key) {
            None => Ok(-2),
            Some(m) => {
                if m.expiry_ms > 0 && m.expiry_ms <= now_ms() {
                    Ok(-2)
                } else {
                    Ok(m.expiry_ms)
                }
            }
        }
    }

    pub fn msetnx(&self, db: usize, pairs: &[(&[u8], &[u8])]) -> Result<i64, RedisError> {
        let _guard = self.shard_w_all();

        // Check none exist
        for &(key, _) in pairs {
            if self.is_live_key_inner(db, key) {
                return Ok(0);
            }
        }

        // Set all
        for &(key, value) in pairs {
            let meta = RedisMetadata {
                r#type: RedisType::String,
                count: 1,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            };
            self.save_meta_inner(db, key, &meta);
            let sk = encode_string_key(db, key);
            self.storage.put(sk, value.to_vec());
        }

        Ok(1)
    }

    // ── Hash extended commands ────────────────────────────────────────────────

    pub fn hsetnx(&self, db: usize, key: &[u8], field: &[u8], value: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        let mut meta = if self.is_live_key_inner(db, key) {
            let m = self.get_meta_inner(db, key).unwrap();
            if m.r#type != RedisType::Hash {
                return Err(RedisError::WrongType);
            }
            m
        } else {
            RedisMetadata {
                r#type: RedisType::Hash,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            }
        };

        let hk = encode_hash_key(db, key, field);
        if self.storage.get(&hk).is_some() {
            return Ok(0);
        }

        meta.r#type = RedisType::Hash;
        meta.count += 1;
        self.storage.put(hk, value.to_vec());
        self.save_meta_inner(db, key, &meta);
        Ok(1)
    }

    // ── List extended commands ────────────────────────────────────────────────

    pub fn linsert(
        &self,
        db: usize,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        element: &[u8],
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        // Get all items
        let items = self.lrange_inner(db, key, &meta);

        // Find pivot
        let pivot_idx = match items.iter().position(|(_, v)| v.as_slice() == pivot) {
            None => return Ok(-1),
            Some(i) => i,
        };

        let insert_before = if before { pivot_idx } else { pivot_idx + 1 };

        // Rebuild list by reinserting after trimming
        // Simple approach: delete all, then re-insert
        let prefix = encode_key_prefix(TAG_LIST, db, key);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);
        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        for (k, _) in entries {
            self.storage.delete(k);
        }

        // New sequence starts fresh
        let new_head: i64 = 0;
        let mut new_tail: i64 = 0;
        let mut new_count: i64 = 0;

        for (i, (_, v)) in items.iter().enumerate() {
            if i == insert_before {
                let lk = encode_list_key(db, key, new_tail);
                self.storage.put(lk, element.to_vec());
                new_tail += 1;
                new_count += 1;
            }
            let lk = encode_list_key(db, key, new_tail);
            self.storage.put(lk, v.clone());
            new_tail += 1;
            new_count += 1;
        }
        if insert_before == items.len() {
            let lk = encode_list_key(db, key, new_tail);
            self.storage.put(lk, element.to_vec());
            new_tail += 1;
            new_count += 1;
        }

        meta.list_head = new_head;
        meta.list_tail = new_tail;
        meta.count = new_count;
        self.save_meta_inner(db, key, &meta);

        Ok(new_count)
    }

    fn lrange_inner(&self, db: usize, key: &[u8], meta: &RedisMetadata) -> Vec<(i64, Vec<u8>)> {
        let mut result = Vec::new();
        for seq in meta.list_head..meta.list_tail {
            let lk = encode_list_key(db, key, seq);
            if let Some(v) = self.storage.get(&lk) {
                result.push((seq, v));
            }
        }
        result
    }

    pub fn lset(&self, db: usize, key: &[u8], index: i64, element: &[u8]) -> Result<(), RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Err(RedisError::Other("ERR no such key".to_string()));
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        let count = meta.count;
        let normalized = if index < 0 { count + index } else { index };
        if normalized < 0 || normalized >= count {
            return Err(RedisError::Other("ERR index out of range".to_string()));
        }

        let seq = meta.list_head + normalized;
        let lk = encode_list_key(db, key, seq);
        self.storage.put(lk, element.to_vec());
        Ok(())
    }

    pub fn lrem(&self, db: usize, key: &[u8], count: i64, element: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        let items = self.lrange_inner(db, key, &meta);
        let mut to_remove: Vec<i64> = Vec::new();

        if count > 0 {
            // Remove from head, up to count
            let mut removed = 0i64;
            for (seq, v) in &items {
                if removed < count && v.as_slice() == element {
                    to_remove.push(*seq);
                    removed += 1;
                }
            }
        } else if count < 0 {
            // Remove from tail, up to |count|
            let limit = (-count) as i64;
            let mut removed = 0i64;
            for (seq, v) in items.iter().rev() {
                if removed < limit && v.as_slice() == element {
                    to_remove.push(*seq);
                    removed += 1;
                }
            }
        } else {
            // Remove all
            for (seq, v) in &items {
                if v.as_slice() == element {
                    to_remove.push(*seq);
                }
            }
        }

        let removed_count = to_remove.len() as i64;
        if removed_count == 0 {
            return Ok(0);
        }

        // Delete removed entries
        for seq in &to_remove {
            let lk = encode_list_key(db, key, *seq);
            self.storage.delete(lk);
        }

        // Rebuild remaining items
        let remaining: Vec<Vec<u8>> = items
            .into_iter()
            .filter(|(seq, _)| !to_remove.contains(seq))
            .map(|(_, v)| v)
            .collect();

        // Clear and rebuild
        let prefix = encode_key_prefix(TAG_LIST, db, key);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);
        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        for (k, _) in entries {
            self.storage.delete(k);
        }

        meta.list_head = 0;
        meta.list_tail = remaining.len() as i64;
        meta.count = remaining.len() as i64;

        for (i, v) in remaining.iter().enumerate() {
            let lk = encode_list_key(db, key, i as i64);
            self.storage.put(lk, v.clone());
        }

        if meta.count == 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(removed_count)
    }

    pub fn ltrim(&self, db: usize, key: &[u8], start: i64, stop: i64) -> Result<(), RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(());
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        let count = meta.count;
        let norm_start = if start < 0 { (count + start).max(0) } else { start.min(count) };
        let norm_stop = if stop < 0 { (count + stop).max(-1) } else { stop.min(count - 1) };

        let items = self.lrange_inner(db, key, &meta);

        // Clear all items
        let prefix = encode_key_prefix(TAG_LIST, db, key);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);
        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        for (k, _) in entries {
            self.storage.delete(k);
        }

        if norm_start > norm_stop {
            self.delete_key_internal(db, key, Some(&meta));
            return Ok(());
        }

        // Re-insert trimmed items
        let trimmed: Vec<Vec<u8>> = items
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i as i64 >= norm_start && *i as i64 <= norm_stop)
            .map(|(_, (_, v))| v)
            .collect();

        meta.list_head = 0;
        meta.list_tail = trimmed.len() as i64;
        meta.count = trimmed.len() as i64;

        for (i, v) in trimmed.iter().enumerate() {
            let lk = encode_list_key(db, key, i as i64);
            self.storage.put(lk, v.clone());
        }

        if meta.count == 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(())
    }

    pub fn lmove(
        &self,
        db: usize,
        src: &[u8],
        dst: &[u8],
        src_left: bool,
        dst_left: bool,
    ) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_w_multi(&[src, dst]);

        if !self.is_live_key_inner(db, src) {
            return Ok(None);
        }

        let src_meta = self.get_meta_inner(db, src).unwrap();
        if src_meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        // Pop from source
        let element = self.pop_inner(db, src, src_left)?;
        let element = match element {
            None => return Ok(None),
            Some(v) => v,
        };

        // Push to destination
        let dst_meta_exists = self.is_live_key_inner(db, dst);
        if dst_meta_exists {
            let dm = self.get_meta_inner(db, dst).unwrap();
            if dm.r#type != RedisType::List {
                return Err(RedisError::WrongType);
            }
        }

        let elem_slice: &[u8] = &element;
        self.push_inner(db, dst, &[elem_slice], dst_left)?;

        Ok(Some(element))
    }

    pub fn lpos(
        &self,
        db: usize,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<i64>,
        maxlen: i64,
    ) -> Result<Vec<i64>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::List {
            return Err(RedisError::WrongType);
        }

        let items = self.lrange_inner(db, key, &meta);
        let list_len = items.len();

        let mut positions: Vec<i64> = Vec::new();
        let max_count = count.unwrap_or(1);

        if rank >= 0 || rank == 0 {
            // Forward scan
            let effective_rank = if rank == 0 { 1 } else { rank };
            let mut rank_counter = 0i64;
            let scan_limit = if maxlen > 0 { maxlen as usize } else { list_len };

            for (i, (_, v)) in items.iter().enumerate().take(scan_limit) {
                if v.as_slice() == element {
                    rank_counter += 1;
                    if rank_counter >= effective_rank {
                        positions.push(i as i64);
                        if count.is_some() {
                            if max_count > 0 && positions.len() as i64 >= max_count {
                                break;
                            }
                        } else {
                            break; // without COUNT, return first match
                        }
                    }
                }
            }
        } else {
            // Reverse scan
            let effective_rank = -rank;
            let mut rank_counter = 0i64;
            let scan_limit = if maxlen > 0 { maxlen as usize } else { list_len };

            for (i, (_, v)) in items.iter().enumerate().rev().take(scan_limit) {
                if v.as_slice() == element {
                    rank_counter += 1;
                    if rank_counter >= effective_rank {
                        positions.push(i as i64);
                        if count.is_some() {
                            if max_count > 0 && positions.len() as i64 >= max_count {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        Ok(positions)
    }

    pub fn lpop_count(&self, db: usize, key: &[u8], count: usize) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);
        let mut results = Vec::new();
        for _ in 0..count {
            match self.pop_inner(db, key, true)? {
                Some(v) => results.push(v),
                None => break,
            }
        }
        Ok(results)
    }

    pub fn rpop_count(&self, db: usize, key: &[u8], count: usize) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);
        let mut results = Vec::new();
        for _ in 0..count {
            match self.pop_inner(db, key, false)? {
                Some(v) => results.push(v),
                None => break,
            }
        }
        Ok(results)
    }

    // ── Set extended commands ─────────────────────────────────────────────────

    pub fn smembers_vec(&self, db: usize, key: &[u8]) -> Result<Vec<Vec<u8>>, RedisError> {
        self.smembers(db, key)
    }

    pub fn smove(
        &self,
        db: usize,
        src: &[u8],
        dst: &[u8],
        member: &[u8],
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w_multi(&[src, dst]);

        if !self.is_live_key_inner(db, src) {
            return Ok(0);
        }

        let mut src_meta = self.get_meta_inner(db, src).unwrap();
        if src_meta.r#type != RedisType::Set {
            return Err(RedisError::WrongType);
        }

        let src_sk = encode_set_key(db, src, member);
        if self.storage.get(&src_sk).is_none() {
            return Ok(0);
        }

        // Check dst type if exists
        if self.is_live_key_inner(db, dst) {
            let dm = self.get_meta_inner(db, dst).unwrap();
            if dm.r#type != RedisType::Set {
                return Err(RedisError::WrongType);
            }
        }

        // Remove from src
        self.storage.delete(src_sk);
        src_meta.count -= 1;
        if src_meta.count <= 0 {
            self.delete_key_internal(db, src, Some(&src_meta));
        } else {
            self.save_meta_inner(db, src, &src_meta);
        }

        // Add to dst
        let dst_sk = encode_set_key(db, dst, member);
        let mut dst_meta = if self.is_live_key_inner(db, dst) {
            self.get_meta_inner(db, dst).unwrap()
        } else {
            RedisMetadata {
                r#type: RedisType::Set,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            }
        };

        if self.storage.get(&dst_sk).is_none() {
            dst_meta.count += 1;
        }
        dst_meta.r#type = RedisType::Set;
        self.storage.put(dst_sk, vec![1u8]);
        self.save_meta_inner(db, dst, &dst_meta);

        Ok(1)
    }

    pub fn spop(&self, db: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(None);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::Set {
            return Err(RedisError::WrongType);
        }

        let prefix = encode_key_prefix(TAG_SET, db, key);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        // Just pick the first one (quasi-random in LSM)
        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            let header_size = 1 + 1 + 2 + key.len();
            if encoded_key.len() < header_size + 2 {
                continue;
            }
            let member_len =
                u16::from_be_bytes(encoded_key[header_size..header_size + 2].try_into().unwrap())
                    as usize;
            if encoded_key.len() < header_size + 2 + member_len {
                continue;
            }
            let member = encoded_key[header_size + 2..header_size + 2 + member_len].to_vec();

            self.storage.delete(encoded_key);
            meta.count -= 1;

            if meta.count <= 0 {
                self.delete_key_internal(db, key, Some(&meta));
            } else {
                self.save_meta_inner(db, key, &meta);
            }

            return Ok(Some(member));
        }

        Ok(None)
    }

    // ── ZSet extended commands ────────────────────────────────────────────────

    /// zadd with full flag support
    pub fn zadd_flags(
        &self,
        db: usize,
        key: &[u8],
        pairs: &[(f64, &[u8])],
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> Result<(i64, Option<f64>), RedisError> {
        let _guard = self.shard_w(key);

        let mut meta = if self.is_live_key_inner(db, key) {
            let m = self.get_meta_inner(db, key).unwrap();
            if m.r#type != RedisType::ZSet {
                return Err(RedisError::WrongType);
            }
            m
        } else {
            RedisMetadata {
                r#type: RedisType::ZSet,
                count: 0,
                expiry_ms: 0,
                list_head: 0,
                list_tail: 0,
                version: 0,
            }
        };
        meta.r#type = RedisType::ZSet;

        let mut added = 0i64;
        let mut changed = 0i64;
        let mut last_score: Option<f64> = None;

        for &(mut score, member) in pairs {
            let member_key = encode_zset_member_key(db, key, member);

            if let Some(old_score_bytes) = self.storage.get(&member_key) {
                // Member exists
                if nx {
                    // NX: don't update existing
                    if incr { last_score = Some(decode_sortable_double(&old_score_bytes)); }
                    continue;
                }

                let old_score = if old_score_bytes.len() == 8 {
                    decode_sortable_double(&old_score_bytes)
                } else {
                    0.0
                };

                if incr {
                    score = old_score + score;
                }

                // GT/LT check
                if gt && score <= old_score {
                    if incr { last_score = Some(old_score); }
                    continue;
                }
                if lt && score >= old_score {
                    if incr { last_score = Some(old_score); }
                    continue;
                }

                if (score - old_score).abs() > f64::EPSILON || score != old_score {
                    // Remove old score key
                    let old_score_key = encode_zset_score_key(db, key, old_score, member);
                    self.storage.delete(old_score_key);

                    let score_bytes = encode_sortable_double(score);
                    self.storage.put(member_key, score_bytes.to_vec());
                    let score_key = encode_zset_score_key(db, key, score, member);
                    self.storage.put(score_key, vec![1u8]);
                    changed += 1;
                }
                last_score = Some(score);
            } else {
                // Member doesn't exist
                if xx {
                    // XX: only update existing
                    last_score = None;
                    continue;
                }
                if gt || lt {
                    // GT/LT imply XX for non-existing members
                    // Actually in Redis, GT/LT don't add new members when used together with NX restriction behavior
                    // But alone they DO add new members
                }

                if incr {
                    // INCR + new member: add with score
                }

                let score_bytes = encode_sortable_double(score);
                self.storage.put(member_key, score_bytes.to_vec());
                let score_key = encode_zset_score_key(db, key, score, member);
                self.storage.put(score_key, vec![1u8]);
                added += 1;
                meta.count += 1;
                last_score = Some(score);
            }
        }

        self.save_meta_inner(db, key, &meta);

        let result = if ch { added + changed } else { added };
        Ok((result, last_score))
    }

    /// Scan score-ordered entries and return all (member, score)
    pub fn zrange_all(&self, db: usize, key: &[u8]) -> Result<Vec<(Vec<u8>, f64)>, RedisError> {
        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let mut prefix = encode_key_prefix(TAG_ZSET, db, key);
        prefix.push(0x02);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let base_len = prefix.len();
        let mut all: Vec<(Vec<u8>, f64)> = Vec::new();

        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            if encoded_key.len() < base_len + 8 + 2 {
                continue;
            }
            let score = decode_sortable_double(&encoded_key[base_len..base_len + 8]);
            let member_len = u16::from_be_bytes(
                encoded_key[base_len + 8..base_len + 10].try_into().unwrap(),
            ) as usize;
            if encoded_key.len() < base_len + 10 + member_len {
                continue;
            }
            let member = encoded_key[base_len + 10..base_len + 10 + member_len].to_vec();
            all.push((member, score));
        }

        Ok(all)
    }

    pub fn zpopmin(&self, db: usize, key: &[u8], count: i64) -> Result<Vec<(Vec<u8>, f64)>, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let to_pop: Vec<(Vec<u8>, f64)> = all.into_iter().take(count as usize).collect();

        let members: Vec<&[u8]> = to_pop.iter().map(|(m, _)| m.as_slice()).collect();
        self.zrem_inner(db, key, &members)?;

        Ok(to_pop)
    }

    pub fn zpopmax(&self, db: usize, key: &[u8], count: i64) -> Result<Vec<(Vec<u8>, f64)>, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let mut all = self.zrange_all_inner(db, key);
        all.reverse();
        let to_pop: Vec<(Vec<u8>, f64)> = all.into_iter().take(count as usize).collect();

        let members: Vec<&[u8]> = to_pop.iter().map(|(m, _)| m.as_slice()).collect();
        self.zrem_inner(db, key, &members)?;

        Ok(to_pop)
    }

    fn zrange_all_inner(&self, db: usize, key: &[u8]) -> Vec<(Vec<u8>, f64)> {
        let mut prefix = encode_key_prefix(TAG_ZSET, db, key);
        prefix.push(0x02);
        let mut end_prefix = prefix.clone();
        increment_last_byte(&mut end_prefix);

        let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
        let base_len = prefix.len();
        let mut all: Vec<(Vec<u8>, f64)> = Vec::new();

        for (encoded_key, value) in entries {
            if value.is_none() {
                continue;
            }
            if encoded_key.len() < base_len + 8 + 2 {
                continue;
            }
            let score = decode_sortable_double(&encoded_key[base_len..base_len + 8]);
            let member_len = u16::from_be_bytes(
                encoded_key[base_len + 8..base_len + 10].try_into().unwrap(),
            ) as usize;
            if encoded_key.len() < base_len + 10 + member_len {
                continue;
            }
            let member = encoded_key[base_len + 10..base_len + 10 + member_len].to_vec();
            all.push((member, score));
        }

        all
    }

    fn zrem_inner(&self, db: usize, key: &[u8], members: &[&[u8]]) -> Result<i64, RedisError> {
        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let mut meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let mut removed = 0i64;
        for &member in members {
            let member_key = encode_zset_member_key(db, key, member);
            if let Some(score_bytes) = self.storage.get(&member_key) {
                if score_bytes.len() == 8 {
                    let score = decode_sortable_double(&score_bytes);
                    let score_key = encode_zset_score_key(db, key, score, member);
                    self.storage.delete(score_key);
                }
                self.storage.delete(member_key);
                removed += 1;
                meta.count -= 1;
            }
        }

        if meta.count <= 0 {
            self.delete_key_internal(db, key, Some(&meta));
        } else {
            self.save_meta_inner(db, key, &meta);
        }

        Ok(removed)
    }

    pub fn zrangebyscore(
        &self,
        db: usize,
        key: &[u8],
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<(Vec<u8>, f64)>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let mut result: Vec<(Vec<u8>, f64)> = all
            .into_iter()
            .filter(|(_, s)| {
                let lo = if min_exclusive { *s > min } else { *s >= min };
                let hi = if max_exclusive { *s < max } else { *s <= max };
                lo && hi
            })
            .collect();

        if offset > 0 {
            let off = offset as usize;
            if off >= result.len() {
                return Ok(vec![]);
            }
            result = result[off..].to_vec();
        }
        if limit >= 0 && limit < result.len() as i64 {
            result.truncate(limit as usize);
        }

        Ok(result)
    }

    pub fn zrangebylex(
        &self,
        db: usize,
        key: &[u8],
        min: &[u8],
        max: &[u8],
        min_inclusive: bool,
        max_inclusive: bool,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let min_is_minus = min == b"-";
        let max_is_plus = max == b"+";

        let mut result: Vec<Vec<u8>> = all
            .into_iter()
            .filter(|(member, _)| {
                let lo = if min_is_minus {
                    true
                } else if min_inclusive {
                    member.as_slice() >= min
                } else {
                    member.as_slice() > min
                };
                let hi = if max_is_plus {
                    true
                } else if max_inclusive {
                    member.as_slice() <= max
                } else {
                    member.as_slice() < max
                };
                lo && hi
            })
            .map(|(m, _)| m)
            .collect();

        if offset > 0 {
            let off = offset as usize;
            if off >= result.len() {
                return Ok(vec![]);
            }
            result = result[off..].to_vec();
        }
        if limit >= 0 && limit < result.len() as i64 {
            result.truncate(limit as usize);
        }

        Ok(result)
    }

    pub fn zremrangebyscore(
        &self,
        db: usize,
        key: &[u8],
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let to_remove: Vec<&[u8]> = all
            .iter()
            .filter(|(_, s)| {
                let lo = if min_exclusive { *s > min } else { *s >= min };
                let hi = if max_exclusive { *s < max } else { *s <= max };
                lo && hi
            })
            .map(|(m, _)| m.as_slice())
            .collect();

        self.zrem_inner(db, key, &to_remove)
    }

    pub fn zremrangebylex(
        &self,
        db: usize,
        key: &[u8],
        min: &[u8],
        max: &[u8],
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let min_is_minus = min == b"-";
        let max_is_plus = max == b"+";

        let to_remove: Vec<&[u8]> = all
            .iter()
            .filter(|(member, _)| {
                let lo = if min_is_minus {
                    true
                } else if min_inclusive {
                    member.as_slice() >= min
                } else {
                    member.as_slice() > min
                };
                let hi = if max_is_plus {
                    true
                } else if max_inclusive {
                    member.as_slice() <= max
                } else {
                    member.as_slice() < max
                };
                lo && hi
            })
            .map(|(m, _)| m.as_slice())
            .collect();

        self.zrem_inner(db, key, &to_remove)
    }

    pub fn zremrangebyrank(
        &self,
        db: usize,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let count = all.len() as i64;
        let norm_start = if start < 0 { (count + start).max(0) } else { start.min(count) };
        let norm_stop = if stop < 0 { (count + stop).max(-1) } else { stop.min(count - 1) };

        if norm_start > norm_stop {
            return Ok(0);
        }

        let to_remove: Vec<&[u8]> = all[norm_start as usize..=norm_stop as usize]
            .iter()
            .map(|(m, _)| m.as_slice())
            .collect();

        self.zrem_inner(db, key, &to_remove)
    }

    pub fn zcount(
        &self,
        db: usize,
        key: &[u8],
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let count = all
            .iter()
            .filter(|(_, s)| {
                let lo = if min_exclusive { *s > min } else { *s >= min };
                let hi = if max_exclusive { *s < max } else { *s <= max };
                lo && hi
            })
            .count() as i64;

        Ok(count)
    }

    pub fn zlexcount(
        &self,
        db: usize,
        key: &[u8],
        min: &[u8],
        max: &[u8],
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Result<i64, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(db, key).unwrap();
        if meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }

        let all = self.zrange_all_inner(db, key);
        let min_is_minus = min == b"-";
        let max_is_plus = max == b"+";

        let count = all
            .iter()
            .filter(|(member, _)| {
                let lo = if min_is_minus {
                    true
                } else if min_inclusive {
                    member.as_slice() >= min
                } else {
                    member.as_slice() > min
                };
                let hi = if max_is_plus {
                    true
                } else if max_inclusive {
                    member.as_slice() <= max
                } else {
                    member.as_slice() < max
                };
                lo && hi
            })
            .count() as i64;

        Ok(count)
    }

    pub fn zunionstore(
        &self,
        db: usize,
        dst: &[u8],
        keys: &[&[u8]],
        weights: &[f64],
        aggregate: &str,
    ) -> Result<i64, RedisError> {
        let mut all_keys: Vec<&[u8]> = vec![dst];
        all_keys.extend_from_slice(keys);
        let _guard = self.shard_w_multi(&all_keys);

        use std::collections::HashMap;
        let mut combined: HashMap<Vec<u8>, f64> = HashMap::new();

        for (i, &key) in keys.iter().enumerate() {
            if !self.is_live_key_inner(db, key) {
                continue;
            }
            let meta = self.get_meta_inner(db, key).unwrap();
            if meta.r#type != RedisType::ZSet {
                return Err(RedisError::WrongType);
            }
            let w = weights.get(i).copied().unwrap_or(1.0);
            let all = self.zrange_all_inner(db, key);
            for (member, score) in all {
                let weighted = score * w;
                let entry = combined.entry(member).or_insert(match aggregate.to_uppercase().as_str() {
                    "MIN" => f64::INFINITY,
                    "MAX" => f64::NEG_INFINITY,
                    _ => 0.0,
                });
                *entry = match aggregate.to_uppercase().as_str() {
                    "MIN" => entry.min(weighted),
                    "MAX" => entry.max(weighted),
                    _ => *entry + weighted,
                };
            }
        }

        // Store to dst
        self.delete_key_internal(db, dst, None);
        let mut meta = RedisMetadata {
            r#type: RedisType::ZSet,
            count: 0,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        };

        for (member, score) in &combined {
            let member_key = encode_zset_member_key(db, dst, member);
            let score_bytes = encode_sortable_double(*score);
            self.storage.put(member_key, score_bytes.to_vec());
            let score_key = encode_zset_score_key(db, dst, *score, member);
            self.storage.put(score_key, vec![1u8]);
            meta.count += 1;
        }

        self.save_meta_inner(db, dst, &meta);
        Ok(meta.count)
    }

    pub fn zinterstore(
        &self,
        db: usize,
        dst: &[u8],
        keys: &[&[u8]],
        weights: &[f64],
        aggregate: &str,
    ) -> Result<i64, RedisError> {
        let mut all_keys: Vec<&[u8]> = vec![dst];
        all_keys.extend_from_slice(keys);
        let _guard = self.shard_w_multi(&all_keys);

        use std::collections::HashMap;

        if keys.is_empty() {
            return Ok(0);
        }

        // Get first set
        let first_key = keys[0];
        if !self.is_live_key_inner(db, first_key) {
            self.delete_key_internal(db, dst, None);
            return Ok(0);
        }
        let first_meta = self.get_meta_inner(db, first_key).unwrap();
        if first_meta.r#type != RedisType::ZSet {
            return Err(RedisError::WrongType);
        }
        let w0 = weights.get(0).copied().unwrap_or(1.0);
        let mut combined: HashMap<Vec<u8>, f64> = self
            .zrange_all_inner(db, first_key)
            .into_iter()
            .map(|(m, s)| (m, s * w0))
            .collect();

        for (i, &key) in keys[1..].iter().enumerate() {
            let wi = weights.get(i + 1).copied().unwrap_or(1.0);
            let members: HashMap<Vec<u8>, f64> = if self.is_live_key_inner(db, key) {
                let meta = self.get_meta_inner(db, key).unwrap();
                if meta.r#type != RedisType::ZSet {
                    return Err(RedisError::WrongType);
                }
                self.zrange_all_inner(db, key)
                    .into_iter()
                    .map(|(m, s)| (m, s * wi))
                    .collect()
            } else {
                HashMap::new()
            };

            // Keep only members in both
            combined = combined
                .into_iter()
                .filter_map(|(m, s)| {
                    members.get(&m).map(|&s2| {
                        let new_s = match aggregate.to_uppercase().as_str() {
                            "MIN" => s.min(s2),
                            "MAX" => s.max(s2),
                            _ => s + s2,
                        };
                        (m, new_s)
                    })
                })
                .collect();
        }

        // Store to dst
        self.delete_key_internal(db, dst, None);
        let mut meta = RedisMetadata {
            r#type: RedisType::ZSet,
            count: 0,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        };

        for (member, score) in &combined {
            let member_key = encode_zset_member_key(db, dst, member);
            let score_bytes = encode_sortable_double(*score);
            self.storage.put(member_key, score_bytes.to_vec());
            let score_key = encode_zset_score_key(db, dst, *score, member);
            self.storage.put(score_key, vec![1u8]);
            meta.count += 1;
        }

        self.save_meta_inner(db, dst, &meta);
        Ok(meta.count)
    }

    pub fn zdiffstore(
        &self,
        db: usize,
        dst: &[u8],
        keys: &[&[u8]],
    ) -> Result<i64, RedisError> {
        let mut all_keys: Vec<&[u8]> = vec![dst];
        all_keys.extend_from_slice(keys);
        let _guard = self.shard_w_multi(&all_keys);

        use std::collections::HashSet;

        if keys.is_empty() {
            return Ok(0);
        }

        let first_key = keys[0];
        let first_all = if self.is_live_key_inner(db, first_key) {
            let meta = self.get_meta_inner(db, first_key).unwrap();
            if meta.r#type != RedisType::ZSet {
                return Err(RedisError::WrongType);
            }
            self.zrange_all_inner(db, first_key)
        } else {
            vec![]
        };

        let mut exclude: HashSet<Vec<u8>> = HashSet::new();
        for &key in &keys[1..] {
            if self.is_live_key_inner(db, key) {
                let meta = self.get_meta_inner(db, key).unwrap();
                if meta.r#type != RedisType::ZSet {
                    return Err(RedisError::WrongType);
                }
                for (m, _) in self.zrange_all_inner(db, key) {
                    exclude.insert(m);
                }
            }
        }

        let result: Vec<(Vec<u8>, f64)> = first_all
            .into_iter()
            .filter(|(m, _)| !exclude.contains(m))
            .collect();

        self.delete_key_internal(db, dst, None);
        let mut meta = RedisMetadata {
            r#type: RedisType::ZSet,
            count: 0,
            expiry_ms: 0,
            list_head: 0,
            list_tail: 0,
            version: 0,
        };

        for (member, score) in &result {
            let member_key = encode_zset_member_key(db, dst, member);
            let score_bytes = encode_sortable_double(*score);
            self.storage.put(member_key, score_bytes.to_vec());
            let score_key = encode_zset_score_key(db, dst, *score, member);
            self.storage.put(score_key, vec![1u8]);
            meta.count += 1;
        }

        if meta.count > 0 {
            self.save_meta_inner(db, dst, &meta);
        }
        Ok(meta.count)
    }

    // ── Key extended commands ─────────────────────────────────────────────────

    /// Copy all data for `src` into `dst` and delete `src`.  Called while the
    /// global write lock is already held; does NOT acquire the lock itself.
    fn rename_data_inner(&self, db: usize, src: &[u8], dst: &[u8], src_meta: &metadata::RedisMetadata) {
        match src_meta.r#type {
            RedisType::String => {
                let sk = encode_string_key(db, src);
                if let Some(val) = self.storage.get(&sk) {
                    let new_sk = encode_string_key(db, dst);
                    self.storage.put(new_sk, val);
                }
            }
            RedisType::Hash => {
                let prefix = encode_key_prefix(TAG_HASH, db, src);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                let header_size = 1 + 1 + 2 + src.len();
                for (k, v) in entries {
                    if let Some(v) = v {
                        if k.len() < header_size + 2 { continue; }
                        let field_len = u16::from_be_bytes(k[header_size..header_size+2].try_into().unwrap()) as usize;
                        if k.len() < header_size + 2 + field_len { continue; }
                        let field = &k[header_size + 2..header_size + 2 + field_len];
                        let new_hk = encode_hash_key(db, dst, field);
                        self.storage.put(new_hk, v);
                    }
                }
            }
            RedisType::List => {
                let prefix = encode_key_prefix(TAG_LIST, db, src);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let header_size = 1 + 1 + 2 + src.len();
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, v) in entries {
                    if let Some(v) = v {
                        if k.len() < header_size + 8 { continue; }
                        let seq = i64::from_be_bytes(k[header_size..header_size+8].try_into().unwrap());
                        let new_lk = encode_list_key(db, dst, seq);
                        self.storage.put(new_lk, v);
                    }
                }
            }
            RedisType::Set => {
                let prefix = encode_key_prefix(TAG_SET, db, src);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let header_size = 1 + 1 + 2 + src.len();
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, v) in entries {
                    if v.is_some() {
                        if k.len() < header_size + 2 { continue; }
                        let member_len = u16::from_be_bytes(k[header_size..header_size+2].try_into().unwrap()) as usize;
                        if k.len() < header_size + 2 + member_len { continue; }
                        let member = &k[header_size + 2..header_size + 2 + member_len];
                        let new_sk = encode_set_key(db, dst, member);
                        self.storage.put(new_sk, vec![1u8]);
                    }
                }
            }
            RedisType::ZSet => {
                let mut prefix = encode_key_prefix(TAG_ZSET, db, src);
                prefix.push(0x02);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let base_len = prefix.len();
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, v) in entries {
                    if v.is_some() {
                        if k.len() < base_len + 8 + 2 { continue; }
                        let score = decode_sortable_double(&k[base_len..base_len+8]);
                        let member_len = u16::from_be_bytes(k[base_len+8..base_len+10].try_into().unwrap()) as usize;
                        if k.len() < base_len + 10 + member_len { continue; }
                        let member = &k[base_len + 10..base_len + 10 + member_len];
                        let new_mk = encode_zset_member_key(db, dst, member);
                        let score_bytes = encode_sortable_double(score);
                        self.storage.put(new_mk, score_bytes.to_vec());
                        let new_sk = encode_zset_score_key(db, dst, score, member);
                        self.storage.put(new_sk, vec![1u8]);
                    }
                }
            }
            RedisType::None => {}
        }

        // Write dst metadata, preserving src expiry and registering the TTL index.
        if src_meta.expiry_ms > 0 {
            let ttl_key = encode_ttl_key(src_meta.expiry_ms, db, dst);
            self.storage.put(ttl_key, vec![1u8]);
        }
        self.save_meta_inner(db, dst, src_meta);

        // Remove src entirely (including its TTL index entry).
        self.delete_key_internal(db, src, Some(src_meta));
    }

    pub fn rename_key(&self, db: usize, src: &[u8], dst: &[u8]) -> Result<(), RedisError> {
        let _guard = self.shard_w_multi(&[src, dst]);

        if !self.is_live_key_inner(db, src) {
            return Err(RedisError::Other("ERR no such key".to_string()));
        }

        let src_meta = self.get_meta_inner(db, src).unwrap();

        if self.is_live_key_inner(db, dst) {
            self.delete_key_internal(db, dst, None);
        }

        self.rename_data_inner(db, src, dst, &src_meta);
        Ok(())
    }

    /// RENAMENX is atomic: the dst-existence check and the rename happen under
    /// a single write lock, eliminating the previous TOCTOU race.
    pub fn renamenx(&self, db: usize, src: &[u8], dst: &[u8]) -> Result<i64, RedisError> {
        let _guard = self.shard_w_multi(&[src, dst]);

        if !self.is_live_key_inner(db, src) {
            return Err(RedisError::Other("ERR no such key".to_string()));
        }

        if self.is_live_key_inner(db, dst) {
            return Ok(0);
        }

        let src_meta = self.get_meta_inner(db, src).unwrap();
        self.rename_data_inner(db, src, dst, &src_meta);
        Ok(1)
    }

    pub fn randomkey(&self, db: usize) -> Result<Option<Vec<u8>>, RedisError> {
        let _guard = self.shard_r_all();
        let keys = self.get_all_live_keys(db);
        if keys.is_empty() {
            return Ok(None);
        }
        // Return "random" key (first one in our case)
        Ok(keys.into_iter().next())
    }

    pub fn touch(&self, db: usize, keys: &[&[u8]]) -> Result<i64, RedisError> {
        let _guard = self.shard_w_multi(keys);
        let mut count = 0i64;
        for &key in keys {
            if self.is_live_key_inner(db, key) {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn scan_with_type(
        &self,
        db: usize,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
        type_filter: Option<&str>,
    ) -> Result<(u64, Vec<Vec<u8>>), RedisError> {
        let _guard = self.shard_r_all();

        let all_keys = self.get_all_live_keys(db);
        let total = all_keys.len();

        if total == 0 {
            return Ok((0, vec![]));
        }

        let start = cursor as usize;
        if start >= total {
            return Ok((0, vec![]));
        }

        let end = (start + count).min(total);
        let next_cursor = if end >= total { 0 } else { end as u64 };

        let mut result = Vec::new();
        for key in &all_keys[start..end] {
            let key_str = std::str::from_utf8(key).unwrap_or("");
            if let Some(pat) = pattern {
                if !Self::glob_match(pat, key_str) {
                    continue;
                }
            }
            if let Some(type_f) = type_filter {
                let meta = self.get_meta_inner(db, key);
                let ktype = match meta {
                    None => {
                        // Check extended type registry
                        ext_type_registry::get_type(db, key).unwrap_or("none")
                    }
                    Some(m) => match m.r#type {
                        RedisType::String => "string",
                        RedisType::Hash => "hash",
                        RedisType::List => "list",
                        RedisType::Set => "set",
                        RedisType::ZSet => "zset",
                        RedisType::None => {
                            ext_type_registry::get_type(db, key).unwrap_or("none")
                        }
                    },
                };
                if ktype.to_lowercase() != type_f.to_lowercase() {
                    continue;
                }
            }
            result.push(key.clone());
        }

        Ok((next_cursor, result))
    }

    pub fn swapdb(&self, db1: usize, db2: usize) -> Result<(), RedisError> {
        let _guard = self.shard_w_all();

        if db1 >= self.num_dbs || db2 >= self.num_dbs {
            return Err(RedisError::Other("ERR invalid DB index".to_string()));
        }

        if db1 == db2 {
            return Ok(());
        }

        // Get all keys in both dbs
        let keys1 = self.get_all_live_keys(db1);
        let keys2 = self.get_all_live_keys(db2);

        // This is a simplified swap - in practice we need a full type-aware copy
        // For now, we'll swap string values only as a basic implementation
        // A full implementation would require copying all data structures

        // Temporary: collect all data before swapping
        // Collect db1 data
        let mut db1_data: Vec<(Vec<u8>, RedisMetadata, Vec<(Vec<u8>, Vec<u8>)>)> = Vec::new();
        for key in &keys1 {
            if let Some(meta) = self.get_meta_inner(db1, key) {
                let data = self.collect_key_data(db1, key, &meta);
                db1_data.push((key.clone(), meta, data));
            }
        }

        let mut db2_data: Vec<(Vec<u8>, RedisMetadata, Vec<(Vec<u8>, Vec<u8>)>)> = Vec::new();
        for key in &keys2 {
            if let Some(meta) = self.get_meta_inner(db2, key) {
                let data = self.collect_key_data(db2, key, &meta);
                db2_data.push((key.clone(), meta, data));
            }
        }

        // Delete all from both
        for key in &keys1 {
            self.delete_key_internal(db1, key, None);
        }
        for key in &keys2 {
            self.delete_key_internal(db2, key, None);
        }

        // Write db1 data to db2
        for (key, meta, raw_pairs) in db1_data {
            self.write_key_data(db2, &key, &meta, &raw_pairs);
        }

        // Write db2 data to db1
        for (key, meta, raw_pairs) in db2_data {
            self.write_key_data(db1, &key, &meta, &raw_pairs);
        }

        Ok(())
    }

    fn collect_key_data(&self, db: usize, key: &[u8], meta: &RedisMetadata) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut data = Vec::new();
        match meta.r#type {
            RedisType::String => {
                let sk = encode_string_key(db, key);
                if let Some(v) = self.storage.get(&sk) {
                    data.push((sk, v));
                }
            }
            RedisType::Hash | RedisType::List | RedisType::Set => {
                let tag = match meta.r#type {
                    RedisType::Hash => TAG_HASH,
                    RedisType::List => TAG_LIST,
                    RedisType::Set => TAG_SET,
                    _ => unreachable!(),
                };
                let prefix = encode_key_prefix(tag, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, v) in entries {
                    if let Some(v) = v {
                        data.push((k, v));
                    }
                }
            }
            RedisType::ZSet => {
                let prefix = encode_key_prefix(TAG_ZSET, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                for (k, v) in entries {
                    if let Some(v) = v {
                        data.push((k, v));
                    }
                }
            }
            RedisType::None => {}
        }
        data
    }

    fn write_key_data(&self, new_db: usize, key: &[u8], meta: &RedisMetadata, raw_pairs: &[(Vec<u8>, Vec<u8>)]) {
        // We need to re-encode keys for new_db
        // The raw_pairs have keys encoded for old_db; we need to re-encode for new_db
        // Instead, we re-encode based on type
        match meta.r#type {
            RedisType::String => {
                let sk = encode_string_key(new_db, key);
                if let Some((_, v)) = raw_pairs.first() {
                    self.storage.put(sk, v.clone());
                }
            }
            RedisType::Hash => {
                // raw_pairs keys are encoded hash keys; we need to extract fields
                // [TAG_HASH:1][old_db:1][keyLen:2BE][key][fieldLen:2BE][field] -> value
                let header_size = 1 + 1 + 2 + key.len();
                for (k, v) in raw_pairs {
                    if k.len() < header_size + 2 { continue; }
                    let field_len = u16::from_be_bytes(k[header_size..header_size+2].try_into().unwrap()) as usize;
                    if k.len() < header_size + 2 + field_len { continue; }
                    let field = &k[header_size + 2..header_size + 2 + field_len];
                    let new_hk = encode_hash_key(new_db, key, field);
                    self.storage.put(new_hk, v.clone());
                }
            }
            RedisType::List => {
                let header_size = 1 + 1 + 2 + key.len();
                for (k, v) in raw_pairs {
                    if k.len() < header_size + 8 { continue; }
                    let seq = i64::from_be_bytes(k[header_size..header_size+8].try_into().unwrap());
                    let new_lk = encode_list_key(new_db, key, seq);
                    self.storage.put(new_lk, v.clone());
                }
            }
            RedisType::Set => {
                let header_size = 1 + 1 + 2 + key.len();
                for (k, _v) in raw_pairs {
                    if k.len() < header_size + 2 { continue; }
                    let member_len = u16::from_be_bytes(k[header_size..header_size+2].try_into().unwrap()) as usize;
                    if k.len() < header_size + 2 + member_len { continue; }
                    let member = &k[header_size + 2..header_size + 2 + member_len];
                    let new_sk = encode_set_key(new_db, key, member);
                    self.storage.put(new_sk, vec![1u8]);
                }
            }
            RedisType::ZSet => {
                // We have mixed 0x01 (member) and 0x02 (score) keys
                // Process them both
                let header_size = 1 + 1 + 2 + key.len();
                for (k, v) in raw_pairs {
                    if k.len() < header_size + 1 { continue; }
                    let subtype = k[header_size];
                    if subtype == 0x01 {
                        // member key: ...[0x01][memberLen:2BE][member] -> sortable_score_bytes
                        let rest_start = header_size + 1;
                        if k.len() < rest_start + 2 { continue; }
                        let member_len = u16::from_be_bytes(k[rest_start..rest_start+2].try_into().unwrap()) as usize;
                        if k.len() < rest_start + 2 + member_len { continue; }
                        let member = &k[rest_start + 2..rest_start + 2 + member_len];
                        let new_mk = encode_zset_member_key(new_db, key, member);
                        self.storage.put(new_mk, v.clone());
                    } else if subtype == 0x02 {
                        // score key: ...[0x02][score:8][memberLen:2BE][member] -> [1u8]
                        let rest_start = header_size + 1;
                        if k.len() < rest_start + 8 + 2 { continue; }
                        let score = decode_sortable_double(&k[rest_start..rest_start+8]);
                        let member_len = u16::from_be_bytes(k[rest_start+8..rest_start+10].try_into().unwrap()) as usize;
                        if k.len() < rest_start + 10 + member_len { continue; }
                        let member = &k[rest_start + 10..rest_start + 10 + member_len];
                        let new_sk = encode_zset_score_key(new_db, key, score, member);
                        self.storage.put(new_sk, vec![1u8]);
                    }
                }
            }
            RedisType::None => {}
        }

        // Write metadata
        let new_meta = RedisMetadata {
            r#type: meta.r#type,
            count: meta.count,
            expiry_ms: meta.expiry_ms,
            list_head: meta.list_head,
            list_tail: meta.list_tail,
            version: 0,
        };
        if new_meta.expiry_ms > 0 {
            let ttl_key = encode_ttl_key(new_meta.expiry_ms, new_db, key);
            self.storage.put(ttl_key, vec![1u8]);
        }
        self.save_meta_inner(new_db, key, &new_meta);
    }

    pub fn zintercard(&self, db: usize, keys: &[&[u8]], limit: i64) -> Result<i64, RedisError> {
        if keys.is_empty() { return Ok(0); }
        let first_members: std::collections::HashSet<Vec<u8>> = match self.zrange_all(db, keys[0]) {
            Ok(items) => items.into_iter().map(|(m, _)| m).collect(),
            Err(_) => return Ok(0),
        };
        let mut intersection: std::collections::HashSet<Vec<u8>> = first_members;
        for key in &keys[1..] {
            let members: std::collections::HashSet<Vec<u8>> = match self.zrange_all(db, key) {
                Ok(items) => items.into_iter().map(|(m, _)| m).collect(),
                Err(_) => return Ok(0),
            };
            intersection = intersection.intersection(&members).cloned().collect();
            if intersection.is_empty() { return Ok(0); }
        }
        let count = intersection.len() as i64;
        if limit > 0 && count > limit { Ok(limit) } else { Ok(count) }
    }

    pub fn lmpop_impl(&self, db: usize, keys: &[&[u8]], from_left: bool, count: usize) -> Result<Option<(Vec<u8>, Vec<Vec<u8>>)>, RedisError> {
        for key in keys {
            let items = if from_left {
                self.lpop_count(db, key, count)?
            } else {
                self.rpop_count(db, key, count)?
            };
            if !items.is_empty() {
                return Ok(Some((key.to_vec(), items)));
            }
        }
        Ok(None)
    }

    pub fn zpop_multi(&self, db: usize, keys: &[&[u8]], pop_min: bool, count: i64) -> Result<Option<(Vec<u8>, Vec<(Vec<u8>, f64)>)>, RedisError> {
        for key in keys {
            let items = if pop_min {
                self.zpopmin(db, key, count)?
            } else {
                self.zpopmax(db, key, count)?
            };
            if !items.is_empty() {
                return Ok(Some((key.to_vec(), items)));
            }
        }
        Ok(None)
    }

    // ── TTL sweep ─────────────────────────────────────────────────────────────

    /// FIXED: properly deletes all data types
    pub fn sweep_expired(&self) {
        let _guard = self.shard_w_all();

        let now = now_ms();

        // Scan TTL index: [TAG_TTL:1][expiry:8BE][db:1][keyLen:2BE][key]
        // We only want keys where expiry <= now
        // Scan from TAG_TTL start to TAG_TTL | expiry=now
        let start = vec![TAG_TTL];
        let mut end = vec![TAG_TTL];
        // Append the current time as 8 BE bytes + 1 to include keys with expiry == now
        let end_expiry = now + 1;
        end.extend_from_slice(&end_expiry.to_be_bytes());

        let entries = self.storage.scan(Some(&start), Some(&end));

        let mut to_delete: Vec<(usize, Vec<u8>)> = Vec::new();

        for (encoded_key, value) in entries {
            if value.is_none() {
                continue; // tombstone
            }
            if encoded_key.len() < 12 {
                continue;
            }
            if encoded_key[0] != TAG_TTL {
                continue;
            }
            let expiry = i64::from_be_bytes(encoded_key[1..9].try_into().unwrap());
            if expiry > now {
                continue;
            }
            let db = encoded_key[9] as usize;
            let key_len = u16::from_be_bytes(encoded_key[10..12].try_into().unwrap()) as usize;
            if encoded_key.len() < 12 + key_len {
                continue;
            }
            let user_key = encoded_key[12..12 + key_len].to_vec();
            to_delete.push((db, user_key));
        }

        for (db, key) in to_delete {
            if db < self.num_dbs {
                // Check if still expired (double-check)
                if let Some(meta) = self.get_meta_inner(db, &key) {
                    if meta.expiry_ms > 0 && meta.expiry_ms <= now {
                        self.delete_key_internal(db, &key, Some(&meta));
                    }
                }
            }
        }
    }

    // ── MOVE/COPY/SORT ────────────────────────────────────────────────────────

    /// MOVE key db - moves key from current db to target db
    pub fn move_key(&self, src_db: usize, key: &[u8], dst_db: usize) -> Result<i64, RedisError> {
        let _guard = self.shard_w(key);

        if src_db >= self.num_dbs || dst_db >= self.num_dbs {
            return Err(RedisError::Other("ERR invalid DB index".to_string()));
        }

        if !self.is_live_key_inner(src_db, key) {
            return Ok(0);
        }

        // Fail if key exists in dst
        if self.is_live_key_inner(dst_db, key) {
            return Ok(0);
        }

        let meta = self.get_meta_inner(src_db, key).unwrap();
        let data = self.collect_key_data(src_db, key, &meta);
        self.write_key_data(dst_db, key, &meta, &data);
        self.delete_key_internal(src_db, key, Some(&meta));

        Ok(1)
    }

    /// COPY src dst [DB destination] [REPLACE] - copies a key
    pub fn copy_key(&self, src_db: usize, src: &[u8], dst_db: usize, dst: &[u8], replace: bool) -> Result<i64, RedisError> {
        let _guard = self.shard_w_multi(&[src, dst]);

        if src_db >= self.num_dbs || dst_db >= self.num_dbs {
            return Err(RedisError::Other("ERR invalid DB index".to_string()));
        }

        if !self.is_live_key_inner(src_db, src) {
            return Ok(0);
        }

        if self.is_live_key_inner(dst_db, dst) {
            if !replace {
                return Ok(0);
            }
            self.delete_key_internal(dst_db, dst, None);
        }

        let meta = self.get_meta_inner(src_db, src).unwrap();
        let data = self.collect_key_data(src_db, src, &meta);
        self.write_key_data(dst_db, dst, &meta, &data);

        Ok(1)
    }

    /// SORT key - sorts a list/set/zset numerically
    pub fn sort_key(&self, db: usize, key: &[u8], alpha: bool, desc: bool, limit: Option<(i64, i64)>) -> Result<Vec<Vec<u8>>, RedisError> {
        let _guard = self.shard_r(key);

        if !self.is_live_key_inner(db, key) {
            return Ok(vec![]);
        }

        let meta = match self.get_meta_inner(db, key) {
            None => return Ok(vec![]),
            Some(m) => m,
        };

        let mut items: Vec<Vec<u8>> = match meta.r#type {
            RedisType::List => {
                let prefix = encode_key_prefix(TAG_LIST, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                let header_size = 1 + 1 + 2 + key.len();
                let mut pairs: Vec<(i64, Vec<u8>)> = Vec::new();
                for (k, v) in entries {
                    if let Some(v) = v {
                        if k.len() < header_size + 8 { continue; }
                        let seq = i64::from_be_bytes(k[header_size..header_size+8].try_into().unwrap());
                        pairs.push((seq, v));
                    }
                }
                pairs.sort_by_key(|(seq, _)| *seq);
                pairs.into_iter().map(|(_, v)| v).collect()
            }
            RedisType::Set => {
                let prefix = encode_key_prefix(TAG_SET, db, key);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                let header_size = 1 + 1 + 2 + key.len();
                let mut result = Vec::new();
                for (k, v) in entries {
                    if v.is_some() && k.len() >= header_size + 2 {
                        let member_len = u16::from_be_bytes(k[header_size..header_size+2].try_into().unwrap()) as usize;
                        if k.len() >= header_size + 2 + member_len {
                            result.push(k[header_size + 2..header_size + 2 + member_len].to_vec());
                        }
                    }
                }
                result
            }
            RedisType::ZSet => {
                let mut prefix = encode_key_prefix(TAG_ZSET, db, key);
                prefix.push(0x01);
                let mut end_prefix = prefix.clone();
                increment_last_byte(&mut end_prefix);
                let base_len = prefix.len();
                let entries = self.storage.scan(Some(&prefix), Some(&end_prefix));
                let mut result = Vec::new();
                for (k, _v) in entries {
                    if k.len() < base_len + 2 { continue; }
                    let member_len = u16::from_be_bytes(k[base_len..base_len+2].try_into().unwrap()) as usize;
                    if k.len() < base_len + 2 + member_len { continue; }
                    result.push(k[base_len + 2..base_len + 2 + member_len].to_vec());
                }
                result
            }
            _ => return Err(RedisError::WrongType),
        };

        // Sort
        if alpha {
            items.sort_by(|a, b| a.cmp(b));
        } else {
            items.sort_by(|a, b| {
                let fa = std::str::from_utf8(a).ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let fb = std::str::from_utf8(b).ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        if desc {
            items.reverse();
        }

        // Apply LIMIT offset count
        if let Some((offset, count)) = limit {
            let start = offset.max(0) as usize;
            let end = if count < 0 {
                items.len()
            } else {
                (start + count as usize).min(items.len())
            };
            items = if start < items.len() { items[start..end].to_vec() } else { vec![] };
        }

        Ok(items)
    }

    // ── Glob matching ─────────────────────────────────────────────────────────

    pub fn glob_match(pattern: &str, s: &str) -> bool {
        glob_match_bytes(pattern.as_bytes(), s.as_bytes())
    }
}

fn glob_match_bytes(pattern: &[u8], s: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut si = 0usize;

    while pi < pattern.len() {
        match pattern[pi] {
            b'*' => {
                // Skip multiple stars
                while pi < pattern.len() && pattern[pi] == b'*' {
                    pi += 1;
                }
                if pi == pattern.len() {
                    return true;
                }
                // Try matching rest of pattern at each position in s
                while si <= s.len() {
                    if glob_match_bytes(&pattern[pi..], &s[si..]) {
                        return true;
                    }
                    si += 1;
                }
                return false;
            }
            b'?' => {
                if si >= s.len() {
                    return false;
                }
                pi += 1;
                si += 1;
            }
            b'[' => {
                if si >= s.len() {
                    return false;
                }
                pi += 1;
                let negate = if pi < pattern.len() && pattern[pi] == b'^' {
                    pi += 1;
                    true
                } else {
                    false
                };

                let mut matched = false;
                let ch = s[si];

                while pi < pattern.len() && pattern[pi] != b']' {
                    if pi + 2 < pattern.len() && pattern[pi + 1] == b'-' {
                        let lo = pattern[pi];
                        let hi = pattern[pi + 2];
                        if ch >= lo && ch <= hi {
                            matched = true;
                        }
                        pi += 3;
                    } else {
                        if pattern[pi] == ch {
                            matched = true;
                        }
                        pi += 1;
                    }
                }

                if pi < pattern.len() {
                    pi += 1; // skip ']'
                }

                if matched == negate {
                    return false;
                }
                si += 1;
            }
            c => {
                if si >= s.len() || s[si] != c {
                    return false;
                }
                pi += 1;
                si += 1;
            }
        }
    }

    si == s.len()
}

/// Increment the last byte of a prefix for range scan end bound
pub fn increment_last_byte(buf: &mut Vec<u8>) {
    for i in (0..buf.len()).rev() {
        if buf[i] < 0xFF {
            buf[i] += 1;
            return;
        } else {
            buf[i] = 0;
        }
    }
    // All bytes were 0xFF - push 0x01 past end (shouldn't happen in practice)
    buf.push(0x01);
}
