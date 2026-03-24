use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};

use crate::config::ServerConfig;
use super::compaction::Compaction;
use super::key_encoding::{TAG_META, TAG_STRING, TAG_HASH, TAG_LIST, TAG_SET, TAG_ZSET, TAG_TTL};
use super::manifest::Manifest;
use super::memtable::MemTable;
use super::sstable::SSTableWriter;
use super::wal::{WalOpType, WriteAheadLog};

const NUM_SHARDS: usize = 64;

/// Per-shard memtable state (active + one immutable slot).
struct MemShard {
    active: MemTable,
    immutable: Option<MemTable>,
}

pub struct LsmStorage {
    data_dir: PathBuf,
    /// 64 independent memtable shards — each has its own write lock.
    mem_shards: Box<[RwLock<MemShard>]>,
    /// 64 WAL files — one per shard, eliminating cross-shard WAL contention.
    wals: Box<[Mutex<WriteAheadLog>]>,
    wal_seq: Arc<AtomicU64>,
    manifest: Arc<Manifest>,
    compaction: Arc<Compaction>,
    /// Per-shard memtable size limit. Total capacity ≈ NUM_SHARDS × this.
    memtable_shard_bytes: usize,
    bg_stop: Arc<AtomicBool>,
}

/// FNV-1a hash → shard index in 0..NUM_SHARDS.
///
/// Routes by the *Redis key* portion of a storage key so that all entries for
/// the same logical key (meta, string-data, hash fields, TTL marker, …) land
/// in the **same shard**.  This means put2(meta_key, data_key) only ever needs
/// ONE WAL lock and ONE memtable write-lock instead of two.
///
/// Storage-key layouts:
///   Standard: [tag:1][db:1][keyLen:2BE][redis_key...]   → skip 4 bytes
///   TTL:      [TTL_TAG:1][expiry:8BE][db:1][keyLen:2BE][redis_key...] → skip 13 bytes
#[inline]
fn shard_of(key: &[u8]) -> usize {
    let payload: &[u8] = match key.first().copied() {
        Some(TAG_META) | Some(TAG_STRING) | Some(TAG_HASH)
        | Some(TAG_LIST) | Some(TAG_SET) | Some(TAG_ZSET) => {
            if key.len() > 4 { &key[4..] } else { key }
        }
        Some(TAG_TTL) => {
            if key.len() > 13 { &key[13..] } else { key }
        }
        _ => key,
    };
    const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut h: u64 = FNV_OFFSET_BASIS;
    for &b in payload {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    (h % NUM_SHARDS as u64) as usize
}

impl LsmStorage {
    pub fn new(config: ServerConfig) -> io::Result<Self> {
        let data_dir = PathBuf::from(&config.dir);
        fs::create_dir_all(&data_dir)?;

        // Divide total memtable budget equally across shards.
        let total_bytes = (config.memtable_size_mb * 1024 * 1024) as usize;
        let memtable_shard_bytes = (total_bytes / NUM_SHARDS).max(1024 * 1024);

        let write_through = config.wal_sync_mode == crate::config::WalSyncMode::Always;

        // Load manifest
        let manifest_path = data_dir.join("MANIFEST");
        let manifest = Arc::new(Manifest::new(manifest_path));
        manifest.load()?;

        let compaction = Arc::new(Compaction::new(data_dir.clone(), manifest.clone()));

        // WAL recovery: replay active WAL files only, get max sequence.
        let (recovered_entries, max_seq) = Self::recover_wal_entries(&data_dir)?;

        // Archive all active WAL files; recovered data will be flushed to L0.
        Self::archive_active_wals(&data_dir, max_seq)?;

        // Shared global sequence counter
        let wal_seq = Arc::new(AtomicU64::new(max_seq));

        // Create 64 fresh WAL files
        let wals: Vec<Mutex<WriteAheadLog>> = (0..NUM_SHARDS)
            .map(|shard| {
                let wal_path = data_dir.join(format!("wal-{:02}.wal", shard));
                WriteAheadLog::new(&wal_path, write_through, wal_seq.clone()).map(Mutex::new)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let wals = wals.into_boxed_slice();

        // Build 64 memtable shards, replaying recovered entries into them.
        let mem_shards: Vec<RwLock<MemShard>> = (0..NUM_SHARDS)
            .map(|_| {
                RwLock::new(MemShard {
                    active: MemTable::new(memtable_shard_bytes),
                    immutable: None,
                })
            })
            .collect();
        for (_, op, key, value) in recovered_entries {
            let shard = shard_of(&key);
            let mut mem = mem_shards[shard].write();
            match op {
                WalOpType::Put => mem.active.put(key, value),
                WalOpType::Delete => mem.active.delete(key),
            }
        }
        let mem_shards = mem_shards.into_boxed_slice();

        let storage = LsmStorage {
            data_dir: data_dir.clone(),
            mem_shards,
            wals,
            wal_seq,
            manifest: manifest.clone(),
            compaction: compaction.clone(),
            memtable_shard_bytes,
            bg_stop: Arc::new(AtomicBool::new(false)),
        };

        // Flush any recovered data to L0
        let any_data = storage.mem_shards.iter().any(|s| s.read().active.count() > 0);
        if any_data {
            storage.force_flush()?;
        }

        // Start background compaction thread
        let bg_stop = storage.bg_stop.clone();
        let bg_compaction = compaction.clone();
        std::thread::spawn(move || {
            while !bg_stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(5));
                if !bg_stop.load(Ordering::Relaxed) {
                    bg_compaction.maybe_compact();
                }
            }
        });

        Ok(storage)
    }

    /// Collect all WAL entries from active WAL files, sorted by global sequence.
    /// Active files: "current.wal" (old format) + "wal-{NN}.wal" (new format).
    /// Archives ("archive-*.wal") are already in SSTables — skip them.
    fn recover_wal_entries(
        data_dir: &PathBuf,
    ) -> io::Result<(Vec<(u64, WalOpType, Vec<u8>, Vec<u8>)>, u64)> {
        let wal_files: Vec<PathBuf> = fs::read_dir(data_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                if p.extension().map(|e| e == "wal").unwrap_or(false) {
                    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    name == "current.wal" || name.starts_with("wal-")
                } else {
                    false
                }
            })
            .collect();

        let mut all_entries: Vec<(u64, WalOpType, Vec<u8>, Vec<u8>)> = Vec::new();
        for wal_file in &wal_files {
            all_entries.extend(WriteAheadLog::replay(wal_file));
        }
        all_entries.sort_unstable_by_key(|(seq, _, _, _)| *seq);

        let max_seq = all_entries.iter().map(|(s, _, _, _)| *s).max().unwrap_or(0);
        Ok((all_entries, max_seq))
    }

    /// Rename active WAL files so fresh ones can be created.
    fn archive_active_wals(data_dir: &PathBuf, max_seq: u64) -> io::Result<()> {
        let active_wals: Vec<PathBuf> = fs::read_dir(data_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                if p.extension().map(|e| e == "wal").unwrap_or(false) {
                    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    name == "current.wal" || name.starts_with("wal-")
                } else {
                    false
                }
            })
            .collect();

        for wal_path in &active_wals {
            let stem = wal_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("old");
            let archive_name = format!("archive-{}-{}.wal.bak", stem, max_seq);
            let _ = fs::rename(wal_path, data_dir.join(archive_name));
        }
        Ok(())
    }

    /// Returns the archive path for a WAL rotation on `shard`.
    /// Uses the current global sequence so each archive name is unique.
    fn wal_archive_path(&self, shard: usize) -> std::path::PathBuf {
        let seq = self.wal_seq.load(Ordering::Relaxed);
        self.data_dir.join(format!("archive-wal-{:02}-{}.wal.bak", shard, seq))
    }

    /// Called while holding BOTH the WAL mutex and the shard write lock.
    ///
    /// If the active memtable is full:
    /// - If the immutable slot is free: rotate the WAL (flush → rename → fresh file)
    ///   and atomically swap active → immutable-snapshot.  Returns `(snapshot, archive)`.
    /// - If the immutable slot is occupied: return its snapshot for flushing without
    ///   rotating (the WAL for that immutable was already rotated in a prior cycle).
    ///
    /// Flushing the returned snapshot to L0 and deleting the archive file must happen
    /// **outside** both locks so that writers are not blocked by disk I/O.
    fn maybe_rotate_and_snapshot(
        &self,
        shard: usize,
        wal: &mut WriteAheadLog,
        mem: &mut MemShard,
    ) -> (Option<BTreeMap<Vec<u8>, Option<Vec<u8>>>>, Option<std::path::PathBuf>) {
        if !mem.active.is_full() {
            return (None, None);
        }

        // Immutable slot occupied — flush it; WAL for it was already rotated.
        if let Some(imm) = mem.immutable.take() {
            return (Some(imm.into_snapshot()), None);
        }

        // Rotate WAL atomically with memtable snapshot.
        let archive = self.wal_archive_path(shard);
        let _ = wal.rotate(&archive);
        let new_active = MemTable::new(self.memtable_shard_bytes);
        let old = std::mem::replace(&mut mem.active, new_active);
        (Some(old.into_snapshot()), Some(archive))
    }

    /// If a snapshot was produced by rotation, flush it to L0 and clean up the archive.
    fn flush_if_needed(
        &self,
        snap: Option<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
        archive: Option<std::path::PathBuf>,
    ) {
        if let Some(s) = snap {
            let _ = self.flush_snapshot_to_l0(s);
            if let Some(path) = archive {
                let _ = fs::remove_file(path);
            }
        }
    }

    /// Batch write: group entries by shard, write each group under one WAL lock.
    /// Used by MSET and other multi-key write commands.
    pub fn put_batch(&self, entries: &[(Vec<u8>, Vec<u8>)]) {
        if entries.is_empty() {
            return;
        }

        // Group entry indices by shard.
        let mut shard_indices: Vec<Vec<usize>> = vec![Vec::new(); NUM_SHARDS];
        for (i, (k, _)) in entries.iter().enumerate() {
            shard_indices[shard_of(k)].push(i);
        }

        let mut to_flush: Vec<(BTreeMap<Vec<u8>, Option<Vec<u8>>>, Option<std::path::PathBuf>)> =
            Vec::new();

        for (shard, indices) in shard_indices.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let pair = {
                let mut wal = self.wals[shard].lock();
                for &i in indices {
                    let (k, v) = &entries[i];
                    let _ = wal.append(WalOpType::Put, k, v);
                }
                let mut mem = self.mem_shards[shard].write();
                for &i in indices {
                    let (k, v) = &entries[i];
                    mem.active.put(k.clone(), v.clone());
                }
                self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
            };
            if let (Some(snap), archive) = pair {
                to_flush.push((snap, archive));
            }
        }

        for (snap, archive) in to_flush {
            self.flush_if_needed(Some(snap), archive);
        }
    }

    /// Write two entries (meta + data) for one logical Redis key under a single
    /// WAL lock and a single memtable write-lock.
    ///
    /// Because shard_of() routes by the Redis-key portion (stripping the tag/db
    /// prefix), the meta key and string-data key for the same logical Redis key
    /// always map to the same shard.  This gives put2 the same lock overhead as
    /// a single put() — half the cost of two independent put() calls.
    pub fn put2(
        &self,
        key1: Vec<u8>, val1: Vec<u8>,
        key2: Vec<u8>, val2: Vec<u8>,
    ) {
        let shard = shard_of(&key1);
        debug_assert_eq!(shard, shard_of(&key2),
            "put2: keys must map to the same shard (same Redis key, different tags)");
        let (snap, archive) = {
            let mut wal = self.wals[shard].lock();
            let _ = wal.append(WalOpType::Put, &key1, &val1);
            let _ = wal.append(WalOpType::Put, &key2, &val2);
            let mut mem = self.mem_shards[shard].write();
            mem.active.put(key1, val1);
            mem.active.put(key2, val2);
            self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
        };
        self.flush_if_needed(snap, archive);
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard = shard_of(&key);
        let (snap, archive) = {
            let mut wal = self.wals[shard].lock();
            let _ = wal.append(WalOpType::Put, &key, &value);
            let mut mem = self.mem_shards[shard].write();
            mem.active.put(key, value);
            self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
        };
        self.flush_if_needed(snap, archive);
    }

    pub fn delete(&self, key: Vec<u8>) {
        let shard = shard_of(&key);
        let (snap, archive) = {
            let mut wal = self.wals[shard].lock();
            let _ = wal.append(WalOpType::Delete, &key, &[]);
            let mut mem = self.mem_shards[shard].write();
            mem.active.delete(key);
            self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
        };
        self.flush_if_needed(snap, archive);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard = shard_of(key);
        let mem = self.mem_shards[shard].read();

        if let Some(result) = mem.active.get(key) {
            return result;
        }
        if let Some(ref imm) = mem.immutable {
            if let Some(result) = imm.get(key) {
                return result;
            }
        }
        drop(mem);

        match self.compaction.search_sstables(key) {
            Some(Some(v)) => Some(v),
            Some(None) => None,
            None => None,
        }
    }

    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        // Collect from SSTables first (lowest priority)
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            self.compaction.scan_sstables(start, end);

        // Merge all 64 shard memtables (immutable first, then active overrides)
        for mem_shard_lock in self.mem_shards.iter() {
            let mem = mem_shard_lock.read();
            if let Some(ref imm) = mem.immutable {
                for (k, v) in imm.scan(start, end) {
                    merged.insert(k, v);
                }
            }
            for (k, v) in mem.active.scan(start, end) {
                merged.insert(k, v);
            }
        }

        merged.into_iter().collect()
    }

    pub fn force_flush(&self) -> io::Result<()> {
        for shard in 0..NUM_SHARDS {
            let (imm_snap, active_snap) = {
                let mut mem = self.mem_shards[shard].write();
                let imm = mem.immutable.take().map(|m| m.into_snapshot());
                let new_active = MemTable::new(self.memtable_shard_bytes);
                let old = std::mem::replace(&mut mem.active, new_active);
                (imm, old.into_snapshot())
            };

            if let Some(imm) = imm_snap {
                if !imm.is_empty() {
                    self.flush_snapshot_to_l0(imm)?;
                }
            }
            if !active_snap.is_empty() {
                self.flush_snapshot_to_l0(active_snap)?;
            }
        }
        Ok(())
    }

    fn flush_snapshot_to_l0(
        &self,
        snapshot: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> io::Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }

        let seq = self.manifest.next_sequence();
        let filename = format!("L0-{}.sst", seq);
        let path = self.data_dir.join(&filename);

        SSTableWriter::write(&path, &snapshot)?;
        self.manifest.add_file(0, filename);
        self.manifest.save()?;

        Ok(())
    }

    pub fn close(&self) {
        self.bg_stop.store(true, Ordering::Relaxed);
        let _ = self.force_flush();
        for wal in self.wals.iter() {
            let mut w = wal.lock();
            let _ = w.flush();
        }
    }
}
