use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

use super::compaction::Compaction;
use super::key_encoding::{TAG_HASH, TAG_LIST, TAG_META, TAG_SET, TAG_STRING, TAG_TTL, TAG_ZSET};
use super::manifest::Manifest;
use super::memtable::MemTable;
use super::sstable::SSTableWriter;
use super::wal::{WalOpType, WriteAheadLog};
use crate::config::ServerConfig;

const NUM_SHARDS: usize = 256;

/// Per-shard memtable state (active + one immutable slot).
struct MemShard {
    active: MemTable,
    immutable: Option<MemTable>,
    immutable_flushing: bool,
}

struct FlushPlan {
    shard: usize,
    snapshot: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    archive: Option<std::path::PathBuf>,
}

/// Snapshot of LSM write/compaction health for INFO / COMPACT diagnostics.
#[derive(Debug, Clone)]
pub struct LsmStats {
    pub memtable_shard_bytes: usize,
    pub pending_flushes: u64,
    pub flushes_completed: u64,
    pub write_stalls: u64,
    pub last_flush_micros: u64,
    pub last_stall_micros: u64,
    pub compacting: bool,
    pub l0_files: usize,
    pub total_sst_files: usize,
    pub level_files: Vec<usize>,
}

pub struct LsmStorage {
    data_dir: PathBuf,
    /// 256 independent memtable shards — each has its own write lock.
    mem_shards: Arc<[RwLock<MemShard>]>,
    /// 256 WAL files — one per shard, eliminating cross-shard WAL contention.
    wals: Box<[Mutex<WriteAheadLog>]>,
    wal_seq: Arc<AtomicU64>,
    manifest: Arc<Manifest>,
    compaction: Arc<Compaction>,
    /// Per-shard memtable size limit. Total capacity ≈ NUM_SHARDS × this.
    memtable_shard_bytes: usize,
    bg_stop: Arc<AtomicBool>,
    /// Bounded queue: SET never waits on SST write / compaction; flush worker does.
    flush_tx: SyncSender<FlushPlan>,
    /// Wake writers blocked because active+immutable are both full.
    flush_cv: Arc<(Mutex<()>, Condvar)>,
    pending_flushes: Arc<AtomicU64>,
    flushes_completed: Arc<AtomicU64>,
    write_stalls: Arc<AtomicU64>,
    last_flush_micros: Arc<AtomicU64>,
    last_stall_micros: Arc<AtomicU64>,
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
        Some(TAG_META) | Some(TAG_STRING) | Some(TAG_HASH) | Some(TAG_LIST) | Some(TAG_SET)
        | Some(TAG_ZSET) => {
            if key.len() > 4 {
                &key[4..]
            } else {
                key
            }
        }
        Some(TAG_TTL) => {
            if key.len() > 13 {
                &key[13..]
            } else {
                key
            }
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
        // Floor at 1MB/shard so low configs are not silently inflated to 2GB
        // (8MB×256), while still avoiding tiny ~KB SST storms.
        // memtable-size-mb 2048 → 8MB/shard; 256 → 1MB/shard.
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

        // Build 256 memtable shards, replaying recovered entries into them.
        let mem_shards: Vec<RwLock<MemShard>> = (0..NUM_SHARDS)
            .map(|_| {
                RwLock::new(MemShard {
                    active: MemTable::new(memtable_shard_bytes),
                    immutable: None,
                    immutable_flushing: false,
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
        let mem_shards: Arc<[RwLock<MemShard>]> = Arc::from(mem_shards.into_boxed_slice());

        // Bound the flush queue so a write storm cannot enqueue unbounded SST work.
        // Capacity ≈ one pending flush per shard (natural LSM immutable slot).
        let (flush_tx, flush_rx) = mpsc::sync_channel::<FlushPlan>(NUM_SHARDS);
        let bg_stop = Arc::new(AtomicBool::new(false));
        let flush_cv = Arc::new((Mutex::new(()), Condvar::new()));
        let pending_flushes = Arc::new(AtomicU64::new(0));
        let flushes_completed = Arc::new(AtomicU64::new(0));
        let write_stalls = Arc::new(AtomicU64::new(0));
        let last_flush_micros = Arc::new(AtomicU64::new(0));
        let last_stall_micros = Arc::new(AtomicU64::new(0));

        let storage = LsmStorage {
            data_dir: data_dir.clone(),
            mem_shards: mem_shards.clone(),
            wals,
            wal_seq: wal_seq.clone(),
            manifest: manifest.clone(),
            compaction: compaction.clone(),
            memtable_shard_bytes,
            bg_stop: bg_stop.clone(),
            flush_tx,
            flush_cv: flush_cv.clone(),
            pending_flushes: pending_flushes.clone(),
            flushes_completed: flushes_completed.clone(),
            write_stalls: write_stalls.clone(),
            last_flush_micros: last_flush_micros.clone(),
            last_stall_micros: last_stall_micros.clone(),
        };

        // Flush any recovered data to L0 (still synchronous at startup).
        let any_data = storage
            .mem_shards
            .iter()
            .any(|s| s.read().active.count() > 0);
        if any_data {
            storage.force_flush()?;
        }
        Self::cleanup_archived_wals(&data_dir)?;

        // Background flush worker: SET returns after WAL+memtable; SST I/O happens here.
        // Compaction is NEVER run on the write path — only on the compaction thread.
        {
            let mem_shards = mem_shards.clone();
            let manifest = manifest.clone();
            let compaction = compaction.clone();
            let data_dir = data_dir.clone();
            let bg_stop = bg_stop.clone();
            let flush_cv = flush_cv.clone();
            let pending_flushes = pending_flushes.clone();
            let flushes_completed = flushes_completed.clone();
            let last_flush_micros = last_flush_micros.clone();
            std::thread::Builder::new()
                .name("forgekv-flush".into())
                .spawn(move || {
                    while !bg_stop.load(Ordering::Relaxed) {
                        match flush_rx.recv_timeout(Duration::from_millis(200)) {
                            Ok(plan) => {
                                let t0 = Instant::now();
                                let flush_ok =
                                    Self::flush_snapshot_to_l0_static(
                                        &data_dir,
                                        &manifest,
                                        &plan.snapshot,
                                    )
                                    .is_ok();
                                {
                                    let mut mem = mem_shards[plan.shard].write();
                                    if flush_ok {
                                        mem.immutable = None;
                                    }
                                    mem.immutable_flushing = false;
                                }
                                if flush_ok {
                                    if let Some(path) = plan.archive {
                                        let _ = fs::remove_file(path);
                                    }
                                }
                                pending_flushes.fetch_sub(1, Ordering::Relaxed);
                                flushes_completed.fetch_add(1, Ordering::Relaxed);
                                last_flush_micros.store(
                                    t0.elapsed().as_micros() as u64,
                                    Ordering::Relaxed,
                                );
                                // Unblock writers waiting for immutable slot.
                                flush_cv.1.notify_all();
                                // Never compact on the flush thread — only signal.
                                compaction.request_compact();
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                })
                .expect("flush thread");
        }

        // Start background compaction thread (250ms tick; single-flight).
        let bg_stop2 = bg_stop.clone();
        let bg_compaction = compaction.clone();
        std::thread::Builder::new()
            .name("forgekv-compact".into())
            .spawn(move || {
                while !bg_stop2.load(Ordering::Relaxed) {
                    // Wake quickly when flushes request work; otherwise idle tick.
                    let mut waited = 0u64;
                    while waited < 250 && !bg_stop2.load(Ordering::Relaxed) {
                        if bg_compaction.take_compact_request() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                        waited += 10;
                    }
                    if !bg_stop2.load(Ordering::Relaxed) {
                        let _ = bg_compaction.take_compact_request();
                        bg_compaction.maybe_compact();
                    }
                }
            })
            .expect("compaction thread");

        Ok(storage)
    }

    /// Collect all WAL entries that must be replayed, sorted by global sequence.
    /// This includes active WALs and archived rotation files that may exist if
    /// the server crashed after rotating but before the SSTable flush finished.
    fn recover_wal_entries(
        data_dir: &PathBuf,
    ) -> io::Result<(Vec<(u64, WalOpType, Vec<u8>, Vec<u8>)>, u64)> {
        let wal_files: Vec<PathBuf> = fs::read_dir(data_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                (p.extension().map(|e| e == "wal").unwrap_or(false)
                    && (name == "current.wal" || name.starts_with("wal-")))
                    || (name.starts_with("archive-") && name.ends_with(".wal.bak"))
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

    fn cleanup_archived_wals(data_dir: &PathBuf) -> io::Result<()> {
        for path in fs::read_dir(data_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.starts_with("archive-") && name.ends_with(".wal.bak")
            })
        {
            let _ = fs::remove_file(path);
        }
        Ok(())
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
        self.data_dir
            .join(format!("archive-wal-{:02}-{}.wal.bak", shard, seq))
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
    ) -> Option<FlushPlan> {
        if !mem.active.is_full() {
            return None;
        }

        // Immutable slot occupied — keep it readable while a single flush is in flight.
        if let Some(imm) = mem.immutable.as_ref() {
            if mem.immutable_flushing {
                return None;
            }
            mem.immutable_flushing = true;
            return Some(FlushPlan {
                shard,
                snapshot: imm.snapshot(),
                archive: None,
            });
        }

        // Rotate WAL atomically with memtable snapshot.
        let archive = self.wal_archive_path(shard);
        if wal.rotate(&archive).is_err() {
            return None;
        }
        let new_active = MemTable::new(self.memtable_shard_bytes);
        let old = std::mem::replace(&mut mem.active, new_active);
        mem.immutable = Some(old);
        mem.immutable_flushing = true;
        Some(FlushPlan {
            shard,
            snapshot: mem
                .immutable
                .as_ref()
                .expect("immutable just assigned")
                .snapshot(),
            archive: Some(archive),
        })
    }

    /// Enqueue an immutable-memtable flush. Never runs compaction on the caller.
    fn enqueue_flush(&self, plan: Option<FlushPlan>) {
        if let Some(plan) = plan {
            self.pending_flushes.fetch_add(1, Ordering::Relaxed);
            if self.flush_tx.send(plan).is_err() {
                self.pending_flushes.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// When active is full and an immutable flush is already in flight, wait so
    /// the memtable cannot grow without bound (and so SET does not silently
    /// absorb multi-GB of pending data under CacheHotels load).
    fn wait_if_memtable_pressured(&self, shard: usize) {
        let max_wait = Duration::from_secs(30);
        let start = Instant::now();
        loop {
            {
                let mem = self.mem_shards[shard].read();
                if !(mem.active.is_full()
                    && mem.immutable.is_some()
                    && mem.immutable_flushing)
                {
                    return;
                }
            }
            self.write_stalls.fetch_add(1, Ordering::Relaxed);
            let (lock, cv) = &*self.flush_cv;
            let mut guard = lock.lock();
            let remaining = max_wait.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return;
            }
            let t0 = Instant::now();
            let _ = cv.wait_for(&mut guard, remaining.min(Duration::from_millis(50)));
            self.last_stall_micros.store(
                t0.elapsed().as_micros() as u64,
                Ordering::Relaxed,
            );
        }
    }

    pub fn stats(&self) -> LsmStats {
        let level_files = self.compaction.level_file_counts();
        let l0_files = level_files.first().copied().unwrap_or(0);
        let total_sst_files = level_files.iter().sum();
        LsmStats {
            memtable_shard_bytes: self.memtable_shard_bytes,
            pending_flushes: self.pending_flushes.load(Ordering::Relaxed),
            flushes_completed: self.flushes_completed.load(Ordering::Relaxed),
            write_stalls: self.write_stalls.load(Ordering::Relaxed),
            last_flush_micros: self.last_flush_micros.load(Ordering::Relaxed),
            last_stall_micros: self.last_stall_micros.load(Ordering::Relaxed),
            compacting: self.compaction.is_compacting(),
            l0_files,
            total_sst_files,
            level_files,
        }
    }

    /// Admin/COMPACT: run compaction passes in the background-friendly API.
    pub fn compact_now(&self) {
        self.compaction.maybe_compact();
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

        let mut to_flush: Vec<FlushPlan> = Vec::new();

        for (shard, indices) in shard_indices.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            self.wait_if_memtable_pressured(shard);
            let plan = {
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
            if let Some(plan) = plan {
                to_flush.push(plan);
            }
        }

        for plan in to_flush {
            self.enqueue_flush(Some(plan));
        }
    }

    /// Write two entries (meta + data) for one logical Redis key under a single
    /// WAL lock and a single memtable write-lock.
    ///
    /// Because shard_of() routes by the Redis-key portion (stripping the tag/db
    /// prefix), the meta key and string-data key for the same logical Redis key
    /// always map to the same shard.  This gives put2 the same lock overhead as
    /// a single put() — half the cost of two independent put() calls.
    pub fn put2(&self, key1: Vec<u8>, val1: Vec<u8>, key2: Vec<u8>, val2: Vec<u8>) {
        let shard = shard_of(&key1);
        debug_assert_eq!(
            shard,
            shard_of(&key2),
            "put2: keys must map to the same shard (same Redis key, different tags)"
        );
        self.wait_if_memtable_pressured(shard);
        let plan = {
            let mut wal = self.wals[shard].lock();
            let _ = wal.append(WalOpType::Put, &key1, &val1);
            let _ = wal.append(WalOpType::Put, &key2, &val2);
            let mut mem = self.mem_shards[shard].write();
            mem.active.put(key1, val1);
            mem.active.put(key2, val2);
            self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
        };
        self.enqueue_flush(plan);
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard = shard_of(&key);
        self.wait_if_memtable_pressured(shard);
        let plan = {
            let mut wal = self.wals[shard].lock();
            let _ = wal.append(WalOpType::Put, &key, &value);
            let mut mem = self.mem_shards[shard].write();
            mem.active.put(key, value);
            self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
        };
        self.enqueue_flush(plan);
    }

    pub fn delete(&self, key: Vec<u8>) {
        let shard = shard_of(&key);
        self.wait_if_memtable_pressured(shard);
        let plan = {
            let mut wal = self.wals[shard].lock();
            let _ = wal.append(WalOpType::Delete, &key, &[]);
            let mut mem = self.mem_shards[shard].write();
            mem.active.delete(key);
            self.maybe_rotate_and_snapshot(shard, &mut wal, &mut mem)
        };
        self.enqueue_flush(plan);
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
        // Memtables FIRST, then SSTables. With async flush, the opposite order
        // can drop keys: SST snapshot taken → flush publishes SST + clears
        // immutable → memtable scan misses the data and the SST list is stale.
        // Mem-first: a concurrent flush may briefly duplicate a key into both
        // layers, but BTreeMap merge keeps a single correct value.
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

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

        for (k, v) in self.compaction.scan_sstables(start, end) {
            // Older SST data must not override newer memtable values.
            merged.entry(k).or_insert(v);
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

    fn flush_snapshot_to_l0(&self, snapshot: BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> io::Result<()> {
        Self::flush_snapshot_to_l0_static(&self.data_dir, &self.manifest, &snapshot)
    }

    /// Write a memtable snapshot to a new L0 SST. Does **not** run compaction —
    /// that belongs on the compaction thread so SET/PING stay responsive.
    fn flush_snapshot_to_l0_static(
        data_dir: &PathBuf,
        manifest: &Manifest,
        snapshot: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> io::Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }

        let seq = manifest.next_sequence();
        let filename = format!("L0-{}.sst", seq);
        let path = data_dir.join(&filename);

        SSTableWriter::write(&path, snapshot)?;
        // Skip empty SSTs (should not happen for non-empty snapshot, but guard disk)
        if std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
            let _ = fs::remove_file(&path);
            return Ok(());
        }
        manifest.add_file(0, filename);
        manifest.save()?;
        Ok(())
    }

    pub fn close(&self) {
        self.bg_stop.store(true, Ordering::Relaxed);
        // Wait briefly for in-flight background flushes before force-flushing.
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.pending_flushes.load(Ordering::Relaxed) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.force_flush();
        for wal in self.wals.iter() {
            let mut w = wal.lock();
            let _ = w.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LsmStorage;
    use crate::storage::wal::{WalOpType, WriteAheadLog};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be available")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("forgekv-{name}-{unique}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }

    #[test]
    fn recover_wal_entries_replays_archived_rotation_files() {
        let dir = temp_dir("wal-recovery");
        let wal_path = dir.join("wal-00.wal");
        let archive_path = dir.join("archive-wal-00-1.wal.bak");

        {
            let seq = Arc::new(AtomicU64::new(0));
            let mut wal = WriteAheadLog::new(&wal_path, true, seq).expect("wal should be created");
            wal.append(WalOpType::Put, b"key", b"value")
                .expect("wal append should succeed");
            wal.rotate(&archive_path)
                .expect("wal rotation should succeed");
        }

        let (entries, max_seq) =
            LsmStorage::recover_wal_entries(&dir).expect("recovery should succeed");

        assert_eq!(max_seq, 1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2, b"key");
        assert_eq!(entries[0].3, b"value");

        fs::remove_dir_all(&dir).expect("temp dir should be removed");
    }
}
