use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use super::manifest::Manifest;
use super::sstable::{SSTableReader, SSTableWriter};

/// Trigger L0→L1 once this many L0 files accumulate.
/// With 256 shards flushing independently, a low threshold lets L0 explode
/// faster than a single compaction thread can drain it.
const L0_THRESHOLD: usize = 64;
/// Cap how many L0 files one compaction pass consumes (bounds peak RAM).
const L0_COMPACT_BATCH: usize = 128;
/// Target SST size after compaction — fewer, larger files → less churn.
const TARGET_FILE_SIZE: usize = 64 * 1024 * 1024;
/// Max bytes per level (index = level). L0 is file-count based.
/// Sized for multi-GB hotel caches without perpetual L1 self-rewrites.
const LEVEL_MAX_BYTES: [u64; 7] = [
    0,
    512 * 1024 * 1024,       // L1: 512MB
    4 * 1024 * 1024 * 1024,  // L2: 4GB
    16 * 1024 * 1024 * 1024, // L3: 16GB
    64 * 1024 * 1024 * 1024, // L4: 64GB
    256 * 1024 * 1024 * 1024,// L5: 256GB
    u64::MAX,                // L6: unbounded sink
];

pub struct Compaction {
    pub(super) data_dir: PathBuf,
    pub(super) manifest: Arc<Manifest>,
    /// Cache of mmapped SST readers. INVARIANT: only files that are in the manifest may be
    /// inserted here. Every compaction path removes a file from the manifest (and saves it)
    /// *before* `retire_files` bumps `retire_gen` and evicts it; `get_or_open_reader` only
    /// inserts if the file was in the manifest and `retire_gen` did not move in between, so a
    /// reader can never re-insert a file that compaction has already dropped (that race
    /// leaked deleted-but-mapped SSTs, ForgeKV <= 0.3.1). The miss path never holds this
    /// lock while waiting on the manifest lock.
    pub(super) open_readers: RwLock<HashMap<String, Arc<SSTableReader>>>,
    /// Readers opened for a file that was no longer in the manifest (served once, not cached).
    pub(super) uncached_opens: AtomicU64,
    /// Cache entries removed by the obsolete-reader sweep. Non-zero is expected while
    /// COMPACT FULL keeps replaced inputs cached for their grace period; it must not grow
    /// with normal compaction.
    pub(super) obsolete_evicted: AtomicU64,
    /// Bumped by `retire_files` after the files left the manifest and before they are
    /// evicted. `get_or_open_reader` only caches when it is unchanged across its manifest
    /// check, which lets it check the manifest without holding the `open_readers` lock.
    retire_gen: AtomicU64,
    /// Rate-limited result of the /proc/self/maps scan used by INFO (see
    /// `deleted_sst_mappings_cached`).
    deleted_maps_cache: Mutex<Option<(Instant, (usize, u64))>>,
    /// Tests only: switches that disable one safety mechanism at a time, so the regression
    /// tests can prove each one is what keeps the leak away.
    #[cfg(test)]
    pub(super) test_hooks: TestHooks,
    /// Prevent overlapping compaction passes (bg thread + post-flush).
    pub(super) compacting: AtomicBool,
    /// Set by flush worker; bg compaction thread clears and runs.
    compact_requested: AtomicBool,
    /// State of the optional throttled `COMPACT FULL` job (see full_compaction.rs).
    pub(super) full: super::full_compaction::FullState,
}

impl Compaction {
    pub fn new(data_dir: PathBuf, manifest: Arc<Manifest>) -> Self {
        Compaction {
            data_dir,
            manifest,
            open_readers: RwLock::new(HashMap::new()),
            uncached_opens: AtomicU64::new(0),
            obsolete_evicted: AtomicU64::new(0),
            retire_gen: AtomicU64::new(0),
            deleted_maps_cache: Mutex::new(None),
            #[cfg(test)]
            test_hooks: TestHooks::default(),
            compacting: AtomicBool::new(false),
            compact_requested: AtomicBool::new(false),
            full: super::full_compaction::FullState::default(),
        }
    }

    pub fn get_or_open_reader(&self, path: &str) -> io::Result<Arc<SSTableReader>> {
        {
            let readers = self.open_readers.read();
            if let Some(r) = readers.get(path) {
                return Ok(r.clone());
            }
        }

        // Miss path: open + check the manifest without holding the open_readers lock.
        // `retire_files` removes a file from the manifest, THEN bumps retire_gen, THEN evicts
        // under the write lock. So if the generation is unchanged when we insert (under the
        // write lock), any retirement of `path` that our manifest check missed has not
        // evicted yet and will evict our entry after we release the lock.
        let gen = self.retire_gen.load(Ordering::SeqCst);
        let full_path = self.data_dir.join(path);
        let reader = Arc::new(SSTableReader::open(&full_path)?);
        let live = self.manifest.contains(path) || self.manifest_check_disabled();
        #[cfg(test)]
        {
            // Fault injection: model a reader preempted between its manifest check and the
            // insert, which is exactly where a concurrent retirement must not slip through.
            let us = self.test_hooks.open_to_insert_delay_us.load(Ordering::Relaxed);
            if us > 0 {
                std::thread::sleep(std::time::Duration::from_micros(us));
            }
        }

        let mut readers = self.open_readers.write();
        if let Some(r) = readers.get(path) {
            // Another thread cached it while we were opening; drop our duplicate mapping.
            return Ok(r.clone());
        }
        if live
            && (self.retire_gen.load(Ordering::SeqCst) == gen || self.manifest_check_disabled())
        {
            readers.insert(path.to_string(), reader.clone());
        } else {
            // Caller is working from a stale manifest snapshot (or a retirement raced us).
            // Serve this one read from a private mapping that is unmapped as soon as the
            // caller drops it.
            self.uncached_opens.fetch_add(1, Ordering::Relaxed);
        }
        Ok(reader)
    }

    #[cfg(test)]
    fn manifest_check_disabled(&self) -> bool {
        self.test_hooks.disable_manifest_check.load(Ordering::SeqCst)
    }
    #[cfg(not(test))]
    #[inline(always)]
    fn manifest_check_disabled(&self) -> bool {
        false
    }

    pub fn close_reader(&self, path: &str) {
        let mut readers = self.open_readers.write();
        readers.remove(path);
    }

    /// Drop the cached readers of files that have been removed from the manifest, delete them
    /// from disk, then evict once more (belt and braces) so no cached mmap outlives the unlink.
    /// Callers must already have removed `files` from the manifest (and saved it).
    pub(super) fn retire_files(&self, files: &[String]) {
        if files.is_empty() {
            return;
        }
        self.retire_gen.fetch_add(1, Ordering::SeqCst);
        for f in files {
            self.close_reader(f);
        }
        for f in files {
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
        for f in files {
            self.close_reader(f);
        }
    }

    /// Evict every cached reader whose file is not in the manifest. Returns how many were
    /// evicted. Runs after each compaction pass and COMPACT FULL step.
    pub fn evict_obsolete_readers(&self) -> usize {
        let mut readers = self.open_readers.write();
        if readers.is_empty() {
            return 0;
        }
        let live: HashSet<String> = self.manifest.snapshot().into_iter().flatten().collect();
        let before = readers.len();
        readers.retain(|k, _| live.contains(k));
        let n = before - readers.len();
        if n > 0 {
            self.obsolete_evicted.fetch_add(n as u64, Ordering::Relaxed);
        }
        n
    }

    /// The sweep run after every compaction pass / COMPACT FULL job.
    pub(super) fn auto_sweep(&self) {
        #[cfg(test)]
        if self.test_hooks.disable_auto_sweep.load(Ordering::SeqCst) {
            return;
        }
        self.evict_obsolete_readers();
    }

    /// `deleted_sst_mappings` for INFO, recomputed at most once per `max_age` (reading
    /// /proc/self/maps costs O(mappings); INFO may be polled often). Returns
    /// (files, bytes, age of the value).
    pub fn deleted_sst_mappings_cached(&self, max_age: Duration) -> (usize, u64, Duration) {
        let mut cache = self.deleted_maps_cache.lock();
        if let Some((at, v)) = *cache {
            let age = at.elapsed();
            if age < max_age {
                return (v.0, v.1, age);
            }
        }
        let v = deleted_sst_mappings(&self.data_dir);
        *cache = Some((Instant::now(), v));
        (v.0, v.1, Duration::ZERO)
    }

    /// (cached readers, cached readers whose file is not in the manifest).
    pub fn reader_cache_stats(&self) -> (usize, usize) {
        let readers = self.open_readers.read();
        let live: HashSet<String> = self.manifest.snapshot().into_iter().flatten().collect();
        let obsolete = readers.keys().filter(|k| !live.contains(*k)).count();
        (readers.len(), obsolete)
    }

    /// Names of the cached readers (tests / diagnostics).
    pub fn cached_reader_names(&self) -> Vec<String> {
        self.open_readers.read().keys().cloned().collect()
    }

    pub fn maybe_compact(&self) {
        // Single-flight: skip if another pass is already running.
        if self
            .compacting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        // Drain while work remains, but bound iterations so writers aren't starved
        // of disk bandwidth. Prefer shorter bursts under write load.
        let mut any_work = false;
        for _ in 0..4 {
            let did_work = self.compact_once();
            if !did_work {
                break;
            }
            any_work = true;
        }
        self.auto_sweep();
        self.compacting.store(false, Ordering::SeqCst);
        if any_work {
            // Merges materialise whole input files in a BTreeMap; return those pages.
            crate::heap::request_release();
        }
    }

    /// Ask the compaction thread to run soon (non-blocking).
    pub fn request_compact(&self) {
        self.compact_requested.store(true, Ordering::Relaxed);
    }

    pub fn take_compact_request(&self) -> bool {
        self.compact_requested.swap(false, Ordering::Relaxed)
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::Relaxed)
    }

    pub fn level_file_counts(&self) -> Vec<usize> {
        let n = self.manifest.level_count().max(7);
        (0..n).map(|lvl| self.manifest.get_level(lvl).len()).collect()
    }

    fn compact_once(&self) -> bool {
        // If L1 is massively oversized, promote it first so L0→L1 cannot
        // keep rewriting against a 10k-file L1 forever.
        {
            let l1 = self.manifest.get_level(1);
            if l1.len() > 512 {
                let total_size: u64 = l1
                    .iter()
                    .filter_map(|f| {
                        std::fs::metadata(self.data_dir.join(f))
                            .ok()
                            .map(|m| m.len())
                    })
                    .sum();
                if total_size > LEVEL_MAX_BYTES[1] || l1.len() > 1024 {
                    self.compact_level_to_next(1, l1);
                    return true;
                }
            }
        }

        let l0_files = self.manifest.get_level(0);
        if l0_files.len() >= L0_THRESHOLD {
            let batch: Vec<String> = l0_files.into_iter().take(L0_COMPACT_BATCH).collect();
            self.compact_l0_to_l1(batch);
            return true;
        }

        for level in 1..LEVEL_MAX_BYTES.len().saturating_sub(1) {
            let files = self.manifest.get_level(level);
            if files.is_empty() {
                continue;
            }

            let total_size: u64 = files
                .iter()
                .filter_map(|f| {
                    let p = self.data_dir.join(f);
                    std::fs::metadata(p).ok().map(|m| m.len())
                })
                .sum();

            if total_size > LEVEL_MAX_BYTES[level] {
                self.compact_level_to_next(level, files);
                return true;
            }
        }
        false
    }

    pub fn search_sstables(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        // Search L0 newest first (L0 files are appended, so last = newest)
        let l0_files = self.manifest.get_level(0);
        for file in l0_files.iter().rev() {
            if let Ok(reader) = self.get_or_open_reader(file) {
                if let Some(result) = reader.get(key) {
                    return Some(result);
                }
            }
        }

        // Search L1+ in order (each level is sorted, non-overlapping)
        let level_count = self.manifest.level_count();
        for level in 1..level_count {
            let files = self.manifest.get_level(level);
            for file in &files {
                if let Ok(reader) = self.get_or_open_reader(file) {
                    // Check key range
                    if key < reader.smallest_key.as_slice() || key > reader.largest_key.as_slice() {
                        continue;
                    }
                    if let Some(result) = reader.get(key) {
                        return Some(result);
                    }
                }
            }
        }

        None
    }

    pub fn scan_sstables(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Scan L1+ from the oldest level down to L1, overwriting so that
        // lower (newer) levels always win over higher (older) levels.
        let level_count = self.manifest.level_count();
        for level in (1..level_count).rev() {
            let files = self.manifest.get_level(level);
            for file in &files {
                if let Ok(reader) = self.get_or_open_reader(file) {
                    for (k, v) in reader.scan(start, end) {
                        merged.insert(k, v);
                    }
                }
            }
        }

        // Then scan L0 (newer, overrides older) - oldest first, then newer overwrites
        let l0_files = self.manifest.get_level(0);
        for file in &l0_files {
            if let Ok(reader) = self.get_or_open_reader(file) {
                for (k, v) in reader.scan(start, end) {
                    merged.insert(k, v);
                }
            }
        }

        merged
    }

    fn compact_l0_to_l1(&self, l0_files: Vec<String>) {
        if l0_files.is_empty() {
            return;
        }
        let merged = self.merge_files(&l0_files);

        // Never merge the entire L1 set when it is huge — that stalls forever
        // (15k tiny SSTs × full rewrite was the production failure mode).
        // Cap L1 inputs; remaining L1 is drained by compact_level_to_next.
        const MAX_L1_MERGE: usize = 64;
        let l1_all = self.manifest.get_level(1);
        let l1_files: Vec<String> = if l1_all.len() > MAX_L1_MERGE {
            // Prefer a small newest slice; L0 still wins on key overwrite.
            l1_all
                .iter()
                .rev()
                .take(MAX_L1_MERGE)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect()
        } else {
            l1_all
        };

        let merged = if !l1_files.is_empty() {
            let mut all_merged = self.merge_files(&l1_files);
            // L0 wins over L1
            for (k, v) in merged {
                all_merged.insert(k, v);
            }
            all_merged
        } else {
            merged
        };

        let new_files = match self.write_compacted_files(merged, 1) {
            Some(f) => f,
            None => return, // write failed: inputs untouched, retried next pass
        };

        // Publish the new version first (manifest), THEN evict + unlink the inputs. Evicting
        // before the manifest update let a concurrent GET/SCAN re-map an input into the cache
        // after close_reader, leaking it as a deleted-but-mapped file forever.
        // Remove only the L1 files we actually merged.
        if !self.publish(&[(0, &l0_files[..]), (1, &l1_files[..])], 1, &new_files) {
            return;
        }
        let mut retired = l0_files;
        retired.extend(l1_files);
        self.retire_files(&retired);
    }

    /// Atomically (w.r.t. the manifest lock) remove `removals` and append `new_files` to
    /// `out_level`, and persist it. On a failed save the manifest change is rolled back,
    /// the unpublished outputs are deleted and false is returned: the inputs must then be
    /// kept (no eviction, no unlink) — unlinking files the on-disk MANIFEST still lists
    /// would lose data on the next restart.
    fn publish(&self, removals: &[(usize, &[String])], out_level: usize, new_files: &[String]) -> bool {
        let res = self.manifest.apply_and_save(|levels| {
            for (lvl, files) in removals {
                if let Some(l) = levels.get_mut(*lvl) {
                    l.retain(|x| !files.contains(x));
                }
            }
            while levels.len() <= out_level {
                levels.push(Vec::new());
            }
            levels[out_level].extend(new_files.iter().cloned());
        });
        match res {
            Ok(()) => true,
            Err(e) => {
                eprintln!(
                    "ERROR compaction: manifest save failed ({e}); keeping {} input file(s), discarding {} unpublished output(s)",
                    removals.iter().map(|(_, f)| f.len()).sum::<usize>(),
                    new_files.len()
                );
                self.discard_unpublished(new_files);
                false
            }
        }
    }

    /// Delete output files that never made it into the manifest.
    fn discard_unpublished(&self, files: &[String]) {
        for f in files {
            self.close_reader(f);
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
    }

    /// Promote an oversized level into the next level (never rewrite in-place).
    /// In-place L1 rewrites were the root cause of 15k+ tiny SST churn.
    fn compact_level_to_next(&self, level: usize, files: Vec<String>) {
        let next_level = level + 1;
        if files.is_empty() {
            return;
        }

        // Bound RAM: if the level is huge, promote only a prefix of files.
        const MAX_FILES_PER_PASS: usize = 256;
        let (batch, _rest): (Vec<_>, Vec<_>) = if files.len() > MAX_FILES_PER_PASS {
            let mut sorted = files;
            sorted.sort();
            let batch = sorted.iter().take(MAX_FILES_PER_PASS).cloned().collect();
            let rest = sorted.into_iter().skip(MAX_FILES_PER_PASS).collect();
            (batch, rest)
        } else {
            (files, Vec::new())
        };

        let merged = self.merge_files(&batch);
        let next_files = self.manifest.get_level(next_level);

        // Full merge with next level when promoting (simple + correct for our sizes).
        // Cap next-level inputs similarly if enormous.
        let next_batch: Vec<String> = if next_files.len() > MAX_FILES_PER_PASS {
            next_files.into_iter().take(MAX_FILES_PER_PASS).collect()
        } else {
            next_files
        };

        let merged = if !next_batch.is_empty() {
            let mut all_merged = self.merge_files(&next_batch);
            for (k, v) in merged {
                all_merged.insert(k, v); // newer level wins
            }
            all_merged
        } else {
            merged
        };

        // Drop tombstones at level >= 2 destinations
        let merged = if next_level >= 2 {
            merged.into_iter().filter(|(_, v)| v.is_some()).collect()
        } else {
            merged
        };

        let new_files = match self.write_compacted_files(merged, next_level) {
            Some(f) => f,
            None => return, // write failed: inputs untouched, retried next pass
        };

        if !self.publish(&[(level, &batch[..]), (next_level, &next_batch[..])], next_level, &new_files) {
            return;
        }

        // Evict + unlink only after the manifest no longer lists the inputs (see compact_l0_to_l1).
        let mut retired = batch;
        retired.extend(next_batch);
        self.retire_files(&retired);
    }

    fn merge_files(&self, files: &[String]) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Oldest first (index 0 = oldest), newest last wins
        for file in files {
            if let Ok(reader) = self.get_or_open_reader(file) {
                for (k, v) in reader.scan(None, None) {
                    merged.insert(k, v);
                }
            }
        }

        merged
    }

    fn write_compacted_files(
        &self,
        entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        level: usize,
    ) -> Option<Vec<String>> {
        let mut new_files = Vec::new();

        if entries.is_empty() {
            return Some(new_files);
        }

        let mut chunk: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut chunk_size = 0usize;

        for (k, v) in entries {
            let entry_size = k.len() + v.as_ref().map(|b| b.len()).unwrap_or(0) + 10;
            chunk_size += entry_size;
            chunk.insert(k, v);

            if chunk_size >= TARGET_FILE_SIZE {
                match self.write_chunk(&chunk, level) {
                    Some(filename) => new_files.push(filename),
                    None => {
                        // A lost chunk would silently drop data once the inputs are unlinked.
                        eprintln!("ERROR compaction: writing an L{level} output failed; aborting this pass, inputs kept");
                        self.discard_unpublished(&new_files);
                        return None;
                    }
                }
                chunk.clear();
                chunk_size = 0;
            }
        }

        if !chunk.is_empty() {
            match self.write_chunk(&chunk, level) {
                Some(filename) => new_files.push(filename),
                None => {
                    eprintln!("ERROR compaction: writing an L{level} output failed; aborting this pass, inputs kept");
                    self.discard_unpublished(&new_files);
                    return None;
                }
            }
        }

        Some(new_files)
    }

    fn write_chunk(
        &self,
        chunk: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        level: usize,
    ) -> Option<String> {
        if chunk.is_empty() {
            return None;
        }
        let seq = self.manifest.next_sequence();
        let filename = format!("L{}-{}.sst", level, seq);
        let path = self.data_dir.join(&filename);
        if SSTableWriter::write(&path, chunk).is_ok() {
            // Avoid leaving zero-byte ghost SSTs in the manifest.
            if std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
                let _ = std::fs::remove_file(&path);
                return None;
            }
            Some(filename)
        } else {
            let _ = std::fs::remove_file(&path);
            None
        }
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestHooks {
    /// Cache every opened reader without the manifest/generation check (pre-0.3.2 behaviour).
    pub disable_manifest_check: AtomicBool,
    /// Skip the obsolete-reader sweep after compaction passes / COMPACT FULL jobs.
    pub disable_auto_sweep: AtomicBool,
    /// Sleep this long between the manifest check and the cache insert on a miss.
    pub open_to_insert_delay_us: AtomicU64,
}

/// Count SST files under `data_dir` that are still mmapped by this process although they have
/// been unlinked ("(deleted)" entries in /proc/self/maps). Returns (files, bytes mapped).
/// Linux only; returns (0, 0) when /proc is unavailable.
pub fn deleted_sst_mappings(data_dir: &Path) -> (usize, u64) {
    let maps = match std::fs::read_to_string("/proc/self/maps") {
        Ok(m) => m,
        Err(_) => return (0, 0),
    };
    let prefix = std::fs::canonicalize(data_dir)
        .ok()
        .map(|p| p.to_string_lossy().to_string());
    count_deleted_sst_maps(&maps, prefix.as_deref())
}

/// Parser for `deleted_sst_mappings`, separated for tests. Counts unique (dev, inode) pairs.
pub fn count_deleted_sst_maps(maps: &str, prefix: Option<&str>) -> (usize, u64) {
    let mut seen: HashMap<(String, String), u64> = HashMap::new();
    for line in maps.lines() {
        if !line.ends_with(".sst (deleted)") {
            continue;
        }
        // addr perms offset dev inode path...
        let mut it = line.split_whitespace();
        let (addr, _perms, _off, dev, inode) = match (it.next(), it.next(), it.next(), it.next(), it.next()) {
            (Some(a), Some(b), Some(c), Some(d), Some(e)) => (a, b, c, d, e),
            _ => continue,
        };
        let path = it.collect::<Vec<_>>().join(" ");
        if let Some(pre) = prefix {
            if !path.starts_with(pre) {
                continue;
            }
        }
        let len = addr
            .split_once('-')
            .and_then(|(a, b)| Some(u64::from_str_radix(b, 16).ok()? - u64::from_str_radix(a, 16).ok()?))
            .unwrap_or(0);
        *seen.entry((dev.to_string(), inode.to_string())).or_insert(0) += len;
    }
    (seen.len(), seen.values().sum())
}

#[cfg(test)]
mod mmap_leak_tests {
    //! Regression tests for the deleted-but-mapped SST leak (ForgeKV <= 0.3.1): a GET/SCAN
    //! that re-opened an input file between `close_reader` and `remove_file` re-inserted it
    //! into `open_readers`, and nothing ever evicted it, so the unlinked file kept its disk
    //! space for the life of the process. The race test disables the automatic sweep
    //! (which would otherwise hide a leak) and samples the cache and /proc/self/maps while it
    //! runs; a companion test proves it fails with the manifest check disabled.
    use super::*;
    use crate::storage::full_compaction::FullCompactConfig;
    use std::collections::{BTreeMap, HashSet};
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    type Entries = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let p = std::env::temp_dir().join(format!("forgekv-mmapleak-{name}-{unique}"));
        std::fs::create_dir_all(&p).unwrap();
        std::fs::canonicalize(&p).unwrap()
    }

    /// Unique deleted .sst inodes under `dir` that this process still maps.
    fn deleted_maps_under(dir: &Path) -> usize {
        deleted_inodes_under(dir).len()
    }

    fn deleted_inodes_under(dir: &Path) -> HashSet<String> {
        let maps = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
        let pre = dir.to_string_lossy().to_string();
        let mut inodes = HashSet::new();
        for line in maps.lines() {
            if line.ends_with(".sst (deleted)") && line.contains(&pre) {
                if let Some(ino) = line.split_whitespace().nth(4) {
                    inodes.insert(ino.to_string());
                }
            }
        }
        inodes
    }

    fn cached_not_in_manifest(comp: &Compaction, manifest: &Manifest) -> Vec<String> {
        let live: HashSet<String> = manifest.snapshot().into_iter().flatten().collect();
        let readers = comp.open_readers.read();
        let mut v: Vec<String> = readers.keys().filter(|k| !live.contains(*k)).cloned().collect();
        v.sort();
        v
    }

    fn write_sst(dir: &Path, manifest: &Manifest, level: usize, m: &Entries) -> String {
        let name = format!("L{}-{}.sst", level, manifest.next_sequence());
        SSTableWriter::write(&dir.join(&name), m).unwrap();
        manifest.add_file(level, name.clone());
        manifest.save().unwrap();
        name
    }

    struct RaceResult {
        flushed: usize,
        full_runs: usize,
        samples: usize,
        /// Max deleted-but-mapped inodes seen in two consecutive samples (>= 100 ms apart).
        persistent_deleted: usize,
        /// Max non-manifest reader-cache entries seen in two consecutive samples.
        persistent_obsolete: usize,
        final_deleted: usize,
        final_obsolete: Vec<String>,
    }

    /// Concurrent GET + SCAN loops while L0 files keep arriving, normal compaction runs
    /// continuously and COMPACT FULL rewrites the deep levels for `secs`. The automatic
    /// obsolete-reader sweep is DISABLED so it cannot hide a leak: the reader cache must stay
    /// clean through the manifest check alone. A monitor samples /proc/self/maps and the
    /// cache every 100 ms; readers sleep 300 us between their manifest check and the cache
    /// insert to widen the race window; an entry that survives two consecutive samples is a leak
    /// (legitimate transients — an uncached read in flight, the 5 ms COMPACT FULL grace,
    /// the evict/unlink window — last microseconds to milliseconds).
    fn run_race(disable_manifest_check: bool, secs: u64) -> RaceResult {
        let dir = temp_dir("race");
        let manifest = Arc::new(Manifest::new(dir.join("MANIFEST")));
        let comp = Arc::new(Compaction::new(dir.clone(), manifest.clone()));
        comp.test_hooks.disable_auto_sweep.store(true, Ordering::SeqCst);
        comp.test_hooks.disable_manifest_check.store(disable_manifest_check, Ordering::SeqCst);
        // Widen the check->insert window (a preempted reader) so a missing check shows up
        // reliably instead of once in a few runs.
        comp.test_hooks.open_to_insert_delay_us.store(300, Ordering::SeqCst);

        // Deep levels with old versions, so COMPACT FULL has shadowed data to drop.
        for part in 0..8u32 {
            let m: Entries = (0..400u32)
                .filter(|i| i % 8 == part)
                .map(|i| (format!("k{i:05}").into_bytes(), Some(vec![b'o'; 300])))
                .collect();
            write_sst(&dir, &manifest, 4, &m);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let flushed = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();

        // "Flush" thread: a new small L0 file every ~0.5 ms (overlapping keys).
        {
            let (dir, manifest, comp, stop, flushed) =
                (dir.clone(), manifest.clone(), comp.clone(), stop.clone(), flushed.clone());
            threads.push(std::thread::spawn(move || {
                let mut n = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    let m: Entries = (0..20u32)
                        .map(|j| (format!("k{:05}", (n * 37 + j * 11) % 400).into_bytes(), Some(format!("v{n}").into_bytes())))
                        .collect();
                    write_sst(&dir, &manifest, 0, &m);
                    flushed.fetch_add(1, Ordering::Relaxed);
                    comp.request_compact();
                    n += 1;
                    std::thread::sleep(Duration::from_micros(500));
                }
            }));
        }
        // Normal background compaction.
        {
            let (comp, stop) = (comp.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    comp.maybe_compact();
                    std::thread::sleep(Duration::from_millis(1));
                }
            }));
        }
        // GET loops (mostly misses so every file of the stale manifest snapshot is opened).
        for t in 0..4u32 {
            let (comp, stop) = (comp.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let mut i = t;
                while !stop.load(Ordering::SeqCst) {
                    let _ = comp.search_sstables(format!("miss{i:07}").as_bytes());
                    let _ = comp.search_sstables(format!("k{:05}", i % 400).as_bytes());
                    i = i.wrapping_add(7);
                }
            }));
        }
        // SCAN loops.
        for _ in 0..2 {
            let (comp, stop) = (comp.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let _ = comp.scan_sstables(Some(b"k00100"), Some(b"k00110"));
                }
            }));
        }
        // Monitor: periodic in-run samples.
        let monitor = {
            let (dir, manifest, comp, stop) = (dir.clone(), manifest.clone(), comp.clone(), stop.clone());
            std::thread::spawn(move || {
                let (mut prev_del, mut prev_obs) = (HashSet::new(), HashSet::new());
                let (mut max_del, mut max_obs, mut samples) = (0usize, 0usize, 0usize);
                while !stop.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(100));
                    let del = deleted_inodes_under(&dir);
                    let obs: HashSet<String> = cached_not_in_manifest(&comp, &manifest).into_iter().collect();
                    max_del = max_del.max(del.intersection(&prev_del).count());
                    max_obs = max_obs.max(obs.intersection(&prev_obs).count());
                    prev_del = del;
                    prev_obs = obs;
                    samples += 1;
                }
                (max_del, max_obs, samples)
            })
        };

        // COMPACT FULL rounds while all of the above runs.
        let t0 = Instant::now();
        let mut full_runs = 0;
        while t0.elapsed() < Duration::from_secs(secs) {
            let cfg = FullCompactConfig {
                rate_mb_s: 0,
                min_free_bytes: 0,
                dry_run: false,
                grace: Duration::from_millis(5),
            };
            if comp.full_start(cfg).is_ok() {
                full_runs += 1;
                while comp.full_is_running() {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        stop.store(true, Ordering::SeqCst);
        for t in threads {
            t.join().unwrap();
        }
        let (persistent_deleted, persistent_obsolete, samples) = monitor.join().unwrap();
        // Quiesce with one more compaction pass (the sweep is still disabled).
        comp.maybe_compact();

        let r = RaceResult {
            flushed: flushed.load(Ordering::Relaxed),
            full_runs,
            samples,
            persistent_deleted,
            persistent_obsolete,
            final_deleted: deleted_maps_under(&dir),
            final_obsolete: cached_not_in_manifest(&comp, &manifest),
        };
        eprintln!(
            "mmap-leak race (manifest_check={}): l0_files_flushed={} full_runs={} samples={} \
             in_run_persistent_deleted_max={} in_run_persistent_obsolete_max={} \
             final_deleted_but_mapped={} final_obsolete_cached={}",
            if disable_manifest_check { "OFF" } else { "on" },
            r.flushed, r.full_runs, r.samples, r.persistent_deleted, r.persistent_obsolete,
            r.final_deleted, r.final_obsolete.len()
        );
        drop(comp);
        let _ = std::fs::remove_dir_all(&dir);
        r
    }

    fn assert_no_leak(r: &RaceResult) {
        assert!(r.flushed > 200, "test must drive several compactions");
        assert!(r.full_runs > 3, "test must drive several COMPACT FULL jobs");
        assert!(r.samples > 10, "monitor must sample during the run");
        assert_eq!(r.persistent_deleted, 0, "deleted-but-mapped SSTs persisted during the run");
        assert_eq!(r.persistent_obsolete, 0, "non-manifest readers persisted in the cache during the run");
        assert_eq!(r.final_obsolete, Vec::<String>::new(), "open_readers must only hold manifest files");
        assert_eq!(r.final_deleted, 0, "no unlinked SST may stay mmapped");
    }

    /// The regression test. `FORGEKV_TEST_DISABLE_MANIFEST_CHECK=1` runs it with the
    /// manifest check switched off, to show that it then fails.
    #[test]
    fn concurrent_get_scan_during_compaction_and_compact_full_leaks_nothing() {
        let off = std::env::var("FORGEKV_TEST_DISABLE_MANIFEST_CHECK").map(|v| v == "1").unwrap_or(false);
        assert_no_leak(&run_race(off, 6));
    }

    /// Mutation check for the test above: with the manifest check disabled (and the sweep
    /// disabled, as in the test), the same workload must produce a detectable leak.
    #[test]
    fn race_test_detects_leak_when_manifest_check_disabled() {
        let r = run_race(true, 4);
        assert!(
            r.final_deleted > 0 || !r.final_obsolete.is_empty() || r.persistent_deleted > 0,
            "the race test no longer detects the leak it guards against"
        );
    }

    /// Deterministic version of the race: a reader holding a stale manifest snapshot opens a
    /// file that compaction has already dropped from the manifest but not yet unlinked.
    #[test]
    fn stale_snapshot_open_is_not_cached_and_unmaps_after_unlink() {
        let dir = temp_dir("stale");
        let manifest = Arc::new(Manifest::new(dir.join("MANIFEST")));
        let comp = Compaction::new(dir.clone(), manifest.clone());
        let mut m: Entries = BTreeMap::new();
        m.insert(b"a".to_vec(), Some(b"1".to_vec()));
        let f = write_sst(&dir, &manifest, 0, &m);
        let stale = manifest.get_level(0); // reader's snapshot
        assert!(comp.get_or_open_reader(&f).is_ok());

        // Compaction: drop from the manifest, evict (the patched order) ...
        manifest.remove_file(0, &f);
        manifest.save().unwrap();
        comp.close_reader(&f);
        // ... and the stale reader re-opens it before the unlink.
        {
            let r = comp.get_or_open_reader(&stale[0]).unwrap();
            assert_eq!(r.get(b"a"), Some(Some(b"1".to_vec())), "stale read still served");
        }
        std::fs::remove_file(dir.join(&f)).unwrap();
        assert_eq!(cached_not_in_manifest(&comp, &manifest), Vec::<String>::new());
        assert_eq!(deleted_maps_under(&dir), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod reader_cache_tests {
    use super::*;

    /// L0->L1 compaction with a failing MANIFEST save: inputs stay on disk, in the manifest
    /// (memory and disk) and readable; the merged outputs are discarded; nothing is retired.
    /// Once saves work again the same compaction succeeds.
    #[test]
    fn failed_manifest_save_in_compaction_keeps_inputs() {
        let dir = std::env::temp_dir().join(format!(
            "forgekv-savefail-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manifest = Arc::new(Manifest::new(dir.join("MANIFEST")));
        let comp = Compaction::new(dir.clone(), manifest.clone());
        let mut inputs = Vec::new();
        for i in 0..(L0_THRESHOLD as u32) {
            let mut m = BTreeMap::new();
            m.insert(format!("k{:03}", i % 40).into_bytes(), Some(format!("v{i}").into_bytes()));
            let name = format!("L0-{}.sst", manifest.next_sequence());
            SSTableWriter::write(&dir.join(&name), &m).unwrap();
            manifest.add_file(0, name.clone());
            inputs.push(name);
        }
        manifest.save().unwrap();
        let read_all = |c: &Compaction| -> Vec<Option<Vec<u8>>> {
            (0..40).map(|i| c.search_sstables(format!("k{i:03}").as_bytes()).flatten()).collect()
        };
        let before = read_all(&comp);
        let snap = manifest.snapshot();
        let disk = std::fs::read_to_string(dir.join("MANIFEST")).unwrap();
        let count_ssts = || {
            std::fs::read_dir(&dir).unwrap().filter(|e| {
                e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".sst")
            }).count()
        };

        manifest.fail_next_saves(1000);
        comp.maybe_compact();
        assert_eq!(manifest.snapshot(), snap, "in-memory manifest rolled back");
        assert_eq!(std::fs::read_to_string(dir.join("MANIFEST")).unwrap(), disk);
        assert!(inputs.iter().all(|f| dir.join(f).exists()), "inputs must not be unlinked");
        assert_eq!(count_ssts(), inputs.len(), "unpublished outputs removed");
        assert_eq!(read_all(&comp), before);
        assert_eq!(comp.reader_cache_stats().1, 0);

        manifest.fail_next_saves(0);
        comp.maybe_compact();
        assert!(manifest.get_level(0).is_empty() && !manifest.get_level(1).is_empty());
        assert!(inputs.iter().all(|f| !dir.join(f).exists()));
        assert_eq!(read_all(&comp), before);
        assert_eq!(comp.reader_cache_stats().1, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn info_deleted_maps_scan_is_rate_limited() {
        let dir = std::env::temp_dir().join(format!(
            "forgekv-mapscache-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let comp = Compaction::new(dir.clone(), Arc::new(Manifest::new(dir.join("MANIFEST"))));
        let (_, _, age0) = comp.deleted_sst_mappings_cached(Duration::from_secs(10));
        assert_eq!(age0, Duration::ZERO, "first call scans");
        std::thread::sleep(Duration::from_millis(20));
        let (_, _, age1) = comp.deleted_sst_mappings_cached(Duration::from_secs(10));
        assert!(age1 >= Duration::from_millis(20), "second call served from cache");
        let (_, _, age2) = comp.deleted_sst_mappings_cached(Duration::from_millis(1));
        assert_eq!(age2, Duration::ZERO, "expired value is rescanned");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sweep_evicts_non_manifest_readers_and_maps_parser_counts_inodes() {
        let maps = "\
7f00-7f10 r--s 00000000 08:01 111 /data/L1-5.sst (deleted)\n\
7f10-7f20 r--s 00000000 08:01 111 /data/L1-5.sst (deleted)\n\
7f20-7f40 r--s 00000000 08:01 222 /data/L2-9.sst (deleted)\n\
7f40-7f50 r--s 00000000 08:01 333 /data/L2-10.sst\n\
7f50-7f60 r--s 00000000 08:01 444 /other/L2-1.sst (deleted)\n";
        assert_eq!(count_deleted_sst_maps(maps, Some("/data")), (2, 0x40));
        assert_eq!(count_deleted_sst_maps(maps, None), (3, 0x50));

        let dir = std::env::temp_dir().join(format!(
            "forgekv-sweep-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manifest = Arc::new(Manifest::new(dir.join("MANIFEST")));
        let comp = Compaction::new(dir.clone(), manifest.clone());
        let mut m = BTreeMap::new();
        m.insert(b"k".to_vec(), Some(b"v".to_vec()));
        for i in 0..3 {
            let name = format!("L0-{i}.sst");
            SSTableWriter::write(&dir.join(&name), &m).unwrap();
            manifest.add_file(0, name.clone());
            comp.get_or_open_reader(&name).unwrap();
        }
        assert_eq!(comp.reader_cache_stats(), (3, 0));
        // Simulate an entry that slipped in (e.g. from an older code path) and left the manifest.
        manifest.remove_file(0, "L0-1.sst");
        assert_eq!(comp.reader_cache_stats(), (3, 1));
        assert_eq!(comp.evict_obsolete_readers(), 1);
        assert_eq!(comp.reader_cache_stats(), (2, 0));
        // Non-manifest files are served but never cached.
        comp.get_or_open_reader("L0-1.sst").unwrap();
        assert_eq!(comp.reader_cache_stats(), (2, 0));
        assert_eq!(comp.uncached_opens.load(Ordering::Relaxed), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
