use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

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
    data_dir: PathBuf,
    manifest: Arc<Manifest>,
    open_readers: RwLock<HashMap<String, Arc<SSTableReader>>>,
    /// Prevent overlapping compaction passes (bg thread + post-flush).
    compacting: AtomicBool,
}

impl Compaction {
    pub fn new(data_dir: PathBuf, manifest: Arc<Manifest>) -> Self {
        Compaction {
            data_dir,
            manifest,
            open_readers: RwLock::new(HashMap::new()),
            compacting: AtomicBool::new(false),
        }
    }

    pub fn get_or_open_reader(&self, path: &str) -> io::Result<Arc<SSTableReader>> {
        {
            let readers = self.open_readers.read();
            if let Some(r) = readers.get(path) {
                return Ok(r.clone());
            }
        }

        let full_path = self.data_dir.join(path);
        let reader = SSTableReader::open(&full_path)?;
        let reader = Arc::new(reader);

        let mut readers = self.open_readers.write();
        readers.insert(path.to_string(), reader.clone());
        Ok(reader)
    }

    pub fn close_reader(&self, path: &str) {
        let mut readers = self.open_readers.write();
        readers.remove(path);
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
        // Drain while work remains, but bound iterations so writers aren't starved.
        for _ in 0..8 {
            let did_work = self.compact_once();
            if !did_work {
                break;
            }
        }
        self.compacting.store(false, Ordering::SeqCst);
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

        let new_files = self.write_compacted_files(merged, 1);

        // Close old readers
        for f in &l0_files {
            self.close_reader(f);
        }
        for f in &l1_files {
            self.close_reader(f);
        }

        // Remove L0 batch
        for f in &l0_files {
            self.manifest.remove_file(0, f);
        }
        // Remove only the L1 files we actually merged
        for f in &l1_files {
            self.manifest.remove_file(1, f);
        }
        // Add new L1 files
        for f in &new_files {
            self.manifest.add_file(1, f.clone());
        }

        let _ = self.manifest.save();

        // Delete old files (release space; close_reader dropped FDs first)
        for f in &l0_files {
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
        for f in &l1_files {
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

        let new_files = self.write_compacted_files(merged, next_level);

        for f in &batch {
            self.close_reader(f);
        }
        for f in &next_batch {
            self.close_reader(f);
        }

        for f in &batch {
            self.manifest.remove_file(level, f);
        }
        for f in &next_batch {
            self.manifest.remove_file(next_level, f);
        }
        for f in &new_files {
            self.manifest.add_file(next_level, f.clone());
        }
        let _ = self.manifest.save();

        for f in &batch {
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
        for f in &next_batch {
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
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
    ) -> Vec<String> {
        let mut new_files = Vec::new();

        if entries.is_empty() {
            return new_files;
        }

        let mut chunk: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut chunk_size = 0usize;

        for (k, v) in entries {
            let entry_size = k.len() + v.as_ref().map(|b| b.len()).unwrap_or(0) + 10;
            chunk_size += entry_size;
            chunk.insert(k, v);

            if chunk_size >= TARGET_FILE_SIZE {
                if let Some(filename) = self.write_chunk(&chunk, level) {
                    new_files.push(filename);
                }
                chunk.clear();
                chunk_size = 0;
            }
        }

        if !chunk.is_empty() {
            if let Some(filename) = self.write_chunk(&chunk, level) {
                new_files.push(filename);
            }
        }

        new_files
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
            None
        }
    }
}
