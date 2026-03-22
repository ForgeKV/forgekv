use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use super::manifest::Manifest;
use super::sstable::{SSTableReader, SSTableWriter};

const L0_THRESHOLD: usize = 4;
/// Max bytes per level: index 0=unused, 1=10MB, 2=100MB, 3=1000MB
const LEVEL_MAX_BYTES: [u64; 4] = [0, 10 * 1024 * 1024, 100 * 1024 * 1024, 1000 * 1024 * 1024];

pub struct Compaction {
    data_dir: PathBuf,
    manifest: Arc<Manifest>,
    open_readers: RwLock<HashMap<String, Arc<SSTableReader>>>,
}

impl Compaction {
    pub fn new(data_dir: PathBuf, manifest: Arc<Manifest>) -> Self {
        Compaction {
            data_dir,
            manifest,
            open_readers: RwLock::new(HashMap::new()),
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
        // Check L0
        let l0_files = self.manifest.get_level(0);
        if l0_files.len() >= L0_THRESHOLD {
            self.compact_l0_to_l1(l0_files);
        }

        // Check L1+ by size
        for level in 1..LEVEL_MAX_BYTES.len() {
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
                self.compact_level(level, files);
            }
        }
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
                    if key < reader.smallest_key.as_slice()
                        || key > reader.largest_key.as_slice()
                    {
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
        let merged = self.merge_files(&l0_files);
        // Also merge with existing L1 files that overlap
        let l1_files = self.manifest.get_level(1);
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

        // Update manifest
        let mut all_input: Vec<String> = l0_files.clone();
        all_input.extend(l1_files.clone());

        // Remove L0 files
        {
            let mut levels = Vec::new();
            for f in &l0_files {
                levels.push((0usize, f.clone()));
            }
            for (level, file) in &levels {
                self.manifest.remove_file(*level, file);
            }
        }
        // Remove L1 files
        for f in &l1_files {
            self.manifest.remove_file(1, f);
        }
        // Add new L1 files
        for f in &new_files {
            self.manifest.add_file(1, f.clone());
        }

        let _ = self.manifest.save();

        // Delete old files
        for f in &l0_files {
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
        for f in &l1_files {
            let _ = std::fs::remove_file(self.data_dir.join(f));
        }
    }

    fn compact_level(&self, level: usize, files: Vec<String>) {
        let merged = self.merge_files(&files);
        // Filter tombstones at level >= 2
        let merged = if level >= 2 {
            merged.into_iter().filter(|(_, v)| v.is_some()).collect()
        } else {
            merged
        };

        let new_files = self.write_compacted_files(merged, level);

        for f in &files {
            self.close_reader(f);
        }

        // Update manifest
        self.manifest.replace_files(level, &files, level, new_files);
        let _ = self.manifest.save();

        // Delete old files
        for f in &files {
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

        // Split into chunks of ~2MB per file
        const TARGET_FILE_SIZE: usize = 2 * 1024 * 1024;

        let mut chunk: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut chunk_size = 0usize;

        for (k, v) in entries {
            let entry_size = k.len() + v.as_ref().map(|b| b.len()).unwrap_or(0) + 10;
            chunk_size += entry_size;
            chunk.insert(k, v);

            if chunk_size >= TARGET_FILE_SIZE {
                let seq = self.manifest.next_sequence();
                let filename = format!("L{}-{}.sst", level, seq);
                let path = self.data_dir.join(&filename);
                if SSTableWriter::write(&path, &chunk).is_ok() {
                    new_files.push(filename);
                }
                chunk.clear();
                chunk_size = 0;
            }
        }

        if !chunk.is_empty() {
            let seq = self.manifest.next_sequence();
            let filename = format!("L{}-{}.sst", level, seq);
            let path = self.data_dir.join(&filename);
            if SSTableWriter::write(&path, &chunk).is_ok() {
                new_files.push(filename);
            }
        }

        new_files
    }
}
