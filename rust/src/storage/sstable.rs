use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use super::bloom::BloomFilter;

/// Magic number for SSTable footer
const MAGIC: u64 = 0xDB4775248B80FB57u64;
const FOOTER_SIZE: usize = 48;
const BLOCK_SIZE: usize = 4096;

pub struct SSTableWriter;

impl SSTableWriter {
    /// Write an SSTable file from a sorted map of entries.
    /// Format:
    ///   [Data blocks]
    ///   [Index block]
    ///   [Filter block]
    ///   [Footer: 48 bytes]
    pub fn write(path: &Path, entries: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let entry_count = entries.len();
        let mut bloom = BloomFilter::new(entry_count.max(1));

        // Build data blocks
        let mut data_buf: Vec<u8> = Vec::new();
        // index: (last_key, offset, size)
        let mut index: Vec<(Vec<u8>, u64, u32)> = Vec::new();

        let mut block_buf: Vec<u8> = Vec::new();
        let mut block_last_key: Vec<u8> = Vec::new();

        for (key, value) in entries.iter() {
            bloom.add(key);

            // Encode entry: [keyLen:4LE][0x00][key][valLen:4LE][0x00 or 0xFF for tombstone][value]
            // Actually the 5-byte "varint": bytes[0..3] = value LE, byte[4] = 0 marker
            Self::write_entry(&mut block_buf, key, value.as_deref());
            block_last_key = key.clone();

            if block_buf.len() >= BLOCK_SIZE {
                let block_offset = data_buf.len() as u64;
                let block_size = block_buf.len() as u32;
                data_buf.extend_from_slice(&block_buf);
                index.push((block_last_key.clone(), block_offset, block_size));
                block_buf.clear();
            }
        }

        // Flush remaining
        if !block_buf.is_empty() {
            let block_offset = data_buf.len() as u64;
            let block_size = block_buf.len() as u32;
            data_buf.extend_from_slice(&block_buf);
            index.push((block_last_key.clone(), block_offset, block_size));
        }

        file.write_all(&data_buf)?;

        // Write index block
        let index_offset = data_buf.len() as u64;
        let mut index_buf: Vec<u8> = Vec::new();
        index_buf.extend_from_slice(&(index.len() as u32).to_le_bytes());
        for (last_key, offset, size) in &index {
            index_buf.extend_from_slice(&(last_key.len() as u32).to_le_bytes());
            index_buf.extend_from_slice(last_key);
            index_buf.extend_from_slice(&offset.to_le_bytes());
            index_buf.extend_from_slice(&size.to_le_bytes());
        }
        let index_size = index_buf.len() as u64;
        file.write_all(&index_buf)?;

        // Write filter block
        let filter_offset = index_offset + index_size;
        let filter_data = bloom.serialize();
        let filter_size = filter_data.len() as u64;
        file.write_all(&filter_data)?;

        // Write footer (48 bytes)
        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&index_size.to_le_bytes());
        footer.extend_from_slice(&filter_offset.to_le_bytes());
        footer.extend_from_slice(&filter_size.to_le_bytes());
        footer.extend_from_slice(&(entry_count as u64).to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        file.write_all(&footer)?;
        file.flush()?;

        Ok(())
    }

    fn write_entry(buf: &mut Vec<u8>, key: &[u8], value: Option<&[u8]>) {
        // [keyLen:4LE][0:1][key][valLen:4LE][0:1][value]
        // For tombstone: valLen = 0xFFFFFFFF (i.e., -1 as i32 LE), marker byte = 0xFF
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.push(0x00);
        buf.extend_from_slice(key);

        match value {
            Some(v) => {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.push(0x00);
                buf.extend_from_slice(v);
            }
            None => {
                // Tombstone: valLen=-1
                buf.extend_from_slice(&0xFFFFFFFFu32.to_le_bytes());
                buf.push(0xFF);
            }
        }
    }
}

pub struct SSTableReader {
    mmap: Mmap,
    index: Vec<(Vec<u8>, u64, u32)>, // (lastKey, offset, size)
    bloom: BloomFilter,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub path: PathBuf,
}

impl SSTableReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let len = mmap.len();
        if len < FOOTER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "SSTable too small"));
        }

        // Read footer
        let footer_start = len - FOOTER_SIZE;
        let footer = &mmap[footer_start..];

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
        let index_size = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
        let filter_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap()) as usize;
        let filter_size = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
        let _entry_count = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[40..48].try_into().unwrap());

        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid SSTable magic"));
        }

        // Read index block
        let index_data = &mmap[index_offset..index_offset + index_size];
        let block_count = u32::from_le_bytes(index_data[0..4].try_into().unwrap()) as usize;
        let mut index = Vec::with_capacity(block_count);
        let mut pos = 4;
        for _ in 0..block_count {
            let key_len = u32::from_le_bytes(index_data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let last_key = index_data[pos..pos + key_len].to_vec();
            pos += key_len;
            let offset = u64::from_le_bytes(index_data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let size = u32::from_le_bytes(index_data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            index.push((last_key, offset, size));
        }

        // Read filter block
        let filter_data = &mmap[filter_offset..filter_offset + filter_size];
        let bloom = BloomFilter::deserialize(filter_data);

        // Find smallest and largest key
        let smallest_key = if !index.is_empty() {
            Self::get_first_key_of_block(&mmap, index[0].1 as usize, index[0].2 as usize)
                .unwrap_or_default()
        } else {
            vec![]
        };

        let largest_key = index.last().map(|(k, _, _)| k.clone()).unwrap_or_default();

        Ok(SSTableReader {
            mmap,
            index,
            bloom,
            smallest_key,
            largest_key,
            path: path.to_path_buf(),
        })
    }

    /// FIXED: Read key from position 0 of block (not position 1)
    /// 5-byte entry: [keyLen:4LE][0x00 marker] then key
    fn get_first_key_of_block(mmap: &Mmap, offset: usize, size: usize) -> Option<Vec<u8>> {
        if size < 5 {
            return None;
        }
        let block = &mmap[offset..offset + size as usize];
        // keyLen is at position 0 (4 bytes LE), byte 4 is the 0x00 marker
        let key_len = u32::from_le_bytes(block[0..4].try_into().ok()?) as usize;
        if 5 + key_len > block.len() {
            return None;
        }
        Some(block[5..5 + key_len].to_vec())
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        if !self.bloom.may_contain(key) {
            return None;
        }

        // Find the block that may contain this key
        // Binary search: find first block whose lastKey >= key
        let block_idx = self.find_block(key)?;
        let (_, offset, size) = &self.index[block_idx];
        self.search_block(*offset as usize, *size as usize, key)
    }

    /// Find first block whose lastKey >= key
    fn find_block(&self, key: &[u8]) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.index.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.index[mid].0.as_slice() < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo < self.index.len() {
            Some(lo)
        } else {
            None
        }
    }

    fn search_block(&self, offset: usize, size: usize, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let block = &self.mmap[offset..offset + size];
        let mut pos = 0;

        while pos + 5 <= block.len() {
            let key_len = u32::from_le_bytes(block[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 5; // skip 4-byte len + 1-byte marker
            if pos + key_len > block.len() {
                break;
            }
            let entry_key = &block[pos..pos + key_len];
            pos += key_len;

            if pos + 5 > block.len() {
                break;
            }
            let val_len_bytes: [u8; 4] = block[pos..pos + 4].try_into().unwrap();
            let val_len_raw = u32::from_le_bytes(val_len_bytes);
            pos += 5; // skip 4-byte len + 1-byte marker

            if val_len_raw == 0xFFFFFFFF {
                // Tombstone
                if entry_key == key {
                    return Some(None);
                }
            } else {
                let val_len = val_len_raw as usize;
                if pos + val_len > block.len() {
                    break;
                }
                if entry_key == key {
                    return Some(Some(block[pos..pos + val_len].to_vec()));
                }
                pos += val_len;
            }
        }

        None
    }

    pub fn scan<'a>(
        &'a self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut results = Vec::new();

        // Find starting block
        let start_block = if let Some(s) = start {
            match self.find_block(s) {
                Some(idx) => idx,
                None => return results,
            }
        } else {
            0
        };

        for block_idx in start_block..self.index.len() {
            let (last_key, offset, size) = &self.index[block_idx];

            // If start key is after the last key of this block, skip
            if let Some(s) = start {
                if last_key.as_slice() < s {
                    continue;
                }
            }

            // If end key is before the start of this block, done
            if let Some(e) = end {
                if !self.index[block_idx].0.is_empty() {
                    // Check first key of block
                    if let Some(first) =
                        Self::get_first_key_of_block(&self.mmap, *offset as usize, *size as usize)
                    {
                        if first.as_slice() >= e {
                            break;
                        }
                    }
                }
            }

            self.scan_block(*offset as usize, *size as usize, start, end, &mut results);
        }

        results
    }

    fn scan_block(
        &self,
        offset: usize,
        size: usize,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        results: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) {
        let block = &self.mmap[offset..offset + size];
        let mut pos = 0;

        while pos + 5 <= block.len() {
            let key_len = u32::from_le_bytes(block[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 5;
            if pos + key_len > block.len() {
                break;
            }
            let entry_key = block[pos..pos + key_len].to_vec();
            pos += key_len;

            if pos + 5 > block.len() {
                break;
            }
            let val_len_raw = u32::from_le_bytes(block[pos..pos + 4].try_into().unwrap());
            pos += 5;

            let value: Option<Vec<u8>>;
            if val_len_raw == 0xFFFFFFFF {
                value = None;
            } else {
                let val_len = val_len_raw as usize;
                if pos + val_len > block.len() {
                    break;
                }
                value = Some(block[pos..pos + val_len].to_vec());
                pos += val_len;
            }

            // Apply range filter
            if let Some(s) = start {
                if entry_key.as_slice() < s {
                    continue;
                }
            }
            if let Some(e) = end {
                if entry_key.as_slice() >= e {
                    return;
                }
            }

            results.push((entry_key, value));
        }
    }
}
