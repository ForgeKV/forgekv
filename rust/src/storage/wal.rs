use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOpType {
    Put = 1,
    Delete = 2,
}

impl WalOpType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(WalOpType::Put),
            2 => Some(WalOpType::Delete),
            _ => None,
        }
    }
}

/// Entry format: [CRC32:4LE][SeqNo:8LE][OpType:1][KeyLen:4LE][Key][ValueLen:4LE][Value]
pub struct WriteAheadLog {
    file: BufWriter<File>,
    path: PathBuf,
    global_seq: Arc<AtomicU64>,
    write_through: bool,
}

impl WriteAheadLog {
    pub fn new(path: &Path, write_through: bool, global_seq: Arc<AtomicU64>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(WriteAheadLog {
            file: BufWriter::with_capacity(256 * 1024, file),
            path: path.to_path_buf(),
            global_seq,
            write_through,
        })
    }

    pub fn append(&mut self, op: WalOpType, key: &[u8], value: &[u8]) -> io::Result<u64> {
        let seq = self.global_seq.fetch_add(1, Ordering::Relaxed) + 1;

        // Compute CRC incrementally — no heap allocation needed.
        let seq_bytes = seq.to_le_bytes();
        let op_byte = [op as u8];
        let key_len_bytes = (key.len() as u32).to_le_bytes();
        let val_len_bytes = (value.len() as u32).to_le_bytes();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&seq_bytes);
        hasher.update(&op_byte);
        hasher.update(&key_len_bytes);
        hasher.update(key);
        hasher.update(&val_len_bytes);
        hasher.update(value);
        let crc = hasher.finalize();

        // Write directly — BufWriter coalesces these small writes.
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&seq_bytes)?;
        self.file.write_all(&op_byte)?;
        self.file.write_all(&key_len_bytes)?;
        self.file.write_all(key)?;
        self.file.write_all(&val_len_bytes)?;
        self.file.write_all(value)?;

        if self.write_through {
            self.file.flush()?;
        }

        Ok(seq)
    }

    pub fn replay(path: &Path) -> Vec<(u64, WalOpType, Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();

        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return entries,
        };

        let mut reader = io::BufReader::new(file);
        let mut buf = Vec::new();
        if reader.read_to_end(&mut buf).is_err() {
            return entries;
        }

        let mut pos = 0;
        while pos + 4 <= buf.len() {
            let stored_crc = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
            pos += 4;

            // Need at least 8 (seq) + 1 (op) + 4 (key_len)
            if pos + 13 > buf.len() {
                break;
            }

            let seq = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
            let op_byte = buf[pos + 8];
            let key_len = u32::from_le_bytes(buf[pos + 9..pos + 13].try_into().unwrap()) as usize;

            if pos + 13 + key_len + 4 > buf.len() {
                break;
            }

            let key = buf[pos + 13..pos + 13 + key_len].to_vec();
            let val_len_offset = pos + 13 + key_len;
            let val_len = u32::from_le_bytes(
                buf[val_len_offset..val_len_offset + 4].try_into().unwrap(),
            ) as usize;

            if val_len_offset + 4 + val_len > buf.len() {
                break;
            }

            let value = buf[val_len_offset + 4..val_len_offset + 4 + val_len].to_vec();
            let payload_end = val_len_offset + 4 + val_len;
            let payload = &buf[pos..payload_end];

            let computed_crc = crc32fast::hash(payload);
            pos = payload_end;

            if computed_crc != stored_crc {
                // Corrupt entry, skip
                continue;
            }

            if let Some(op) = WalOpType::from_u8(op_byte) {
                entries.push((seq, op, key, value));
            }
        }

        entries
    }

    /// Flush the current WAL buffer, rename the file to `archive_path`, then
    /// open a fresh file at the original path.  Must be called while the
    /// caller holds the WAL mutex so no new entries can be appended to the
    /// old file after it is archived.
    pub fn rotate(&mut self, archive_path: &Path) -> io::Result<()> {
        self.file.flush()?;
        fs::rename(&self.path, archive_path)?;
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.file = BufWriter::with_capacity(256 * 1024, new_file);
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
