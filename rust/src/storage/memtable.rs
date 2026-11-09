use std::collections::BTreeMap;
use std::ops::Bound;

/// Approximate per-entry heap overhead of `BTreeMap<Vec<u8>, Option<Vec<u8>>>` beyond the raw
/// key/value bytes: two `Vec` headers (48 B), amortised B-tree node slack (~16 B) and two
/// allocator chunk headers/rounding (~16 B). Counting it keeps the memtable budget close to the
/// real heap use for small values (the translation cache averages ~150 B per value, where the
/// raw-bytes-only count under-reported memtable memory by ~1.5x).
pub const ENTRY_OVERHEAD_BYTES: usize = 80;

pub struct MemTable {
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    size_bytes: usize,
    max_size_bytes: usize,
}

impl MemTable {
    pub fn new(max_size_bytes: usize) -> Self {
        MemTable {
            data: BTreeMap::new(),
            size_bytes: 0,
            max_size_bytes,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let delta = key.len() + value.len() + ENTRY_OVERHEAD_BYTES;
        if let Some(old) = self.data.get(&key) {
            let old_size =
                key.len() + old.as_ref().map(|v| v.len()).unwrap_or(0) + ENTRY_OVERHEAD_BYTES;
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        }
        self.size_bytes += delta;
        self.data.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        if let Some(old) = self.data.get(&key) {
            let old_size =
                key.len() + old.as_ref().map(|v| v.len()).unwrap_or(0) + ENTRY_OVERHEAD_BYTES;
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        }
        // Tombstone: insert None
        self.size_bytes += key.len() + ENTRY_OVERHEAD_BYTES;
        self.data.insert(key, None);
    }

    /// Outer None = not found, inner None = tombstone
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.data.get(key).cloned()
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size_bytes
    }

    /// Accounted bytes (raw key/value bytes + per-entry overhead).
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Borrow the sorted entries (used to flush an immutable memtable without cloning it).
    pub fn data(&self) -> &BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        &self.data
    }

    pub fn count(&self) -> usize {
        self.data.len()
    }

    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let start_bound = match start {
            Some(s) => Bound::Included(s.to_vec()),
            None => Bound::Unbounded,
        };
        let end_bound = match end {
            Some(e) => Bound::Excluded(e.to_vec()),
            None => Bound::Unbounded,
        };
        self.data
            .range((start_bound, end_bound))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn snapshot(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.data.clone()
    }

    pub fn into_snapshot(self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::{MemTable, ENTRY_OVERHEAD_BYTES};

    #[test]
    fn size_accounting_includes_overhead_and_overwrites() {
        let mut m = MemTable::new(1 << 20);
        m.put(b"k1".to_vec(), vec![0u8; 100]);
        assert_eq!(m.size_bytes(), 2 + 100 + ENTRY_OVERHEAD_BYTES);
        m.put(b"k1".to_vec(), vec![0u8; 10]);
        assert_eq!(m.size_bytes(), 2 + 10 + ENTRY_OVERHEAD_BYTES);
        m.delete(b"k1".to_vec());
        assert_eq!(m.size_bytes(), 2 + ENTRY_OVERHEAD_BYTES);
        assert_eq!(m.count(), 1);
    }

    #[test]
    fn small_values_fill_the_budget_with_overhead() {
        // 1 MiB budget, 150-byte values: without the overhead term this would take ~6.9k
        // entries; with it the table reports full at ~4.6k entries.
        let mut m = MemTable::new(1 << 20);
        let mut n = 0usize;
        while !m.is_full() {
            m.put(format!("key:{n:08}").into_bytes(), vec![1u8; 150]);
            n += 1;
        }
        assert!(n < 5_000, "filled after {n} entries");
    }
}
