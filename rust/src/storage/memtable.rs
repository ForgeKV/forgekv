use std::collections::BTreeMap;
use std::ops::Bound;

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
        let delta = key.len() + value.len();
        if let Some(old) = self.data.get(&key) {
            let old_size = key.len() + old.as_ref().map(|v| v.len()).unwrap_or(0);
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        }
        self.size_bytes += delta;
        self.data.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        if let Some(old) = self.data.get(&key) {
            let old_size = key.len() + old.as_ref().map(|v| v.len()).unwrap_or(0);
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        }
        // Tombstone: insert None
        self.size_bytes += key.len();
        self.data.insert(key, None);
    }

    /// Outer None = not found, inner None = tombstone
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.data.get(key).cloned()
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size_bytes
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

    pub fn into_snapshot(self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.data
    }
}
