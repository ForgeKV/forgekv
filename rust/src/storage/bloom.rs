pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: usize,
    hash_count: usize,
    items_inserted: u64,
    capacity: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize) -> Self {
        let bits_per_item = 10usize;
        let hash_count = 7usize;
        let bit_count = (expected_items * bits_per_item).max(64);
        let byte_count = (bit_count + 7) / 8;
        BloomFilter {
            bits: vec![0u8; byte_count],
            bit_count,
            hash_count,
            items_inserted: 0,
            capacity: expected_items,
        }
    }

    pub fn from_parts(bits: Vec<u8>, bit_count: usize, hash_count: usize) -> Self {
        let capacity = bit_count / 10;
        BloomFilter {
            bits,
            bit_count,
            hash_count,
            items_inserted: 0,
            capacity,
        }
    }

    pub fn items_inserted(&self) -> u64 {
        self.items_inserted
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn add(&mut self, key: &[u8]) {
        let already_present = self.may_contain(key);
        let (h1, h2) = hash_key(key);
        for i in 0..self.hash_count {
            let bit = ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.bit_count as u64) as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
        if !already_present {
            self.items_inserted += 1;
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = hash_key(key);
        for i in 0..self.hash_count {
            let bit = ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.bit_count as u64) as usize;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize: [bitCount:4LE][hashCount:4LE][items_inserted:8LE][bits...]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.bits.len());
        out.extend_from_slice(&(self.bit_count as u32).to_le_bytes());
        out.extend_from_slice(&(self.hash_count as u32).to_le_bytes());
        out.extend_from_slice(&self.items_inserted.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn deserialize(data: &[u8]) -> Self {
        if data.len() < 8 {
            return BloomFilter::new(100);
        }
        let bit_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let hash_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        // Support both old format (8 byte header) and new format (16 byte header)
        let (items_inserted, bits) = if data.len() >= 16 {
            let items = u64::from_le_bytes(data[8..16].try_into().unwrap());
            (items, data[16..].to_vec())
        } else {
            (0u64, data[8..].to_vec())
        };
        let capacity = bit_count / 10;
        BloomFilter {
            bits,
            bit_count,
            hash_count,
            items_inserted,
            capacity,
        }
    }
}

/// Double hashing using FNV-1a with two seeds
fn hash_key(key: &[u8]) -> (u64, u64) {
    // FNV-1a hash with offset basis 1
    let h1 = fnv1a(key, 0xcbf29ce484222325u64);
    // FNV-1a hash with offset basis 2 (different seed)
    let h2 = fnv1a(key, 0x84222325cbf29ce4u64);
    (h1, h2 | 1) // h2 must be odd for double hashing
}

fn fnv1a(data: &[u8], seed: u64) -> u64 {
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = seed;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
