pub const TAG_STRING: u8 = 0x01;
pub const TAG_HASH: u8 = 0x02;
pub const TAG_LIST: u8 = 0x03;
pub const TAG_SET: u8 = 0x04;
pub const TAG_ZSET: u8 = 0x05;
pub const TAG_META: u8 = 0x10;
pub const TAG_TTL: u8 = 0x20;

/// Encode: [tag:1][db:1][keyLen:2BE][key]
pub fn encode_string_key(db: usize, key: &[u8]) -> Vec<u8> {
    encode_key_prefix(TAG_STRING, db, key)
}

/// Encode: [tag:1][db:1][keyLen:2BE][key][fieldLen:2BE][field]
pub fn encode_hash_key(db: usize, key: &[u8], field: &[u8]) -> Vec<u8> {
    let mut buf = encode_key_prefix(TAG_HASH, db, key);
    let field_len = field.len() as u16;
    buf.extend_from_slice(&field_len.to_be_bytes());
    buf.extend_from_slice(field);
    buf
}

/// Encode: [tag:1][db:1][keyLen:2BE][key][seq:8BE]
pub fn encode_list_key(db: usize, key: &[u8], seq: i64) -> Vec<u8> {
    let mut buf = encode_key_prefix(TAG_LIST, db, key);
    buf.extend_from_slice(&seq.to_be_bytes());
    buf
}

/// Encode: [tag:1][db:1][keyLen:2BE][key][memberLen:2BE][member]
pub fn encode_set_key(db: usize, key: &[u8], member: &[u8]) -> Vec<u8> {
    let mut buf = encode_key_prefix(TAG_SET, db, key);
    let member_len = member.len() as u16;
    buf.extend_from_slice(&member_len.to_be_bytes());
    buf.extend_from_slice(member);
    buf
}

/// Encode: [tag:1][db:1][keyLen:2BE][key][0x01][memberLen:2BE][member]
pub fn encode_zset_member_key(db: usize, key: &[u8], member: &[u8]) -> Vec<u8> {
    let mut buf = encode_key_prefix(TAG_ZSET, db, key);
    buf.push(0x01);
    let member_len = member.len() as u16;
    buf.extend_from_slice(&member_len.to_be_bytes());
    buf.extend_from_slice(member);
    buf
}

/// Encode: [tag:1][db:1][keyLen:2BE][key][0x02][score:8sortable][memberLen:2BE][member]
pub fn encode_zset_score_key(db: usize, key: &[u8], score: f64, member: &[u8]) -> Vec<u8> {
    let mut buf = encode_key_prefix(TAG_ZSET, db, key);
    buf.push(0x02);
    buf.extend_from_slice(&encode_sortable_double(score));
    let member_len = member.len() as u16;
    buf.extend_from_slice(&member_len.to_be_bytes());
    buf.extend_from_slice(member);
    buf
}

/// Encode: [TagMeta:1][db:1][keyLen:2BE][key]
pub fn encode_meta_key(db: usize, key: &[u8]) -> Vec<u8> {
    encode_key_prefix(TAG_META, db, key)
}

/// Encode: [TagTtl:1][expiry:8BE][db:1][keyLen:2BE][key]
pub fn encode_ttl_key(expiry_ms: i64, db: usize, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 1 + 2 + key.len());
    buf.push(TAG_TTL);
    buf.extend_from_slice(&expiry_ms.to_be_bytes());
    buf.push(db as u8);
    let key_len = key.len() as u16;
    buf.extend_from_slice(&key_len.to_be_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Decode: [TagTtl:1][expiry:8BE][db:1][keyLen:2BE][key] -> (expiry_ms, db, key)
pub fn decode_ttl_key(encoded: &[u8]) -> (i64, usize, Vec<u8>) {
    // encoded[0] = TAG_TTL
    let expiry_ms = i64::from_be_bytes(encoded[1..9].try_into().unwrap());
    let db = encoded[9] as usize;
    let key_len = u16::from_be_bytes(encoded[10..12].try_into().unwrap()) as usize;
    let key = encoded[12..12 + key_len].to_vec();
    (expiry_ms, db, key)
}

/// Encode: [tag:1][db:1][keyLen:2BE][key]
pub fn encode_key_prefix(tag: u8, db: usize, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 1 + 2 + key.len());
    buf.push(tag);
    buf.push(db as u8);
    let key_len = key.len() as u16;
    buf.extend_from_slice(&key_len.to_be_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Encode f64 as 8 bytes with sort ordering:
/// Flip sign bit. For negative floats, flip all bits.
/// This ensures lexicographic order == numeric order.
pub fn encode_sortable_double(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let sortable = if value < 0.0 || value.is_sign_negative() {
        // Negative: flip all bits
        !bits
    } else {
        // Positive: flip sign bit only
        bits ^ 0x8000_0000_0000_0000u64
    };
    sortable.to_be_bytes()
}

/// Decode sortable double from 8 bytes
pub fn decode_sortable_double(data: &[u8]) -> f64 {
    let sortable = u64::from_be_bytes(data[..8].try_into().unwrap());
    // If high bit is set, it was a positive float (sign bit was flipped to 1)
    let bits = if sortable & 0x8000_0000_0000_0000u64 != 0 {
        sortable ^ 0x8000_0000_0000_0000u64
    } else {
        // Was negative: all bits flipped
        !sortable
    };
    f64::from_bits(bits)
}

/// Lexicographic byte comparison
pub fn bytes_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
}
