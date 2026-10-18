//! Optional transparent zstd compression of **string values**.
//!
//! # Stored value format
//!
//! A string value in the LSM store is one of:
//!
//! | Stored bytes                                   | Meaning                                   |
//! |-----------------------------------------------|-------------------------------------------|
//! | anything not starting with `FF 5A 4B` ("\xffZK") | legacy / plain raw value (verbatim)       |
//! | `FF 5A 4B 01` + payload                         | *escaped raw*: payload is the real value  |
//! | `FF 5A 4B 02` + `orig_len:u32 LE` + zstd frame  | zstd-compressed value                     |
//!
//! * Values written by this version that do not start with the 3-byte magic
//!   are stored verbatim (identical to the old format, so old binaries read
//!   them fine).
//! * Values written by this version that DO start with the magic are always
//!   stored *escaped* (`FF 5A 4B 01` header), so decoding is unambiguous for
//!   everything this version writes, whether compression is on or off.
//! * Reads always decode all formats, independent of the `value-compression`
//!   setting.
//!
//! Residual limit: a *legacy* raw value (written by a pre-compression
//! binary) that already starts with `FF 5A 4B 01` or `FF 5A 4B 02` is
//! indistinguishable from the formats above. Such a value is misread
//! (kind 01: first 4 bytes stripped; kind 02: returned verbatim if it is not
//! a valid zstd frame of the declared length, else decompressed). Values that
//! start with 0xFF are not valid UTF-8, so this is only possible for binary
//! values that begin with exactly those bytes.
//!
//! A binary *without* this module cannot read compressed values; see
//! `docs/value-compression.md` for the required rollout order.

use std::cell::RefCell;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicU8, AtomicUsize, Ordering};

pub const MAGIC: [u8; 3] = [0xFF, b'Z', b'K'];
pub const KIND_RAW_ESCAPED: u8 = 0x01;
pub const KIND_ZSTD: u8 = 0x02;
/// magic(3) + kind(1) + orig_len(4)
pub const ZSTD_HEADER_LEN: usize = 8;
pub const ESCAPE_HEADER_LEN: usize = 4;
/// A compressed value is only kept if it saves at least this many percent.
pub const MIN_SAVING_PERCENT: usize = 10;
/// Upper bound accepted for the declared decompressed length (decompression-bomb guard).
pub const MAX_DECOMPRESSED_LEN: usize = 512 * 1024 * 1024;

pub const DEFAULT_LEVEL: i32 = 3;
pub const DEFAULT_MIN_SIZE: usize = 512;
pub const MIN_LEVEL: i32 = 1;
pub const MAX_LEVEL: i32 = 19;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMode {
    Off,
    Zstd,
}

impl CompressionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionMode::Off => "off",
            CompressionMode::Zstd => "zstd",
        }
    }

    /// Strict parse used by CONFIG SET (returns None for unknown values).
    pub fn parse(s: &str) -> Option<CompressionMode> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" | "no" | "none" => Some(CompressionMode::Off),
            "zstd" => Some(CompressionMode::Zstd),
            _ => None,
        }
    }
}

pub fn parse_level(s: &str) -> Option<i32> {
    let v: i32 = s.trim().parse().ok()?;
    if (MIN_LEVEL..=MAX_LEVEL).contains(&v) {
        Some(v)
    } else {
        None
    }
}

/// How a stored value is encoded on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredFormat {
    /// Plain value (legacy or new, no header).
    Raw,
    /// Raw value protected by an escape header.
    EscapedRaw,
    /// zstd-compressed value.
    Zstd,
}

/// Classify stored bytes without decompressing.
pub fn classify(stored: &[u8]) -> StoredFormat {
    if stored.len() >= ESCAPE_HEADER_LEN && stored[..3] == MAGIC {
        match stored[3] {
            KIND_RAW_ESCAPED => return StoredFormat::EscapedRaw,
            KIND_ZSTD if stored.len() >= ZSTD_HEADER_LEN => return StoredFormat::Zstd,
            _ => {}
        }
    }
    StoredFormat::Raw
}

/// Encode a raw value with the escape header if (and only if) it would be
/// mistaken for a header-bearing value. Never compresses.
pub fn encode_raw(value: &[u8]) -> Vec<u8> {
    if value.len() >= 3 && value[..3] == MAGIC {
        let mut out = Vec::with_capacity(value.len() + ESCAPE_HEADER_LEN);
        out.extend_from_slice(&MAGIC);
        out.push(KIND_RAW_ESCAPED);
        out.extend_from_slice(value);
        out
    } else {
        value.to_vec()
    }
}

thread_local! {
    static COMPRESSOR: RefCell<Option<(i32, zstd::bulk::Compressor<'static>)>> = const { RefCell::new(None) };
    static DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
}

fn zstd_compress(value: &[u8], level: i32) -> Option<Vec<u8>> {
    COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        let need_new = !matches!(&*slot, Some((l, _)) if *l == level);
        if need_new {
            *slot = Some((level, zstd::bulk::Compressor::new(level).ok()?));
        }
        slot.as_mut()?.1.compress(value).ok()
    })
}

fn zstd_decompress(frame: &[u8], orig_len: usize) -> Option<Vec<u8>> {
    DECOMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd::bulk::Decompressor::new().ok()?);
        }
        let out = slot.as_mut()?.decompress(frame, orig_len).ok()?;
        if out.len() == orig_len {
            Some(out)
        } else {
            None
        }
    })
}

/// Try to compress `value`; returns the full stored representation (with
/// header) only if it satisfies the minimum-saving rule.
pub fn try_compress(value: &[u8], level: i32) -> Option<Vec<u8>> {
    if value.len() > u32::MAX as usize || value.len() > MAX_DECOMPRESSED_LEN {
        return None;
    }
    let frame = zstd_compress(value, level)?;
    let stored_len = ZSTD_HEADER_LEN + frame.len();
    if stored_len * 100 > value.len() * (100 - MIN_SAVING_PERCENT) {
        return None;
    }
    let mut out = Vec::with_capacity(stored_len);
    out.extend_from_slice(&MAGIC);
    out.push(KIND_ZSTD);
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(&frame);
    Some(out)
}

/// Decode stored bytes into the logical value. Returns the value and whether
/// decoding fell back to returning bytes verbatim because a header-looking
/// value was not a valid zstd frame.
pub fn decode_stored(mut stored: Vec<u8>) -> (Vec<u8>, DecodeInfo) {
    match classify(&stored) {
        StoredFormat::Raw => {
            // Header-looking but too short / unknown kind is returned verbatim too.
            (stored, DecodeInfo::Raw)
        }
        StoredFormat::EscapedRaw => {
            stored.drain(..ESCAPE_HEADER_LEN);
            (stored, DecodeInfo::Escaped)
        }
        StoredFormat::Zstd => {
            let orig_len = u32::from_le_bytes(stored[4..8].try_into().unwrap()) as usize;
            if orig_len <= MAX_DECOMPRESSED_LEN {
                if let Some(v) = zstd_decompress(&stored[ZSTD_HEADER_LEN..], orig_len) {
                    return (v, DecodeInfo::Decompressed);
                }
            }
            (stored, DecodeInfo::InvalidFrameFallback)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeInfo {
    Raw,
    Escaped,
    Decompressed,
    InvalidFrameFallback,
}

/// Runtime settings + counters. One instance per `RedisDatabase`.
pub struct CompressionSettings {
    mode: AtomicU8,
    level: AtomicI32,
    min_size: AtomicUsize,

    // Write-path counters (since process start).
    pub writes_compressed: AtomicU64,
    pub writes_raw: AtomicU64,
    pub writes_below_min: AtomicU64,
    pub writes_incompressible: AtomicU64,
    pub writes_escaped: AtomicU64,
    pub write_bytes_in: AtomicU64,
    pub write_bytes_stored: AtomicU64,
    // Read-path counters.
    pub reads_decompressed: AtomicU64,
    pub decode_fallbacks: AtomicU64,
}

impl Default for CompressionSettings {
    fn default() -> Self {
        CompressionSettings {
            mode: AtomicU8::new(0),
            level: AtomicI32::new(DEFAULT_LEVEL),
            min_size: AtomicUsize::new(DEFAULT_MIN_SIZE),
            writes_compressed: AtomicU64::new(0),
            writes_raw: AtomicU64::new(0),
            writes_below_min: AtomicU64::new(0),
            writes_incompressible: AtomicU64::new(0),
            writes_escaped: AtomicU64::new(0),
            write_bytes_in: AtomicU64::new(0),
            write_bytes_stored: AtomicU64::new(0),
            reads_decompressed: AtomicU64::new(0),
            decode_fallbacks: AtomicU64::new(0),
        }
    }
}

impl CompressionSettings {
    pub fn new(mode: CompressionMode, level: i32, min_size: usize) -> Self {
        let s = CompressionSettings::default();
        s.set_mode(mode);
        s.set_level(level);
        s.set_min_size(min_size);
        s
    }

    pub fn mode(&self) -> CompressionMode {
        match self.mode.load(Ordering::Relaxed) {
            1 => CompressionMode::Zstd,
            _ => CompressionMode::Off,
        }
    }
    pub fn set_mode(&self, m: CompressionMode) {
        self.mode.store(
            match m {
                CompressionMode::Off => 0,
                CompressionMode::Zstd => 1,
            },
            Ordering::Relaxed,
        );
    }
    pub fn level(&self) -> i32 {
        self.level.load(Ordering::Relaxed)
    }
    pub fn set_level(&self, l: i32) {
        self.level
            .store(l.clamp(MIN_LEVEL, MAX_LEVEL), Ordering::Relaxed);
    }
    pub fn min_size(&self) -> usize {
        self.min_size.load(Ordering::Relaxed)
    }
    pub fn set_min_size(&self, n: usize) {
        self.min_size.store(n, Ordering::Relaxed);
    }

    /// Encode a value for storage according to the *current* settings.
    pub fn encode(&self, value: &[u8]) -> Vec<u8> {
        self.encode_with_mode(value, self.mode())
    }

    /// Like `encode`, but with an explicit mode (used by the migration job).
    pub fn encode_with_mode(&self, value: &[u8], mode: CompressionMode) -> Vec<u8> {
        let stored = self.encode_inner(value, mode);
        self.write_bytes_in
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        self.write_bytes_stored
            .fetch_add(stored.len() as u64, Ordering::Relaxed);
        stored
    }

    fn encode_inner(&self, value: &[u8], mode: CompressionMode) -> Vec<u8> {
        if mode == CompressionMode::Zstd {
            if value.len() < self.min_size() {
                self.writes_below_min.fetch_add(1, Ordering::Relaxed);
            } else if let Some(c) = try_compress(value, self.level()) {
                self.writes_compressed.fetch_add(1, Ordering::Relaxed);
                return c;
            } else {
                self.writes_incompressible.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.writes_raw.fetch_add(1, Ordering::Relaxed);
        if value.len() >= 3 && value[..3] == MAGIC {
            self.writes_escaped.fetch_add(1, Ordering::Relaxed);
        }
        encode_raw(value)
    }

    /// Decode stored bytes (always handles every format, ignores `mode`).
    pub fn decode(&self, stored: Vec<u8>) -> Vec<u8> {
        let (v, info) = decode_stored(stored);
        match info {
            DecodeInfo::Decompressed => {
                self.reads_decompressed.fetch_add(1, Ordering::Relaxed);
            }
            DecodeInfo::InvalidFrameFallback => {
                self.decode_fallbacks.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes (incompressible).
    fn noise(n: usize, mut seed: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            v.push((seed >> 33) as u8);
        }
        v
    }

    fn json_like(n: usize) -> Vec<u8> {
        let mut s = String::from("[");
        let mut i = 0;
        while s.len() < n {
            s.push_str(&format!(
                "{{\"hotelId\":{i},\"name\":\"Example Hotel {i}\",\"city\":\"Testville\",\"price\":{}.50,\"currency\":\"USD\",\"available\":true}},",
                100 + i % 50
            ));
            i += 1;
        }
        s.push(']');
        s.into_bytes()
    }

    fn on() -> CompressionSettings {
        CompressionSettings::new(CompressionMode::Zstd, 3, 512)
    }

    #[test]
    fn roundtrip_compressible() {
        let c = on();
        let v = json_like(50_000);
        let stored = c.encode(&v);
        assert_eq!(classify(&stored), StoredFormat::Zstd);
        assert!(stored.len() < v.len() / 3, "expected >3x on json-like data");
        assert_eq!(c.decode(stored), v);
        assert_eq!(c.writes_compressed.load(Ordering::Relaxed), 1);
        assert_eq!(c.reads_decompressed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn roundtrip_all_levels() {
        let v = json_like(20_000);
        for level in [1, 3, 9, 19] {
            let c = CompressionSettings::new(CompressionMode::Zstd, level, 512);
            let stored = c.encode(&v);
            assert_eq!(classify(&stored), StoredFormat::Zstd, "level {level}");
            assert_eq!(c.decode(stored), v);
        }
    }

    #[test]
    fn legacy_raw_values_are_read_verbatim() {
        let c = on();
        for v in [
            b"".to_vec(),
            b"hello".to_vec(),
            vec![0u8, 1, 2, 3, 255],
            json_like(5000),
            vec![0xFF],
            vec![0xFF, b'Z'],
            vec![0xFF, b'Z', b'K'], // exactly the magic, too short for a kind
            vec![0xFF, b'Z', b'K', 0x07, 1], // unknown kind
        ] {
            assert_eq!(c.decode(v.clone()), v);
        }
    }

    #[test]
    fn magic_prefixed_raw_is_escaped_and_roundtrips_in_every_mode() {
        for mode in [CompressionMode::Off, CompressionMode::Zstd] {
            let c = CompressionSettings::new(mode, 3, 512);
            for tail in [
                vec![],
                vec![KIND_RAW_ESCAPED],
                vec![KIND_ZSTD, 1, 0, 0, 0, 0x28],
                b"abc".to_vec(),
            ] {
                let mut v = MAGIC.to_vec();
                v.extend_from_slice(&tail);
                let stored = c.encode(&v);
                assert_eq!(classify(&stored), StoredFormat::EscapedRaw);
                assert_eq!(&stored[..4], &[0xFF, b'Z', b'K', KIND_RAW_ESCAPED]);
                assert_eq!(c.decode(stored), v);
            }
            assert_eq!(c.writes_escaped.load(Ordering::Relaxed), 4);
        }
    }

    #[test]
    fn large_compressible_value_with_magic_prefix_compresses_and_roundtrips() {
        let c = on();
        let mut v = MAGIC.to_vec();
        v.extend_from_slice(&json_like(10_000));
        let stored = c.encode(&v);
        assert_eq!(classify(&stored), StoredFormat::Zstd);
        assert_eq!(c.decode(stored), v);
    }

    #[test]
    fn below_min_size_stays_raw() {
        let c = on();
        let v = vec![b'a'; 511];
        let stored = c.encode(&v);
        assert_eq!(stored, v);
        assert_eq!(c.writes_below_min.load(Ordering::Relaxed), 1);
        // exactly min-size compresses
        let v = vec![b'a'; 512];
        assert_eq!(classify(&c.encode(&v)), StoredFormat::Zstd);
    }

    #[test]
    fn incompressible_stays_raw() {
        let c = on();
        let v = noise(10_000, 42);
        let stored = c.encode(&v);
        assert_eq!(stored, v);
        assert_eq!(c.writes_incompressible.load(Ordering::Relaxed), 1);
        assert_eq!(c.decode(stored), v);
    }

    #[test]
    fn minimum_saving_rule() {
        // Mostly-noise value with a compressible tail: saves < 10% => raw.
        let mut v = noise(9_500, 7);
        v.extend(std::iter::repeat(b'x').take(500));
        assert!(try_compress(&v, 3).is_none());
        // Strongly compressible: kept.
        assert!(try_compress(&vec![b'x'; 10_000], 3).is_some());
    }

    #[test]
    fn off_mode_never_compresses_but_still_reads_compressed() {
        let on = on();
        let off = CompressionSettings::new(CompressionMode::Off, 3, 512);
        let v = json_like(8_000);
        assert_eq!(off.encode(&v), v);
        let compressed = on.encode(&v);
        assert_eq!(classify(&compressed), StoredFormat::Zstd);
        // Reads decode regardless of the flag.
        assert_eq!(off.decode(compressed), v);
    }

    #[test]
    fn corrupt_frame_falls_back_to_verbatim() {
        let c = on();
        let mut stored = MAGIC.to_vec();
        stored.push(KIND_ZSTD);
        stored.extend_from_slice(&100u32.to_le_bytes());
        stored.extend_from_slice(b"not a zstd frame");
        assert_eq!(c.decode(stored.clone()), stored);
        assert_eq!(c.decode_fallbacks.load(Ordering::Relaxed), 1);
        // Absurd declared length is rejected (no huge allocation).
        let mut huge = MAGIC.to_vec();
        huge.push(KIND_ZSTD);
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        huge.extend_from_slice(b"zz");
        assert_eq!(c.decode(huge.clone()), huge);
    }

    #[test]
    fn declared_length_mismatch_is_rejected() {
        let c = on();
        let v = json_like(4_000);
        let mut stored = c.encode(&v);
        assert_eq!(classify(&stored), StoredFormat::Zstd);
        // Lie about the length (smaller): zstd bulk decode errors => fallback.
        stored[4..8].copy_from_slice(&10u32.to_le_bytes());
        assert_eq!(c.decode(stored.clone()), stored);
    }

    #[test]
    fn config_parsing() {
        assert_eq!(CompressionMode::parse("ZSTD"), Some(CompressionMode::Zstd));
        assert_eq!(CompressionMode::parse("off"), Some(CompressionMode::Off));
        assert_eq!(CompressionMode::parse("lz4"), None);
        assert_eq!(parse_level("3"), Some(3));
        assert_eq!(parse_level("0"), None);
        assert_eq!(parse_level("20"), None);
        assert_eq!(parse_level("x"), None);
    }
}
