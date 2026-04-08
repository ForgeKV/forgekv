use std::sync::Arc;

use crate::database::RedisDatabase;
use crate::resp::RespValue;

use super::CommandHandler;

// ── Constants ─────────────────────────────────────────────────────────────────

const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LONG_MIN: f64 = -180.0;
const GEO_LONG_MAX: f64 = 180.0;
const GEO_STEP_MAX: u32 = 26;

/// Redis uses this exact Earth radius value.
const EARTH_RADIUS_M: f64 = 6372797.560856;

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

// ── Geo math ──────────────────────────────────────────────────────────────────

/// Encode lon/lat into a 52-bit integer geohash (interleaved bits, returned as f64).
/// Matches Redis's geohashEncodeWGS84 with step=26.
fn geo_encode(lon: f64, lat: f64) -> f64 {
    let mut min_lon = GEO_LONG_MIN;
    let mut max_lon = GEO_LONG_MAX;
    let mut min_lat = GEO_LAT_MIN;
    let mut max_lat = GEO_LAT_MAX;
    let mut bits: u64 = 0;

    for _ in 0..GEO_STEP_MAX {
        // longitude bit (high bit of each pair)
        let mid_lon = (min_lon + max_lon) / 2.0;
        bits <<= 1;
        if lon > mid_lon {
            bits |= 1;
            min_lon = mid_lon;
        } else {
            max_lon = mid_lon;
        }
        // latitude bit (low bit of each pair)
        let mid_lat = (min_lat + max_lat) / 2.0;
        bits <<= 1;
        if lat > mid_lat {
            bits |= 1;
            min_lat = mid_lat;
        } else {
            max_lat = mid_lat;
        }
    }
    bits as f64
}

/// Decode a 52-bit geohash score back into (lon, lat).
fn geo_decode(score: f64) -> (f64, f64) {
    let bits = score as u64;
    let mut min_lon = GEO_LONG_MIN;
    let mut max_lon = GEO_LONG_MAX;
    let mut min_lat = GEO_LAT_MIN;
    let mut max_lat = GEO_LAT_MAX;

    for i in 0..GEO_STEP_MAX {
        // Bits are laid out MSB-first: bit 51 = lon_0, bit 50 = lat_0, ...
        let bit_lon = (bits >> (2 * (GEO_STEP_MAX - 1 - i) + 1)) & 1;
        let bit_lat = (bits >> (2 * (GEO_STEP_MAX - 1 - i))) & 1;

        let mid_lon = (min_lon + max_lon) / 2.0;
        if bit_lon == 1 {
            min_lon = mid_lon;
        } else {
            max_lon = mid_lon;
        }

        let mid_lat = (min_lat + max_lat) / 2.0;
        if bit_lat == 1 {
            min_lat = mid_lat;
        } else {
            max_lat = mid_lat;
        }
    }

    let lon = (min_lon + max_lon) / 2.0;
    let lat = (min_lat + max_lat) / 2.0;
    (lon, lat)
}

/// Haversine distance in metres between two (lon, lat) points.
fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * c
}

/// Convert a WGS84-encoded 52-bit geohash score to an 11-character base32 string.
///
/// Redis GEOHASH output uses standard latitude bounds (-90/+90), not the internal
/// WGS84 bounds (-85.05/+85.05).  The algorithm is:
///   1. Decode the stored score back to (lon, lat) using WGS84 bounds.
///   2. Re-encode (lon, lat) with standard bounds, producing a new 52-bit value.
///   3. Clear the bottom 2 bits (they encode below the precision of 11 chars).
///   4. Base32-encode the 52-bit value into 11 characters.
fn geo_hash_string(score: f64) -> String {
    // Step 1: decode stored WGS84 bits to lon/lat.
    let (lon, lat) = geo_decode(score);

    // Step 2: re-encode with standard latitude bounds (-90/+90).
    let mut min_lon = GEO_LONG_MIN;
    let mut max_lon = GEO_LONG_MAX;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;
    let mut std_bits: u64 = 0;

    for _ in 0..GEO_STEP_MAX {
        let mid_lon = (min_lon + max_lon) / 2.0;
        std_bits <<= 1;
        if lon > mid_lon {
            std_bits |= 1;
            min_lon = mid_lon;
        } else {
            max_lon = mid_lon;
        }
        let mid_lat = (min_lat + max_lat) / 2.0;
        std_bits <<= 1;
        if lat > mid_lat {
            std_bits |= 1;
            min_lat = mid_lat;
        } else {
            max_lat = mid_lat;
        }
    }

    // Step 3: clear the bottom 2 bits (sub-precision noise).
    std_bits &= !0x3u64;

    // Step 4: base32-encode as 11 chars from a 52-bit value.
    // 10 full chars use bits 51..2; the 11th char uses bits 1..0 shifted left by 3 (= 0).
    let bit_length: u32 = 52;
    let mut result = String::with_capacity(11);
    for i in 0..10usize {
        let shift = bit_length - 5 * (i as u32 + 1);
        let idx = ((std_bits >> shift) & 0x1F) as usize;
        result.push(BASE32[idx] as char);
    }
    // 11th char: remaining 2 bits shifted left by 3 (bottom 2 bits are 0 after masking).
    let last_idx = ((std_bits << (5 - (bit_length % 5))) & 0x1F) as usize;
    result.push(BASE32[last_idx] as char);
    result
}

// ── Unit helpers ──────────────────────────────────────────────────────────────

/// Parse a unit string (case-insensitive) and return the metres-per-unit factor.
fn parse_unit(s: &str) -> Option<f64> {
    match s.to_uppercase().as_str() {
        "M" => Some(1.0),
        "KM" => Some(1000.0),
        "FT" => Some(0.3048),
        "MI" => Some(1609.34),
        _ => None,
    }
}

fn format_dist(dist_m: f64, unit_factor: f64, decimals: usize) -> String {
    let val = dist_m / unit_factor;
    format!("{:.prec$}", val, prec = decimals)
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn wrong_type() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn map_err(e: crate::database::RedisError) -> RespValue {
    match e {
        crate::database::RedisError::WrongType => wrong_type(),
        other => RespValue::error(&other.to_string()),
    }
}

// ── Shared search logic ───────────────────────────────────────────────────────

struct SearchOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    any: bool,
    asc: bool,
    desc: bool,
}

impl Default for SearchOptions {
    fn default() -> Self {
        SearchOptions {
            with_coord: false,
            with_dist: false,
            with_hash: false,
            count: None,
            any: false,
            asc: false,
            desc: false,
        }
    }
}

/// Result row for a single member after radius/box search.
struct GeoMember {
    name: Vec<u8>,
    score: f64,
    lon: f64,
    lat: f64,
    dist_m: f64,
}

fn build_result(members: Vec<GeoMember>, opts: &SearchOptions, unit_factor: f64) -> RespValue {
    let rows: Vec<RespValue> = members
        .into_iter()
        .map(|m| {
            let has_extras = opts.with_coord || opts.with_dist || opts.with_hash;
            if !has_extras {
                return RespValue::bulk_bytes(m.name);
            }
            let mut row: Vec<RespValue> = vec![RespValue::bulk_bytes(m.name)];
            if opts.with_dist {
                row.push(RespValue::BulkString(Some(
                    format_dist(m.dist_m, unit_factor, 4).into_bytes(),
                )));
            }
            if opts.with_hash {
                let hash_int = m.score as i64;
                row.push(RespValue::Integer(hash_int));
            }
            if opts.with_coord {
                let coord = RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(format!("{:.17}", m.lon).into_bytes())),
                    RespValue::BulkString(Some(format!("{:.17}", m.lat).into_bytes())),
                ]));
                row.push(coord);
            }
            RespValue::Array(Some(row))
        })
        .collect();
    RespValue::Array(Some(rows))
}

/// Perform a radius search (circle) on all members of a zset key.
/// Returns members within `radius_m` of (center_lon, center_lat).
fn radius_search(
    db: &RedisDatabase,
    db_index: usize,
    key: &[u8],
    center_lon: f64,
    center_lat: f64,
    radius_m: f64,
    opts: &SearchOptions,
    _unit_factor: f64,
) -> Result<Vec<GeoMember>, crate::database::RedisError> {
    let all = db.zrange_all(db_index, key)?;
    let mut results: Vec<GeoMember> = all
        .into_iter()
        .filter_map(|(name, score)| {
            let (lon, lat) = geo_decode(score);
            let dist_m = haversine_m(center_lon, center_lat, lon, lat);
            if dist_m <= radius_m {
                Some(GeoMember {
                    name,
                    score,
                    lon,
                    lat,
                    dist_m,
                })
            } else {
                None
            }
        })
        .collect();

    apply_sort_and_limit(&mut results, opts);
    Ok(results)
}

/// Perform a box search on all members of a zset key.
/// The box is axis-aligned in lon/lat space (approximation, same as Redis).
fn box_search(
    db: &RedisDatabase,
    db_index: usize,
    key: &[u8],
    center_lon: f64,
    center_lat: f64,
    width_m: f64,
    height_m: f64,
    opts: &SearchOptions,
    _unit_factor: f64,
) -> Result<Vec<GeoMember>, crate::database::RedisError> {
    // Convert half-dimensions to degrees (approximate).
    // Redis uses the same approximation.
    let half_w_deg = (width_m / 2.0)
        / (EARTH_RADIUS_M * (center_lat.to_radians().cos()) * std::f64::consts::PI / 180.0);
    let half_h_deg = (height_m / 2.0) / (EARTH_RADIUS_M * std::f64::consts::PI / 180.0);

    let min_lon = center_lon - half_w_deg;
    let max_lon = center_lon + half_w_deg;
    let min_lat = center_lat - half_h_deg;
    let max_lat = center_lat + half_h_deg;

    let all = db.zrange_all(db_index, key)?;
    let mut results: Vec<GeoMember> = all
        .into_iter()
        .filter_map(|(name, score)| {
            let (lon, lat) = geo_decode(score);
            if lon >= min_lon && lon <= max_lon && lat >= min_lat && lat <= max_lat {
                let dist_m = haversine_m(center_lon, center_lat, lon, lat);
                Some(GeoMember {
                    name,
                    score,
                    lon,
                    lat,
                    dist_m,
                })
            } else {
                None
            }
        })
        .collect();

    apply_sort_and_limit(&mut results, opts);
    Ok(results)
}

fn apply_sort_and_limit(results: &mut Vec<GeoMember>, opts: &SearchOptions) {
    if opts.asc {
        results.sort_by(|a, b| a.dist_m.partial_cmp(&b.dist_m).unwrap());
    } else if opts.desc {
        results.sort_by(|a, b| b.dist_m.partial_cmp(&a.dist_m).unwrap());
    } else if opts.count.is_some() && !opts.any {
        // COUNT without ANY implies ASC sort (closest first) in Redis.
        results.sort_by(|a, b| a.dist_m.partial_cmp(&b.dist_m).unwrap());
    }
    // With ANY: return N members without sorting (score-order from zrange_all).
    if let Some(count) = opts.count {
        results.truncate(count);
    }
}

/// Parse WITHCOORD / WITHDIST / WITHHASH / COUNT n [ANY] / ASC / DESC from args slice.
/// Returns the number of tokens consumed.
fn parse_search_opts(args: &[RespValue], opts: &mut SearchOptions) -> usize {
    let mut i = 0;
    while i < args.len() {
        let upper = match args[i].as_str() {
            Some(s) => s.to_uppercase(),
            None => break,
        };
        match upper.as_str() {
            "WITHCOORD" => {
                opts.with_coord = true;
                i += 1;
            }
            "WITHDIST" => {
                opts.with_dist = true;
                i += 1;
            }
            "WITHHASH" => {
                opts.with_hash = true;
                i += 1;
            }
            "ASC" => {
                opts.asc = true;
                opts.desc = false;
                i += 1;
            }
            "DESC" => {
                opts.desc = true;
                opts.asc = false;
                i += 1;
            }
            "COUNT" => {
                if i + 1 < args.len() {
                    if let Some(s) = args[i + 1].as_str() {
                        if let Ok(n) = s.parse::<usize>() {
                            opts.count = Some(n);
                            i += 2;
                            // Check for ANY immediately after
                            if i < args.len() {
                                if let Some(s2) = args[i].as_str() {
                                    if s2.to_uppercase() == "ANY" {
                                        opts.any = true;
                                        i += 1;
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }
                i += 1;
            }
            "ANY" => {
                opts.any = true;
                i += 1;
            }
            _ => break,
        }
    }
    i
}

// ── GEOADD ────────────────────────────────────────────────────────────────────

pub struct GeoAddCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GeoAddCommand {
    fn name(&self) -> &str {
        "GEOADD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEOADD key [NX|XX] [GT|LT] [CH] lon lat member [lon lat member ...]
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'geoadd' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut i = 2;
        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;

        // Parse optional flags
        loop {
            if i >= args.len() {
                break;
            }
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("NX") => {
                    nx = true;
                    i += 1;
                }
                Some("XX") => {
                    xx = true;
                    i += 1;
                }
                Some("GT") => {
                    gt = true;
                    i += 1;
                }
                Some("LT") => {
                    lt = true;
                    i += 1;
                }
                Some("CH") => {
                    ch = true;
                    i += 1;
                }
                _ => break,
            }
        }

        if (args.len() - i) % 3 != 0 {
            return RespValue::error("ERR syntax error");
        }

        let mut pairs: Vec<(f64, Vec<u8>)> = Vec::new();
        while i + 2 < args.len() {
            let lon_str = match args[i].as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR not a float"),
            };
            let lat_str = match args[i + 1].as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR not a float"),
            };
            let lon: f64 = match lon_str.parse() {
                Ok(f) => f,
                Err(_) => return RespValue::error("ERR value is not a valid float"),
            };
            let lat: f64 = match lat_str.parse() {
                Ok(f) => f,
                Err(_) => return RespValue::error("ERR value is not a valid float"),
            };

            // Validate ranges
            if lon < GEO_LONG_MIN || lon > GEO_LONG_MAX {
                return RespValue::error("ERR invalid longitude (valid range: -180, 180)");
            }
            if lat < GEO_LAT_MIN || lat > GEO_LAT_MAX {
                return RespValue::error(
                    "ERR invalid latitude (valid range: -85.05112878, 85.05112878)",
                );
            }

            let member = match args[i + 2].as_bytes() {
                Some(m) => m.to_vec(),
                None => return RespValue::error("ERR invalid member"),
            };
            let score = geo_encode(lon, lat);
            pairs.push((score, member));
            i += 3;
        }

        let pairs_ref: Vec<(f64, &[u8])> = pairs.iter().map(|(s, m)| (*s, m.as_slice())).collect();

        match self
            .db
            .zadd_flags(*db_index, key, &pairs_ref, nx, xx, gt, lt, ch, false)
        {
            Ok((n, _)) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// ── GEODIST ───────────────────────────────────────────────────────────────────

pub struct GeoDistCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GeoDistCommand {
    fn name(&self) -> &str {
        "GEODIST"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEODIST key member1 member2 [M|KM|FT|MI]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'geodist' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let member1 = match args[2].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };
        let member2 = match args[3].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };

        let unit_factor = if args.len() >= 5 {
            match args[4].as_str() {
                Some(s) => match parse_unit(s) {
                    Some(f) => f,
                    None => {
                        return RespValue::error(
                            "ERR unsupported unit provided. please use M, KM, FT, MI",
                        )
                    }
                },
                None => return RespValue::error("ERR unsupported unit"),
            }
        } else {
            1.0 // default metres
        };

        let score1 = match self.db.zscore(*db_index, key, member1) {
            Ok(Some(s)) => s,
            Ok(None) => return RespValue::null_bulk(),
            Err(e) => return map_err(e),
        };
        let score2 = match self.db.zscore(*db_index, key, member2) {
            Ok(Some(s)) => s,
            Ok(None) => return RespValue::null_bulk(),
            Err(e) => return map_err(e),
        };

        let (lon1, lat1) = geo_decode(score1);
        let (lon2, lat2) = geo_decode(score2);
        let dist_m = haversine_m(lon1, lat1, lon2, lat2);

        RespValue::BulkString(Some(format_dist(dist_m, unit_factor, 4).into_bytes()))
    }
}

// ── GEOPOS ────────────────────────────────────────────────────────────────────

pub struct GeoPosCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GeoPosCommand {
    fn name(&self) -> &str {
        "GEOPOS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEOPOS key member [member ...]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'geopos' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut results: Vec<RespValue> = Vec::new();
        for member_arg in &args[2..] {
            let member = match member_arg.as_bytes() {
                Some(m) => m,
                None => {
                    results.push(RespValue::null_bulk());
                    continue;
                }
            };
            match self.db.zscore(*db_index, key, member) {
                Ok(Some(score)) => {
                    let (lon, lat) = geo_decode(score);
                    let pair = RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(format!("{:.17}", lon).into_bytes())),
                        RespValue::BulkString(Some(format!("{:.17}", lat).into_bytes())),
                    ]));
                    results.push(pair);
                }
                Ok(None) => results.push(RespValue::null_bulk()),
                Err(e) => return map_err(e),
            }
        }

        RespValue::Array(Some(results))
    }
}

// ── GEOHASH ───────────────────────────────────────────────────────────────────

pub struct GeoHashCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GeoHashCommand {
    fn name(&self) -> &str {
        "GEOHASH"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEOHASH key member [member ...]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'geohash' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut results: Vec<RespValue> = Vec::new();
        for member_arg in &args[2..] {
            let member = match member_arg.as_bytes() {
                Some(m) => m,
                None => {
                    results.push(RespValue::null_bulk());
                    continue;
                }
            };
            match self.db.zscore(*db_index, key, member) {
                Ok(Some(score)) => {
                    let hash_str = geo_hash_string(score);
                    results.push(RespValue::BulkString(Some(hash_str.into_bytes())));
                }
                Ok(None) => results.push(RespValue::null_bulk()),
                Err(e) => return map_err(e),
            }
        }

        RespValue::Array(Some(results))
    }
}

// ── GEORADIUS (and _RO) ───────────────────────────────────────────────────────

pub struct GeoRadiusCommand {
    pub db: Arc<RedisDatabase>,
    pub read_only: bool,
}

impl CommandHandler for GeoRadiusCommand {
    fn name(&self) -> &str {
        if self.read_only {
            "GEORADIUS_RO"
        } else {
            "GEORADIUS"
        }
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEORADIUS key lon lat radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH]
        //           [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
        if args.len() < 6 {
            return RespValue::error("ERR wrong number of arguments for 'georadius' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let lon: f64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(f) => f,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let lat: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(f) => f,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let radius: f64 = match args[4].as_str().and_then(|s| s.parse().ok()) {
            Some(f) => f,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let unit_str = match args[5].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR unsupported unit"),
        };
        let unit_factor = match parse_unit(unit_str) {
            Some(f) => f,
            None => {
                return RespValue::error("ERR unsupported unit provided. please use M, KM, FT, MI")
            }
        };
        let radius_m = radius * unit_factor;

        let mut opts = SearchOptions::default();
        let rest = &args[6..];
        let consumed = parse_search_opts(rest, &mut opts);

        // Check for STORE / STOREDIST (non-RO only)
        let mut store_key: Option<Vec<u8>> = None;
        let mut store_dist = false;
        if !self.read_only {
            let mut j = consumed;
            while j < rest.len() {
                match rest[j].as_str().map(|s| s.to_uppercase()).as_deref() {
                    Some("STORE") if j + 1 < rest.len() => {
                        store_key = rest[j + 1].as_bytes().map(|b| b.to_vec());
                        store_dist = false;
                        j += 2;
                    }
                    Some("STOREDIST") if j + 1 < rest.len() => {
                        store_key = rest[j + 1].as_bytes().map(|b| b.to_vec());
                        store_dist = true;
                        j += 2;
                    }
                    _ => {
                        j += 1;
                    }
                }
            }
        }

        let members = match radius_search(
            &self.db,
            *db_index,
            key,
            lon,
            lat,
            radius_m,
            &opts,
            unit_factor,
        ) {
            Ok(m) => m,
            Err(e) => return map_err(e),
        };

        if let Some(sk) = store_key {
            return store_results(&self.db, *db_index, &sk, &members, store_dist, unit_factor);
        }

        build_result(members, &opts, unit_factor)
    }
}

// ── GEORADIUSBYMEMBER (and _RO) ───────────────────────────────────────────────

pub struct GeoRadiusByMemberCommand {
    pub db: Arc<RedisDatabase>,
    pub read_only: bool,
}

impl CommandHandler for GeoRadiusByMemberCommand {
    fn name(&self) -> &str {
        if self.read_only {
            "GEORADIUSBYMEMBER_RO"
        } else {
            "GEORADIUSBYMEMBER"
        }
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEORADIUSBYMEMBER key member radius M|KM|FT|MI [options...]
        if args.len() < 5 {
            return RespValue::error(
                "ERR wrong number of arguments for 'georadiusbymember' command",
            );
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let member = match args[2].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };
        let radius: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(f) => f,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let unit_str = match args[4].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR unsupported unit"),
        };
        let unit_factor = match parse_unit(unit_str) {
            Some(f) => f,
            None => {
                return RespValue::error("ERR unsupported unit provided. please use M, KM, FT, MI")
            }
        };
        let radius_m = radius * unit_factor;

        // Look up the member's coordinates.
        let score = match self.db.zscore(*db_index, key, member) {
            Ok(Some(s)) => s,
            Ok(None) => return RespValue::empty_array(),
            Err(_) => return RespValue::empty_array(),
        };
        let (center_lon, center_lat) = geo_decode(score);

        let mut opts = SearchOptions::default();
        let rest = &args[5..];
        let consumed = parse_search_opts(rest, &mut opts);

        // STORE / STOREDIST
        let mut store_key: Option<Vec<u8>> = None;
        let mut store_dist = false;
        if !self.read_only {
            let mut j = consumed;
            while j < rest.len() {
                match rest[j].as_str().map(|s| s.to_uppercase()).as_deref() {
                    Some("STORE") if j + 1 < rest.len() => {
                        store_key = rest[j + 1].as_bytes().map(|b| b.to_vec());
                        store_dist = false;
                        j += 2;
                    }
                    Some("STOREDIST") if j + 1 < rest.len() => {
                        store_key = rest[j + 1].as_bytes().map(|b| b.to_vec());
                        store_dist = true;
                        j += 2;
                    }
                    _ => {
                        j += 1;
                    }
                }
            }
        }

        let members = match radius_search(
            &self.db,
            *db_index,
            key,
            center_lon,
            center_lat,
            radius_m,
            &opts,
            unit_factor,
        ) {
            Ok(m) => m,
            Err(e) => return map_err(e),
        };

        if let Some(sk) = store_key {
            return store_results(&self.db, *db_index, &sk, &members, store_dist, unit_factor);
        }

        build_result(members, &opts, unit_factor)
    }
}

// ── GEOSEARCH ─────────────────────────────────────────────────────────────────

pub struct GeoSearchCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GeoSearchCommand {
    fn name(&self) -> &str {
        "GEOSEARCH"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat
        //           BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
        //           [ASC|DESC] [COUNT count [ANY]]
        //           [WITHCOORD] [WITHDIST] [WITHHASH]
        if args.len() < 6 {
            return RespValue::error("ERR wrong number of arguments for 'geosearch' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut i = 2usize;

        // FROM
        let (center_lon, center_lat) = match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("FROMMEMBER") => {
                i += 1;
                let member = match args.get(i).and_then(|a| a.as_bytes()) {
                    Some(m) => m,
                    None => return RespValue::error("ERR syntax error"),
                };
                i += 1;
                match self.db.zscore(*db_index, key, member) {
                    Ok(Some(s)) => geo_decode(s),
                    Ok(None) => return RespValue::empty_array(),
                    Err(_) => return RespValue::empty_array(),
                }
            }
            Some("FROMLONLAT") => {
                i += 1;
                let lon: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let lat: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                (lon, lat)
            }
            _ => return RespValue::error("ERR syntax error"),
        };

        // BY
        let by_upper = args
            .get(i)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase());
        let unit_factor;
        let mut by_box = false;
        let mut box_width_m = 0.0f64;
        let mut box_height_m = 0.0f64;
        let mut radius_m = 0.0f64;

        match by_upper.as_deref() {
            Some("BYRADIUS") => {
                i += 1;
                let radius: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit_str = match args.get(i).and_then(|a| a.as_str()) {
                    Some(s) => s,
                    None => return RespValue::error("ERR syntax error"),
                };
                unit_factor = match parse_unit(unit_str) {
                    Some(f) => f,
                    None => {
                        return RespValue::error(
                            "ERR unsupported unit provided. please use M, KM, FT, MI",
                        )
                    }
                };
                i += 1;
                radius_m = radius * unit_factor;
            }
            Some("BYBOX") => {
                by_box = true;
                i += 1;
                let width: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let height: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit_str = match args.get(i).and_then(|a| a.as_str()) {
                    Some(s) => s,
                    None => return RespValue::error("ERR syntax error"),
                };
                unit_factor = match parse_unit(unit_str) {
                    Some(f) => f,
                    None => {
                        return RespValue::error(
                            "ERR unsupported unit provided. please use M, KM, FT, MI",
                        )
                    }
                };
                i += 1;
                box_width_m = width * unit_factor;
                box_height_m = height * unit_factor;
            }
            _ => return RespValue::error("ERR syntax error"),
        }

        let mut opts = SearchOptions::default();
        let _consumed = parse_search_opts(&args[i..], &mut opts);

        let members = if by_box {
            match box_search(
                &self.db,
                *db_index,
                key,
                center_lon,
                center_lat,
                box_width_m,
                box_height_m,
                &opts,
                unit_factor,
            ) {
                Ok(m) => m,
                Err(e) => return map_err(e),
            }
        } else {
            match radius_search(
                &self.db,
                *db_index,
                key,
                center_lon,
                center_lat,
                radius_m,
                &opts,
                unit_factor,
            ) {
                Ok(m) => m,
                Err(e) => return map_err(e),
            }
        };

        build_result(members, &opts, unit_factor)
    }
}

// ── GEOSEARCHSTORE ────────────────────────────────────────────────────────────

pub struct GeoSearchStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GeoSearchStoreCommand {
    fn name(&self) -> &str {
        "GEOSEARCHSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT lon lat
        //                 BYRADIUS radius unit | BYBOX width height unit
        //                 [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
        if args.len() < 7 {
            return RespValue::error("ERR wrong number of arguments for 'geosearchstore' command");
        }
        let dest_key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let source_key = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut i = 3usize;

        // FROM
        let (center_lon, center_lat) = match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("FROMMEMBER") => {
                i += 1;
                let member = match args.get(i).and_then(|a| a.as_bytes()) {
                    Some(m) => m,
                    None => return RespValue::error("ERR syntax error"),
                };
                i += 1;
                match self.db.zscore(*db_index, source_key, member) {
                    Ok(Some(s)) => geo_decode(s),
                    Ok(None) => return RespValue::Integer(0), // member or key not found
                    Err(_) => return RespValue::Integer(0),
                }
            }
            Some("FROMLONLAT") => {
                i += 1;
                let lon: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let lat: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                (lon, lat)
            }
            _ => return RespValue::error("ERR syntax error"),
        };

        // BY
        let by_upper = args
            .get(i)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase());
        let unit_factor;
        let mut by_box = false;
        let mut box_width_m = 0.0f64;
        let mut box_height_m = 0.0f64;
        let mut radius_m = 0.0f64;

        match by_upper.as_deref() {
            Some("BYRADIUS") => {
                i += 1;
                let radius: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit_str = match args.get(i).and_then(|a| a.as_str()) {
                    Some(s) => s,
                    None => return RespValue::error("ERR syntax error"),
                };
                unit_factor = match parse_unit(unit_str) {
                    Some(f) => f,
                    None => {
                        return RespValue::error(
                            "ERR unsupported unit provided. please use M, KM, FT, MI",
                        )
                    }
                };
                i += 1;
                radius_m = radius * unit_factor;
            }
            Some("BYBOX") => {
                by_box = true;
                i += 1;
                let width: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let height: f64 = match args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit_str = match args.get(i).and_then(|a| a.as_str()) {
                    Some(s) => s,
                    None => return RespValue::error("ERR syntax error"),
                };
                unit_factor = match parse_unit(unit_str) {
                    Some(f) => f,
                    None => {
                        return RespValue::error(
                            "ERR unsupported unit provided. please use M, KM, FT, MI",
                        )
                    }
                };
                i += 1;
                box_width_m = width * unit_factor;
                box_height_m = height * unit_factor;
            }
            _ => return RespValue::error("ERR syntax error"),
        }

        let mut opts = SearchOptions::default();
        let consumed = parse_search_opts(&args[i..], &mut opts);

        // Check for STOREDIST flag
        let mut store_dist = false;
        let mut j = i + consumed;
        while j < args.len() {
            if let Some(s) = args[j].as_str() {
                if s.to_uppercase() == "STOREDIST" {
                    store_dist = true;
                }
            }
            j += 1;
        }

        let members = if by_box {
            match box_search(
                &self.db,
                *db_index,
                source_key,
                center_lon,
                center_lat,
                box_width_m,
                box_height_m,
                &opts,
                unit_factor,
            ) {
                Ok(m) => m,
                Err(e) => return map_err(e),
            }
        } else {
            match radius_search(
                &self.db,
                *db_index,
                source_key,
                center_lon,
                center_lat,
                radius_m,
                &opts,
                unit_factor,
            ) {
                Ok(m) => m,
                Err(e) => return map_err(e),
            }
        };

        store_results(
            &self.db,
            *db_index,
            &dest_key,
            &members,
            store_dist,
            unit_factor,
        )
    }
}

// ── STORE helper ──────────────────────────────────────────────────────────────

fn store_results(
    db: &RedisDatabase,
    db_index: usize,
    dest_key: &[u8],
    members: &[GeoMember],
    store_dist: bool,
    unit_factor: f64,
) -> RespValue {
    let count = members.len() as i64;
    if members.is_empty() {
        // Delete the destination key if it exists.
        let _ = db.delete(db_index, &[dest_key]);
        return RespValue::Integer(0);
    }

    let pairs: Vec<(f64, &[u8])> = members
        .iter()
        .map(|m| {
            let score = if store_dist {
                m.dist_m / unit_factor
            } else {
                m.score
            };
            (score, m.name.as_slice())
        })
        .collect();

    match db.zadd(db_index, dest_key, &pairs) {
        Ok(_) => RespValue::Integer(count),
        Err(e) => match e {
            crate::database::RedisError::WrongType => wrong_type(),
            other => RespValue::error(&other.to_string()),
        },
    }
}
