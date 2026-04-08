use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use lazy_static::lazy_static;
use parking_lot::Mutex;

use crate::ext_type_registry;
use crate::resp::RespValue;

use super::CommandHandler;

// ── Data structures ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct StreamEntry {
    id: (u64, u64), // (ms, seq)
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug)]
struct PendingEntry {
    id: (u64, u64),
    consumer: String,
    delivery_time: u64,
    delivery_count: u64,
}

#[derive(Clone, Debug)]
struct ConsumerGroup {
    last_id: (u64, u64),
    pending: Vec<PendingEntry>, // PEL
    /// consumer name → last-seen timestamp (ms)
    consumers: HashMap<String, u64>,
}

#[derive(Clone, Debug)]
struct Stream {
    entries: Vec<StreamEntry>,
    groups: HashMap<String, ConsumerGroup>,
    last_id: (u64, u64),
}

impl Stream {
    fn new() -> Self {
        Stream {
            entries: Vec::new(),
            groups: HashMap::new(),
            last_id: (0, 0),
        }
    }
}

// Global store: (db_index, key) -> Stream
lazy_static! {
    static ref STREAMS: Mutex<HashMap<(usize, Vec<u8>), Stream>> = Mutex::new(HashMap::new());
}

// ── Helper functions ───────────────────────────────────────────────────────────

fn current_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn id_to_string(id: (u64, u64)) -> String {
    format!("{}-{}", id.0, id.1)
}

/// Parse a stream ID string. Returns (ms, seq, exclusive).
/// Special IDs: "-" = (0,0), "+" = (u64::MAX, u64::MAX)
/// Exclusive: "(1-0" means exclusive (not including 1-0)
fn parse_id_bound(s: &str) -> Option<((u64, u64), bool)> {
    if s == "-" {
        return Some(((0, 0), false));
    }
    if s == "+" {
        return Some(((u64::MAX, u64::MAX), false));
    }
    if s == "0" {
        return Some(((0, 0), false));
    }

    let (exclusive, rest) = if s.starts_with('(') {
        (true, &s[1..])
    } else {
        (false, s)
    };

    if let Some(dash_pos) = rest.find('-') {
        let ms_str = &rest[..dash_pos];
        let seq_str = &rest[dash_pos + 1..];
        let ms: u64 = ms_str.parse().ok()?;
        let seq: u64 = seq_str.parse().ok()?;
        Some(((ms, seq), exclusive))
    } else {
        let ms: u64 = rest.parse().ok()?;
        Some(((ms, 0), exclusive))
    }
}

/// Parse a stream ID for XADD (supports *, ms-*, ms-seq, ms).
/// Returns None if parsing fails.
/// last_id is the stream's current last_id.
fn parse_xadd_id(s: &str, last_id: (u64, u64)) -> Option<(u64, u64)> {
    if s == "*" {
        // Auto-generate: use current ms; if same ms as last, increment seq
        let ms = current_ms();
        let seq = if ms == last_id.0 {
            last_id.1 + 1
        } else if ms < last_id.0 {
            // clock went backward, use last_id ms + 1 seq
            return Some((last_id.0, last_id.1 + 1));
        } else {
            0
        };
        return Some((ms, seq));
    }

    if let Some(dash_pos) = s.find('-') {
        let ms_str = &s[..dash_pos];
        let seq_str = &s[dash_pos + 1..];
        let ms: u64 = ms_str.parse().ok()?;
        if seq_str == "*" {
            // Auto seq: if same ms as last, use last_seq+1, else 0
            let seq = if ms == last_id.0 { last_id.1 + 1 } else { 0 };
            Some((ms, seq))
        } else {
            let seq: u64 = seq_str.parse().ok()?;
            Some((ms, seq))
        }
    } else {
        // Just ms, seq = 0 (or last_seq+1 if same ms)
        let ms: u64 = s.parse().ok()?;
        let seq = if ms == last_id.0 {
            0 // explicit ms with no seq always means seq=0 for a new explicit id
        } else {
            0
        };
        Some((ms, seq))
    }
}

fn format_entry(entry: &StreamEntry) -> RespValue {
    let id_str = id_to_string(entry.id);
    let mut field_values: Vec<RespValue> = Vec::new();
    for (k, v) in &entry.fields {
        field_values.push(RespValue::bulk_bytes(k.clone()));
        field_values.push(RespValue::bulk_bytes(v.clone()));
    }
    RespValue::Array(Some(vec![
        RespValue::bulk_str(&id_str),
        RespValue::Array(Some(field_values)),
    ]))
}

/// Apply MAXLEN trimming. Returns number of entries removed.
fn trim_maxlen(stream: &mut Stream, max_count: usize, _approx: bool) -> i64 {
    let len = stream.entries.len();
    if len <= max_count {
        return 0;
    }
    let remove_count = len - max_count;
    stream.entries.drain(0..remove_count);
    remove_count as i64
}

/// Apply MINID trimming. Returns number of entries removed.
fn trim_minid(stream: &mut Stream, min_id: (u64, u64), _approx: bool) -> i64 {
    let before = stream.entries.len();
    stream.entries.retain(|e| e.id >= min_id);
    (before - stream.entries.len()) as i64
}

// ── XADD ──────────────────────────────────────────────────────────────────────

pub struct XAddCommand;

impl CommandHandler for XAddCommand {
    fn name(&self) -> &str {
        "XADD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'xadd' command");
        }

        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };

        let mut i = 2usize;
        let mut nomkstream = false;
        let mut trim_kind: Option<&str> = None; // "MAXLEN" or "MINID"
        let mut trim_approx = false;
        let mut trim_threshold_str: Option<String> = None;
        // LIMIT is parsed but not stored as a separate field since it doesn't change behavior for in-memory

        // Parse optional flags
        loop {
            if i >= args.len() {
                break;
            }
            let upper = match args[i].as_str() {
                Some(s) => s.to_uppercase(),
                None => break,
            };
            match upper.as_str() {
                "NOMKSTREAM" => {
                    nomkstream = true;
                    i += 1;
                }
                "MAXLEN" => {
                    trim_kind = Some("MAXLEN");
                    i += 1;
                    // Check for ~ or =
                    if i < args.len() {
                        if let Some(s) = args[i].as_str() {
                            if s == "~" || s == "=" {
                                trim_approx = s == "~";
                                i += 1;
                            }
                        }
                    }
                    if i < args.len() {
                        trim_threshold_str = args[i].as_str().map(|s| s.to_string());
                        i += 1;
                    }
                    // Skip optional LIMIT count
                    if i < args.len() {
                        if let Some(s) = args[i].as_str() {
                            if s.to_uppercase() == "LIMIT" {
                                i += 2; // skip "LIMIT" and the count
                            }
                        }
                    }
                }
                "MINID" => {
                    trim_kind = Some("MINID");
                    i += 1;
                    // Check for ~ or =
                    if i < args.len() {
                        if let Some(s) = args[i].as_str() {
                            if s == "~" || s == "=" {
                                trim_approx = s == "~";
                                i += 1;
                            }
                        }
                    }
                    if i < args.len() {
                        trim_threshold_str = args[i].as_str().map(|s| s.to_string());
                        i += 1;
                    }
                    // Skip optional LIMIT count
                    if i < args.len() {
                        if let Some(s) = args[i].as_str() {
                            if s.to_uppercase() == "LIMIT" {
                                i += 2;
                            }
                        }
                    }
                }
                _ => break,
            }
        }

        // Next arg is the ID
        if i >= args.len() {
            return RespValue::error("ERR syntax error");
        }
        let id_str = match args[i].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid stream ID"),
        };
        i += 1;

        // Remaining args are field-value pairs
        if i >= args.len() || (args.len() - i) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'xadd' command");
        }

        let mut fields: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while i + 1 < args.len() {
            let field = match args[i].as_bytes() {
                Some(f) => f.to_vec(),
                None => return RespValue::error("ERR invalid field"),
            };
            let value = match args[i + 1].as_bytes() {
                Some(v) => v.to_vec(),
                None => return RespValue::error("ERR invalid value"),
            };
            fields.push((field, value));
            i += 2;
        }

        let db = *db_index;
        let mut store = STREAMS.lock();

        if nomkstream && !store.contains_key(&(db, key.clone())) {
            return RespValue::null_bulk();
        }

        let is_new = !store.contains_key(&(db, key.clone()));
        let stream = store.entry((db, key.clone())).or_insert_with(Stream::new);
        if is_new {
            ext_type_registry::register(db, &key, "stream");
        }

        let entry_id = match parse_xadd_id(&id_str, stream.last_id) {
            Some(id) => id,
            None => {
                return RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                )
            }
        };

        // Validate ID is greater than last_id (unless it's 0-0 and stream is empty)
        if !stream.entries.is_empty() && entry_id <= stream.last_id {
            return RespValue::error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            );
        }

        stream.last_id = entry_id;
        stream.entries.push(StreamEntry {
            id: entry_id,
            fields,
        });

        // Apply trimming
        if let Some(kind) = trim_kind {
            if let Some(ref threshold_str) = trim_threshold_str {
                match kind {
                    "MAXLEN" => {
                        if let Ok(max_count) = threshold_str.parse::<usize>() {
                            trim_maxlen(stream, max_count, trim_approx);
                        }
                    }
                    "MINID" => {
                        if let Some((min_id, _)) = parse_id_bound(threshold_str) {
                            trim_minid(stream, min_id, trim_approx);
                        }
                    }
                    _ => {}
                }
            }
        }

        RespValue::bulk_str(&id_to_string(entry_id))
    }
}

// ── XLEN ──────────────────────────────────────────────────────────────────────

pub struct XLenCommand;

impl CommandHandler for XLenCommand {
    fn name(&self) -> &str {
        "XLEN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'xlen' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let store = STREAMS.lock();
        let len = store
            .get(&(*db_index, key))
            .map(|s| s.entries.len())
            .unwrap_or(0);
        RespValue::integer(len as i64)
    }
}

// ── XRANGE ────────────────────────────────────────────────────────────────────

pub struct XRangeCommand;

impl CommandHandler for XRangeCommand {
    fn name(&self) -> &str {
        "XRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XRANGE key start end [COUNT count]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'xrange' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let start_str = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid start"),
        };
        let end_str = match args[3].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid end"),
        };

        let mut count: Option<usize> = None;
        if args.len() >= 6 {
            if let Some(s) = args[4].as_str() {
                if s.to_uppercase() == "COUNT" {
                    count = args[5].as_str().and_then(|n| n.parse().ok());
                }
            }
        }

        let (start_id, start_exclusive) = match parse_id_bound(&start_str) {
            Some(v) => v,
            None => return RespValue::error("ERR invalid start ID"),
        };
        let (end_id, end_exclusive) = match parse_id_bound(&end_str) {
            Some(v) => v,
            None => return RespValue::error("ERR invalid end ID"),
        };

        let store = STREAMS.lock();
        let stream = match store.get(&(*db_index, key)) {
            Some(s) => s,
            None => return RespValue::empty_array(),
        };

        let results: Vec<RespValue> = stream
            .entries
            .iter()
            .filter(|e| {
                let after_start = if start_exclusive {
                    e.id > start_id
                } else {
                    e.id >= start_id
                };
                let before_end = if end_exclusive {
                    e.id < end_id
                } else {
                    e.id <= end_id
                };
                after_start && before_end
            })
            .take(count.unwrap_or(usize::MAX))
            .map(format_entry)
            .collect();

        RespValue::Array(Some(results))
    }
}

// ── XREVRANGE ─────────────────────────────────────────────────────────────────

pub struct XRevRangeCommand;

impl CommandHandler for XRevRangeCommand {
    fn name(&self) -> &str {
        "XREVRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XREVRANGE key end start [COUNT count]
        // Note: end comes before start (reversed from XRANGE)
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'xrevrange' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        // In XREVRANGE: args[2] = end, args[3] = start
        let end_str = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid end"),
        };
        let start_str = match args[3].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid start"),
        };

        let mut count: Option<usize> = None;
        if args.len() >= 6 {
            if let Some(s) = args[4].as_str() {
                if s.to_uppercase() == "COUNT" {
                    count = args[5].as_str().and_then(|n| n.parse().ok());
                }
            }
        }

        let (end_id, end_exclusive) = match parse_id_bound(&end_str) {
            Some(v) => v,
            None => return RespValue::error("ERR invalid end ID"),
        };
        let (start_id, start_exclusive) = match parse_id_bound(&start_str) {
            Some(v) => v,
            None => return RespValue::error("ERR invalid start ID"),
        };

        let store = STREAMS.lock();
        let stream = match store.get(&(*db_index, key)) {
            Some(s) => s,
            None => return RespValue::empty_array(),
        };

        let results: Vec<RespValue> = stream
            .entries
            .iter()
            .rev()
            .filter(|e| {
                let before_end = if end_exclusive {
                    e.id < end_id
                } else {
                    e.id <= end_id
                };
                let after_start = if start_exclusive {
                    e.id > start_id
                } else {
                    e.id >= start_id
                };
                before_end && after_start
            })
            .take(count.unwrap_or(usize::MAX))
            .map(format_entry)
            .collect();

        RespValue::Array(Some(results))
    }
}

// ── XDEL ──────────────────────────────────────────────────────────────────────

pub struct XDelCommand;

impl CommandHandler for XDelCommand {
    fn name(&self) -> &str {
        "XDEL"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XDEL key id [id ...]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'xdel' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };

        let mut ids: Vec<(u64, u64)> = Vec::new();
        for arg in &args[2..] {
            let id_str = match arg.as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR invalid ID"),
            };
            match parse_id_bound(id_str) {
                Some((id, _)) => ids.push(id),
                None => return RespValue::error("ERR invalid stream ID"),
            }
        }

        let db = *db_index;
        let mut store = STREAMS.lock();
        let stream = match store.get_mut(&(db, key.clone())) {
            Some(s) => s,
            None => return RespValue::integer(0),
        };

        let before = stream.entries.len();
        stream.entries.retain(|e| !ids.contains(&e.id));
        let removed = before - stream.entries.len();
        // If stream is now empty, remove from store and registry
        if stream.entries.is_empty() {
            store.remove(&(db, key.clone()));
            ext_type_registry::unregister(db, &key);
        }
        RespValue::integer(removed as i64)
    }
}

// ── XTRIM ─────────────────────────────────────────────────────────────────────

pub struct XTrimCommand;

impl CommandHandler for XTrimCommand {
    fn name(&self) -> &str {
        "XTRIM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'xtrim' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let strategy = match args[2].as_str() {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };

        let mut i = 3usize;
        let mut approx = false;

        // Check for ~ or =
        if i < args.len() {
            if let Some(s) = args[i].as_str() {
                if s == "~" || s == "=" {
                    approx = s == "~";
                    i += 1;
                }
            }
        }

        if i >= args.len() {
            return RespValue::error("ERR syntax error");
        }
        let threshold_str = match args[i].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR syntax error"),
        };
        i += 1;

        // Skip optional LIMIT count
        if i < args.len() {
            if let Some(s) = args[i].as_str() {
                if s.to_uppercase() == "LIMIT" {
                    // skip LIMIT and its value
                    // i += 2; not needed since we don't use these
                }
            }
        }

        let mut store = STREAMS.lock();
        let stream = match store.get_mut(&(*db_index, key)) {
            Some(s) => s,
            None => return RespValue::integer(0),
        };

        let removed = match strategy.as_str() {
            "MAXLEN" => match threshold_str.parse::<usize>() {
                Ok(max_count) => trim_maxlen(stream, max_count, approx),
                Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
            },
            "MINID" => match parse_id_bound(&threshold_str) {
                Some((min_id, _)) => trim_minid(stream, min_id, approx),
                None => return RespValue::error("ERR invalid stream ID"),
            },
            _ => return RespValue::error("ERR syntax error"),
        };

        RespValue::integer(removed)
    }
}

// ── XREAD ─────────────────────────────────────────────────────────────────────

pub struct XReadCommand;

impl CommandHandler for XReadCommand {
    fn name(&self) -> &str {
        "XREAD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'xread' command");
        }

        let mut i = 1usize;
        let mut count: Option<usize> = None;
        let mut _block_ms: Option<u64> = None;

        // Parse COUNT and BLOCK options
        loop {
            if i >= args.len() {
                break;
            }
            let upper = match args[i].as_str() {
                Some(s) => s.to_uppercase(),
                None => break,
            };
            match upper.as_str() {
                "COUNT" => {
                    i += 1;
                    if i < args.len() {
                        count = args[i].as_str().and_then(|s| s.parse().ok());
                        i += 1;
                    }
                }
                "BLOCK" => {
                    i += 1;
                    if i < args.len() {
                        _block_ms = args[i].as_str().and_then(|s| s.parse().ok());
                        i += 1;
                    }
                }
                "STREAMS" => {
                    i += 1;
                    break;
                }
                _ => break,
            }
        }

        // Remaining args: keys and IDs interleaved
        // STREAMS key1 key2 ... id1 id2 ...
        let remaining = &args[i..];
        if remaining.len() < 2 || remaining.len() % 2 != 0 {
            return RespValue::error("ERR syntax error");
        }

        let half = remaining.len() / 2;
        let keys: Vec<Vec<u8>> = remaining[..half]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();
        let id_strs: Vec<String> = remaining[half..]
            .iter()
            .filter_map(|a| a.as_str().map(|s| s.to_string()))
            .collect();

        if keys.len() != half || id_strs.len() != half {
            return RespValue::error("ERR syntax error");
        }

        // BLOCK: we don't support actual blocking; if BLOCK is specified and no
        // messages are available, return null.
        let store = STREAMS.lock();
        let db = *db_index;

        let mut result_streams: Vec<RespValue> = Vec::new();
        let mut any_results = false;

        for (key, id_str) in keys.iter().zip(id_strs.iter()) {
            let (after_id, exclusive) = if id_str == "$" {
                // $ means only new messages; since we don't block, return nothing
                ((u64::MAX, u64::MAX), false)
            } else {
                match parse_id_bound(id_str) {
                    Some(v) => v,
                    None => return RespValue::error("ERR invalid stream ID"),
                }
            };

            let entries: Vec<RespValue> = match store.get(&(db, key.clone())) {
                Some(stream) => stream
                    .entries
                    .iter()
                    .filter(|e| {
                        if exclusive {
                            e.id > after_id
                        } else {
                            e.id > after_id
                        }
                    })
                    .take(count.unwrap_or(usize::MAX))
                    .map(format_entry)
                    .collect(),
                None => Vec::new(),
            };

            if !entries.is_empty() {
                any_results = true;
                result_streams.push(RespValue::Array(Some(vec![
                    RespValue::bulk_bytes(key.clone()),
                    RespValue::Array(Some(entries)),
                ])));
            }
        }

        if !any_results {
            // If BLOCK was specified, we'd wait - but we return null immediately
            return RespValue::null_array();
        }

        RespValue::Array(Some(result_streams))
    }
}

// ── XGROUP ────────────────────────────────────────────────────────────────────

pub struct XGroupCommand;

impl CommandHandler for XGroupCommand {
    fn name(&self) -> &str {
        "XGROUP"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XGROUP CREATE key group id [MKSTREAM] [ENTRIESREAD entries-read]
        // XGROUP SETID key group id
        // XGROUP DESTROY key group
        // XGROUP CREATECONSUMER key group consumer
        // XGROUP DELCONSUMER key group consumer
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'xgroup' command");
        }

        let subcommand = match args[1].as_str() {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };

        match subcommand.as_str() {
            "CREATE" => {
                // XGROUP CREATE key group id [MKSTREAM]
                if args.len() < 5 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|create' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let group = match args[3].as_str() {
                    Some(g) => g.to_string(),
                    None => return RespValue::error("ERR invalid group name"),
                };
                let id_str = match args[4].as_str() {
                    Some(s) => s.to_string(),
                    None => return RespValue::error("ERR invalid stream ID"),
                };

                let mut mkstream = false;
                for extra in &args[5..] {
                    if let Some(s) = extra.as_str() {
                        if s.to_uppercase() == "MKSTREAM" {
                            mkstream = true;
                        }
                    }
                }

                let db = *db_index;
                let mut store = STREAMS.lock();

                if !store.contains_key(&(db, key.clone())) {
                    if mkstream {
                        store.insert((db, key.clone()), Stream::new());
                    } else {
                        return RespValue::error("ERR no such key");
                    }
                }

                let stream = store.get_mut(&(db, key)).unwrap();

                let last_id = if id_str == "$" {
                    stream.last_id
                } else if id_str == "0" {
                    (0, 0)
                } else {
                    match parse_id_bound(&id_str) {
                        Some((id, _)) => id,
                        None => return RespValue::error("ERR invalid stream ID"),
                    }
                };

                if stream.groups.contains_key(&group) {
                    return RespValue::error("BUSYGROUP Consumer Group name already exists");
                }

                stream.groups.insert(
                    group,
                    ConsumerGroup {
                        last_id,
                        pending: Vec::new(),
                        consumers: HashMap::new(),
                    },
                );

                RespValue::ok()
            }
            "SETID" => {
                // XGROUP SETID key group id
                if args.len() < 5 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|setid' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let group = match args[3].as_str() {
                    Some(g) => g.to_string(),
                    None => return RespValue::error("ERR invalid group name"),
                };
                let id_str = match args[4].as_str() {
                    Some(s) => s.to_string(),
                    None => return RespValue::error("ERR invalid stream ID"),
                };

                let db = *db_index;
                let mut store = STREAMS.lock();
                let stream = match store.get_mut(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::error("ERR no such key"),
                };

                let last_id = if id_str == "$" {
                    stream.last_id
                } else if id_str == "0" {
                    (0, 0)
                } else {
                    match parse_id_bound(&id_str) {
                        Some((id, _)) => id,
                        None => return RespValue::error("ERR invalid stream ID"),
                    }
                };

                match stream.groups.get_mut(&group) {
                    Some(g) => {
                        g.last_id = last_id;
                        RespValue::ok()
                    }
                    None => RespValue::error("ERR no such consumer group"),
                }
            }
            "DESTROY" => {
                // XGROUP DESTROY key group
                if args.len() < 4 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|destroy' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let group = match args[3].as_str() {
                    Some(g) => g.to_string(),
                    None => return RespValue::error("ERR invalid group name"),
                };

                let db = *db_index;
                let mut store = STREAMS.lock();
                let stream = match store.get_mut(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::integer(0),
                };

                if stream.groups.remove(&group).is_some() {
                    RespValue::integer(1)
                } else {
                    RespValue::integer(0)
                }
            }
            "CREATECONSUMER" => {
                // XGROUP CREATECONSUMER key group consumer
                if args.len() < 5 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|createconsumer' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let group = match args[3].as_str() {
                    Some(g) => g.to_string(),
                    None => return RespValue::error("ERR invalid group name"),
                };
                let consumer = match args[4].as_str() {
                    Some(c) => c.to_string(),
                    None => return RespValue::error("ERR invalid consumer name"),
                };

                let db = *db_index;
                let mut store = STREAMS.lock();
                let stream = match store.get_mut(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::error("ERR no such key"),
                };

                let grp = match stream.groups.get_mut(&group) {
                    Some(g) => g,
                    None => return RespValue::error("ERR no such consumer group"),
                };

                if grp.consumers.contains_key(&consumer) {
                    RespValue::integer(0)
                } else {
                    grp.consumers.insert(consumer, current_ms());
                    RespValue::integer(1)
                }
            }
            "DELCONSUMER" => {
                // XGROUP DELCONSUMER key group consumer
                if args.len() < 5 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|delconsumer' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let group = match args[3].as_str() {
                    Some(g) => g.to_string(),
                    None => return RespValue::error("ERR invalid group name"),
                };
                let consumer = match args[4].as_str() {
                    Some(c) => c.to_string(),
                    None => return RespValue::error("ERR invalid consumer name"),
                };

                let db = *db_index;
                let mut store = STREAMS.lock();
                let stream = match store.get_mut(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::integer(0),
                };

                let grp = match stream.groups.get_mut(&group) {
                    Some(g) => g,
                    None => return RespValue::integer(0),
                };

                // Count pending messages for this consumer
                let pending_count = grp
                    .pending
                    .iter()
                    .filter(|p| p.consumer == consumer)
                    .count();
                grp.pending.retain(|p| p.consumer != consumer);
                grp.consumers.remove(&consumer);

                RespValue::integer(pending_count as i64)
            }
            _ => RespValue::error(&format!(
                "ERR unknown subcommand '{}' for 'xgroup' command",
                subcommand
            )),
        }
    }
}

// ── XREADGROUP ────────────────────────────────────────────────────────────────

pub struct XReadGroupCommand;

impl CommandHandler for XReadGroupCommand {
    fn name(&self) -> &str {
        "XREADGROUP"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
        if args.len() < 7 {
            return RespValue::error("ERR wrong number of arguments for 'xreadgroup' command");
        }

        // args[1] must be "GROUP"
        match args[1].as_str() {
            Some(s) if s.to_uppercase() == "GROUP" => {}
            _ => return RespValue::error("ERR syntax error"),
        }

        let group = match args[2].as_str() {
            Some(g) => g.to_string(),
            None => return RespValue::error("ERR invalid group name"),
        };
        let consumer = match args[3].as_str() {
            Some(c) => c.to_string(),
            None => return RespValue::error("ERR invalid consumer name"),
        };

        let mut i = 4usize;
        let mut count: Option<usize> = None;
        let mut noack = false;

        loop {
            if i >= args.len() {
                break;
            }
            let upper = match args[i].as_str() {
                Some(s) => s.to_uppercase(),
                None => break,
            };
            match upper.as_str() {
                "COUNT" => {
                    i += 1;
                    if i < args.len() {
                        count = args[i].as_str().and_then(|s| s.parse().ok());
                        i += 1;
                    }
                }
                "BLOCK" => {
                    i += 2; // skip block and its value
                }
                "NOACK" => {
                    noack = true;
                    i += 1;
                }
                "STREAMS" => {
                    i += 1;
                    break;
                }
                _ => break,
            }
        }

        // Remaining: keys then ids
        let remaining = &args[i..];
        if remaining.len() < 2 || remaining.len() % 2 != 0 {
            return RespValue::error("ERR syntax error");
        }

        let half = remaining.len() / 2;
        let keys: Vec<Vec<u8>> = remaining[..half]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();
        let id_strs: Vec<String> = remaining[half..]
            .iter()
            .filter_map(|a| a.as_str().map(|s| s.to_string()))
            .collect();

        if keys.len() != half || id_strs.len() != half {
            return RespValue::error("ERR syntax error");
        }

        let db = *db_index;
        let now_ms = current_ms();
        let mut store = STREAMS.lock();

        let mut result_streams: Vec<RespValue> = Vec::new();

        for (key, id_str) in keys.iter().zip(id_strs.iter()) {
            let stream = match store.get_mut(&(db, key.clone())) {
                Some(s) => s,
                None => return RespValue::error("ERR no such key"),
            };

            let grp = match stream.groups.get_mut(&group) {
                Some(g) => g,
                None => {
                    return RespValue::error(&format!(
                        "-NOGROUP No such consumer group '{}' for key name '{}'",
                        group,
                        String::from_utf8_lossy(key)
                    ))
                }
            };

            // Ensure consumer exists in group, update last-seen
            grp.consumers.insert(consumer.clone(), now_ms);

            let entries_to_deliver: Vec<StreamEntry>;

            if id_str == ">" {
                // Deliver new (undelivered) messages
                let last_id = grp.last_id;
                entries_to_deliver = stream
                    .entries
                    .iter()
                    .filter(|e| e.id > last_id)
                    .take(count.unwrap_or(usize::MAX))
                    .cloned()
                    .collect();

                if !noack {
                    // Add to PEL
                    for entry in &entries_to_deliver {
                        grp.pending.push(PendingEntry {
                            id: entry.id,
                            consumer: consumer.clone(),
                            delivery_time: now_ms,
                            delivery_count: 1,
                        });
                    }
                }

                if let Some(last) = entries_to_deliver.last() {
                    grp.last_id = last.id;
                }
            } else {
                // Re-deliver pending messages for this consumer with id >= given id
                let (after_id, _) = match parse_id_bound(id_str) {
                    Some(v) => v,
                    None => return RespValue::error("ERR invalid stream ID"),
                };

                let pending_ids: Vec<(u64, u64)> = grp
                    .pending
                    .iter_mut()
                    .filter(|p| p.consumer == consumer && p.id >= after_id)
                    .map(|p| {
                        p.delivery_time = now_ms;
                        p.delivery_count += 1;
                        p.id
                    })
                    .collect();

                entries_to_deliver = stream
                    .entries
                    .iter()
                    .filter(|e| pending_ids.contains(&e.id))
                    .take(count.unwrap_or(usize::MAX))
                    .cloned()
                    .collect();
            }

            let formatted: Vec<RespValue> = entries_to_deliver.iter().map(format_entry).collect();
            result_streams.push(RespValue::Array(Some(vec![
                RespValue::bulk_bytes(key.clone()),
                RespValue::Array(Some(formatted)),
            ])));
        }

        if result_streams.is_empty() {
            return RespValue::null_array();
        }

        RespValue::Array(Some(result_streams))
    }
}

// ── XACK ──────────────────────────────────────────────────────────────────────

pub struct XAckCommand;

impl CommandHandler for XAckCommand {
    fn name(&self) -> &str {
        "XACK"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XACK key group id [id ...]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'xack' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let group = match args[2].as_str() {
            Some(g) => g.to_string(),
            None => return RespValue::error("ERR invalid group name"),
        };

        let mut ids: Vec<(u64, u64)> = Vec::new();
        for arg in &args[3..] {
            let id_str = match arg.as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR invalid ID"),
            };
            match parse_id_bound(id_str) {
                Some((id, _)) => ids.push(id),
                None => return RespValue::error("ERR invalid stream ID"),
            }
        }

        let db = *db_index;
        let mut store = STREAMS.lock();
        let stream = match store.get_mut(&(db, key)) {
            Some(s) => s,
            None => return RespValue::integer(0),
        };

        let grp = match stream.groups.get_mut(&group) {
            Some(g) => g,
            None => return RespValue::integer(0),
        };

        let before = grp.pending.len();
        grp.pending.retain(|p| !ids.contains(&p.id));
        let acked = before - grp.pending.len();
        RespValue::integer(acked as i64)
    }
}

// ── XCLAIM ────────────────────────────────────────────────────────────────────

pub struct XClaimCommand;

impl CommandHandler for XClaimCommand {
    fn name(&self) -> &str {
        "XCLAIM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME unix-time-ms]
        //        [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID streamid]
        if args.len() < 6 {
            return RespValue::error("ERR wrong number of arguments for 'xclaim' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let group = match args[2].as_str() {
            Some(g) => g.to_string(),
            None => return RespValue::error("ERR invalid group name"),
        };
        let new_consumer = match args[3].as_str() {
            Some(c) => c.to_string(),
            None => return RespValue::error("ERR invalid consumer name"),
        };
        let min_idle_time: u64 = match args[4].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let mut claim_ids: Vec<(u64, u64)> = Vec::new();
        let mut i = 5usize;
        while i < args.len() {
            if let Some(s) = args[i].as_str() {
                let upper = s.to_uppercase();
                if upper == "IDLE"
                    || upper == "TIME"
                    || upper == "RETRYCOUNT"
                    || upper == "FORCE"
                    || upper == "JUSTID"
                    || upper == "LASTID"
                {
                    break;
                }
                match parse_id_bound(s) {
                    Some((id, _)) => {
                        claim_ids.push(id);
                        i += 1;
                    }
                    None => break,
                }
            } else {
                break;
            }
        }

        // Parse optional flags (simplified - not all are implemented)
        let mut justid = false;
        let mut force = false;
        let mut new_idle: Option<u64> = None;
        let now_ms = current_ms();

        while i < args.len() {
            let upper = match args[i].as_str() {
                Some(s) => s.to_uppercase(),
                None => {
                    i += 1;
                    continue;
                }
            };
            match upper.as_str() {
                "JUSTID" => {
                    justid = true;
                    i += 1;
                }
                "FORCE" => {
                    force = true;
                    i += 1;
                }
                "IDLE" => {
                    i += 1;
                    if i < args.len() {
                        new_idle = args[i].as_str().and_then(|s| s.parse().ok());
                        i += 1;
                    }
                }
                "TIME" => {
                    i += 2; // skip value
                }
                "RETRYCOUNT" => {
                    i += 2; // skip value
                }
                "LASTID" => {
                    i += 2; // skip value
                }
                _ => {
                    i += 1;
                }
            }
        }

        let db = *db_index;
        let mut store = STREAMS.lock();
        let stream = match store.get_mut(&(db, key)) {
            Some(s) => s,
            None => return RespValue::empty_array(),
        };

        let grp = match stream.groups.get_mut(&group) {
            Some(g) => g,
            None => return RespValue::empty_array(),
        };

        let mut claimed_ids: Vec<(u64, u64)> = Vec::new();

        for claim_id in &claim_ids {
            // Find in PEL
            if let Some(pel_entry) = grp.pending.iter_mut().find(|p| p.id == *claim_id) {
                let idle_time = now_ms.saturating_sub(pel_entry.delivery_time);
                if idle_time >= min_idle_time || force {
                    pel_entry.consumer = new_consumer.clone();
                    if let Some(idle) = new_idle {
                        pel_entry.delivery_time = now_ms.saturating_sub(idle);
                    } else {
                        pel_entry.delivery_time = now_ms;
                    }
                    pel_entry.delivery_count += 1;
                    claimed_ids.push(*claim_id);
                }
            } else if force {
                // FORCE: add to PEL even if not there
                grp.pending.push(PendingEntry {
                    id: *claim_id,
                    consumer: new_consumer.clone(),
                    delivery_time: now_ms,
                    delivery_count: 1,
                });
                claimed_ids.push(*claim_id);
            }
        }

        if justid {
            let ids: Vec<RespValue> = claimed_ids
                .iter()
                .map(|id| RespValue::bulk_str(&id_to_string(*id)))
                .collect();
            return RespValue::Array(Some(ids));
        }

        // Return full entries for claimed IDs
        let results: Vec<RespValue> = stream
            .entries
            .iter()
            .filter(|e| claimed_ids.contains(&e.id))
            .map(format_entry)
            .collect();

        RespValue::Array(Some(results))
    }
}

// ── XPENDING ──────────────────────────────────────────────────────────────────

pub struct XPendingCommand;

impl CommandHandler for XPendingCommand {
    fn name(&self) -> &str {
        "XPENDING"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
        // Summary form: XPENDING key group
        // Range form:   XPENDING key group [IDLE idle] start end count [consumer]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'xpending' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let group = match args[2].as_str() {
            Some(g) => g.to_string(),
            None => return RespValue::error("ERR invalid group name"),
        };

        let db = *db_index;
        let store = STREAMS.lock();
        let stream = match store.get(&(db, key)) {
            Some(s) => s,
            None => {
                // Return summary with empty data for non-existent key
                return RespValue::Array(Some(vec![
                    RespValue::integer(0),
                    RespValue::null_bulk(),
                    RespValue::null_bulk(),
                    RespValue::Array(Some(vec![])),
                ]));
            }
        };

        let grp = match stream.groups.get(&group) {
            Some(g) => g,
            None => return RespValue::error("ERR no such consumer group"),
        };

        // Check if this is summary form or range form
        // Summary: args.len() == 3
        // Range: args.len() >= 6 (with optional IDLE)
        if args.len() == 3 {
            // Summary form
            let count = grp.pending.len() as i64;

            if grp.pending.is_empty() {
                return RespValue::Array(Some(vec![
                    RespValue::integer(0),
                    RespValue::null_bulk(),
                    RespValue::null_bulk(),
                    RespValue::Array(Some(vec![])),
                ]));
            }

            let min_id = grp.pending.iter().map(|p| p.id).min().unwrap();
            let max_id = grp.pending.iter().map(|p| p.id).max().unwrap();

            // Build per-consumer count
            let mut consumer_counts: HashMap<&str, i64> = HashMap::new();
            for p in &grp.pending {
                *consumer_counts.entry(p.consumer.as_str()).or_insert(0) += 1;
            }

            let consumer_list: Vec<RespValue> = consumer_counts
                .iter()
                .map(|(c, cnt)| {
                    RespValue::Array(Some(vec![
                        RespValue::bulk_str(c),
                        RespValue::bulk_str(&cnt.to_string()),
                    ]))
                })
                .collect();

            RespValue::Array(Some(vec![
                RespValue::integer(count),
                RespValue::bulk_str(&id_to_string(min_id)),
                RespValue::bulk_str(&id_to_string(max_id)),
                RespValue::Array(Some(consumer_list)),
            ]))
        } else {
            // Range form: XPENDING key group [IDLE ms] start end count [consumer]
            let mut i = 3usize;
            let mut _idle_filter: Option<u64> = None;

            if i < args.len() {
                if let Some(s) = args[i].as_str() {
                    if s.to_uppercase() == "IDLE" {
                        i += 1;
                        if i < args.len() {
                            _idle_filter = args[i].as_str().and_then(|s| s.parse().ok());
                            i += 1;
                        }
                    }
                }
            }

            if i + 2 >= args.len() {
                return RespValue::error("ERR syntax error");
            }

            let start_str = match args[i].as_str() {
                Some(s) => s.to_string(),
                None => return RespValue::error("ERR invalid start"),
            };
            i += 1;
            let end_str = match args[i].as_str() {
                Some(s) => s.to_string(),
                None => return RespValue::error("ERR invalid end"),
            };
            i += 1;
            let range_count: usize = match args[i].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR value is not an integer or out of range"),
            };
            i += 1;

            let consumer_filter: Option<String> = if i < args.len() {
                args[i].as_str().map(|s| s.to_string())
            } else {
                None
            };

            let (start_id, start_excl) = match parse_id_bound(&start_str) {
                Some(v) => v,
                None => return RespValue::error("ERR invalid start ID"),
            };
            let (end_id, end_excl) = match parse_id_bound(&end_str) {
                Some(v) => v,
                None => return RespValue::error("ERR invalid end ID"),
            };

            let now_ms = current_ms();
            let results: Vec<RespValue> = grp
                .pending
                .iter()
                .filter(|p| {
                    let after_start = if start_excl {
                        p.id > start_id
                    } else {
                        p.id >= start_id
                    };
                    let before_end = if end_excl {
                        p.id < end_id
                    } else {
                        p.id <= end_id
                    };
                    let consumer_match =
                        consumer_filter.as_ref().map_or(true, |c| &p.consumer == c);
                    after_start && before_end && consumer_match
                })
                .take(range_count)
                .map(|p| {
                    let idle = now_ms.saturating_sub(p.delivery_time);
                    RespValue::Array(Some(vec![
                        RespValue::bulk_str(&id_to_string(p.id)),
                        RespValue::bulk_str(&p.consumer),
                        RespValue::integer(idle as i64),
                        RespValue::integer(p.delivery_count as i64),
                    ]))
                })
                .collect();

            RespValue::Array(Some(results))
        }
    }
}

// ── XINFO ──────────────────────────────────────────────────────────────────────

pub struct XInfoCommand;

impl CommandHandler for XInfoCommand {
    fn name(&self) -> &str {
        "XINFO"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'xinfo' command");
        }
        let sub = match args[1].as_str() {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };

        match sub.as_str() {
            "STREAM" => {
                // XINFO STREAM key [FULL [COUNT count]]
                if args.len() < 3 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xinfo|stream' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let db = *db_index;
                let store = STREAMS.lock();
                let stream = match store.get(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::error("ERR no such key"),
                };
                let length = stream.entries.len() as i64;
                let last_id = id_to_string(stream.last_id);
                let groups_count = stream.groups.len() as i64;
                let first_entry_id = if stream.entries.is_empty() {
                    "0-0".to_string()
                } else {
                    id_to_string(stream.entries[0].id)
                };
                let first_entry = stream
                    .entries
                    .first()
                    .map(format_entry)
                    .unwrap_or(RespValue::null_bulk());
                let last_entry = stream
                    .entries
                    .last()
                    .map(format_entry)
                    .unwrap_or(RespValue::null_bulk());
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("length"),
                    RespValue::integer(length),
                    RespValue::bulk_str("radix-tree-keys"),
                    RespValue::integer(1),
                    RespValue::bulk_str("radix-tree-nodes"),
                    RespValue::integer(2),
                    RespValue::bulk_str("last-generated-id"),
                    RespValue::bulk_str(&last_id),
                    RespValue::bulk_str("max-deleted-entry-id"),
                    RespValue::bulk_str("0-0"),
                    RespValue::bulk_str("entries-added"),
                    RespValue::integer(length),
                    RespValue::bulk_str("recorded-first-entry-id"),
                    RespValue::bulk_str(&first_entry_id),
                    RespValue::bulk_str("groups"),
                    RespValue::integer(groups_count),
                    RespValue::bulk_str("first-entry"),
                    first_entry,
                    RespValue::bulk_str("last-entry"),
                    last_entry,
                ]))
            }
            "GROUPS" => {
                // XINFO GROUPS key
                if args.len() < 3 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xinfo|groups' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let db = *db_index;
                let store = STREAMS.lock();
                let stream = match store.get(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::error("ERR no such key"),
                };
                let groups: Vec<RespValue> = stream
                    .groups
                    .iter()
                    .map(|(name, grp)| {
                        let pending = grp.pending.len() as i64;
                        let consumers = grp.consumers.len() as i64;
                        let last_id = id_to_string(grp.last_id);
                        // lag = entries in stream after last-delivered-id
                        let lag =
                            stream.entries.iter().filter(|e| e.id > grp.last_id).count() as i64;
                        RespValue::Array(Some(vec![
                            RespValue::bulk_str("name"),
                            RespValue::bulk_str(name),
                            RespValue::bulk_str("consumers"),
                            RespValue::integer(consumers),
                            RespValue::bulk_str("pending"),
                            RespValue::integer(pending),
                            RespValue::bulk_str("last-delivered-id"),
                            RespValue::bulk_str(&last_id),
                            RespValue::bulk_str("entries-read"),
                            RespValue::integer(
                                stream
                                    .entries
                                    .iter()
                                    .filter(|e| e.id <= grp.last_id)
                                    .count() as i64,
                            ),
                            RespValue::bulk_str("lag"),
                            RespValue::integer(lag),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(groups))
            }
            "CONSUMERS" => {
                // XINFO CONSUMERS key group
                if args.len() < 4 {
                    return RespValue::error(
                        "ERR wrong number of arguments for 'xinfo|consumers' command",
                    );
                }
                let key = match args[2].as_bytes() {
                    Some(k) => k.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                let group = match args[3].as_str() {
                    Some(g) => g.to_string(),
                    None => return RespValue::error("ERR invalid group"),
                };
                let db = *db_index;
                let now_ms = current_ms();
                let store = STREAMS.lock();
                let stream = match store.get(&(db, key)) {
                    Some(s) => s,
                    None => return RespValue::error("ERR no such key"),
                };
                let grp = match stream.groups.get(&group) {
                    Some(g) => g,
                    None => return RespValue::error("ERR no such consumer group"),
                };
                let consumers: Vec<RespValue> = grp
                    .consumers
                    .iter()
                    .map(|(name, &seen)| {
                        let pending =
                            grp.pending.iter().filter(|p| &p.consumer == name).count() as i64;
                        let idle = now_ms.saturating_sub(seen) as i64;
                        RespValue::Array(Some(vec![
                            RespValue::bulk_str("name"),
                            RespValue::bulk_str(name),
                            RespValue::bulk_str("pending"),
                            RespValue::integer(pending),
                            RespValue::bulk_str("idle"),
                            RespValue::integer(idle),
                            RespValue::bulk_str("inactive"),
                            RespValue::integer(idle),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(consumers))
            }
            "HELP" => RespValue::Array(Some(vec![
                RespValue::bulk_str("XINFO <subcommand> [<arg> ...]. Subcommands are:"),
                RespValue::bulk_str("CONSUMERS <key> <groupname> -- Show stream consumers."),
                RespValue::bulk_str("GROUPS <key> -- Show stream consumer groups."),
                RespValue::bulk_str("STREAM <key> [FULL [COUNT <n>]] -- Show stream info."),
            ])),
            _ => RespValue::error(&format!("ERR unknown subcommand '{}' for 'xinfo'", sub)),
        }
    }
}

// ── XAUTOCLAIM ─────────────────────────────────────────────────────────────────

pub struct XAutoClaimCommand;

impl CommandHandler for XAutoClaimCommand {
    fn name(&self) -> &str {
        "XAUTOCLAIM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
        if args.len() < 6 {
            return RespValue::error("ERR wrong number of arguments for 'xautoclaim' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let group = match args[2].as_str() {
            Some(g) => g.to_string(),
            None => return RespValue::error("ERR invalid group"),
        };
        let new_consumer = match args[3].as_str() {
            Some(c) => c.to_string(),
            None => return RespValue::error("ERR invalid consumer"),
        };
        let min_idle_ms: u64 = match args[4].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let start_str = match args[5].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid start"),
        };

        let mut count: usize = 100; // default per Redis
        let mut justid = false;
        let mut i = 6usize;
        while i < args.len() {
            let upper = match args[i].as_str() {
                Some(s) => s.to_uppercase(),
                None => {
                    i += 1;
                    continue;
                }
            };
            match upper.as_str() {
                "COUNT" => {
                    i += 1;
                    if i < args.len() {
                        if let Some(n) = args[i].as_str().and_then(|s| s.parse().ok()) {
                            count = n;
                        }
                        i += 1;
                    }
                }
                "JUSTID" => {
                    justid = true;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }

        let (start_id, _) = match parse_id_bound(&start_str) {
            Some(v) => v,
            None => return RespValue::error("ERR invalid start ID"),
        };

        let db = *db_index;
        let now_ms = current_ms();
        let mut store = STREAMS.lock();

        let stream = match store.get_mut(&(db, key)) {
            Some(s) => s,
            None => {
                return RespValue::Array(Some(vec![
                    RespValue::bulk_str("0-0"),
                    RespValue::Array(Some(vec![])),
                    RespValue::Array(Some(vec![])),
                ]))
            }
        };

        let grp = match stream.groups.get_mut(&group) {
            Some(g) => g,
            None => return RespValue::error("ERR no such consumer group"),
        };

        // Ensure new consumer is tracked
        grp.consumers.entry(new_consumer.clone()).or_insert(now_ms);

        // Find eligible PEL entries: id >= start_id, idle >= min_idle_ms
        let mut claimed_ids: Vec<(u64, u64)> = Vec::new();
        let mut next_start: Option<(u64, u64)> = None;

        for pel in grp.pending.iter_mut() {
            if pel.id < start_id {
                continue;
            }
            if claimed_ids.len() >= count {
                next_start = Some(pel.id);
                break;
            }
            let idle = now_ms.saturating_sub(pel.delivery_time);
            if idle >= min_idle_ms {
                pel.consumer = new_consumer.clone();
                pel.delivery_time = now_ms;
                pel.delivery_count += 1;
                claimed_ids.push(pel.id);
            }
        }

        let cursor = next_start
            .map(|id| id_to_string(id))
            .unwrap_or_else(|| "0-0".to_string());

        if justid {
            let ids: Vec<RespValue> = claimed_ids
                .iter()
                .map(|id| RespValue::bulk_str(&id_to_string(*id)))
                .collect();
            return RespValue::Array(Some(vec![
                RespValue::bulk_str(&cursor),
                RespValue::Array(Some(ids)),
                RespValue::Array(Some(vec![])),
            ]));
        }

        // Return full entries for claimed IDs
        let entries: Vec<RespValue> = stream
            .entries
            .iter()
            .filter(|e| claimed_ids.contains(&e.id))
            .map(format_entry)
            .collect();

        RespValue::Array(Some(vec![
            RespValue::bulk_str(&cursor),
            RespValue::Array(Some(entries)),
            RespValue::Array(Some(vec![])),
        ]))
    }
}

// ── XSETID ───────────────────────────────────────────────────────────────────

pub struct XSetIdCommand;

impl CommandHandler for XSetIdCommand {
    fn name(&self) -> &str {
        "XSETID"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'xsetid' command");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => (db, b.to_vec()),
            None => return RespValue::error("ERR"),
        };
        let id_str = match args[2].as_str() {
            Some(s) => s.to_owned(),
            None => return RespValue::error("ERR"),
        };
        let new_id = match parse_id_bound(&id_str) {
            Some((id, _)) => id,
            None => {
                return RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                )
            }
        };

        let mut store = STREAMS.lock();
        let stream = store.entry(key).or_insert_with(|| Stream {
            entries: Vec::new(),
            groups: HashMap::new(),
            last_id: (0, 0),
        });
        if new_id >= stream.last_id {
            stream.last_id = new_id;
        }
        RespValue::ok()
    }
}

// ── Flush support ─────────────────────────────────────────────────────────────

/// Clear all stream entries for a specific db index.
pub fn flush_db(db_index: usize) {
    let mut store = STREAMS.lock();
    store.retain(|(db, _), _| *db != db_index);
    ext_type_registry::flush_db(db_index);
}

/// Clear all stream entries across all databases.
pub fn flush_all() {
    let mut store = STREAMS.lock();
    store.clear();
    // Note: ext_type_registry is cleared by the caller (FlushAllCommand)
}
