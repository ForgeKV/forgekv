use std::sync::Arc;

use crate::database::RedisDatabase;
use crate::resp::RespValue;

use super::CommandHandler;

fn wrong_type() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn map_err(e: crate::database::RedisError) -> RespValue {
    match e {
        crate::database::RedisError::WrongType => wrong_type(),
        other => RespValue::error(&other.to_string()),
    }
}

pub struct HSetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HSetCommand {
    fn name(&self) -> &str {
        "HSET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 || (args.len() - 2) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'hset' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() {
            let field = match args[i].as_bytes() {
                Some(f) => f,
                None => return RespValue::error("ERR invalid field"),
            };
            let value = match args[i + 1].as_bytes() {
                Some(v) => v,
                None => return RespValue::error("ERR invalid value"),
            };
            pairs.push((field, value));
            i += 2;
        }

        match self.db.hset(*db_index, key, &pairs) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct HGetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HGetCommand {
    fn name(&self) -> &str {
        "HGET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'hget' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let field = match args[2].as_bytes() {
            Some(f) => f,
            None => return RespValue::error("ERR invalid field"),
        };
        match self.db.hget(*db_index, key, field) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct HDelCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HDelCommand {
    fn name(&self) -> &str {
        "HDEL"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'hdel' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let fields: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.hdel(*db_index, key, &fields) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct HGetAllCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HGetAllCommand {
    fn name(&self) -> &str {
        "HGETALL"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'hgetall' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.hgetall(*db_index, key) {
            Ok(pairs) => {
                let mut items = Vec::with_capacity(pairs.len() * 2);
                for (field, value) in pairs {
                    items.push(RespValue::bulk_bytes(field));
                    items.push(RespValue::bulk_bytes(value));
                }
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct HMSetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HMSetCommand {
    fn name(&self) -> &str {
        "HMSET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 || (args.len() - 2) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'hmset' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() {
            let field = match args[i].as_bytes() {
                Some(f) => f,
                None => return RespValue::error("ERR invalid field"),
            };
            let value = match args[i + 1].as_bytes() {
                Some(v) => v,
                None => return RespValue::error("ERR invalid value"),
            };
            pairs.push((field, value));
            i += 2;
        }

        match self.db.hset(*db_index, key, &pairs) {
            Ok(_) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}

pub struct HMGetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HMGetCommand {
    fn name(&self) -> &str {
        "HMGET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'hmget' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut results = Vec::new();
        for arg in &args[2..] {
            let field = match arg.as_bytes() {
                Some(f) => f,
                None => {
                    results.push(RespValue::null_bulk());
                    continue;
                }
            };
            match self.db.hget(*db_index, key, field) {
                Ok(Some(v)) => results.push(RespValue::bulk_bytes(v)),
                Ok(None) => results.push(RespValue::null_bulk()),
                Err(_) => results.push(RespValue::null_bulk()),
            }
        }

        RespValue::Array(Some(results))
    }
}

pub struct HExistsCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HExistsCommand {
    fn name(&self) -> &str {
        "HEXISTS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'hexists' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let field = match args[2].as_bytes() {
            Some(f) => f,
            None => return RespValue::error("ERR invalid field"),
        };
        match self.db.hexists(*db_index, key, field) {
            Ok(true) => RespValue::integer(1),
            Ok(false) => RespValue::integer(0),
            Err(e) => map_err(e),
        }
    }
}

pub struct HLenCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HLenCommand {
    fn name(&self) -> &str {
        "HLEN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'hlen' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.hlen(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct HKeysCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HKeysCommand {
    fn name(&self) -> &str {
        "HKEYS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'hkeys' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.hgetall(*db_index, key) {
            Ok(pairs) => {
                let items: Vec<RespValue> = pairs.into_iter().map(|(f, _)| RespValue::bulk_bytes(f)).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct HValsCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HValsCommand {
    fn name(&self) -> &str {
        "HVALS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'hvals' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.hgetall(*db_index, key) {
            Ok(pairs) => {
                let items: Vec<RespValue> = pairs.into_iter().map(|(_, v)| RespValue::bulk_bytes(v)).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct HSetNxCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HSetNxCommand {
    fn name(&self) -> &str {
        "HSETNX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'hsetnx' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let field = match args[2].as_bytes() {
            Some(f) => f,
            None => return RespValue::error("ERR invalid field"),
        };
        let value = match args[3].as_bytes() {
            Some(v) => v,
            None => return RespValue::error("ERR invalid value"),
        };
        match self.db.hsetnx(*db_index, key, field, value) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct HIncrByFloatCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HIncrByFloatCommand {
    fn name(&self) -> &str {
        "HINCRBYFLOAT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'hincrbyfloat' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let field = match args[2].as_bytes() {
            Some(f) => f,
            None => return RespValue::error("ERR invalid field"),
        };
        let delta: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not a valid float"),
        };

        let current: f64 = match self.db.hget(*db_index, key, field) {
            Ok(Some(v)) => match std::str::from_utf8(&v).ok().and_then(|s| s.trim().parse().ok()) {
                Some(f) => f,
                None => return RespValue::error("ERR hash value is not a float"),
            },
            Ok(None) => 0.0,
            Err(e) => return map_err(e),
        };

        let new_val = current + delta;
        let new_str = format_hfloat(new_val);
        match self.db.hset(*db_index, key, &[(field, new_str.as_bytes())]) {
            Ok(_) => RespValue::bulk_str(&new_str),
            Err(e) => map_err(e),
        }
    }
}

fn format_hfloat(f: f64) -> String {
    if f == f.floor() && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{}", f)
    }
}

pub struct HStrLenCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HStrLenCommand {
    fn name(&self) -> &str {
        "HSTRLEN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'hstrlen' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let field = match args[2].as_bytes() {
            Some(f) => f,
            None => return RespValue::error("ERR invalid field"),
        };
        match self.db.hget(*db_index, key, field) {
            Ok(Some(v)) => RespValue::integer(v.len() as i64),
            Ok(None) => RespValue::integer(0),
            Err(e) => map_err(e),
        }
    }
}

pub struct HRandFieldCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HRandFieldCommand {
    fn name(&self) -> &str {
        "HRANDFIELD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'hrandfield' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let pairs = match self.db.hgetall(*db_index, key) {
            Ok(p) => p,
            Err(e) => return map_err(e),
        };

        if args.len() == 2 {
            // Return single random field
            if pairs.is_empty() {
                return RespValue::null_bulk();
            }
            return RespValue::bulk_bytes(pairs.into_iter().next().unwrap().0);
        }

        let count: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let withvalues = args.get(3)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase() == "WITHVALUES")
            .unwrap_or(false);

        if pairs.is_empty() {
            return RespValue::Array(Some(vec![]));
        }

        let mut result = Vec::new();
        if count >= 0 {
            // Unique fields up to count
            let take = (count as usize).min(pairs.len());
            for (field, val) in pairs.iter().take(take) {
                result.push(RespValue::bulk_bytes(field.clone()));
                if withvalues {
                    result.push(RespValue::bulk_bytes(val.clone()));
                }
            }
        } else {
            // Allow duplicates, |count| items
            let abs_count = (-count) as usize;
            let len = pairs.len();
            for i in 0..abs_count {
                let idx = i % len;
                result.push(RespValue::bulk_bytes(pairs[idx].0.clone()));
                if withvalues {
                    result.push(RespValue::bulk_bytes(pairs[idx].1.clone()));
                }
            }
        }

        RespValue::Array(Some(result))
    }
}

pub struct HScanCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HScanCommand {
    fn name(&self) -> &str {
        "HSCAN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'hscan' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let _cursor: u64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let mut pattern: Option<String> = None;
        let mut i = 3;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("MATCH") => {
                    i += 1;
                    if i < args.len() {
                        pattern = args[i].as_str().map(|s| s.to_string());
                    }
                }
                Some("COUNT") => { i += 1; } // ignore count
                _ => {}
            }
            i += 1;
        }

        let pairs = match self.db.hgetall(*db_index, key) {
            Ok(p) => p,
            Err(e) => return map_err(e),
        };

        let mut items = Vec::new();
        for (field, value) in pairs {
            let field_str = String::from_utf8_lossy(&field).into_owned();
            if let Some(ref pat) = pattern {
                if !crate::database::RedisDatabase::glob_match(pat, &field_str) {
                    continue;
                }
            }
            items.push(RespValue::bulk_bytes(field));
            items.push(RespValue::bulk_bytes(value));
        }

        RespValue::Array(Some(vec![
            RespValue::bulk_str("0"),
            RespValue::Array(Some(items)),
        ]))
    }
}

pub struct HIncrByCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for HIncrByCommand {
    fn name(&self) -> &str {
        "HINCRBY"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'hincrby' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let field = match args[2].as_bytes() {
            Some(f) => f,
            None => return RespValue::error("ERR invalid field"),
        };
        let delta: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let current: i64 = match self.db.hget(*db_index, key, field) {
            Ok(Some(v)) => match std::str::from_utf8(&v).ok().and_then(|s| s.trim().parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR hash value is not an integer"),
            },
            Ok(None) => 0,
            Err(e) => return map_err(e),
        };

        let new_val = match current.checked_add(delta) {
            Some(n) => n,
            None => return RespValue::error("ERR increment or decrement would overflow"),
        };

        let new_str = new_val.to_string();
        match self.db.hset(*db_index, key, &[(field, new_str.as_bytes())]) {
            Ok(_) => RespValue::integer(new_val),
            Err(e) => map_err(e),
        }
    }
}

// ── Hash field TTL commands (Redis 7.4+ / Dragonfly) ─────────────────────────

use parking_lot::Mutex as ParkingMutex;

fn now_ms_hash() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

lazy_static::lazy_static! {
    static ref HFIELD_TTL: ParkingMutex<std::collections::HashMap<(usize, Vec<u8>, Vec<u8>), i64>> =
        ParkingMutex::new(std::collections::HashMap::new());
}

fn hfield_expiry(db: usize, key: &[u8], field: &[u8]) -> i64 {
    *HFIELD_TTL.lock().get(&(db, key.to_vec(), field.to_vec())).unwrap_or(&0)
}

fn hfield_set_expiry(db: usize, key: &[u8], field: &[u8], expiry_ms: i64) {
    HFIELD_TTL.lock().insert((db, key.to_vec(), field.to_vec()), expiry_ms);
}

fn hfield_clear_expiry(db: usize, key: &[u8], field: &[u8]) {
    HFIELD_TTL.lock().remove(&(db, key.to_vec(), field.to_vec()));
}

pub fn hfield_ttl_flush_all() { HFIELD_TTL.lock().clear(); }
pub fn hfield_ttl_flush_db(db_index: usize) {
    HFIELD_TTL.lock().retain(|(db, _, _), _| *db != db_index);
}

fn parse_fields_hexp(args: &[RespValue], pos: usize) -> Option<Vec<Vec<u8>>> {
    if pos + 1 >= args.len() { return None; }
    if args[pos].as_str().map(|s| s.to_uppercase()).as_deref() != Some("FIELDS") { return None; }
    let numfields: usize = args[pos+1].as_str()?.parse().ok()?;
    if pos + 2 + numfields > args.len() { return None; }
    let fields: Vec<Vec<u8>> = args[pos+2..pos+2+numfields].iter()
        .filter_map(|a| a.as_bytes().map(|b| b.to_vec())).collect();
    if fields.len() == numfields { Some(fields) } else { None }
}

pub struct HExpireCommand { pub db: Arc<RedisDatabase>, pub name: &'static str, pub in_ms: bool, pub is_at: bool }
impl CommandHandler for HExpireCommand {
    fn name(&self) -> &str { self.name }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 5 { return RespValue::error("ERR wrong number of arguments"); }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR") };
        let time_val: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let expiry_ms = if self.is_at {
            if self.in_ms { time_val } else { time_val * 1000 }
        } else {
            let delta = if self.in_ms { time_val } else { time_val * 1000 };
            now_ms_hash() + delta
        };
        let fields = match parse_fields_hexp(args, 3) {
            Some(f) => f,
            None => return RespValue::error("ERR syntax error"),
        };
        let db = *db_index;
        let results: Vec<RespValue> = fields.iter().map(|field| {
            match self.db.hget(db, &key, field) {
                Ok(Some(_)) => { hfield_set_expiry(db, &key, field, expiry_ms); RespValue::integer(1) }
                Ok(None) => RespValue::integer(2),
                Err(_) => RespValue::integer(-1),
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

pub struct HTtlCommand { pub db: Arc<RedisDatabase>, pub name: &'static str, pub in_ms: bool }
impl CommandHandler for HTtlCommand {
    fn name(&self) -> &str { self.name }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 { return RespValue::error("ERR wrong number of arguments"); }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR") };
        let fields = match parse_fields_hexp(args, 2) {
            Some(f) => f,
            None => return RespValue::error("ERR syntax error"),
        };
        let db = *db_index;
        let now = now_ms_hash();
        let results: Vec<RespValue> = fields.iter().map(|field| {
            match self.db.hget(db, &key, field) {
                Ok(Some(_)) => {
                    let exp = hfield_expiry(db, &key, field);
                    if exp == 0 { return RespValue::integer(-1); }
                    let remaining_ms = exp - now;
                    if remaining_ms <= 0 { hfield_clear_expiry(db, &key, field); RespValue::integer(-2) }
                    else if self.in_ms { RespValue::integer(remaining_ms) }
                    else { RespValue::integer((remaining_ms + 999) / 1000) }
                }
                Ok(None) => RespValue::integer(-2),
                Err(_) => RespValue::integer(-1),
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

pub struct HPersistCommand { pub db: Arc<RedisDatabase> }
impl CommandHandler for HPersistCommand {
    fn name(&self) -> &str { "HPERSIST" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 { return RespValue::error("ERR wrong number of arguments"); }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR") };
        let fields = match parse_fields_hexp(args, 2) {
            Some(f) => f,
            None => return RespValue::error("ERR syntax error"),
        };
        let db = *db_index;
        let results: Vec<RespValue> = fields.iter().map(|field| {
            match self.db.hget(db, &key, field) {
                Ok(Some(_)) => { hfield_clear_expiry(db, &key, field); RespValue::integer(1) }
                Ok(None) => RespValue::integer(-2),
                Err(_) => RespValue::integer(-1),
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}

pub struct HExpireTimeCommand { pub db: Arc<RedisDatabase>, pub name: &'static str, pub in_ms: bool }
impl CommandHandler for HExpireTimeCommand {
    fn name(&self) -> &str { self.name }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 { return RespValue::error("ERR wrong number of arguments"); }
        let key = match args[1].as_bytes() { Some(b) => b.to_vec(), None => return RespValue::error("ERR") };
        let fields = match parse_fields_hexp(args, 2) {
            Some(f) => f,
            None => return RespValue::error("ERR syntax error"),
        };
        let db = *db_index;
        let results: Vec<RespValue> = fields.iter().map(|field| {
            match self.db.hget(db, &key, field) {
                Ok(Some(_)) => {
                    let exp = hfield_expiry(db, &key, field);
                    if exp == 0 { RespValue::integer(-1) }
                    else if self.in_ms { RespValue::integer(exp) }
                    else { RespValue::integer(exp / 1000) }
                }
                Ok(None) => RespValue::integer(-2),
                Err(_) => RespValue::integer(-1),
            }
        }).collect();
        RespValue::Array(Some(results))
    }
}
