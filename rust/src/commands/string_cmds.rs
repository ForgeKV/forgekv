use std::sync::Arc;

use crate::database::{RedisDatabase, RedisError};
use crate::resp::RespValue;

use super::CommandHandler;

fn wrong_type() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn not_integer() -> RespValue {
    RespValue::error("ERR value is not an integer or out of range")
}

fn overflow_error() -> RespValue {
    RespValue::error("ERR increment or decrement would overflow")
}

fn map_err(e: RedisError) -> RespValue {
    match e {
        RedisError::WrongType => wrong_type(),
        RedisError::NotInteger => not_integer(),
        RedisError::Overflow => overflow_error(),
        RedisError::NxFail | RedisError::XxFail => RespValue::null_bulk(),
        RedisError::Other(s) => RespValue::error(&format!("ERR {}", s)),
    }
}

// SET command
pub struct SetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SetCommand {
    fn name(&self) -> &str {
        "SET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'set' command");
        }

        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let value = match args[2].as_bytes() {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };

        let mut expiry_ms: i64 = 0;
        let mut nx = false;
        let mut xx = false;
        let mut i = 3;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("EX") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let secs: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if secs <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = now_ms() + secs * 1000;
                }
                Some("PX") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let ms: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if ms <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = now_ms() + ms;
                }
                Some("EXAT") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let secs: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if secs <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = secs * 1000;
                }
                Some("PXAT") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let ms: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if ms <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = ms;
                }
                Some("NX") => nx = true,
                Some("XX") => xx = true,
                _ => return RespValue::error("ERR syntax error"),
            }
            i += 1;
        }

        match self.db.string_set(*db_index, &key, &value, expiry_ms, nx, xx) {
            Ok(()) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// GET command
pub struct GetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GetCommand {
    fn name(&self) -> &str {
        "GET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'get' command");
        }

        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

// INCR command
pub struct IncrCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for IncrCommand {
    fn name(&self) -> &str {
        "INCR"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'incr' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.string_incr(*db_index, key, 1) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// DECR command
pub struct DecrCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for DecrCommand {
    fn name(&self) -> &str {
        "DECR"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'decr' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.string_incr(*db_index, key, -1) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// INCRBY command
pub struct IncrByCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for IncrByCommand {
    fn name(&self) -> &str {
        "INCRBY"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'incrby' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let delta: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        match self.db.string_incr(*db_index, key, delta) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// DECRBY command
pub struct DecrByCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for DecrByCommand {
    fn name(&self) -> &str {
        "DECRBY"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'decrby' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let delta: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        match self.db.string_incr(*db_index, key, -delta) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// APPEND command
pub struct AppendCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for AppendCommand {
    fn name(&self) -> &str {
        "APPEND"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'append' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let to_append = match args[2].as_bytes() {
            Some(v) => v,
            None => return RespValue::error("ERR invalid value"),
        };
        match self.db.string_append(*db_index, key, to_append) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// MGET command
pub struct MGetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for MGetCommand {
    fn name(&self) -> &str {
        "MGET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'mget' command");
        }

        let mut results = Vec::new();
        for arg in &args[1..] {
            let key = match arg.as_bytes() {
                Some(k) => k,
                None => {
                    results.push(RespValue::null_bulk());
                    continue;
                }
            };
            match self.db.string_get(*db_index, key) {
                Ok(Some(v)) => results.push(RespValue::bulk_bytes(v)),
                Ok(None) => results.push(RespValue::null_bulk()),
                Err(_) => results.push(RespValue::null_bulk()),
            }
        }

        RespValue::Array(Some(results))
    }
}

// MSET command
pub struct MSetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for MSetCommand {
    fn name(&self) -> &str {
        "MSET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'mset' command");
        }

        // Collect all pairs first (validate), then batch-write with ONE lock acquisition
        let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity((args.len() - 1) / 2);
        let mut i = 1;
        while i < args.len() {
            let key = match args[i].as_bytes() {
                Some(k) => k,
                None => return RespValue::error("ERR invalid key"),
            };
            let value = match args[i + 1].as_bytes() {
                Some(v) => v,
                None => return RespValue::error("ERR invalid value"),
            };
            pairs.push((key, value));
            i += 2;
        }

        match self.db.string_set_batch(*db_index, &pairs) {
            Ok(()) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}

// GETSET command
pub struct GetSetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GetSetCommand {
    fn name(&self) -> &str {
        "GETSET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'getset' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let value = match args[2].as_bytes() {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };
        let old = match self.db.string_get(*db_index, &key) {
            Ok(v) => v,
            Err(e) => return map_err(e),
        };
        if let Err(e) = self.db.string_set(*db_index, &key, &value, 0, false, false) {
            return map_err(e);
        }
        match old {
            Some(v) => RespValue::bulk_bytes(v),
            None => RespValue::null_bulk(),
        }
    }
}

// SETNX command
pub struct SetNxCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SetNxCommand {
    fn name(&self) -> &str {
        "SETNX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'setnx' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let value = match args[2].as_bytes() {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };
        match self.db.string_set(*db_index, &key, &value, 0, true, false) {
            Ok(()) => RespValue::integer(1),
            Err(RedisError::NxFail) => RespValue::integer(0),
            Err(e) => map_err(e),
        }
    }
}

// SETEX command
pub struct SetExCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SetExCommand {
    fn name(&self) -> &str {
        "SETEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'setex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let secs: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        if secs <= 0 {
            return RespValue::error("ERR invalid expire time in 'setex' command");
        }
        let value = match args[3].as_bytes() {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };
        let expiry_ms = now_ms() + secs * 1000;
        match self.db.string_set(*db_index, &key, &value, expiry_ms, false, false) {
            Ok(()) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}

// PSETEX command
pub struct PSetExCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PSetExCommand {
    fn name(&self) -> &str {
        "PSETEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'psetex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let ms: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        if ms <= 0 {
            return RespValue::error("ERR invalid expire time in 'psetex' command");
        }
        let value = match args[3].as_bytes() {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };
        let expiry_ms = now_ms() + ms;
        match self.db.string_set(*db_index, &key, &value, expiry_ms, false, false) {
            Ok(()) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}

// INCRBYFLOAT command
pub struct IncrByFloatCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for IncrByFloatCommand {
    fn name(&self) -> &str {
        "INCRBYFLOAT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'incrbyfloat' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let delta: f64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not a valid float"),
        };

        let current: f64 = match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => match std::str::from_utf8(&v).ok().and_then(|s| s.parse().ok()) {
                Some(f) => f,
                None => return RespValue::error("ERR value is not a valid float"),
            },
            Ok(None) => 0.0,
            Err(e) => return map_err(e),
        };

        let new_val = current + delta;
        let new_str = format_float(new_val);
        if let Err(e) = self.db.string_set(*db_index, key, new_str.as_bytes(), 0, false, false) {
            return map_err(e);
        }
        RespValue::bulk_str(&new_str)
    }
}

fn format_float(f: f64) -> String {
    // Format without trailing zeros like Redis
    if f == f.floor() && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{}", f)
    }
}

// STRLEN command
pub struct StrLenCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for StrLenCommand {
    fn name(&self) -> &str {
        "STRLEN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'strlen' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => RespValue::integer(v.len() as i64),
            Ok(None) => RespValue::integer(0),
            Err(e) => map_err(e),
        }
    }
}

// GETRANGE / SUBSTR command
pub struct GetRangeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GetRangeCommand {
    fn name(&self) -> &str {
        "GETRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'getrange' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let start: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        let end: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };

        let val = match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => v,
            Ok(None) => return RespValue::bulk_str(""),
            Err(e) => return map_err(e),
        };

        let len = val.len() as i64;
        let norm_start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let norm_end = if end < 0 { (len + end).max(-1) } else { end.min(len - 1) };

        if norm_start > norm_end || norm_start >= len {
            return RespValue::bulk_str("");
        }

        let slice = &val[norm_start as usize..=norm_end as usize];
        RespValue::bulk_bytes(slice.to_vec())
    }
}

// SUBSTR command (alias for GETRANGE)
pub struct SubStrCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SubStrCommand {
    fn name(&self) -> &str {
        "SUBSTR"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // Same as GETRANGE
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'substr' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let start: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        let end: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };

        let val = match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => v,
            Ok(None) => return RespValue::bulk_str(""),
            Err(e) => return map_err(e),
        };

        let len = val.len() as i64;
        let norm_start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let norm_end = if end < 0 { (len + end).max(-1) } else { end.min(len - 1) };

        if norm_start > norm_end || norm_start >= len {
            return RespValue::bulk_str("");
        }

        let slice = &val[norm_start as usize..=norm_end as usize];
        RespValue::bulk_bytes(slice.to_vec())
    }
}

// SETRANGE command
pub struct SetRangeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SetRangeCommand {
    fn name(&self) -> &str {
        "SETRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'setrange' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let offset: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        if offset < 0 {
            return RespValue::error("ERR bit offset is not an integer or out of range");
        }
        let value = match args[3].as_bytes() {
            Some(v) => v,
            None => return RespValue::error("ERR invalid value"),
        };

        match self.db.string_setrange(*db_index, &key, offset as usize, value) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// GETDEL command
pub struct GetDelCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GetDelCommand {
    fn name(&self) -> &str {
        "GETDEL"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'getdel' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.string_getdel(*db_index, &key) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

// GETEX command
pub struct GetExCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GetExCommand {
    fn name(&self) -> &str {
        "GETEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'getex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };

        let mut new_expiry: Option<i64> = None; // None = don't change
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("EX") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let secs: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return not_integer(),
                    };
                    new_expiry = Some(now_ms() + secs * 1000);
                }
                Some("PX") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let ms: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return not_integer(),
                    };
                    new_expiry = Some(now_ms() + ms);
                }
                Some("EXAT") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let secs: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return not_integer(),
                    };
                    new_expiry = Some(secs * 1000);
                }
                Some("PXAT") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let ms: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return not_integer(),
                    };
                    new_expiry = Some(ms);
                }
                Some("PERSIST") => {
                    new_expiry = Some(0); // Remove expiry
                }
                _ => return RespValue::error("ERR syntax error"),
            }
            i += 1;
        }

        match self.db.string_getex(*db_index, &key, new_expiry) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

// MSETNX command
pub struct MSetNxCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for MSetNxCommand {
    fn name(&self) -> &str {
        "MSETNX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'msetnx' command");
        }

        let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
        let mut i = 1;
        while i < args.len() {
            let key = match args[i].as_bytes() {
                Some(k) => k,
                None => return RespValue::error("ERR invalid key"),
            };
            let value = match args[i + 1].as_bytes() {
                Some(v) => v,
                None => return RespValue::error("ERR invalid value"),
            };
            pairs.push((key, value));
            i += 2;
        }

        match self.db.msetnx(*db_index, &pairs) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

// Updated SET command with GET and KEEPTTL options
pub struct SetCommandV2 {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SetCommandV2 {
    fn name(&self) -> &str {
        "SET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'set' command");
        }

        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let value = match args[2].as_bytes() {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };

        let mut expiry_ms: i64 = 0;
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut keepttl = false;
        let mut i = 3;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("EX") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let secs: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if secs <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = now_ms() + secs * 1000;
                }
                Some("PX") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let ms: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if ms <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = now_ms() + ms;
                }
                Some("EXAT") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let secs: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if secs <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = secs * 1000;
                }
                Some("PXAT") => {
                    i += 1;
                    if i >= args.len() {
                        return RespValue::error("ERR syntax error");
                    }
                    let ms: i64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    if ms <= 0 {
                        return RespValue::error("ERR invalid expire time in 'set' command");
                    }
                    expiry_ms = ms;
                }
                Some("NX") => nx = true,
                Some("XX") => xx = true,
                Some("GET") => get = true,
                Some("KEEPTTL") => keepttl = true,
                _ => return RespValue::error("ERR syntax error"),
            }
            i += 1;
        }

        // Fast path: no GET option and no KEEPTTL — use the optimized put2 path.
        if !get && !keepttl {
            return match self.db.string_set(*db_index, &key, &value, expiry_ms, nx, xx) {
                Ok(()) => RespValue::ok(),
                Err(crate::database::RedisError::NxFail) => RespValue::null_bulk(),
                Err(crate::database::RedisError::XxFail) => RespValue::null_bulk(),
                Err(e) => map_err(e),
            };
        }

        match self.db.string_set_get_old(*db_index, &key, &value, expiry_ms, nx, xx, keepttl) {
            Ok((set_ok, old_val)) => {
                if get {
                    match old_val {
                        Some(v) => RespValue::bulk_bytes(v),
                        None => RespValue::null_bulk(),
                    }
                } else if set_ok {
                    RespValue::ok()
                } else {
                    RespValue::null_bulk()
                }
            }
            Err(e) => map_err(e),
        }
    }
}

// GETBIT command
pub struct GetBitCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for GetBitCommand {
    fn name(&self) -> &str {
        "GETBIT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'getbit' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let offset: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        if offset < 0 {
            return RespValue::error("ERR bit offset is not an integer or out of range");
        }

        let val = match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => v,
            Ok(None) => return RespValue::integer(0),
            Err(e) => return map_err(e),
        };

        let byte_idx = (offset / 8) as usize;
        let bit_idx = 7 - (offset % 8) as u8;

        if byte_idx >= val.len() {
            return RespValue::integer(0);
        }

        let bit = (val[byte_idx] >> bit_idx) & 1;
        RespValue::integer(bit as i64)
    }
}

// SETBIT command
pub struct SetBitCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SetBitCommand {
    fn name(&self) -> &str {
        "SETBIT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'setbit' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let offset: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return not_integer(),
        };
        if offset < 0 {
            return RespValue::error("ERR bit offset is not an integer or out of range");
        }
        let bit_val: u8 = match args[3].as_str().and_then(|s| s.parse::<u8>().ok()) {
            Some(n) if n <= 1 => n,
            _ => return RespValue::error("ERR bit is not an integer or out of range"),
        };

        let byte_idx = (offset / 8) as usize;
        let bit_idx = 7 - (offset % 8) as u8;

        let mut val = match self.db.string_get(*db_index, &key) {
            Ok(Some(v)) => v,
            Ok(None) => vec![],
            Err(e) => return map_err(e),
        };

        if byte_idx >= val.len() {
            val.resize(byte_idx + 1, 0u8);
        }

        let old_bit = (val[byte_idx] >> bit_idx) & 1;

        if bit_val == 1 {
            val[byte_idx] |= 1 << bit_idx;
        } else {
            val[byte_idx] &= !(1 << bit_idx);
        }

        match self.db.string_set(*db_index, &key, &val, 0, false, false) {
            Ok(()) => RespValue::integer(old_bit as i64),
            Err(e) => map_err(e),
        }
    }
}

// BITCOUNT command
pub struct BitCountCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BitCountCommand {
    fn name(&self) -> &str {
        "BITCOUNT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'bitcount' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let val = match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => v,
            Ok(None) => return RespValue::integer(0),
            Err(e) => return map_err(e),
        };

        if args.len() >= 4 {
            let start: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return not_integer(),
            };
            let end: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return not_integer(),
            };
            // Check for BYTE/BIT unit (5th arg)
            let bit_mode = args.get(4)
                .and_then(|a| a.as_str())
                .map(|s| s.to_uppercase() == "BIT")
                .unwrap_or(false);

            if bit_mode {
                // BIT mode: start/end are bit indices
                let total_bits = (val.len() * 8) as i64;
                let norm_start = if start < 0 { (total_bits + start).max(0) } else { start.min(total_bits) };
                let norm_end = if end < 0 { (total_bits + end).max(-1) } else { end.min(total_bits - 1) };
                if norm_start > norm_end {
                    return RespValue::integer(0);
                }
                let mut count = 0i64;
                for bit_pos in norm_start..=norm_end {
                    let byte_idx = (bit_pos / 8) as usize;
                    let bit_idx = 7 - (bit_pos % 8) as u8;
                    if byte_idx < val.len() && (val[byte_idx] >> bit_idx) & 1 == 1 {
                        count += 1;
                    }
                }
                return RespValue::integer(count);
            } else {
                // BYTE mode (default)
                let len = val.len() as i64;
                let norm_start = if start < 0 { (len + start).max(0) } else { start.min(len) };
                let norm_end = if end < 0 { (len + end).max(-1) } else { end.min(len - 1) };
                let bytes = if norm_start > norm_end {
                    &val[0..0]
                } else {
                    &val[norm_start as usize..=norm_end as usize]
                };
                let count: i64 = bytes.iter().map(|b| b.count_ones() as i64).sum();
                return RespValue::integer(count);
            }
        }

        let count: i64 = val.iter().map(|b| b.count_ones() as i64).sum();
        RespValue::integer(count)
    }
}

// BITOP command
pub struct BitOpCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BitOpCommand {
    fn name(&self) -> &str {
        "BITOP"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'bitop' command");
        }
        let op = match args[1].as_str() {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR invalid operation"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };

        let keys: Vec<&[u8]> = args[3..].iter().filter_map(|a| a.as_bytes()).collect();

        let mut values: Vec<Vec<u8>> = Vec::new();
        for key in &keys {
            match self.db.string_get(*db_index, key) {
                Ok(Some(v)) => values.push(v),
                Ok(None) => values.push(vec![]),
                Err(e) => return map_err(e),
            }
        }

        if op == "NOT" {
            if keys.len() != 1 {
                return RespValue::error("ERR BITOP NOT must be called with a single source key");
            }
            let result: Vec<u8> = values[0].iter().map(|b| !b).collect();
            let len = result.len() as i64;
            match self.db.string_set(*db_index, &dst, &result, 0, false, false) {
                Ok(()) => return RespValue::integer(len),
                Err(e) => return map_err(e),
            }
        }

        let max_len = values.iter().map(|v| v.len()).max().unwrap_or(0);
        let mut result: Vec<u8> = vec![0u8; max_len];

        match op.as_str() {
            "AND" => {
                // Start with all 0xFF, AND with each
                result = vec![0xFFu8; max_len];
                for v in &values {
                    for (i, b) in result.iter_mut().enumerate() {
                        *b &= v.get(i).copied().unwrap_or(0);
                    }
                }
            }
            "OR" => {
                for v in &values {
                    for (i, b) in result.iter_mut().enumerate() {
                        *b |= v.get(i).copied().unwrap_or(0);
                    }
                }
            }
            "XOR" => {
                if let Some(first) = values.first() {
                    for (i, b) in result.iter_mut().enumerate() {
                        *b = first.get(i).copied().unwrap_or(0);
                    }
                }
                for v in values.iter().skip(1) {
                    for (i, b) in result.iter_mut().enumerate() {
                        *b ^= v.get(i).copied().unwrap_or(0);
                    }
                }
            }
            _ => return RespValue::error("ERR unknown BITOP operation"),
        }

        let len = result.len() as i64;
        match self.db.string_set(*db_index, &dst, &result, 0, false, false) {
            Ok(()) => RespValue::integer(len),
            Err(e) => map_err(e),
        }
    }
}

// BITPOS command
pub struct BitPosCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BitPosCommand {
    fn name(&self) -> &str {
        "BITPOS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'bitpos' command");
        }
        let key = match args[1].as_bytes() { Some(k) => k, None => return RespValue::error("ERR invalid key") };
        let bit: u8 = match args[2].as_str().and_then(|s| s.parse::<u8>().ok()) {
            Some(n) if n <= 1 => n,
            _ => return RespValue::error("ERR bit is not an integer or out of range"),
        };
        let val = match self.db.string_get(*db_index, key) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return if bit == 0 { RespValue::integer(0) } else { RespValue::integer(-1) };
            }
            Err(e) => return map_err(e),
        };

        if args.len() >= 4 {
            let start: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return not_integer(),
            };
            // Check for BIT mode (may be in args[5] or args[4] if no end given)
            let bit_mode = args.iter().skip(4).any(|a| a.as_str().map(|s| s.to_uppercase() == "BIT").unwrap_or(false));

            if bit_mode {
                let total_bits = (val.len() * 8) as i64;
                let norm_start = if start < 0 { (total_bits + start).max(0) } else { start.min(total_bits) };
                let norm_end = if args.len() >= 5 {
                    // check if args[4] is a number (not BYTE/BIT)
                    if let Some(e) = args[4].as_str().and_then(|s| s.parse::<i64>().ok()) {
                        if e < 0 { (total_bits + e).max(-1) } else { e.min(total_bits - 1) }
                    } else {
                        total_bits - 1
                    }
                } else {
                    total_bits - 1
                };
                for bit_pos in norm_start..=norm_end {
                    let byte_idx = (bit_pos / 8) as usize;
                    let bit_idx = 7 - (bit_pos % 8) as u8;
                    if byte_idx < val.len() {
                        let b = (val[byte_idx] >> bit_idx) & 1;
                        if b == bit {
                            return RespValue::integer(bit_pos);
                        }
                    }
                }
                return if bit == 0 { RespValue::integer(total_bits) } else { RespValue::integer(-1) };
            }

            // BYTE mode (default)
            let len = val.len() as i64;
            let norm_start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
            let norm_end = if args.len() >= 5 {
                if let Some(e) = args[4].as_str().and_then(|s| s.parse::<i64>().ok()) {
                    (if e < 0 { (len + e).max(-1) } else { e.min(len - 1) }) as usize
                } else {
                    (len - 1).max(0) as usize
                }
            } else {
                (len - 1).max(0) as usize
            };

            if norm_start > norm_end || norm_start >= val.len() {
                return if bit == 0 { RespValue::integer((val.len() * 8) as i64) } else { RespValue::integer(-1) };
            }
            let search_slice = &val[norm_start..=norm_end.min(val.len() - 1)];
            for (byte_idx, &byte) in search_slice.iter().enumerate() {
                for bit_idx in (0..8u8).rev() {
                    let b = (byte >> bit_idx) & 1;
                    if b == bit {
                        let global_bit_pos = (norm_start + byte_idx) * 8 + (7 - bit_idx as usize);
                        return RespValue::integer(global_bit_pos as i64);
                    }
                }
            }
            return if bit == 0 { RespValue::integer((val.len() * 8) as i64) } else { RespValue::integer(-1) };
        }

        // No range — search all
        for (byte_idx, &byte) in val.iter().enumerate() {
            for bit_idx in (0..8u8).rev() {
                let b = (byte >> bit_idx) & 1;
                if b == bit {
                    let global_bit_pos = byte_idx * 8 + (7 - bit_idx as usize);
                    return RespValue::integer(global_bit_pos as i64);
                }
            }
        }
        if bit == 0 {
            RespValue::integer((val.len() * 8) as i64)
        } else {
            RespValue::integer(-1)
        }
    }
}

// BITFIELD / BITFIELD_RO
pub struct BitFieldCommand {
    pub db: Arc<RedisDatabase>,
}

#[derive(Clone, Copy)]
enum BitfieldOverflow { Wrap, Sat, Fail }

fn bitfield_parse_type(s: &str) -> Option<(bool, u8)> {
    // returns (signed, bits)
    if s.starts_with('u') || s.starts_with('U') {
        s[1..].parse::<u8>().ok().filter(|&b| b >= 1 && b <= 63).map(|b| (false, b))
    } else if s.starts_with('i') || s.starts_with('I') {
        s[1..].parse::<u8>().ok().filter(|&b| b >= 1 && b <= 64).map(|b| (true, b))
    } else {
        None
    }
}

fn bitfield_get_bits(buf: &[u8], bit_offset: u64, bits: u8, signed: bool) -> i64 {
    let mut val: u64 = 0;
    for i in 0..bits as u64 {
        let byte_idx = ((bit_offset + i) / 8) as usize;
        let bit_idx = 7 - ((bit_offset + i) % 8);
        if byte_idx < buf.len() && (buf[byte_idx] >> bit_idx) & 1 == 1 {
            val |= 1u64 << (bits as u64 - 1 - i);
        }
    }
    if signed && bits < 64 && (val >> (bits - 1)) & 1 == 1 {
        // sign extend
        val |= !((1u64 << bits) - 1);
    }
    val as i64
}

fn bitfield_set_bits(buf: &mut Vec<u8>, bit_offset: u64, bits: u8, value: u64) {
    let needed = ((bit_offset + bits as u64 + 7) / 8) as usize;
    if buf.len() < needed { buf.resize(needed, 0); }
    for i in 0..bits as u64 {
        let byte_idx = ((bit_offset + i) / 8) as usize;
        let bit_idx = 7 - ((bit_offset + i) % 8);
        let src_bit = (value >> (bits as u64 - 1 - i)) & 1;
        if src_bit == 1 { buf[byte_idx] |= 1 << bit_idx; } else { buf[byte_idx] &= !(1 << bit_idx); }
    }
}

fn bitfield_overflow(old: i64, delta: i64, signed: bool, bits: u8, overflow: BitfieldOverflow) -> Option<i64> {
    if signed {
        let min = -(1i64 << (bits - 1));
        let max = (1i64 << (bits - 1)) - 1;
        let result = old.wrapping_add(delta);
        match overflow {
            BitfieldOverflow::Wrap => {
                // wrap within signed range
                let range = 1i64 << bits;
                let mut r = result % range;
                if r > max { r -= range; }
                if r < min { r += range; }
                Some(r)
            }
            BitfieldOverflow::Sat => Some(result.max(min).min(max)),
            BitfieldOverflow::Fail => if result < min || result > max { None } else { Some(result) },
        }
    } else {
        let max = if bits == 63 { i64::MAX as u64 } else { (1u64 << bits) - 1 };
        let old_u = old as u64;
        let (result_u, overflow_happened) = old_u.overflowing_add(delta as u64);
        match overflow {
            BitfieldOverflow::Wrap => Some((result_u & max) as i64),
            BitfieldOverflow::Sat => {
                if delta > 0 && overflow_happened { Some(max as i64) }
                else if delta < 0 && (result_u > max || overflow_happened) { Some(0) }
                else { Some((result_u & max) as i64) }
            }
            BitfieldOverflow::Fail => {
                if overflow_happened || result_u > max { None } else { Some(result_u as i64) }
            }
        }
    }
}

fn execute_bitfield(db: &Arc<RedisDatabase>, db_index: usize, args: &[RespValue], readonly: bool) -> RespValue {
    if args.len() < 2 { return RespValue::error("ERR wrong number of arguments for 'bitfield' command"); }
    let key = match args[1].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::error("ERR invalid key") };

    // Load current string value
    let mut buf: Vec<u8> = match db.string_get(db_index, &key) {
        Ok(Some(v)) => v,
        Ok(None) => vec![],
        Err(_) => vec![],
    };

    let mut results: Vec<RespValue> = Vec::new();
    let mut overflow = BitfieldOverflow::Wrap;
    let mut i = 2;

    while i < args.len() {
        let op = match args[i].as_str().map(|s| s.to_uppercase()) {
            Some(s) => s,
            None => { i += 1; continue; }
        };
        match op.as_str() {
            "OVERFLOW" => {
                i += 1;
                if i < args.len() {
                    overflow = match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                        Some("SAT") => BitfieldOverflow::Sat,
                        Some("FAIL") => BitfieldOverflow::Fail,
                        _ => BitfieldOverflow::Wrap,
                    };
                }
                i += 1;
            }
            "GET" => {
                i += 1;
                if i + 1 >= args.len() { break; }
                let type_str = match args[i].as_str() { Some(s) => s.to_string(), None => { i += 2; continue; } };
                let bit_offset: u64 = match args[i+1].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => { i += 2; continue; } };
                i += 2;
                let (signed, bits) = match bitfield_parse_type(&type_str) { Some(v) => v, None => continue };
                results.push(RespValue::integer(bitfield_get_bits(&buf, bit_offset, bits, signed)));
            }
            "SET" => {
                if readonly { i += 3; continue; }
                i += 1;
                if i + 2 >= args.len() { break; }
                let type_str = match args[i].as_str() { Some(s) => s.to_string(), None => { i += 3; continue; } };
                let bit_offset: u64 = match args[i+1].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => { i += 3; continue; } };
                let new_val: i64 = match args[i+2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => { i += 3; continue; } };
                i += 3;
                let (signed, bits) = match bitfield_parse_type(&type_str) { Some(v) => v, None => continue };
                let old_val = bitfield_get_bits(&buf, bit_offset, bits, signed);
                let mask = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
                bitfield_set_bits(&mut buf, bit_offset, bits, new_val as u64 & mask);
                results.push(RespValue::integer(old_val));
            }
            "INCRBY" => {
                if readonly { i += 3; continue; }
                i += 1;
                if i + 2 >= args.len() { break; }
                let type_str = match args[i].as_str() { Some(s) => s.to_string(), None => { i += 3; continue; } };
                let bit_offset: u64 = match args[i+1].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => { i += 3; continue; } };
                let delta: i64 = match args[i+2].as_str().and_then(|s| s.parse().ok()) { Some(n) => n, None => { i += 3; continue; } };
                i += 3;
                let (signed, bits) = match bitfield_parse_type(&type_str) { Some(v) => v, None => continue };
                let old_val = bitfield_get_bits(&buf, bit_offset, bits, signed);
                match bitfield_overflow(old_val, delta, signed, bits, overflow) {
                    Some(new_val) => {
                        let mask = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
                        bitfield_set_bits(&mut buf, bit_offset, bits, new_val as u64 & mask);
                        results.push(RespValue::integer(new_val));
                    }
                    None => results.push(RespValue::null_bulk()),
                }
            }
            _ => { i += 1; }
        }
    }

    // Save modified buffer (only for non-readonly)
    if !readonly && !buf.is_empty() {
        let _ = db.string_set(db_index, &key, &buf, 0, false, false);
    } else if !readonly {
        // Even if buf is empty after operations, we may need to clear key
    }

    RespValue::Array(Some(results))
}

impl CommandHandler for BitFieldCommand {
    fn name(&self) -> &str { "BITFIELD" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        execute_bitfield(&self.db, *db_index, args, false)
    }
}

pub struct BitFieldRoCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BitFieldRoCommand {
    fn name(&self) -> &str { "BITFIELD_RO" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        execute_bitfield(&self.db, *db_index, args, true)
    }
}

pub struct LcsCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LcsCommand {
    fn name(&self) -> &str { "LCS" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // LCS key1 key2 [LEN] [IDX [MINMATCHLEN min] [WITHMATCHLEN]]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'lcs' command");
        }
        let key1 = match args[1].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::error("ERR invalid key") };
        let key2 = match args[2].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::error("ERR invalid key") };

        let s1 = match self.db.string_get(*db_index, &key1) {
            Ok(Some(v)) => v,
            Ok(None) => vec![],
            Err(e) => return map_err(e),
        };
        let s2 = match self.db.string_get(*db_index, &key2) {
            Ok(Some(v)) => v,
            Ok(None) => vec![],
            Err(e) => return map_err(e),
        };

        let flags: Vec<String> = args[3..].iter().filter_map(|a| a.as_str().map(|s| s.to_uppercase())).collect();
        let len_only = flags.contains(&"LEN".to_string());
        let idx_mode = flags.contains(&"IDX".to_string());
        let withmatchlen = flags.contains(&"WITHMATCHLEN".to_string());
        let minmatchlen: usize = {
            let mut m = 0usize;
            for i in 0..flags.len() {
                if flags[i] == "MINMATCHLEN" && i + 1 < flags.len() {
                    m = flags[i+1].parse().unwrap_or(0);
                }
            }
            m
        };

        // Compute LCS via DP
        let n = s1.len();
        let m = s2.len();
        let mut dp = vec![vec![0usize; m + 1]; n + 1];
        for i in 1..=n {
            for j in 1..=m {
                if s1[i-1] == s2[j-1] {
                    dp[i][j] = dp[i-1][j-1] + 1;
                } else {
                    dp[i][j] = dp[i-1][j].max(dp[i][j-1]);
                }
            }
        }
        let lcs_len = dp[n][m];

        if len_only {
            return RespValue::integer(lcs_len as i64);
        }

        if idx_mode {
            // Collect all matches via DP backtrack
            let mut temp_matches: Vec<(usize, usize, usize, usize, usize)> = Vec::new();
            let mut i2 = n;
            let mut j2 = m;
            let mut lcs_bytes: Vec<u8> = Vec::new();
            let mut in_match = false;
            let mut match_s1_end = 0usize;
            let mut match_s2_end = 0usize;
            let mut current_len = 0usize;

            while i2 > 0 && j2 > 0 {
                if s1[i2-1] == s2[j2-1] {
                    if !in_match {
                        match_s1_end = i2 - 1;
                        match_s2_end = j2 - 1;
                        current_len = 1;
                        in_match = true;
                    } else {
                        current_len += 1;
                    }
                    lcs_bytes.push(s1[i2-1]);
                    i2 -= 1;
                    j2 -= 1;
                } else {
                    if in_match {
                        let s1_start = i2;
                        let s2_start = j2;
                        if current_len >= minmatchlen {
                            temp_matches.push((s1_start, match_s1_end, s2_start, match_s2_end, current_len));
                        }
                        in_match = false;
                        current_len = 0;
                    }
                    if dp[i2-1][j2] > dp[i2][j2-1] {
                        i2 -= 1;
                    } else {
                        j2 -= 1;
                    }
                }
            }
            if in_match {
                if current_len >= minmatchlen {
                    temp_matches.push((i2, match_s1_end, j2, match_s2_end, current_len));
                }
            }
            lcs_bytes.reverse();

            // Build IDX response: {matches: [...], len: N}
            let match_resp: Vec<RespValue> = temp_matches.iter().map(|(s1s, s1e, s2s, s2e, mlen)| {
                let s1_range = RespValue::Array(Some(vec![
                    RespValue::integer(*s1s as i64),
                    RespValue::integer(*s1e as i64),
                ]));
                let s2_range = RespValue::Array(Some(vec![
                    RespValue::integer(*s2s as i64),
                    RespValue::integer(*s2e as i64),
                ]));
                if withmatchlen {
                    RespValue::Array(Some(vec![s1_range, s2_range, RespValue::integer(*mlen as i64)]))
                } else {
                    RespValue::Array(Some(vec![s1_range, s2_range]))
                }
            }).collect();

            return RespValue::Array(Some(vec![
                RespValue::bulk_str("matches"),
                RespValue::Array(Some(match_resp)),
                RespValue::bulk_str("len"),
                RespValue::integer(lcs_len as i64),
            ]));
        }

        // Default: return LCS string
        let mut result = Vec::new();
        let mut i = n;
        let mut j = m;
        while i > 0 && j > 0 {
            if s1[i-1] == s2[j-1] {
                result.push(s1[i-1]);
                i -= 1;
                j -= 1;
            } else if dp[i-1][j] > dp[i][j-1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
        result.reverse();
        RespValue::bulk_bytes(result)
    }
}
