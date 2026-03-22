use std::sync::Arc;

use crate::blocking::BLOCKING_NOTIFIER;
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

pub struct LPushCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LPushCommand {
    fn name(&self) -> &str {
        "LPUSH"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'lpush' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let elements: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.lpush(*db_index, key, &elements) {
            Ok(n) => {
                BLOCKING_NOTIFIER.notify(*db_index, key.to_vec());
                RespValue::integer(n)
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct RPushCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RPushCommand {
    fn name(&self) -> &str {
        "RPUSH"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'rpush' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let elements: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.rpush(*db_index, key, &elements) {
            Ok(n) => {
                BLOCKING_NOTIFIER.notify(*db_index, key.to_vec());
                RespValue::integer(n)
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct LPushXCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LPushXCommand {
    fn name(&self) -> &str {
        "LPUSHX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'lpushx' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        // Only push if key exists
        match self.db.llen(*db_index, key) {
            Ok(0) => return RespValue::integer(0),
            Ok(_) => {}
            Err(e) => return map_err(e),
        }

        let elements: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.lpush(*db_index, key, &elements) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct RPushXCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RPushXCommand {
    fn name(&self) -> &str {
        "RPUSHX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'rpushx' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        // Only push if key exists
        match self.db.llen(*db_index, key) {
            Ok(0) => return RespValue::integer(0),
            Ok(_) => {}
            Err(e) => return map_err(e),
        }

        let elements: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.rpush(*db_index, key, &elements) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct LPopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LPopCommand {
    fn name(&self) -> &str {
        "LPOP"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'lpop' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        // Optional count argument
        if args.len() >= 3 {
            let count: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR value is not an integer or out of range"),
            };
            let mut results = Vec::new();
            for _ in 0..count {
                match self.db.lpop(*db_index, key) {
                    Ok(Some(v)) => results.push(RespValue::bulk_bytes(v)),
                    Ok(None) => break,
                    Err(e) => return map_err(e),
                }
            }
            return RespValue::Array(Some(results));
        }

        match self.db.lpop(*db_index, key) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct RPopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RPopCommand {
    fn name(&self) -> &str {
        "RPOP"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'rpop' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        // Optional count argument
        if args.len() >= 3 {
            let count: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR value is not an integer or out of range"),
            };
            let mut results = Vec::new();
            for _ in 0..count {
                match self.db.rpop(*db_index, key) {
                    Ok(Some(v)) => results.push(RespValue::bulk_bytes(v)),
                    Ok(None) => break,
                    Err(e) => return map_err(e),
                }
            }
            return RespValue::Array(Some(results));
        }

        match self.db.rpop(*db_index, key) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct LRangeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LRangeCommand {
    fn name(&self) -> &str {
        "LRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'lrange' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let start: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let stop: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        match self.db.lrange(*db_index, key, start, stop) {
            Ok(items) => {
                let results: Vec<RespValue> = items.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(results))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct LLenCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LLenCommand {
    fn name(&self) -> &str {
        "LLEN"
    }

    /// FIXED: calls db.llen() which uses meta.count (O(1))
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'llen' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.llen(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct LIndexCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LIndexCommand {
    fn name(&self) -> &str {
        "LINDEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'lindex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let index: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        match self.db.lrange(*db_index, key, index, index) {
            Ok(mut items) => {
                if items.is_empty() {
                    RespValue::null_bulk()
                } else {
                    RespValue::bulk_bytes(items.remove(0))
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct LInsertCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LInsertCommand {
    fn name(&self) -> &str {
        "LINSERT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 5 {
            return RespValue::error("ERR wrong number of arguments for 'linsert' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let before = match args[2].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("BEFORE") => true,
            Some("AFTER") => false,
            _ => return RespValue::error("ERR syntax error"),
        };
        let pivot = match args[3].as_bytes() {
            Some(p) => p,
            None => return RespValue::error("ERR invalid pivot"),
        };
        let element = match args[4].as_bytes() {
            Some(e) => e,
            None => return RespValue::error("ERR invalid element"),
        };
        match self.db.linsert(*db_index, key, before, pivot, element) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct LSetCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LSetCommand {
    fn name(&self) -> &str {
        "LSET"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'lset' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let index: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let element = match args[3].as_bytes() {
            Some(e) => e,
            None => return RespValue::error("ERR invalid element"),
        };
        match self.db.lset(*db_index, key, index, element) {
            Ok(()) => RespValue::ok(),
            Err(crate::database::RedisError::WrongType) => wrong_type(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct LRemCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LRemCommand {
    fn name(&self) -> &str {
        "LREM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'lrem' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let count: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let element = match args[3].as_bytes() {
            Some(e) => e,
            None => return RespValue::error("ERR invalid element"),
        };
        match self.db.lrem(*db_index, key, count, element) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct LTrimCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LTrimCommand {
    fn name(&self) -> &str {
        "LTRIM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'ltrim' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let start: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let stop: i64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        match self.db.ltrim(*db_index, key, start, stop) {
            Ok(()) => RespValue::ok(),
            Err(e) => map_err(e),
        }
    }
}

pub struct LMoveCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LMoveCommand {
    fn name(&self) -> &str {
        "LMOVE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 5 {
            return RespValue::error("ERR wrong number of arguments for 'lmove' command");
        }
        let src = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let src_dir = match args[3].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("LEFT") => true,
            Some("RIGHT") => false,
            _ => return RespValue::error("ERR syntax error"),
        };
        let dst_dir = match args[4].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("LEFT") => true,
            Some("RIGHT") => false,
            _ => return RespValue::error("ERR syntax error"),
        };
        match self.db.lmove(*db_index, src, dst, src_dir, dst_dir) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct RPoplPushCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RPoplPushCommand {
    fn name(&self) -> &str {
        "RPOPLPUSH"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'rpoplpush' command");
        }
        let src = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        // rpoplpush = lmove src dst RIGHT LEFT
        match self.db.lmove(*db_index, src, dst, false, true) {
            Ok(Some(v)) => RespValue::bulk_bytes(v),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct LPosCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LPosCommand {
    fn name(&self) -> &str {
        "LPOS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'lpos' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let element = match args[2].as_bytes() {
            Some(e) => e,
            None => return RespValue::error("ERR invalid element"),
        };

        let mut rank: i64 = 0;
        let mut count: Option<i64> = None;
        let mut maxlen: i64 = 0;
        let mut i = 3;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("RANK") => {
                    i += 1;
                    if i < args.len() {
                        rank = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                }
                Some("COUNT") => {
                    i += 1;
                    if i < args.len() {
                        count = args[i].as_str().and_then(|s| s.parse().ok());
                    }
                }
                Some("MAXLEN") => {
                    i += 1;
                    if i < args.len() {
                        maxlen = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                }
                _ => {}
            }
            i += 1;
        }

        match self.db.lpos(*db_index, key, element, rank, count, maxlen) {
            Ok(positions) => {
                if count.is_some() {
                    let items: Vec<RespValue> = positions.into_iter().map(RespValue::integer).collect();
                    RespValue::Array(Some(items))
                } else {
                    match positions.first() {
                        Some(&pos) => RespValue::integer(pos),
                        None => RespValue::null_bulk(),
                    }
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct BlpopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BlpopCommand {
    fn name(&self) -> &str { "BLPOP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'blpop' command");
        }
        // Non-blocking: try to pop from each key in order
        let keys: Vec<_> = args[1..args.len()-1].iter().filter_map(|a| a.as_bytes()).collect();
        for key in &keys {
            match self.db.lpop(*db_index, key) {
                Ok(Some(val)) => {
                    return RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(key.to_vec()),
                        RespValue::bulk_bytes(val),
                    ]));
                }
                Ok(None) => continue,
                Err(e) => return RespValue::error(&e.to_string()),
            }
        }
        RespValue::null_array()
    }
}

pub struct BrpopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BrpopCommand {
    fn name(&self) -> &str { "BRPOP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'brpop' command");
        }
        let keys: Vec<_> = args[1..args.len()-1].iter().filter_map(|a| a.as_bytes()).collect();
        for key in &keys {
            match self.db.rpop(*db_index, key) {
                Ok(Some(val)) => {
                    return RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(key.to_vec()),
                        RespValue::bulk_bytes(val),
                    ]));
                }
                Ok(None) => continue,
                Err(e) => return RespValue::error(&e.to_string()),
            }
        }
        RespValue::null_array()
    }
}

pub struct BlmoveCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BlmoveCommand {
    fn name(&self) -> &str { "BLMOVE" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // BLMOVE src dst wherefrom whereto timeout -> non-blocking LMOVE
        if args.len() < 6 {
            return RespValue::error("ERR wrong number of arguments for 'blmove' command");
        }
        let src = match args[1].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
        let dst = match args[2].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
        let wherefrom = args[3].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
        let whereto = args[4].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
        let left_from = wherefrom == "LEFT";
        let left_to = whereto == "LEFT";
        match self.db.lmove(*db_index, &src, &dst, left_from, left_to) {
            Ok(Some(val)) => RespValue::bulk_bytes(val),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct BrpoplpushCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BrpoplpushCommand {
    fn name(&self) -> &str { "BRPOPLPUSH" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // BRPOPLPUSH src dst timeout -> non-blocking RPOPLPUSH
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'brpoplpush' command");
        }
        let src = match args[1].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
        let dst = match args[2].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
        match self.db.lmove(*db_index, &src, &dst, false, true) {
            Ok(Some(val)) => RespValue::bulk_bytes(val),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct LmpopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for LmpopCommand {
    fn name(&self) -> &str { "LMPOP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'lmpop' command");
        }
        let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return RespValue::error("ERR syntax error");
        }
        let keys: Vec<&[u8]> = args[2..2+numkeys].iter().filter_map(|a| a.as_bytes()).collect();
        let direction = args[2+numkeys].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
        let from_left = direction == "LEFT";
        let count: usize = {
            let rest = &args[3+numkeys..];
            let mut c = 1usize;
            let mut i = 0;
            while i < rest.len() {
                if rest[i].as_str().map(|s| s.to_uppercase() == "COUNT").unwrap_or(false) {
                    if i + 1 < rest.len() {
                        c = rest[i+1].as_str().and_then(|s| s.parse().ok()).unwrap_or(1);
                    }
                }
                i += 1;
            }
            c
        };
        match self.db.lmpop_impl(*db_index, &keys, from_left, count) {
            Ok(None) => RespValue::null_array(),
            Ok(Some((key, items))) => {
                let elements: Vec<RespValue> = items.into_iter().map(|v| RespValue::bulk_bytes(v)).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_bytes(key),
                    RespValue::Array(Some(elements)),
                ]))
            }
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct BlmpopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BlmpopCommand {
    fn name(&self) -> &str { "BLMPOP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'blmpop' command");
        }
        // args[1] = timeout (ignored, non-blocking)
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if numkeys == 0 || args.len() < 3 + numkeys + 1 {
            return RespValue::error("ERR syntax error");
        }
        let keys: Vec<&[u8]> = args[3..3+numkeys].iter().filter_map(|a| a.as_bytes()).collect();
        let direction = args[3+numkeys].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
        let from_left = direction == "LEFT";
        let count: usize = {
            let rest = &args[4+numkeys..];
            let mut c = 1usize;
            let mut i = 0;
            while i < rest.len() {
                if rest[i].as_str().map(|s| s.to_uppercase() == "COUNT").unwrap_or(false) {
                    if i + 1 < rest.len() {
                        c = rest[i+1].as_str().and_then(|s| s.parse().ok()).unwrap_or(1);
                    }
                }
                i += 1;
            }
            c
        };
        match self.db.lmpop_impl(*db_index, &keys, from_left, count) {
            Ok(None) => RespValue::null_array(),
            Ok(Some((key, items))) => {
                let elements: Vec<RespValue> = items.into_iter().map(|v| RespValue::bulk_bytes(v)).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_bytes(key),
                    RespValue::Array(Some(elements)),
                ]))
            }
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}
