use std::sync::Arc;

use crate::database::RedisDatabase;
use crate::resp::RespValue;

use super::CommandHandler;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub struct DelCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for DelCommand {
    fn name(&self) -> &str {
        "DEL"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'del' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.delete(*db_index, &keys) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct UnlinkCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for UnlinkCommand {
    fn name(&self) -> &str {
        "UNLINK"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'unlink' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.delete(*db_index, &keys) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct ExistsCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ExistsCommand {
    fn name(&self) -> &str {
        "EXISTS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'exists' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.exists(*db_index, &keys) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct ExpireCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ExpireCommand {
    fn name(&self) -> &str {
        "EXPIRE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || args.len() > 4 {
            return RespValue::error("ERR wrong number of arguments for 'expire' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let secs: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        // Negative or zero: expire immediately (key is deleted on next access)
        let expiry_ms = if secs <= 0 { 1 } else { now_ms() + secs * 1000 };
        if args.len() == 4 {
            let opt = args[3].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let cond = match opt.as_str() {
                "NX" => 0u8,
                "XX" => 1u8,
                "GT" => 2u8,
                "LT" => 3u8,
                _ => return RespValue::error("ERR invalid expire option"),
            };
            match self.db.expire_cond(*db_index, key, expiry_ms, cond) {
                Ok(n) => RespValue::integer(n),
                Err(e) => RespValue::error(&e.to_string()),
            }
        } else {
            match self.db.expire(*db_index, key, expiry_ms) {
                Ok(true) => RespValue::integer(1),
                Ok(false) => RespValue::integer(0),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }
    }
}

pub struct PExpireCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PExpireCommand {
    fn name(&self) -> &str {
        "PEXPIRE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || args.len() > 4 {
            return RespValue::error("ERR wrong number of arguments for 'pexpire' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let ms: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        // Negative or zero: expire immediately (key is deleted on next access)
        let expiry_ms = if ms <= 0 { 1 } else { now_ms() + ms };
        if args.len() == 4 {
            let opt = args[3].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let cond = match opt.as_str() {
                "NX" => 0u8,
                "XX" => 1u8,
                "GT" => 2u8,
                "LT" => 3u8,
                _ => return RespValue::error("ERR invalid expire option"),
            };
            match self.db.expire_cond(*db_index, key, expiry_ms, cond) {
                Ok(n) => RespValue::integer(n),
                Err(e) => RespValue::error(&e.to_string()),
            }
        } else {
            match self.db.expire(*db_index, key, expiry_ms) {
                Ok(true) => RespValue::integer(1),
                Ok(false) => RespValue::integer(0),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }
    }
}

pub struct ExpireAtCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ExpireAtCommand {
    fn name(&self) -> &str {
        "EXPIREAT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || args.len() > 4 {
            return RespValue::error("ERR wrong number of arguments for 'expireat' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let unix_secs: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        // Negative or zero timestamp: expire immediately
        let expiry_ms = if unix_secs <= 0 { 1 } else { unix_secs * 1000 };
        if args.len() == 4 {
            let opt = args[3].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let cond = match opt.as_str() {
                "NX" => 0u8,
                "XX" => 1u8,
                "GT" => 2u8,
                "LT" => 3u8,
                _ => return RespValue::error("ERR invalid expire option"),
            };
            match self.db.expire_cond(*db_index, key, expiry_ms, cond) {
                Ok(n) => RespValue::integer(n),
                Err(e) => RespValue::error(&e.to_string()),
            }
        } else {
            match self.db.expire(*db_index, key, expiry_ms) {
                Ok(true) => RespValue::integer(1),
                Ok(false) => RespValue::integer(0),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }
    }
}

pub struct PExpireAtCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PExpireAtCommand {
    fn name(&self) -> &str {
        "PEXPIREAT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || args.len() > 4 {
            return RespValue::error("ERR wrong number of arguments for 'pexpireat' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let raw_expiry_ms: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        // Negative or zero timestamp: expire immediately
        let expiry_ms = if raw_expiry_ms <= 0 { 1 } else { raw_expiry_ms };
        if args.len() == 4 {
            let opt = args[3].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let cond = match opt.as_str() {
                "NX" => 0u8,
                "XX" => 1u8,
                "GT" => 2u8,
                "LT" => 3u8,
                _ => return RespValue::error("ERR invalid expire option"),
            };
            match self.db.expire_cond(*db_index, key, expiry_ms, cond) {
                Ok(n) => RespValue::integer(n),
                Err(e) => RespValue::error(&e.to_string()),
            }
        } else {
            match self.db.expire(*db_index, key, expiry_ms) {
                Ok(true) => RespValue::integer(1),
                Ok(false) => RespValue::integer(0),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }
    }
}

pub struct TtlCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for TtlCommand {
    fn name(&self) -> &str {
        "TTL"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'ttl' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.ttl_secs(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct PttlCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PttlCommand {
    fn name(&self) -> &str {
        "PTTL"
    }

    /// FIXED: call ttl_ms() instead of ttl_secs() * 1000
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'pttl' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.ttl_ms(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct KeysCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for KeysCommand {
    fn name(&self) -> &str {
        "KEYS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'keys' command");
        }
        let pattern = match args[1].as_str() {
            Some(p) => p.to_string(),
            None => return RespValue::error("ERR invalid pattern"),
        };
        match self.db.keys(*db_index, &pattern) {
            Ok(keys) => {
                let items: Vec<RespValue> = keys.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct ScanCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ScanCommand {
    fn name(&self) -> &str {
        "SCAN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'scan' command");
        }
        let cursor: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let mut pattern: Option<String> = None;
        let mut count: usize = 10;
        let mut i = 2;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("MATCH") => {
                    i += 1;
                    if i < args.len() {
                        pattern = args[i].as_str().map(|s| s.to_string());
                    }
                }
                Some("COUNT") => {
                    i += 1;
                    if i < args.len() {
                        count = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(10);
                    }
                }
                _ => {}
            }
            i += 1;
        }

        match self.db.scan(*db_index, cursor, pattern.as_deref(), count) {
            Ok((next_cursor, keys)) => {
                let items: Vec<RespValue> = keys.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_str(&next_cursor.to_string()),
                    RespValue::Array(Some(items)),
                ]))
            }
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct TypeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for TypeCommand {
    fn name(&self) -> &str {
        "TYPE"
    }

    /// FIXED: reads actual metadata and returns correct type
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'type' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.key_type(*db_index, key) {
            Ok(t) => RespValue::simple(t),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct PersistCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PersistCommand {
    fn name(&self) -> &str {
        "PERSIST"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'persist' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.persist(*db_index, key) {
            Ok(true) => RespValue::integer(1),
            Ok(false) => RespValue::integer(0),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct RenameCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RenameCommand {
    fn name(&self) -> &str {
        "RENAME"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'rename' command");
        }
        let src = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        match self.db.rename_key(*db_index, src, dst) {
            Ok(()) => RespValue::ok(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct RenameNxCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RenameNxCommand {
    fn name(&self) -> &str {
        "RENAMENX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'renamenx' command");
        }
        let src = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        match self.db.renamenx(*db_index, src, dst) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct RandomKeyCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RandomKeyCommand {
    fn name(&self) -> &str {
        "RANDOMKEY"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 1 {
            return RespValue::error("ERR wrong number of arguments for 'randomkey' command");
        }
        match self.db.randomkey(*db_index) {
            Ok(Some(k)) => RespValue::bulk_bytes(k),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct TouchCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for TouchCommand {
    fn name(&self) -> &str {
        "TOUCH"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'touch' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.touch(*db_index, &keys) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct ScanCommandV2 {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ScanCommandV2 {
    fn name(&self) -> &str {
        "SCAN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'scan' command");
        }
        let cursor: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let mut pattern: Option<String> = None;
        let mut count: usize = 10;
        let mut type_filter: Option<String> = None;
        let mut i = 2;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("MATCH") => {
                    i += 1;
                    if i < args.len() {
                        pattern = args[i].as_str().map(|s| s.to_string());
                    }
                }
                Some("COUNT") => {
                    i += 1;
                    if i < args.len() {
                        count = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(10);
                    }
                }
                Some("TYPE") => {
                    i += 1;
                    if i < args.len() {
                        type_filter = args[i].as_str().map(|s| s.to_lowercase());
                    }
                }
                _ => {}
            }
            i += 1;
        }

        match self.db.scan_with_type(*db_index, cursor, pattern.as_deref(), count, type_filter.as_deref()) {
            Ok((next_cursor, keys)) => {
                let items: Vec<RespValue> = keys.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_str(&next_cursor.to_string()),
                    RespValue::Array(Some(items)),
                ]))
            }
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct MoveCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for MoveCommand {
    fn name(&self) -> &str { "MOVE" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'move' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let dst_db: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        match self.db.move_key(*db_index, key, dst_db) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct CopyCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for CopyCommand {
    fn name(&self) -> &str { "COPY" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'copy' command");
        }
        let src = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let mut dst_db = *db_index;
        let mut replace = false;
        let mut i = 3;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("DB") => {
                    i += 1;
                    if i < args.len() {
                        dst_db = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(*db_index);
                    }
                }
                Some("REPLACE") => { replace = true; }
                _ => {}
            }
            i += 1;
        }
        match self.db.copy_key(*db_index, &src, dst_db, &dst, replace) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct SortCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SortCommand {
    fn name(&self) -> &str { "SORT" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'sort' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let mut alpha = false;
        let mut desc = false;
        let mut limit: Option<(i64, i64)> = None;
        let mut store_key: Option<Vec<u8>> = None;
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("ALPHA") => { alpha = true; }
                Some("DESC") => { desc = true; }
                Some("ASC") => { desc = false; }
                Some("LIMIT") => {
                    if i + 2 < args.len() {
                        let offset = args[i+1].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                        let count = args[i+2].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(-1);
                        limit = Some((offset, count));
                        i += 2;
                    }
                }
                Some("STORE") => {
                    i += 1;
                    if i < args.len() {
                        store_key = args[i].as_bytes().map(|b| b.to_vec());
                    }
                }
                // BY and GET are not implemented (stubs to avoid error)
                Some("BY") | Some("GET") => { i += 1; }
                _ => {}
            }
            i += 1;
        }
        match self.db.sort_key(*db_index, &key, alpha, desc, limit) {
            Ok(items) => {
                if let Some(dst) = store_key {
                    // Delete existing destination key, then store as list
                    let _ = self.db.delete(*db_index, &[dst.as_slice()]);
                    let count = items.len() as i64;
                    if !items.is_empty() {
                        let refs: Vec<&[u8]> = items.iter().map(|v| v.as_slice()).collect();
                        let _ = self.db.rpush(*db_index, &dst, &refs);
                    }
                    RespValue::integer(count)
                } else {
                    let vals: Vec<RespValue> = items.into_iter().map(RespValue::bulk_bytes).collect();
                    RespValue::Array(Some(vals))
                }
            }
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct DumpCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for DumpCommand {
    fn name(&self) -> &str { "DUMP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'dump' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::null_bulk(),
        };

        // Determine type
        let ktype = match self.db.key_type(*db_index, key) {
            Ok(t) => t,
            Err(_) => return RespValue::null_bulk(),
        };

        let mut payload = Vec::new();

        match ktype {
            "string" => {
                let val = match self.db.string_get(*db_index, key) {
                    Ok(Some(v)) => v,
                    _ => return RespValue::null_bulk(),
                };
                payload.push(0x00u8); // RDB_TYPE_STRING
                rdb_encode_string(&mut payload, &val);
            }
            "list" => {
                let items = match self.db.lrange(*db_index, key, 0, -1) {
                    Ok(v) => v,
                    Err(_) => return RespValue::null_bulk(),
                };
                payload.push(18u8); // RDB_TYPE_LIST_QUICKLIST_2
                rdb_encode_length(&mut payload, items.len() as u64);
                for item in &items {
                    rdb_encode_string(&mut payload, item);
                    rdb_encode_length(&mut payload, 1); // QUICKLIST_NODE_CONTAINER_PLAIN
                }
            }
            "hash" => {
                let pairs = match self.db.hgetall(*db_index, key) {
                    Ok(v) => v,
                    Err(_) => return RespValue::null_bulk(),
                };
                payload.push(4u8); // RDB_TYPE_HASH
                rdb_encode_length(&mut payload, pairs.len() as u64);
                for (field, val) in &pairs {
                    rdb_encode_string(&mut payload, field);
                    rdb_encode_string(&mut payload, val);
                }
            }
            "set" => {
                let members = match self.db.smembers(*db_index, key) {
                    Ok(v) => v,
                    Err(_) => return RespValue::null_bulk(),
                };
                payload.push(2u8); // RDB_TYPE_SET
                rdb_encode_length(&mut payload, members.len() as u64);
                for m in &members {
                    rdb_encode_string(&mut payload, m);
                }
            }
            "zset" => {
                let items = match self.db.zrange_all(*db_index, key) {
                    Ok(v) => v,
                    Err(_) => return RespValue::null_bulk(),
                };
                payload.push(5u8); // RDB_TYPE_ZSET_2 (f64 scores)
                rdb_encode_length(&mut payload, items.len() as u64);
                for (member, score) in &items {
                    rdb_encode_string(&mut payload, member);
                    payload.extend_from_slice(&score.to_le_bytes());
                }
            }
            _ => return RespValue::null_bulk(),
        }

        // Append RDB version (10) as 2 bytes LE + 8 zero bytes (CRC placeholder)
        payload.push(0x0a);
        payload.push(0x00);
        payload.extend_from_slice(&[0u8; 8]);

        RespValue::BulkString(Some(payload))
    }
}

fn rdb_encode_length(buf: &mut Vec<u8>, len: u64) {
    if len < 64 {
        buf.push(len as u8);
    } else if len < 16384 {
        buf.push(0x40 | ((len >> 8) as u8 & 0x3f));
        buf.push((len & 0xff) as u8);
    } else {
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn rdb_encode_string(buf: &mut Vec<u8>, s: &[u8]) {
    rdb_encode_length(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

pub struct RestoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for RestoreCommand {
    fn name(&self) -> &str { "RESTORE" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'restore' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let ttl_ms: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
        let data = match args[3].as_bytes() {
            Some(d) => d.to_vec(),
            None => return RespValue::error("ERR invalid payload"),
        };

        let mut replace = false;
        let mut absttl = false;
        let mut i = 4;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("REPLACE") => { replace = true; }
                Some("ABSTTL") => { absttl = true; }
                Some("IDLETIME") => { i += 1; } // skip value
                Some("FREQ") => { i += 1; } // skip value
                _ => {}
            }
            i += 1;
        }

        // Check if key exists
        if !replace {
            match self.db.exists(*db_index, &[key.as_slice()]) {
                Ok(n) if n > 0 => return RespValue::error("BUSYKEY Target key name already exists."),
                _ => {}
            }
        }

        // Decode RDB payload: [type:1][encoded_value][rdb_ver:2LE][crc64:8]
        if data.len() < 10 {
            return RespValue::error("ERR DUMP payload version or checksum are wrong");
        }
        let type_byte = data[0];
        let content = &data[1..data.len()-10]; // strip type byte, rdb_ver(2), crc64(8)

        // Compute expiry
        let expiry = if ttl_ms == 0 {
            0
        } else if absttl {
            ttl_ms
        } else {
            now_ms() + ttl_ms
        };

        let err = RespValue::error("ERR DUMP payload version or checksum are wrong");

        match type_byte {
            0 => {
                // String type
                let (val, _) = match decode_rdb_string(content) {
                    Some(r) => r,
                    None => return err,
                };
                match self.db.string_set(*db_index, &key, &val, expiry, false, false) {
                    Ok(()) => RespValue::ok(),
                    Err(e) => RespValue::error(&e.to_string()),
                }
            }
            18 => {
                // RDB_TYPE_LIST_QUICKLIST_2: [count][item[rdb_string][container]...]
                let (count, mut pos) = match decode_rdb_length(content) {
                    Some(r) => r,
                    None => return err,
                };
                // Delete existing key first
                let _ = self.db.delete(*db_index, &[key.as_slice()]);
                for _ in 0..count {
                    let (item, consumed) = match decode_rdb_string(&content[pos..]) {
                        Some(r) => r,
                        None => return err,
                    };
                    pos += consumed;
                    // Skip container byte(s) (encoded as length)
                    let (_, cont_consumed) = match decode_rdb_length(&content[pos..]) {
                        Some(r) => r,
                        None => return err,
                    };
                    pos += cont_consumed;
                    if self.db.rpush(*db_index, &key, &[item.as_slice()]).is_err() {
                        return RespValue::error("ERR error restoring list");
                    }
                }
                if expiry > 0 { let _ = self.db.expire(*db_index, &key, expiry); }
                RespValue::ok()
            }
            4 => {
                // RDB_TYPE_HASH: [count][field][value]...
                let (count, mut pos) = match decode_rdb_length(content) {
                    Some(r) => r,
                    None => return err,
                };
                let _ = self.db.delete(*db_index, &[key.as_slice()]);
                for _ in 0..count {
                    let (field, fc) = match decode_rdb_string(&content[pos..]) {
                        Some(r) => r, None => return err,
                    };
                    pos += fc;
                    let (val, vc) = match decode_rdb_string(&content[pos..]) {
                        Some(r) => r, None => return err,
                    };
                    pos += vc;
                    if self.db.hset(*db_index, &key, &[(field.as_slice(), val.as_slice())]).is_err() {
                        return RespValue::error("ERR error restoring hash");
                    }
                }
                if expiry > 0 { let _ = self.db.expire(*db_index, &key, expiry); }
                RespValue::ok()
            }
            2 => {
                // RDB_TYPE_SET: [count][member]...
                let (count, mut pos) = match decode_rdb_length(content) {
                    Some(r) => r, None => return err,
                };
                let _ = self.db.delete(*db_index, &[key.as_slice()]);
                for _ in 0..count {
                    let (member, mc) = match decode_rdb_string(&content[pos..]) {
                        Some(r) => r, None => return err,
                    };
                    pos += mc;
                    if self.db.sadd(*db_index, &key, &[member.as_slice()]).is_err() {
                        return RespValue::error("ERR error restoring set");
                    }
                }
                if expiry > 0 { let _ = self.db.expire(*db_index, &key, expiry); }
                RespValue::ok()
            }
            5 => {
                // RDB_TYPE_ZSET_2: [count][member][score_f64_le]...
                let (count, mut pos) = match decode_rdb_length(content) {
                    Some(r) => r, None => return err,
                };
                let _ = self.db.delete(*db_index, &[key.as_slice()]);
                for _ in 0..count {
                    let (member, mc) = match decode_rdb_string(&content[pos..]) {
                        Some(r) => r, None => return err,
                    };
                    pos += mc;
                    if pos + 8 > content.len() { return err; }
                    let score = f64::from_le_bytes(content[pos..pos+8].try_into().unwrap());
                    pos += 8;
                    if self.db.zadd(*db_index, &key, &[(score, member.as_slice())]).is_err() {
                        return RespValue::error("ERR error restoring zset");
                    }
                }
                if expiry > 0 { let _ = self.db.expire(*db_index, &key, expiry); }
                RespValue::ok()
            }
            _ => {
                RespValue::error("ERR DUMP payload version or checksum are wrong")
            }
        }
    }
}

fn decode_rdb_length(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() { return None; }
    let first = data[0];
    let enc_type = (first & 0xC0) >> 6;
    match enc_type {
        0 => Some(((first & 0x3F) as u64, 1)),
        1 => {
            if data.len() < 2 { return None; }
            let len = (((first & 0x3F) as u64) << 8) | data[1] as u64;
            Some((len, 2))
        }
        2 => {
            if data.len() < 5 { return None; }
            let len = u32::from_be_bytes(data[1..5].try_into().ok()?) as u64;
            Some((len, 5))
        }
        _ => None,
    }
}

fn decode_rdb_string(data: &[u8]) -> Option<(Vec<u8>, usize)> {
    if data.is_empty() {
        return None;
    }
    let first = data[0];
    let enc_type = (first & 0xC0) >> 6;
    match enc_type {
        0 => {
            // 6-bit length
            let len = (first & 0x3F) as usize;
            if data.len() < 1 + len {
                return None;
            }
            Some((data[1..1+len].to_vec(), 1 + len))
        }
        1 => {
            // 14-bit length
            if data.len() < 2 {
                return None;
            }
            let len = (((first & 0x3F) as usize) << 8) | data[1] as usize;
            if data.len() < 2 + len {
                return None;
            }
            Some((data[2..2+len].to_vec(), 2 + len))
        }
        2 => {
            // 32-bit length (big-endian)
            if data.len() < 5 {
                return None;
            }
            let len = u32::from_be_bytes(data[1..5].try_into().ok()?) as usize;
            if data.len() < 5 + len {
                return None;
            }
            Some((data[5..5+len].to_vec(), 5 + len))
        }
        3 => {
            // Special encoding
            let enc = first & 0x3F;
            match enc {
                0 => {
                    // 8-bit integer
                    if data.len() < 2 { return None; }
                    let n = data[1] as i8;
                    Some((n.to_string().into_bytes(), 2))
                }
                1 => {
                    // 16-bit integer LE
                    if data.len() < 3 { return None; }
                    let n = i16::from_le_bytes(data[1..3].try_into().ok()?);
                    Some((n.to_string().into_bytes(), 3))
                }
                2 => {
                    // 32-bit integer LE
                    if data.len() < 5 { return None; }
                    let n = i32::from_le_bytes(data[1..5].try_into().ok()?);
                    Some((n.to_string().into_bytes(), 5))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

pub struct ExpiretimeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ExpiretimeCommand {
    fn name(&self) -> &str { "EXPIRETIME" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'expiretime' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.expiretime_secs(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct PExpiretimeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for PExpiretimeCommand {
    fn name(&self) -> &str { "PEXPIRETIME" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'pexpiretime' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.expiretime_ms(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

// ── SORT_RO ──────────────────────────────────────────────────────────────────

pub struct SortRoCommand {
    pub db: Arc<RedisDatabase>,
}

impl super::CommandHandler for SortRoCommand {
    fn name(&self) -> &str { "SORT_RO" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // SORT_RO is SORT without STORE option
        SortCommand { db: self.db.clone() }.execute(db_index, args)
    }
}
