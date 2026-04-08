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

pub struct SAddCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SAddCommand {
    fn name(&self) -> &str {
        "SADD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'sadd' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let members: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.sadd(*db_index, key, &members) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct SRemCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SRemCommand {
    fn name(&self) -> &str {
        "SREM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'srem' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let members: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.srem(*db_index, key, &members) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct SMembersCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SMembersCommand {
    fn name(&self) -> &str {
        "SMEMBERS"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'smembers' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.smembers(*db_index, key) {
            Ok(members) => {
                let items: Vec<RespValue> =
                    members.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SIsMemberCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SIsMemberCommand {
    fn name(&self) -> &str {
        "SISMEMBER"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'sismember' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let member = match args[2].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };
        match self.db.sismember(*db_index, key, member) {
            Ok(true) => RespValue::integer(1),
            Ok(false) => RespValue::integer(0),
            Err(e) => map_err(e),
        }
    }
}

pub struct SCardCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SCardCommand {
    fn name(&self) -> &str {
        "SCARD"
    }

    /// FIXED: calls db.scard() which uses meta.count (O(1))
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'scard' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.scard(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct SMIsMemberCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SMIsMemberCommand {
    fn name(&self) -> &str {
        "SMISMEMBER"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'smismember' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let mut results = Vec::new();
        for arg in &args[2..] {
            let member = match arg.as_bytes() {
                Some(m) => m,
                None => {
                    results.push(RespValue::integer(0));
                    continue;
                }
            };
            match self.db.sismember(*db_index, key, member) {
                Ok(true) => results.push(RespValue::integer(1)),
                Ok(false) => results.push(RespValue::integer(0)),
                Err(_) => results.push(RespValue::integer(0)),
            }
        }
        RespValue::Array(Some(results))
    }
}

pub struct SDiffCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SDiffCommand {
    fn name(&self) -> &str {
        "SDIFF"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'sdiff' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        let result = sdiff_sets(self.db.as_ref(), *db_index, &keys);
        match result {
            Ok(members) => {
                let items: Vec<RespValue> =
                    members.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SDiffStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SDiffStoreCommand {
    fn name(&self) -> &str {
        "SDIFFSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'sdiffstore' command");
        }
        let dst = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let keys: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match sdiff_sets(self.db.as_ref(), *db_index, &keys) {
            Ok(members) => {
                // Delete dst then add members
                let _ = self.db.delete(*db_index, &[dst.as_slice()]);
                if members.is_empty() {
                    return RespValue::integer(0);
                }
                let m_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                match self.db.sadd(*db_index, &dst, &m_refs) {
                    Ok(n) => RespValue::integer(n),
                    Err(e) => map_err(e),
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SInterCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SInterCommand {
    fn name(&self) -> &str {
        "SINTER"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'sinter' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        match sinter_sets(self.db.as_ref(), *db_index, &keys) {
            Ok(members) => {
                let items: Vec<RespValue> =
                    members.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SInterStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SInterStoreCommand {
    fn name(&self) -> &str {
        "SINTERSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'sinterstore' command");
        }
        let dst = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let keys: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match sinter_sets(self.db.as_ref(), *db_index, &keys) {
            Ok(members) => {
                let _ = self.db.delete(*db_index, &[dst.as_slice()]);
                if members.is_empty() {
                    return RespValue::integer(0);
                }
                let m_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                match self.db.sadd(*db_index, &dst, &m_refs) {
                    Ok(n) => RespValue::integer(n),
                    Err(e) => map_err(e),
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SUnionCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SUnionCommand {
    fn name(&self) -> &str {
        "SUNION"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'sunion' command");
        }
        let keys: Vec<&[u8]> = args[1..].iter().filter_map(|a| a.as_bytes()).collect();
        match sunion_sets(self.db.as_ref(), *db_index, &keys) {
            Ok(members) => {
                let items: Vec<RespValue> =
                    members.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(items))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SUnionStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SUnionStoreCommand {
    fn name(&self) -> &str {
        "SUNIONSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'sunionstore' command");
        }
        let dst = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let keys: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match sunion_sets(self.db.as_ref(), *db_index, &keys) {
            Ok(members) => {
                let _ = self.db.delete(*db_index, &[dst.as_slice()]);
                if members.is_empty() {
                    return RespValue::integer(0);
                }
                let m_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                match self.db.sadd(*db_index, &dst, &m_refs) {
                    Ok(n) => RespValue::integer(n),
                    Err(e) => map_err(e),
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct SMoveCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SMoveCommand {
    fn name(&self) -> &str {
        "SMOVE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'smove' command");
        }
        let src = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let dst = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let member = match args[3].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };
        match self.db.smove(*db_index, src, dst, member) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct SPopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SPopCommand {
    fn name(&self) -> &str {
        "SPOP"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'spop' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        if args.len() >= 3 {
            let count: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR value is not an integer or out of range"),
            };
            let mut results = Vec::new();
            for _ in 0..count {
                match self.db.spop(*db_index, key) {
                    Ok(Some(m)) => results.push(RespValue::bulk_bytes(m)),
                    Ok(None) => break,
                    Err(e) => return map_err(e),
                }
            }
            return RespValue::Array(Some(results));
        }

        match self.db.spop(*db_index, key) {
            Ok(Some(m)) => RespValue::bulk_bytes(m),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct SRandMemberCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SRandMemberCommand {
    fn name(&self) -> &str {
        "SRANDMEMBER"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'srandmember' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let members = match self.db.smembers(*db_index, key) {
            Ok(m) => m,
            Err(e) => return map_err(e),
        };

        if args.len() == 2 {
            match members.into_iter().next() {
                Some(m) => return RespValue::bulk_bytes(m),
                None => return RespValue::null_bulk(),
            }
        }

        let count: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        if members.is_empty() {
            return RespValue::Array(Some(vec![]));
        }

        let len = members.len();
        let mut result = Vec::new();

        if count >= 0 {
            // Unique members up to count
            let take = (count as usize).min(len);
            for m in members.into_iter().take(take) {
                result.push(RespValue::bulk_bytes(m));
            }
        } else {
            // Allow duplicates
            let abs = (-count) as usize;
            for i in 0..abs {
                result.push(RespValue::bulk_bytes(members[i % len].clone()));
            }
        }

        RespValue::Array(Some(result))
    }
}

pub struct SScanCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SScanCommand {
    fn name(&self) -> &str {
        "SSCAN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'sscan' command");
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
                Some("COUNT") => {
                    i += 1;
                }
                _ => {}
            }
            i += 1;
        }

        let members = match self.db.smembers(*db_index, key) {
            Ok(m) => m,
            Err(e) => return map_err(e),
        };

        let mut items = Vec::new();
        for member in members {
            let s = String::from_utf8_lossy(&member).into_owned();
            if let Some(ref pat) = pattern {
                if !crate::database::RedisDatabase::glob_match(pat, &s) {
                    continue;
                }
            }
            items.push(RespValue::bulk_bytes(member));
        }

        RespValue::Array(Some(vec![
            RespValue::bulk_str("0"),
            RespValue::Array(Some(items)),
        ]))
    }
}

fn sdiff_sets(
    db: &crate::database::RedisDatabase,
    db_index: usize,
    keys: &[&[u8]],
) -> Result<Vec<Vec<u8>>, crate::database::RedisError> {
    use std::collections::HashSet;

    if keys.is_empty() {
        return Ok(vec![]);
    }

    let first = db.smembers(db_index, keys[0])?;
    let mut result: HashSet<Vec<u8>> = first.into_iter().collect();

    for &key in &keys[1..] {
        let members = db.smembers(db_index, key)?;
        for m in members {
            result.remove(&m);
        }
    }

    Ok(result.into_iter().collect())
}

fn sinter_sets(
    db: &crate::database::RedisDatabase,
    db_index: usize,
    keys: &[&[u8]],
) -> Result<Vec<Vec<u8>>, crate::database::RedisError> {
    use std::collections::HashSet;

    if keys.is_empty() {
        return Ok(vec![]);
    }

    let first = db.smembers(db_index, keys[0])?;
    let mut result: HashSet<Vec<u8>> = first.into_iter().collect();

    for &key in &keys[1..] {
        let members: HashSet<Vec<u8>> = db.smembers(db_index, key)?.into_iter().collect();
        result = result.into_iter().filter(|m| members.contains(m)).collect();
    }

    Ok(result.into_iter().collect())
}

fn sunion_sets(
    db: &crate::database::RedisDatabase,
    db_index: usize,
    keys: &[&[u8]],
) -> Result<Vec<Vec<u8>>, crate::database::RedisError> {
    use std::collections::HashSet;

    let mut result: HashSet<Vec<u8>> = HashSet::new();
    for &key in keys {
        let members = db.smembers(db_index, key)?;
        for m in members {
            result.insert(m);
        }
    }

    Ok(result.into_iter().collect())
}

pub struct SInterCardCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SInterCardCommand {
    fn name(&self) -> &str {
        "SINTERCARD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'sintercard' command");
        }
        let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 2 + numkeys {
            return RespValue::error("ERR syntax error");
        }
        let keys: Vec<&[u8]> = args[2..2 + numkeys]
            .iter()
            .filter_map(|a| a.as_bytes())
            .collect();

        let mut limit: usize = 0;
        let mut i = 2 + numkeys;
        while i < args.len() {
            if args[i].as_str().map(|s| s.to_uppercase()).as_deref() == Some("LIMIT") {
                i += 1;
                if i < args.len() {
                    limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                }
            }
            i += 1;
        }

        match sinter_sets(self.db.as_ref(), *db_index, &keys) {
            Ok(members) => {
                let count = if limit > 0 {
                    members.len().min(limit)
                } else {
                    members.len()
                };
                RespValue::integer(count as i64)
            }
            Err(e) => map_err(e),
        }
    }
}
