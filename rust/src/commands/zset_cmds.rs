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

fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        "inf".to_string()
    } else if score == f64::NEG_INFINITY {
        "-inf".to_string()
    } else if score == score.floor() && score.abs() < 1e15 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

pub struct ZAddCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZAddCommand {
    fn name(&self) -> &str {
        "ZADD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zadd' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        // Parse optional flags: NX, XX, GT, LT, CH
        let mut i = 2;
        // Skip any flags (NX, XX, GT, LT, CH)
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("NX") | Some("XX") | Some("GT") | Some("LT") | Some("CH") => i += 1,
                _ => break,
            }
        }

        if (args.len() - i) % 2 != 0 {
            return RespValue::error("ERR syntax error");
        }

        let mut pairs: Vec<(f64, &[u8])> = Vec::new();
        while i + 1 < args.len() {
            let score_str = match args[i].as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR not a float"),
            };
            let score: f64 = match score_str.to_lowercase().as_str() {
                "inf" | "+inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                s => match s.parse() {
                    Ok(f) => f,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                },
            };
            let member = match args[i + 1].as_bytes() {
                Some(m) => m,
                None => return RespValue::error("ERR invalid member"),
            };
            pairs.push((score, member));
            i += 2;
        }

        match self.db.zadd(*db_index, key, &pairs) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRemCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRemCommand {
    fn name(&self) -> &str {
        "ZREM"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zrem' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let members: Vec<&[u8]> = args[2..].iter().filter_map(|a| a.as_bytes()).collect();
        match self.db.zrem(*db_index, key, &members) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRangeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRangeCommand {
    fn name(&self) -> &str {
        "ZRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrange' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() { Some(s) => s.to_string(), None => return RespValue::error("ERR invalid min") };
        let max_str = match args[3].as_str() { Some(s) => s.to_string(), None => return RespValue::error("ERR invalid max") };

        let mut byscore = false;
        let mut bylex = false;
        let mut rev = false;
        let mut withscores = false;
        let mut offset: i64 = 0;
        let mut limit: i64 = -1;
        let mut i = 4;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("BYSCORE") => byscore = true,
                Some("BYLEX") => bylex = true,
                Some("REV") => rev = true,
                Some("WITHSCORES") => withscores = true,
                Some("LIMIT") => {
                    i += 1;
                    if i < args.len() { offset = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0); }
                    i += 1;
                    if i < args.len() { limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1); }
                }
                _ => {}
            }
            i += 1;
        }

        if byscore {
            // min/max are scores; REV means swap them and return descending
            let (min_s, max_s) = if rev { (&max_str, &min_str) } else { (&min_str, &max_str) };
            let (min, min_excl) = match parse_score(min_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not a float") };
            let (max, max_excl) = match parse_score(max_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not a float") };
            match self.db.zrangebyscore(*db_index, key, min, max, min_excl, max_excl, 0, -1) {
                Ok(mut items) => {
                    if rev { items.reverse(); }
                    let off = offset as usize;
                    if off < items.len() { items = items[off..].to_vec(); } else { items = vec![]; }
                    if limit >= 0 { items.truncate(limit as usize); }
                    if withscores {
                        let mut result = Vec::with_capacity(items.len() * 2);
                        for (m, s) in items { result.push(RespValue::bulk_bytes(m)); result.push(RespValue::bulk_str(&format_score(s))); }
                        RespValue::Array(Some(result))
                    } else {
                        RespValue::Array(Some(items.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect()))
                    }
                }
                Err(e) => map_err(e),
            }
        } else if bylex {
            let (min_s, max_s) = if rev { (&max_str, &min_str) } else { (&min_str, &max_str) };
            let (min, min_inc) = match parse_lex_bound(min_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not valid string range item") };
            let (max, max_inc) = match parse_lex_bound(max_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not valid string range item") };
            match self.db.zrangebylex(*db_index, key, &min, &max, min_inc, max_inc, 0, -1) {
                Ok(mut items) => {
                    if rev { items.reverse(); }
                    let off = offset as usize;
                    if off < items.len() { items = items[off..].to_vec(); } else { items = vec![]; }
                    if limit >= 0 { items.truncate(limit as usize); }
                    RespValue::Array(Some(items.into_iter().map(RespValue::bulk_bytes).collect()))
                }
                Err(e) => map_err(e),
            }
        } else {
            // rank-based
            let start: i64 = match min_str.parse() { Ok(n) => n, Err(_) => return RespValue::error("ERR value is not an integer or out of range") };
            let stop: i64 = match max_str.parse() { Ok(n) => n, Err(_) => return RespValue::error("ERR value is not an integer or out of range") };
            if rev {
                // REV: indices are from the end (highest score first). Convert to forward indices.
                let card = match self.db.zcard(*db_index, key) { Ok(n) => n, Err(e) => return map_err(e) };
                // start/stop are positions in rev order: pos 0 = highest (forward index count-1)
                // forward_start = count - 1 - stop, forward_stop = count - 1 - start
                let fwd_start = if stop < 0 { -stop - 1 } else { (card - 1 - stop).max(0) };
                let fwd_stop = if start < 0 { -start - 1 } else { (card - 1 - start).max(0) };
                match self.db.zrange(*db_index, key, fwd_start, fwd_stop) {
                    Ok(mut items) => {
                        items.reverse();
                        if withscores {
                            let mut result = Vec::with_capacity(items.len() * 2);
                            for (m, s) in items { result.push(RespValue::bulk_bytes(m)); result.push(RespValue::bulk_str(&format_score(s))); }
                            RespValue::Array(Some(result))
                        } else {
                            RespValue::Array(Some(items.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect()))
                        }
                    }
                    Err(e) => map_err(e),
                }
            } else {
                match self.db.zrange(*db_index, key, start, stop) {
                    Ok(items) => {
                        if withscores {
                            let mut result = Vec::with_capacity(items.len() * 2);
                            for (m, s) in items { result.push(RespValue::bulk_bytes(m)); result.push(RespValue::bulk_str(&format_score(s))); }
                            RespValue::Array(Some(result))
                        } else {
                            RespValue::Array(Some(items.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect()))
                        }
                    }
                    Err(e) => map_err(e),
                }
            }
        }
    }
}

pub struct ZRangeWithScoresCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRangeWithScoresCommand {
    fn name(&self) -> &str {
        "ZRANGEWITHSCORES"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments");
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
        match self.db.zrange(*db_index, key, start, stop) {
            Ok(items) => {
                let mut result = Vec::with_capacity(items.len() * 2);
                for (member, score) in items {
                    result.push(RespValue::bulk_bytes(member));
                    result.push(RespValue::bulk_str(&format_score(score)));
                }
                RespValue::Array(Some(result))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZScoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZScoreCommand {
    fn name(&self) -> &str {
        "ZSCORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'zscore' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let member = match args[2].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };
        match self.db.zscore(*db_index, key, member) {
            Ok(Some(score)) => RespValue::bulk_str(&format_score(score)),
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZCardCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZCardCommand {
    fn name(&self) -> &str {
        "ZCARD"
    }

    /// FIXED: calls db.zcard() which uses meta.count (O(1))
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'zcard' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match self.db.zcard(*db_index, key) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRankCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRankCommand {
    fn name(&self) -> &str {
        "ZRANK"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || args.len() > 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrank' command");
        }
        let key = match args[1].as_bytes() { Some(k) => k, None => return RespValue::error("ERR invalid key") };
        let member = match args[2].as_bytes() { Some(m) => m, None => return RespValue::error("ERR invalid member") };
        let withscore = args.get(3).and_then(|a| a.as_str()).map(|s| s.to_uppercase() == "WITHSCORE").unwrap_or(false);
        match self.db.zrank(*db_index, key, member) {
            Ok(Some(rank)) => {
                if withscore {
                    match self.db.zscore(*db_index, key, member) {
                        Ok(Some(score)) => RespValue::Array(Some(vec![
                            RespValue::integer(rank),
                            RespValue::bulk_str(&format_score(score)),
                        ])),
                        _ => RespValue::null_array(),
                    }
                } else {
                    RespValue::integer(rank)
                }
            }
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRevRangeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRevRangeCommand {
    fn name(&self) -> &str {
        "ZREVRANGE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrevrange' command");
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

        let withscores = args[4..]
            .iter()
            .any(|a| a.as_str().map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false));

        // ZREVRANGE indices are into the reversed list; convert to ascending indices
        let size = match self.db.zcard(*db_index, key) {
            Ok(n) => n,
            Err(e) => return map_err(e),
        };
        let (asc_start, asc_stop) = if size == 0 {
            (0i64, -1i64)
        } else {
            let len = size;
            // Normalize negative indices relative to reversed list
            let rev_start = if start < 0 { (len + start).max(0) } else { start.min(len - 1) };
            let rev_stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) };
            // Convert to ascending: asc_idx = (len - 1) - rev_idx
            let asc_s = (len - 1) - rev_stop;
            let asc_e = (len - 1) - rev_start;
            (asc_s, asc_e)
        };

        match self.db.zrange(*db_index, key, asc_start, asc_stop) {
            Ok(mut items) => {
                items.reverse();
                if withscores {
                    let mut result = Vec::with_capacity(items.len() * 2);
                    for (member, score) in items {
                        result.push(RespValue::bulk_bytes(member));
                        result.push(RespValue::bulk_str(&format_score(score)));
                    }
                    RespValue::Array(Some(result))
                } else {
                    let result: Vec<RespValue> =
                        items.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect();
                    RespValue::Array(Some(result))
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRevRankCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRevRankCommand {
    fn name(&self) -> &str {
        "ZREVRANK"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || args.len() > 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrevrank' command");
        }
        let key = match args[1].as_bytes() { Some(k) => k, None => return RespValue::error("ERR invalid key") };
        let member = match args[2].as_bytes() { Some(m) => m, None => return RespValue::error("ERR invalid member") };
        let withscore = args.get(3).and_then(|a| a.as_str()).map(|s| s.to_uppercase() == "WITHSCORE").unwrap_or(false);
        // Get card first
        let card = match self.db.zcard(*db_index, key) {
            Ok(n) => n,
            Err(e) => return map_err(e),
        };
        match self.db.zrank(*db_index, key, member) {
            Ok(Some(rank)) => {
                let revrank = card - 1 - rank;
                if withscore {
                    match self.db.zscore(*db_index, key, member) {
                        Ok(Some(score)) => RespValue::Array(Some(vec![
                            RespValue::integer(revrank),
                            RespValue::bulk_str(&format_score(score)),
                        ])),
                        _ => RespValue::null_array(),
                    }
                } else {
                    RespValue::integer(revrank)
                }
            }
            Ok(None) => RespValue::null_bulk(),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZIncrByCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZIncrByCommand {
    fn name(&self) -> &str {
        "ZINCRBY"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'zincrby' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let delta: f64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(f) => f,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let member = match args[3].as_bytes() {
            Some(m) => m,
            None => return RespValue::error("ERR invalid member"),
        };

        let current = match self.db.zscore(*db_index, key, member) {
            Ok(Some(s)) => s,
            Ok(None) => 0.0,
            Err(e) => return map_err(e),
        };

        let new_score = current + delta;
        match self.db.zadd(*db_index, key, &[(new_score, member)]) {
            Ok(_) => {
                BLOCKING_NOTIFIER.notify(*db_index, key.to_vec());
                RespValue::bulk_str(&format_score(new_score))
            }
            Err(e) => map_err(e),
        }
    }
}

fn parse_score(s: &str) -> Option<(f64, bool)> {
    // Returns (score, exclusive)
    if let Some(rest) = s.strip_prefix('(') {
        let score = match rest.to_lowercase().as_str() {
            "+inf" | "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            r => r.parse().ok()?,
        };
        Some((score, true))
    } else {
        let score = match s.to_lowercase().as_str() {
            "+inf" | "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            r => r.parse().ok()?,
        };
        Some((score, false))
    }
}

fn parse_lex_bound(s: &str) -> Option<(Vec<u8>, bool)> {
    // Returns (value, inclusive)
    if s == "-" || s == "+" {
        return Some((s.as_bytes().to_vec(), true));
    }
    if let Some(rest) = s.strip_prefix('[') {
        Some((rest.as_bytes().to_vec(), true))
    } else if let Some(rest) = s.strip_prefix('(') {
        Some((rest.as_bytes().to_vec(), false))
    } else {
        None
    }
}

// Updated ZADD with full flag support
pub struct ZAddCommandV2 {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZAddCommandV2 {
    fn name(&self) -> &str {
        "ZADD"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zadd' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut incr = false;
        let mut i = 2;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("NX") => { nx = true; i += 1; }
                Some("XX") => { xx = true; i += 1; }
                Some("GT") => { gt = true; i += 1; }
                Some("LT") => { lt = true; i += 1; }
                Some("CH") => { ch = true; i += 1; }
                Some("INCR") => { incr = true; i += 1; }
                _ => break,
            }
        }

        if (args.len() - i) % 2 != 0 {
            return RespValue::error("ERR syntax error");
        }

        let mut pairs: Vec<(f64, &[u8])> = Vec::new();
        while i + 1 < args.len() {
            let score_str = match args[i].as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR not a float"),
            };
            let score: f64 = match score_str.to_lowercase().as_str() {
                "inf" | "+inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                s => match s.parse() {
                    Ok(f) => f,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                },
            };
            let member = match args[i + 1].as_bytes() {
                Some(m) => m,
                None => return RespValue::error("ERR invalid member"),
            };
            pairs.push((score, member));
            i += 2;
        }

        if incr && pairs.len() != 1 {
            return RespValue::error("ERR INCR option supports a single increment-element pair");
        }

        match self.db.zadd_flags(*db_index, key, &pairs, nx, xx, gt, lt, ch, incr) {
            Ok((n, last_score)) => {
                // Notify blocked BZPOPMIN/BZPOPMAX waiters
                BLOCKING_NOTIFIER.notify(*db_index, key.to_vec());
                if incr {
                    match last_score {
                        Some(s) => RespValue::bulk_str(&format_score(s)),
                        None => RespValue::null_bulk(),
                    }
                } else {
                    RespValue::integer(n)
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZCountCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZCountCommand {
    fn name(&self) -> &str {
        "ZCOUNT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'zcount' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };

        let (min, min_excl) = match parse_score(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };
        let (max, max_excl) = match parse_score(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };

        match self.db.zcount(*db_index, key, min, max, min_excl, max_excl) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZLexCountCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZLexCountCommand {
    fn name(&self) -> &str {
        "ZLEXCOUNT"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'zlexcount' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };

        let (min, min_inc) = match parse_lex_bound(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };
        let (max, max_inc) = match parse_lex_bound(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };

        match self.db.zlexcount(*db_index, key, &min, &max, min_inc, max_inc) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRangeByScoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRangeByScoreCommand {
    fn name(&self) -> &str {
        "ZRANGEBYSCORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrangebyscore' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };

        let (min, min_excl) = match parse_score(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };
        let (max, max_excl) = match parse_score(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };

        let mut withscores = false;
        let mut offset: i64 = 0;
        let mut limit: i64 = -1;
        let mut i = 4;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("WITHSCORES") => withscores = true,
                Some("LIMIT") => {
                    i += 1;
                    if i < args.len() {
                        offset = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                    i += 1;
                    if i < args.len() {
                        limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
                    }
                }
                _ => {}
            }
            i += 1;
        }

        match self.db.zrangebyscore(*db_index, key, min, max, min_excl, max_excl, offset, limit) {
            Ok(items) => {
                if withscores {
                    let mut result = Vec::with_capacity(items.len() * 2);
                    for (member, score) in items {
                        result.push(RespValue::bulk_bytes(member));
                        result.push(RespValue::bulk_str(&format_score(score)));
                    }
                    RespValue::Array(Some(result))
                } else {
                    let result: Vec<RespValue> = items.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect();
                    RespValue::Array(Some(result))
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRangeByLexCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRangeByLexCommand {
    fn name(&self) -> &str {
        "ZRANGEBYLEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrangebylex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };

        let (min, min_inc) = match parse_lex_bound(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };
        let (max, max_inc) = match parse_lex_bound(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };

        let mut offset: i64 = 0;
        let mut limit: i64 = -1;
        let mut i = 4;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("LIMIT") => {
                    i += 1;
                    if i < args.len() {
                        offset = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                    i += 1;
                    if i < args.len() {
                        limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
                    }
                }
                _ => {}
            }
            i += 1;
        }

        match self.db.zrangebylex(*db_index, key, &min, &max, min_inc, max_inc, offset, limit) {
            Ok(members) => {
                let result: Vec<RespValue> = members.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(result))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRevRangeByScoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRevRangeByScoreCommand {
    fn name(&self) -> &str {
        "ZREVRANGEBYSCORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrevrangebyscore' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        // Note: ZREVRANGEBYSCORE takes max first, then min
        let max_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };
        let min_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };

        let (min, min_excl) = match parse_score(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };
        let (max, max_excl) = match parse_score(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };

        let mut withscores = false;
        let mut offset: i64 = 0;
        let mut limit: i64 = -1;
        let mut i = 4;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("WITHSCORES") => withscores = true,
                Some("LIMIT") => {
                    i += 1;
                    if i < args.len() {
                        offset = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                    i += 1;
                    if i < args.len() {
                        limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
                    }
                }
                _ => {}
            }
            i += 1;
        }

        // Get all items in range without limit, then reverse, then apply limit
        match self.db.zrangebyscore(*db_index, key, min, max, min_excl, max_excl, 0, -1) {
            Ok(mut items) => {
                items.reverse();
                let off = offset as usize;
                if off < items.len() { items = items[off..].to_vec(); } else { items = vec![]; }
                if limit >= 0 { items.truncate(limit as usize); }
                if withscores {
                    let mut result = Vec::with_capacity(items.len() * 2);
                    for (member, score) in items {
                        result.push(RespValue::bulk_bytes(member));
                        result.push(RespValue::bulk_str(&format_score(score)));
                    }
                    RespValue::Array(Some(result))
                } else {
                    let result: Vec<RespValue> = items.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect();
                    RespValue::Array(Some(result))
                }
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRevRangeByLexCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRevRangeByLexCommand {
    fn name(&self) -> &str {
        "ZREVRANGEBYLEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zrevrangebylex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        // ZREVRANGEBYLEX: max min (reversed from ZRANGEBYLEX)
        let max_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };
        let min_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };

        let (min, min_inc) = match parse_lex_bound(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };
        let (max, max_inc) = match parse_lex_bound(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };

        let mut offset: i64 = 0;
        let mut limit: i64 = -1;
        let mut i = 4;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("LIMIT") => {
                    i += 1;
                    if i < args.len() {
                        offset = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                    i += 1;
                    if i < args.len() {
                        limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
                    }
                }
                _ => {}
            }
            i += 1;
        }

        // Get all items without limit, then reverse, then apply limit
        match self.db.zrangebylex(*db_index, key, &min, &max, min_inc, max_inc, 0, -1) {
            Ok(mut members) => {
                members.reverse();
                let off = offset as usize;
                if off < members.len() { members = members[off..].to_vec(); } else { members = vec![]; }
                if limit >= 0 { members.truncate(limit as usize); }
                let result: Vec<RespValue> = members.into_iter().map(RespValue::bulk_bytes).collect();
                RespValue::Array(Some(result))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZPopMaxCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZPopMaxCommand {
    fn name(&self) -> &str {
        "ZPOPMAX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'zpopmax' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let count: i64 = if args.len() >= 3 {
            args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(1)
        } else {
            1
        };

        match self.db.zpopmax(*db_index, key, count) {
            Ok(items) => {
                let mut result = Vec::with_capacity(items.len() * 2);
                for (member, score) in items {
                    result.push(RespValue::bulk_bytes(member));
                    result.push(RespValue::bulk_str(&format_score(score)));
                }
                RespValue::Array(Some(result))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZPopMinCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZPopMinCommand {
    fn name(&self) -> &str {
        "ZPOPMIN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'zpopmin' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let count: i64 = if args.len() >= 3 {
            args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(1)
        } else {
            1
        };

        match self.db.zpopmin(*db_index, key, count) {
            Ok(items) => {
                let mut result = Vec::with_capacity(items.len() * 2);
                for (member, score) in items {
                    result.push(RespValue::bulk_bytes(member));
                    result.push(RespValue::bulk_str(&format_score(score)));
                }
                RespValue::Array(Some(result))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRemRangeByScoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRemRangeByScoreCommand {
    fn name(&self) -> &str {
        "ZREMRANGEBYSCORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'zremrangebyscore' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };

        let (min, min_excl) = match parse_score(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };
        let (max, max_excl) = match parse_score(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not a float"),
        };

        match self.db.zremrangebyscore(*db_index, key, min, max, min_excl, max_excl) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRemRangeByLexCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRemRangeByLexCommand {
    fn name(&self) -> &str {
        "ZREMRANGEBYLEX"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'zremrangebylex' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[2].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR invalid max"),
        };

        let (min, min_inc) = match parse_lex_bound(min_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };
        let (max, max_inc) = match parse_lex_bound(max_str) {
            Some(v) => v,
            None => return RespValue::error("ERR min or max is not valid string range item"),
        };

        match self.db.zremrangebylex(*db_index, key, &min, &max, min_inc, max_inc) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZRemRangeByRankCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRemRangeByRankCommand {
    fn name(&self) -> &str {
        "ZREMRANGEBYRANK"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 4 {
            return RespValue::error("ERR wrong number of arguments for 'zremrangebyrank' command");
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

        match self.db.zremrangebyrank(*db_index, key, start, stop) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZMScoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZMScoreCommand {
    fn name(&self) -> &str {
        "ZMSCORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zmscore' command");
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
                    results.push(RespValue::null_bulk());
                    continue;
                }
            };
            match self.db.zscore(*db_index, key, member) {
                Ok(Some(score)) => results.push(RespValue::bulk_str(&format_score(score))),
                Ok(None) => results.push(RespValue::null_bulk()),
                Err(_) => results.push(RespValue::null_bulk()),
            }
        }

        RespValue::Array(Some(results))
    }
}

pub struct ZUnionStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZUnionStoreCommand {
    fn name(&self) -> &str {
        "ZUNIONSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        zstore_execute(self.db.as_ref(), *db_index, args, "ZUNIONSTORE", false)
    }
}

pub struct ZInterStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZInterStoreCommand {
    fn name(&self) -> &str {
        "ZINTERSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        zstore_execute(self.db.as_ref(), *db_index, args, "ZINTERSTORE", true)
    }
}

fn zstore_execute(
    db: &crate::database::RedisDatabase,
    db_index: usize,
    args: &[RespValue],
    cmd: &str,
    intersect: bool,
) -> RespValue {
    if args.len() < 4 {
        return RespValue::error(&format!("ERR wrong number of arguments for '{}' command", cmd.to_lowercase()));
    }
    let dst = match args[1].as_bytes() {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if args.len() < 3 + numkeys {
        return RespValue::error("ERR syntax error");
    }

    let keys: Vec<&[u8]> = args[3..3 + numkeys].iter().filter_map(|a| a.as_bytes()).collect();

    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM".to_string();
    let mut i = 3 + numkeys;

    while i < args.len() {
        match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("WEIGHTS") => {
                for j in 0..numkeys {
                    i += 1;
                    if i < args.len() {
                        weights[j] = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(1.0);
                    }
                }
            }
            Some("AGGREGATE") => {
                i += 1;
                if i < args.len() {
                    aggregate = args[i].as_str().unwrap_or("SUM").to_uppercase();
                }
            }
            _ => {}
        }
        i += 1;
    }

    if intersect {
        match db.zinterstore(db_index, dst, &keys, &weights, &aggregate) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    } else {
        match db.zunionstore(db_index, dst, &keys, &weights, &aggregate) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZDiffStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZDiffStoreCommand {
    fn name(&self) -> &str {
        "ZDIFFSTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zdiffstore' command");
        }
        let dst = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR syntax error");
        }
        let keys: Vec<&[u8]> = args[3..3 + numkeys].iter().filter_map(|a| a.as_bytes()).collect();

        match self.db.zdiffstore(*db_index, dst, &keys) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZDiffCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZDiffCommand {
    fn name(&self) -> &str {
        "ZDIFF"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zdiff' command");
        }
        let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 2 + numkeys {
            return RespValue::error("ERR syntax error");
        }

        let keys: Vec<&[u8]> = args[2..2 + numkeys].iter().filter_map(|a| a.as_bytes()).collect();
        let withscores = args[2 + numkeys..]
            .iter()
            .any(|a| a.as_str().map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false));

        // Compute diff in memory using a temp key approach
        use std::collections::HashSet;
        let first_all = match self.db.zrange_all(*db_index, keys[0]) {
            Ok(v) => v,
            Err(e) => return map_err(e),
        };

        let mut exclude: HashSet<Vec<u8>> = HashSet::new();
        for &key in &keys[1..] {
            match self.db.zrange_all(*db_index, key) {
                Ok(items) => {
                    for (m, _) in items {
                        exclude.insert(m);
                    }
                }
                Err(e) => return map_err(e),
            }
        }

        let result: Vec<(Vec<u8>, f64)> = first_all
            .into_iter()
            .filter(|(m, _)| !exclude.contains(m))
            .collect();

        if withscores {
            let mut out = Vec::with_capacity(result.len() * 2);
            for (m, s) in result {
                out.push(RespValue::bulk_bytes(m));
                out.push(RespValue::bulk_str(&format_score(s)));
            }
            RespValue::Array(Some(out))
        } else {
            let out: Vec<RespValue> = result.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect();
            RespValue::Array(Some(out))
        }
    }
}

pub struct ZUnionCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZUnionCommand {
    fn name(&self) -> &str {
        "ZUNION"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zunion' command");
        }
        let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 2 + numkeys {
            return RespValue::error("ERR syntax error");
        }

        let keys: Vec<&[u8]> = args[2..2 + numkeys].iter().filter_map(|a| a.as_bytes()).collect();

        let mut weights: Vec<f64> = vec![1.0; numkeys];
        let mut aggregate = "SUM".to_string();
        let mut withscores = false;
        let mut i = 2 + numkeys;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("WITHSCORES") => withscores = true,
                Some("WEIGHTS") => {
                    for j in 0..numkeys {
                        i += 1;
                        if i < args.len() {
                            weights[j] = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(1.0);
                        }
                    }
                }
                Some("AGGREGATE") => {
                    i += 1;
                    if i < args.len() {
                        aggregate = args[i].as_str().unwrap_or("SUM").to_uppercase();
                    }
                }
                _ => {}
            }
            i += 1;
        }

        use std::collections::HashMap;
        let mut combined: HashMap<Vec<u8>, f64> = HashMap::new();
        for (ki, &key) in keys.iter().enumerate() {
            let w = weights.get(ki).copied().unwrap_or(1.0);
            match self.db.zrange_all(*db_index, key) {
                Ok(items) => {
                    for (member, score) in items {
                        let weighted = score * w;
                        let entry = combined.entry(member).or_insert(match aggregate.as_str() {
                            "MIN" => f64::INFINITY,
                            "MAX" => f64::NEG_INFINITY,
                            _ => 0.0,
                        });
                        *entry = match aggregate.as_str() {
                            "MIN" => entry.min(weighted),
                            "MAX" => entry.max(weighted),
                            _ => *entry + weighted,
                        };
                    }
                }
                Err(e) => return map_err(e),
            }
        }

        let mut result: Vec<(Vec<u8>, f64)> = combined.into_iter().collect();
        result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        if withscores {
            let mut out = Vec::with_capacity(result.len() * 2);
            for (m, s) in result {
                out.push(RespValue::bulk_bytes(m));
                out.push(RespValue::bulk_str(&format_score(s)));
            }
            RespValue::Array(Some(out))
        } else {
            let out: Vec<RespValue> = result.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect();
            RespValue::Array(Some(out))
        }
    }
}

pub struct ZInterCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZInterCommand {
    fn name(&self) -> &str {
        "ZINTER"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zinter' command");
        }
        let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 2 + numkeys {
            return RespValue::error("ERR syntax error");
        }

        let keys: Vec<&[u8]> = args[2..2 + numkeys].iter().filter_map(|a| a.as_bytes()).collect();

        let mut weights: Vec<f64> = vec![1.0; numkeys];
        let mut aggregate = "SUM".to_string();
        let mut withscores = false;
        let mut i = 2 + numkeys;

        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("WITHSCORES") => withscores = true,
                Some("WEIGHTS") => {
                    for j in 0..numkeys {
                        i += 1;
                        if i < args.len() {
                            weights[j] = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(1.0);
                        }
                    }
                }
                Some("AGGREGATE") => {
                    i += 1;
                    if i < args.len() {
                        aggregate = args[i].as_str().unwrap_or("SUM").to_uppercase();
                    }
                }
                _ => {}
            }
            i += 1;
        }

        use std::collections::HashMap;

        if keys.is_empty() {
            return RespValue::Array(Some(vec![]));
        }

        let w0 = weights.get(0).copied().unwrap_or(1.0);
        let mut combined: HashMap<Vec<u8>, f64> = match self.db.zrange_all(*db_index, keys[0]) {
            Ok(items) => items.into_iter().map(|(m, s)| (m, s * w0)).collect(),
            Err(e) => return map_err(e),
        };

        for (ki, &key) in keys[1..].iter().enumerate() {
            let wi = weights.get(ki + 1).copied().unwrap_or(1.0);
            let others: HashMap<Vec<u8>, f64> = match self.db.zrange_all(*db_index, key) {
                Ok(items) => items.into_iter().map(|(m, s)| (m, s * wi)).collect(),
                Err(e) => return map_err(e),
            };
            combined = combined
                .into_iter()
                .filter_map(|(m, s)| {
                    others.get(&m).map(|&s2| {
                        let new_s = match aggregate.as_str() {
                            "MIN" => s.min(s2),
                            "MAX" => s.max(s2),
                            _ => s + s2,
                        };
                        (m, new_s)
                    })
                })
                .collect();
        }

        let mut result: Vec<(Vec<u8>, f64)> = combined.into_iter().collect();
        result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        if withscores {
            let mut out = Vec::with_capacity(result.len() * 2);
            for (m, s) in result {
                out.push(RespValue::bulk_bytes(m));
                out.push(RespValue::bulk_str(&format_score(s)));
            }
            RespValue::Array(Some(out))
        } else {
            let out: Vec<RespValue> = result.into_iter().map(|(m, _)| RespValue::bulk_bytes(m)).collect();
            RespValue::Array(Some(out))
        }
    }
}

pub struct ZRandMemberCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRandMemberCommand {
    fn name(&self) -> &str {
        "ZRANDMEMBER"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'zrandmember' command");
        }
        let key = match args[1].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };

        let all = match self.db.zrange_all(*db_index, key) {
            Ok(v) => v,
            Err(e) => return map_err(e),
        };

        if args.len() == 2 {
            match all.into_iter().next() {
                Some((m, _)) => return RespValue::bulk_bytes(m),
                None => return RespValue::null_bulk(),
            }
        }

        let count: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        let withscores = args.get(3)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase() == "WITHSCORES")
            .unwrap_or(false);

        if all.is_empty() {
            return RespValue::Array(Some(vec![]));
        }

        let len = all.len();
        let mut result = Vec::new();

        if count >= 0 {
            let take = (count as usize).min(len);
            for (m, s) in all.into_iter().take(take) {
                result.push(RespValue::bulk_bytes(m));
                if withscores {
                    result.push(RespValue::bulk_str(&format_score(s)));
                }
            }
        } else {
            let abs = (-count) as usize;
            for i in 0..abs {
                let idx = i % len;
                result.push(RespValue::bulk_bytes(all[idx].0.clone()));
                if withscores {
                    result.push(RespValue::bulk_str(&format_score(all[idx].1)));
                }
            }
        }

        RespValue::Array(Some(result))
    }
}

pub struct ZScanCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZScanCommand {
    fn name(&self) -> &str {
        "ZSCAN"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zscan' command");
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
                Some("COUNT") => { i += 1; }
                _ => {}
            }
            i += 1;
        }

        let all = match self.db.zrange_all(*db_index, key) {
            Ok(v) => v,
            Err(e) => return map_err(e),
        };

        let mut items = Vec::new();
        for (member, score) in all {
            let s = String::from_utf8_lossy(&member).into_owned();
            if let Some(ref pat) = pattern {
                if !crate::database::RedisDatabase::glob_match(pat, &s) {
                    continue;
                }
            }
            items.push(RespValue::bulk_bytes(member));
            items.push(RespValue::bulk_str(&format_score(score)));
        }

        RespValue::Array(Some(vec![
            RespValue::bulk_str("0"),
            RespValue::Array(Some(items)),
        ]))
    }
}

pub struct ZRangeStoreCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZRangeStoreCommand {
    fn name(&self) -> &str {
        "ZRANGESTORE"
    }

    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'zrangestore' command");
        }
        let dst = match args[1].as_bytes() {
            Some(k) => k.to_vec(),
            None => return RespValue::error("ERR invalid key"),
        };
        let src = match args[2].as_bytes() {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let min_str = match args[3].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid min"),
        };
        let max_str = match args[4].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid max"),
        };

        let mut byscore = false;
        let mut bylex = false;
        let mut rev = false;
        let mut offset: i64 = 0;
        let mut limit: i64 = -1;
        let mut i = 5;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("BYSCORE") => byscore = true,
                Some("BYLEX") => bylex = true,
                Some("REV") => rev = true,
                Some("LIMIT") => {
                    i += 1;
                    if i < args.len() { offset = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(0); }
                    i += 1;
                    if i < args.len() { limit = args[i].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1); }
                }
                _ => {}
            }
            i += 1;
        }

        let items: Vec<(Vec<u8>, f64)> = if byscore {
            let (min_s, max_s) = if rev { (&max_str, &min_str) } else { (&min_str, &max_str) };
            let (min, min_excl) = match parse_score(min_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not a float") };
            let (max, max_excl) = match parse_score(max_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not a float") };
            match self.db.zrangebyscore(*db_index, src, min, max, min_excl, max_excl, 0, -1) {
                Ok(mut v) => {
                    if rev { v.reverse(); }
                    let off = offset as usize;
                    if off < v.len() { v = v[off..].to_vec(); } else { v = vec![]; }
                    if limit >= 0 { v.truncate(limit as usize); }
                    v
                }
                Err(e) => return map_err(e),
            }
        } else if bylex {
            let (min_s, max_s) = if rev { (&max_str, &min_str) } else { (&min_str, &max_str) };
            let (min, min_inc) = match parse_lex_bound(min_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not valid string range item") };
            let (max, max_inc) = match parse_lex_bound(max_s) { Some(v) => v, None => return RespValue::error("ERR min or max is not valid string range item") };
            match self.db.zrangebylex(*db_index, src, &min, &max, min_inc, max_inc, 0, -1) {
                Ok(mut v) => {
                    if rev { v.reverse(); }
                    let off = offset as usize;
                    if off < v.len() { v = v[off..].to_vec(); } else { v = vec![]; }
                    if limit >= 0 { v.truncate(limit as usize); }
                    // Need scores for storage - fetch them
                    let mut result = Vec::new();
                    for m in v {
                        let score = self.db.zscore(*db_index, src, &m).unwrap_or(None).unwrap_or(0.0);
                        result.push((m, score));
                    }
                    result
                }
                Err(e) => return map_err(e),
            }
        } else {
            let start: i64 = match min_str.parse() { Ok(n) => n, Err(_) => return RespValue::error("ERR value is not an integer or out of range") };
            let stop: i64 = match max_str.parse() { Ok(n) => n, Err(_) => return RespValue::error("ERR value is not an integer or out of range") };
            match self.db.zrange(*db_index, src, start, stop) {
                Ok(mut v) => { if rev { v.reverse(); } v }
                Err(e) => return map_err(e),
            }
        };

        let count = items.len() as i64;
        let _ = self.db.delete(*db_index, &[dst.as_slice()]);
        if !items.is_empty() {
            let pairs: Vec<(f64, &[u8])> = items.iter().map(|(m, s)| (*s, m.as_slice())).collect();
            if let Err(e) = self.db.zadd(*db_index, &dst, &pairs) {
                return map_err(e);
            }
        }

        RespValue::integer(count)
    }
}

pub struct BzpopmaxCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BzpopmaxCommand {
    fn name(&self) -> &str { "BZPOPMAX" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // BZPOPMAX key [key...] timeout -> non-blocking ZPOPMAX
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'bzpopmax' command");
        }
        let keys: Vec<_> = args[1..args.len()-1].iter().filter_map(|a| a.as_bytes()).collect();
        for key in &keys {
            match self.db.zpopmax(*db_index, key, 1) {
                Ok(items) if !items.is_empty() => {
                    let (member, score) = &items[0];
                    return RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(key.to_vec()),
                        RespValue::bulk_bytes(member.clone()),
                        RespValue::bulk_str(&format_score(*score)),
                    ]));
                }
                Ok(_) => continue,
                Err(e) => return map_err(e),
            }
        }
        RespValue::null_array()
    }
}

pub struct BzpopminCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BzpopminCommand {
    fn name(&self) -> &str { "BZPOPMIN" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'bzpopmin' command");
        }
        let keys: Vec<_> = args[1..args.len()-1].iter().filter_map(|a| a.as_bytes()).collect();
        for key in &keys {
            match self.db.zpopmin(*db_index, key, 1) {
                Ok(items) if !items.is_empty() => {
                    let (member, score) = &items[0];
                    return RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(key.to_vec()),
                        RespValue::bulk_bytes(member.clone()),
                        RespValue::bulk_str(&format_score(*score)),
                    ]));
                }
                Ok(_) => continue,
                Err(e) => return map_err(e),
            }
        }
        RespValue::null_array()
    }
}

pub struct ZInterCardCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZInterCardCommand {
    fn name(&self) -> &str { "ZINTERCARD" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'zintercard' command");
        }
        let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if numkeys == 0 || args.len() < 2 + numkeys {
            return RespValue::error("ERR numkeys can't be zero or larger than number of keys");
        }
        let keys: Vec<&[u8]> = args[2..2+numkeys].iter().filter_map(|a| a.as_bytes()).collect();
        let limit: i64 = {
            let rest = &args[2+numkeys..];
            let mut l = 0i64;
            let mut i = 0;
            while i < rest.len() {
                if rest[i].as_str().map(|s| s.to_uppercase() == "LIMIT").unwrap_or(false) {
                    if i + 1 < rest.len() {
                        l = rest[i+1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                    }
                }
                i += 1;
            }
            l
        };
        match self.db.zintercard(*db_index, &keys, limit) {
            Ok(n) => RespValue::integer(n),
            Err(e) => map_err(e),
        }
    }
}

pub struct ZmpopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for ZmpopCommand {
    fn name(&self) -> &str { "ZMPOP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'zmpop' command");
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
        let pop_min = direction == "MIN";
        let count: i64 = {
            let rest = &args[3+numkeys..];
            let mut c = 1i64;
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
        match self.db.zpop_multi(*db_index, &keys, pop_min, count) {
            Ok(None) => RespValue::null_array(),
            Ok(Some((key, items))) => {
                let elements: Vec<RespValue> = items.into_iter().map(|(member, score)| {
                    RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(member),
                        RespValue::bulk_str(&format_score(score)),
                    ]))
                }).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_bytes(key),
                    RespValue::Array(Some(elements)),
                ]))
            }
            Err(e) => map_err(e),
        }
    }
}

pub struct BzmpopCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for BzmpopCommand {
    fn name(&self) -> &str { "BZMPOP" }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'bzmpop' command");
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
        let pop_min = direction == "MIN";
        let count: i64 = {
            let rest = &args[4+numkeys..];
            let mut c = 1i64;
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
        match self.db.zpop_multi(*db_index, &keys, pop_min, count) {
            Ok(None) => RespValue::null_array(),
            Ok(Some((key, items))) => {
                let elements: Vec<RespValue> = items.into_iter().map(|(member, score)| {
                    RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(member),
                        RespValue::bulk_str(&format_score(score)),
                    ]))
                }).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_bytes(key),
                    RespValue::Array(Some(elements)),
                ]))
            }
            Err(e) => map_err(e),
        }
    }
}
