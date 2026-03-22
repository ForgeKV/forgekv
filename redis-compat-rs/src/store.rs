use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::error::{RedisError, RedisResult};
use crate::value::RedisValue;

#[derive(Debug, Clone)]
enum StoredValue {
    Str(String),
    Int(i64),
    List(Vec<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
}

impl StoredValue {
    fn type_name(&self) -> &'static str {
        match self {
            StoredValue::Str(_) | StoredValue::Int(_) => "string",
            StoredValue::List(_) => "list",
            StoredValue::Set(_) => "set",
            StoredValue::Hash(_) => "hash",
        }
    }
}

#[derive(Debug)]
struct Entry {
    value: StoredValue,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: StoredValue) -> Self {
        Entry { value, expires_at: None }
    }

    fn with_expiry(value: StoredValue, expires_at: Instant) -> Self {
        Entry { value, expires_at: Some(expires_at) }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.map(|t| Instant::now() >= t).unwrap_or(false)
    }
}

pub struct RedisStore {
    data: HashMap<String, Entry>,
}

fn validate_key(key: &str) -> RedisResult<()> {
    if key.is_empty() {
        return Err(RedisError::InvalidArgument("key must not be empty".into()));
    }
    Ok(())
}

fn wrong_type_err() -> RedisError {
    RedisError::WrongType(
        "Operation against a key holding the wrong kind of value".into(),
    )
}

impl RedisStore {
    pub fn new() -> Self {
        RedisStore { data: HashMap::new() }
    }

    fn get_live(&self, key: &str) -> Option<&Entry> {
        self.data.get(key).filter(|e| !e.is_expired())
    }

    fn remove_expired(&mut self, key: &str) -> bool {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                self.data.remove(key);
                return true;
            }
        }
        false
    }

    // ── String commands ──────────────────────────────────────────────────

    pub fn set(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<Duration>,
        nx: bool,
        xx: bool,
    ) -> RedisResult<RedisValue> {
        validate_key(key)?;
        let exists = self.get_live(key).is_some();
        if nx && exists {
            return Ok(RedisValue::null());
        }
        if xx && !exists {
            return Ok(RedisValue::null());
        }
        let expires_at = expiry.map(|d| Instant::now() + d);
        let entry = match expires_at {
            Some(t) => Entry::with_expiry(StoredValue::Str(value.to_owned()), t),
            None => Entry::new(StoredValue::Str(value.to_owned())),
        };
        self.data.insert(key.to_owned(), entry);
        Ok(RedisValue::ok())
    }

    pub fn get(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        match self.data.get(key) {
            None => Ok(RedisValue::null()),
            Some(entry) => match &entry.value {
                StoredValue::Str(s) => Ok(RedisValue::from_string(s.clone())),
                StoredValue::Int(n) => Ok(RedisValue::from_string(n.to_string())),
                _ => Err(wrong_type_err()),
            },
        }
    }

    pub fn mset(&mut self, pairs: &[(&str, &str)]) -> RedisResult<RedisValue> {
        if pairs.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one pair must be provided".into(),
            ));
        }
        for (k, _) in pairs {
            validate_key(k)?;
        }
        for (k, v) in pairs {
            self.data.insert(k.to_string(), Entry::new(StoredValue::Str(v.to_string())));
        }
        Ok(RedisValue::ok())
    }

    pub fn mget(&mut self, keys: &[&str]) -> RedisResult<RedisValue> {
        if keys.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one key must be provided".into(),
            ));
        }
        let mut results: Vec<Option<String>> = Vec::with_capacity(keys.len());
        for &k in keys {
            self.remove_expired(k);
            let val = match self.data.get(k) {
                None => None,
                Some(entry) => match &entry.value {
                    StoredValue::Str(s) => Some(s.clone()),
                    StoredValue::Int(n) => Some(n.to_string()),
                    _ => None,
                },
            };
            results.push(val);
        }
        Ok(RedisValue::from_list(results))
    }

    pub fn incr(&mut self, key: &str) -> RedisResult<RedisValue> {
        self.incr_by(key, 1)
    }

    pub fn decr(&mut self, key: &str) -> RedisResult<RedisValue> {
        self.incr_by(key, -1)
    }

    pub fn incr_by(&mut self, key: &str, delta: i64) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        let next = match self.data.get(key) {
            None => delta,
            Some(entry) => match &entry.value {
                StoredValue::Int(n) => n.checked_add(delta).ok_or_else(|| {
                    RedisError::OutOfRange("increment or decrement would overflow".into())
                })?,
                StoredValue::Str(s) => {
                    let n: i64 = s.parse().map_err(|_| {
                        RedisError::InvalidArgument(
                            "value is not an integer or out of range".into(),
                        )
                    })?;
                    n.checked_add(delta).ok_or_else(|| {
                        RedisError::OutOfRange("increment or decrement would overflow".into())
                    })?
                }
                _ => return Err(wrong_type_err()),
            },
        };
        self.data.insert(key.to_owned(), Entry::new(StoredValue::Int(next)));
        Ok(RedisValue::from_integer(next))
    }

    pub fn append(&mut self, key: &str, value: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        let new_str = match self.data.get(key) {
            None => value.to_owned(),
            Some(entry) => match &entry.value {
                StoredValue::Str(s) => format!("{}{}", s, value),
                StoredValue::Int(n) => format!("{}{}", n, value),
                _ => return Err(wrong_type_err()),
            },
        };
        let len = new_str.len() as i64;
        self.data.insert(key.to_owned(), Entry::new(StoredValue::Str(new_str)));
        Ok(RedisValue::from_integer(len))
    }

    pub fn strlen(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        match self.data.get(key) {
            None => Ok(RedisValue::from_integer(0)),
            Some(entry) => match &entry.value {
                StoredValue::Str(s) => Ok(RedisValue::from_integer(s.len() as i64)),
                StoredValue::Int(n) => Ok(RedisValue::from_integer(n.to_string().len() as i64)),
                _ => Err(wrong_type_err()),
            },
        }
    }

    pub fn setnx(&mut self, key: &str, value: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        if self.data.contains_key(key) {
            return Ok(RedisValue::from_integer(0));
        }
        self.data.insert(key.to_owned(), Entry::new(StoredValue::Str(value.to_owned())));
        Ok(RedisValue::from_integer(1))
    }

    // ── List commands ────────────────────────────────────────────────────

    fn get_or_create_list(&mut self, key: &str) -> RedisResult<&mut Vec<String>> {
        self.remove_expired(key);
        if !self.data.contains_key(key) {
            self.data.insert(key.to_owned(), Entry::new(StoredValue::List(Vec::new())));
        }
        match self.data.get_mut(key) {
            Some(entry) => match &mut entry.value {
                StoredValue::List(l) => Ok(l),
                _ => Err(wrong_type_err()),
            },
            None => unreachable!(),
        }
    }

    fn get_existing_list(&mut self, key: &str) -> RedisResult<Option<&mut Vec<String>>> {
        self.remove_expired(key);
        match self.data.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &mut entry.value {
                StoredValue::List(l) => Ok(Some(l)),
                _ => Err(wrong_type_err()),
            },
        }
    }

    fn normalize_list_index(index: i64, len: usize) -> i64 {
        if index < 0 {
            (len as i64 + index).max(0)
        } else {
            index
        }
    }

    pub fn lpush(&mut self, key: &str, values: &[&str]) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if values.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one value must be provided".into(),
            ));
        }
        let list = self.get_or_create_list(key)?;
        for &v in values {
            list.insert(0, v.to_owned());
        }
        let len = list.len() as i64;
        Ok(RedisValue::from_integer(len))
    }

    pub fn rpush(&mut self, key: &str, values: &[&str]) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if values.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one value must be provided".into(),
            ));
        }
        let list = self.get_or_create_list(key)?;
        for &v in values {
            list.push(v.to_owned());
        }
        let len = list.len() as i64;
        Ok(RedisValue::from_integer(len))
    }

    pub fn lpop(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_list(key)? {
            None => Ok(RedisValue::null()),
            Some(list) => {
                if list.is_empty() {
                    return Ok(RedisValue::null());
                }
                let val = list.remove(0);
                if list.is_empty() {
                    self.data.remove(key);
                }
                Ok(RedisValue::from_string(val))
            }
        }
    }

    pub fn rpop(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_list(key)? {
            None => Ok(RedisValue::null()),
            Some(list) => {
                match list.pop() {
                    None => Ok(RedisValue::null()),
                    Some(val) => {
                        if list.is_empty() {
                            self.data.remove(key);
                        }
                        Ok(RedisValue::from_string(val))
                    }
                }
            }
        }
    }

    pub fn llen(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_list(key)? {
            None => Ok(RedisValue::from_integer(0)),
            Some(list) => Ok(RedisValue::from_integer(list.len() as i64)),
        }
    }

    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        match self.data.get(key) {
            None => return Ok(RedisValue::from_list(vec![])),
            Some(entry) => match &entry.value {
                StoredValue::List(list) => {
                    let len = list.len();
                    if len == 0 {
                        return Ok(RedisValue::from_list(vec![]));
                    }
                    let s = Self::normalize_list_index(start, len) as usize;
                    let e = {
                        let e = Self::normalize_list_index(stop, len);
                        if e < 0 { return Ok(RedisValue::from_list(vec![])); }
                        (e as usize).min(len - 1)
                    };
                    if s > e || s >= len {
                        return Ok(RedisValue::from_list(vec![]));
                    }
                    let result: Vec<Option<String>> = list[s..=e]
                        .iter()
                        .map(|v| Some(v.clone()))
                        .collect();
                    Ok(RedisValue::from_list(result))
                }
                _ => Err(wrong_type_err()),
            },
        }
    }

    pub fn lindex(&mut self, key: &str, index: i64) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        match self.data.get(key) {
            None => Ok(RedisValue::null()),
            Some(entry) => match &entry.value {
                StoredValue::List(list) => {
                    let i = Self::normalize_list_index(index, list.len());
                    if i < 0 || i as usize >= list.len() {
                        return Ok(RedisValue::null());
                    }
                    Ok(RedisValue::from_string(list[i as usize].clone()))
                }
                _ => Err(wrong_type_err()),
            },
        }
    }

    // ── Hash commands ────────────────────────────────────────────────────

    fn get_or_create_hash(&mut self, key: &str) -> RedisResult<&mut HashMap<String, String>> {
        self.remove_expired(key);
        if !self.data.contains_key(key) {
            self.data.insert(key.to_owned(), Entry::new(StoredValue::Hash(HashMap::new())));
        }
        match self.data.get_mut(key) {
            Some(entry) => match &mut entry.value {
                StoredValue::Hash(h) => Ok(h),
                _ => Err(wrong_type_err()),
            },
            None => unreachable!(),
        }
    }

    fn get_existing_hash(
        &mut self,
        key: &str,
    ) -> RedisResult<Option<&mut HashMap<String, String>>> {
        self.remove_expired(key);
        match self.data.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &mut entry.value {
                StoredValue::Hash(h) => Ok(Some(h)),
                _ => Err(wrong_type_err()),
            },
        }
    }

    pub fn hset(&mut self, key: &str, field: &str, value: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if field.is_empty() {
            return Err(RedisError::InvalidArgument("field must not be empty".into()));
        }
        let hash = self.get_or_create_hash(key)?;
        let is_new = !hash.contains_key(field);
        hash.insert(field.to_owned(), value.to_owned());
        Ok(RedisValue::from_integer(if is_new { 1 } else { 0 }))
    }

    pub fn hget(&mut self, key: &str, field: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if field.is_empty() {
            return Err(RedisError::InvalidArgument("field must not be empty".into()));
        }
        match self.get_existing_hash(key)? {
            None => Ok(RedisValue::null()),
            Some(hash) => match hash.get(field) {
                None => Ok(RedisValue::null()),
                Some(v) => Ok(RedisValue::from_string(v.clone())),
            },
        }
    }

    pub fn hdel(&mut self, key: &str, fields: &[&str]) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if fields.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one field must be provided".into(),
            ));
        }
        match self.get_existing_hash(key)? {
            None => Ok(RedisValue::from_integer(0)),
            Some(hash) => {
                let removed = fields.iter().filter(|&&f| !f.is_empty() && hash.remove(f).is_some()).count() as i64;
                if hash.is_empty() {
                    self.data.remove(key);
                }
                Ok(RedisValue::from_integer(removed))
            }
        }
    }

    pub fn hgetall(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_hash(key)? {
            None => Ok(RedisValue::from_hash(HashMap::new())),
            Some(hash) => Ok(RedisValue::from_hash(hash.clone())),
        }
    }

    pub fn hexists(&mut self, key: &str, field: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if field.is_empty() {
            return Err(RedisError::InvalidArgument("field must not be empty".into()));
        }
        match self.get_existing_hash(key)? {
            None => Ok(RedisValue::from_integer(0)),
            Some(hash) => Ok(RedisValue::from_integer(if hash.contains_key(field) { 1 } else { 0 })),
        }
    }

    pub fn hlen(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_hash(key)? {
            None => Ok(RedisValue::from_integer(0)),
            Some(hash) => Ok(RedisValue::from_integer(hash.len() as i64)),
        }
    }

    pub fn hmset(&mut self, key: &str, pairs: &[(&str, &str)]) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if pairs.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one field-value pair must be provided".into(),
            ));
        }
        for (f, _) in pairs {
            if f.is_empty() {
                return Err(RedisError::InvalidArgument("field must not be empty".into()));
            }
        }
        let hash = self.get_or_create_hash(key)?;
        for (f, v) in pairs {
            hash.insert(f.to_string(), v.to_string());
        }
        Ok(RedisValue::ok())
    }

    // ── Set commands ─────────────────────────────────────────────────────

    fn get_or_create_set(&mut self, key: &str) -> RedisResult<&mut HashSet<String>> {
        self.remove_expired(key);
        if !self.data.contains_key(key) {
            self.data.insert(key.to_owned(), Entry::new(StoredValue::Set(HashSet::new())));
        }
        match self.data.get_mut(key) {
            Some(entry) => match &mut entry.value {
                StoredValue::Set(s) => Ok(s),
                _ => Err(wrong_type_err()),
            },
            None => unreachable!(),
        }
    }

    fn get_existing_set_ref(&self, key: &str) -> RedisResult<Option<&HashSet<String>>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(None);
                }
                match &entry.value {
                    StoredValue::Set(s) => Ok(Some(s)),
                    _ => Err(wrong_type_err()),
                }
            }
        }
    }

    pub fn sadd(&mut self, key: &str, members: &[&str]) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if members.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one member must be provided".into(),
            ));
        }
        let set = self.get_or_create_set(key)?;
        let added = members.iter().filter(|&&m| set.insert(m.to_owned())).count() as i64;
        Ok(RedisValue::from_integer(added))
    }

    pub fn srem(&mut self, key: &str, members: &[&str]) -> RedisResult<RedisValue> {
        validate_key(key)?;
        if members.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one member must be provided".into(),
            ));
        }
        self.remove_expired(key);
        match self.data.get_mut(key) {
            None => Ok(RedisValue::from_integer(0)),
            Some(entry) => match &mut entry.value {
                StoredValue::Set(set) => {
                    let removed = members.iter().filter(|&&m| set.remove(m)).count() as i64;
                    if set.is_empty() {
                        self.data.remove(key);
                    }
                    Ok(RedisValue::from_integer(removed))
                }
                _ => Err(wrong_type_err()),
            },
        }
    }

    pub fn sismember(&mut self, key: &str, member: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_set_ref(key)? {
            None => Ok(RedisValue::from_integer(0)),
            Some(set) => Ok(RedisValue::from_integer(if set.contains(member) { 1 } else { 0 })),
        }
    }

    pub fn smembers(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_set_ref(key)? {
            None => Ok(RedisValue::from_set(HashSet::new())),
            Some(set) => Ok(RedisValue::from_set(set.clone())),
        }
    }

    pub fn scard(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        match self.get_existing_set_ref(key)? {
            None => Ok(RedisValue::from_integer(0)),
            Some(set) => Ok(RedisValue::from_integer(set.len() as i64)),
        }
    }

    pub fn sunion(&self, keys: &[&str]) -> RedisResult<RedisValue> {
        if keys.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one key must be provided".into(),
            ));
        }
        let mut result: HashSet<String> = HashSet::new();
        for &k in keys {
            validate_key(k)?;
            if let Some(set) = self.get_existing_set_ref(k)? {
                result.extend(set.iter().cloned());
            }
        }
        Ok(RedisValue::from_set(result))
    }

    pub fn sinter(&self, keys: &[&str]) -> RedisResult<RedisValue> {
        if keys.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one key must be provided".into(),
            ));
        }
        let mut result: Option<HashSet<String>> = None;
        for &k in keys {
            validate_key(k)?;
            match self.get_existing_set_ref(k)? {
                None => return Ok(RedisValue::from_set(HashSet::new())),
                Some(set) => {
                    result = Some(match result {
                        None => set.clone(),
                        Some(acc) => acc.intersection(set).cloned().collect(),
                    });
                    if result.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
                        return Ok(RedisValue::from_set(HashSet::new()));
                    }
                }
            }
        }
        Ok(RedisValue::from_set(result.unwrap_or_default()))
    }

    // ── Key commands ─────────────────────────────────────────────────────

    pub fn del(&mut self, keys: &[&str]) -> RedisResult<RedisValue> {
        if keys.is_empty() {
            return Err(RedisError::InvalidArgument(
                "at least one key must be provided".into(),
            ));
        }
        let mut deleted = 0i64;
        for &k in keys {
            validate_key(k)?;
            let live = self.data.get(k).map(|e| !e.is_expired()).unwrap_or(false);
            if live {
                self.data.remove(k);
                deleted += 1;
            } else {
                self.data.remove(k);
            }
        }
        Ok(RedisValue::from_integer(deleted))
    }

    pub fn exists(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        Ok(RedisValue::from_integer(if self.data.contains_key(key) { 1 } else { 0 }))
    }

    pub fn expire(&mut self, key: &str, seconds: i64) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        if !self.data.contains_key(key) {
            return Ok(RedisValue::from_integer(0));
        }
        if seconds <= 0 {
            self.data.remove(key);
            return Ok(RedisValue::from_integer(1));
        }
        if let Some(entry) = self.data.get_mut(key) {
            entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds as u64));
        }
        Ok(RedisValue::from_integer(1))
    }

    pub fn ttl(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        match self.data.get(key) {
            None => Ok(RedisValue::from_integer(-2)),
            Some(entry) => match entry.expires_at {
                None => Ok(RedisValue::from_integer(-1)),
                Some(t) => {
                    let now = Instant::now();
                    if now >= t {
                        Ok(RedisValue::from_integer(-2))
                    } else {
                        Ok(RedisValue::from_integer((t - now).as_secs() as i64))
                    }
                }
            },
        }
    }

    pub fn type_of(&mut self, key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        self.remove_expired(key);
        match self.data.get(key) {
            None => Ok(RedisValue::from_string("none")),
            Some(entry) => Ok(RedisValue::from_string(entry.value.type_name())),
        }
    }

    pub fn keys(&mut self, pattern: &str) -> RedisResult<RedisValue> {
        let regex = glob_to_regex(pattern);
        let expired: Vec<String> = self.data
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            self.data.remove(&k);
        }
        let matching: Vec<Option<String>> = self.data
            .keys()
            .filter(|k| matches_glob(k, &regex))
            .map(|k| Some(k.clone()))
            .collect();
        Ok(RedisValue::from_list(matching))
    }

    pub fn rename(&mut self, key: &str, new_key: &str) -> RedisResult<RedisValue> {
        validate_key(key)?;
        validate_key(new_key)?;
        self.remove_expired(key);
        match self.data.remove(key) {
            None => Err(RedisError::KeyNotFound(format!("no such key '{}'", key))),
            Some(entry) => {
                self.data.insert(new_key.to_owned(), entry);
                Ok(RedisValue::ok())
            }
        }
    }

    pub fn dbsize(&mut self) -> RedisResult<RedisValue> {
        let expired: Vec<String> = self.data
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            self.data.remove(&k);
        }
        Ok(RedisValue::from_integer(self.data.len() as i64))
    }

    pub fn flushdb(&mut self) -> RedisResult<RedisValue> {
        self.data.clear();
        Ok(RedisValue::ok())
    }
}

impl Default for RedisStore {
    fn default() -> Self {
        Self::new()
    }
}

fn glob_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '^' | '$' | '{' | '}' | '(' | ')' | '[' | ']' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            c => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

fn matches_glob(key: &str, regex: &str) -> bool {
    glob_match(key, regex)
}

fn glob_match(key: &str, pattern: &str) -> bool {
    let pattern = pattern
        .strip_prefix('^')
        .and_then(|p| p.strip_suffix('$'))
        .unwrap_or(pattern);
    glob_match_inner(key.as_bytes(), pattern.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> RedisStore {
        RedisStore::new()
    }

    // ── String commands ───────────────────────────────────────────────────

    #[test]
    fn set_and_get_roundtrip() {
        let mut s = store();
        s.set("key", "value", None, false, false).unwrap();
        assert_eq!(s.get("key").unwrap(), RedisValue::from_string("value"));
    }

    #[test]
    fn get_missing_key_returns_null() {
        let mut s = store();
        assert!(s.get("missing").unwrap().is_null());
    }

    #[test]
    fn set_nx_does_not_overwrite() {
        let mut s = store();
        s.set("k", "first", None, false, false).unwrap();
        s.set("k", "second", None, true, false).unwrap();
        assert_eq!(s.get("k").unwrap(), RedisValue::from_string("first"));
    }

    #[test]
    fn set_xx_only_updates_existing() {
        let mut s = store();
        let r = s.set("k", "v", None, false, true).unwrap();
        assert!(r.is_null());
        s.set("k", "v1", None, false, false).unwrap();
        s.set("k", "v2", None, false, true).unwrap();
        assert_eq!(s.get("k").unwrap(), RedisValue::from_string("v2"));
    }

    #[test]
    fn incr_missing_key_sets_to_one() {
        let mut s = store();
        assert_eq!(s.incr("counter").unwrap(), RedisValue::from_integer(1));
    }

    #[test]
    fn incr_existing_key() {
        let mut s = store();
        s.set("counter", "10", None, false, false).unwrap();
        assert_eq!(s.incr("counter").unwrap(), RedisValue::from_integer(11));
    }

    #[test]
    fn decr_decrements() {
        let mut s = store();
        s.set("k", "5", None, false, false).unwrap();
        assert_eq!(s.decr("k").unwrap(), RedisValue::from_integer(4));
    }

    #[test]
    fn append_appends_to_string() {
        let mut s = store();
        s.set("k", "hello", None, false, false).unwrap();
        let r = s.append("k", " world").unwrap();
        assert_eq!(r, RedisValue::from_integer(11));
        assert_eq!(s.get("k").unwrap(), RedisValue::from_string("hello world"));
    }

    #[test]
    fn strlen_returns_length() {
        let mut s = store();
        s.set("k", "hello", None, false, false).unwrap();
        assert_eq!(s.strlen("k").unwrap(), RedisValue::from_integer(5));
        assert_eq!(s.strlen("missing").unwrap(), RedisValue::from_integer(0));
    }

    #[test]
    fn setnx_true_on_new_false_on_existing() {
        let mut s = store();
        assert_eq!(s.setnx("k", "v").unwrap(), RedisValue::from_integer(1));
        assert_eq!(s.setnx("k", "v2").unwrap(), RedisValue::from_integer(0));
        assert_eq!(s.get("k").unwrap(), RedisValue::from_string("v"));
    }

    #[test]
    fn empty_key_returns_error() {
        let mut s = store();
        assert!(s.get("").is_err());
        assert!(s.set("", "v", None, false, false).is_err());
    }

    #[test]
    fn wrong_type_returns_error() {
        let mut s = store();
        s.lpush("list", &["a"]).unwrap();
        assert!(s.get("list").is_err());
    }

    // ── List commands ─────────────────────────────────────────────────────

    #[test]
    fn lpush_and_lrange() {
        let mut s = store();
        s.lpush("list", &["c", "b", "a"]).unwrap();
        let r = s.lrange("list", 0, -1).unwrap();
        assert_eq!(r, RedisValue::from_list(vec![
            Some("a".into()), Some("b".into()), Some("c".into()),
        ]));
    }

    #[test]
    fn rpush_and_llen() {
        let mut s = store();
        s.rpush("list", &["a", "b", "c"]).unwrap();
        assert_eq!(s.llen("list").unwrap(), RedisValue::from_integer(3));
    }

    #[test]
    fn lpop_returns_first() {
        let mut s = store();
        s.rpush("list", &["x", "y"]).unwrap();
        assert_eq!(s.lpop("list").unwrap(), RedisValue::from_string("x"));
    }

    #[test]
    fn rpop_returns_last() {
        let mut s = store();
        s.rpush("list", &["x", "y"]).unwrap();
        assert_eq!(s.rpop("list").unwrap(), RedisValue::from_string("y"));
    }

    #[test]
    fn lpop_empty_returns_null() {
        let mut s = store();
        assert!(s.lpop("missing").unwrap().is_null());
    }

    // ── Hash commands ─────────────────────────────────────────────────────

    #[test]
    fn hset_and_hget() {
        let mut s = store();
        s.hset("h", "field", "value").unwrap();
        assert_eq!(s.hget("h", "field").unwrap(), RedisValue::from_string("value"));
    }

    #[test]
    fn hget_missing_field_returns_null() {
        let mut s = store();
        s.hset("h", "f", "v").unwrap();
        assert!(s.hget("h", "missing").unwrap().is_null());
    }

    #[test]
    fn hdel_removes_field() {
        let mut s = store();
        s.hset("h", "f", "v").unwrap();
        assert_eq!(s.hdel("h", &["f"]).unwrap(), RedisValue::from_integer(1));
        assert!(s.hget("h", "f").unwrap().is_null());
    }

    #[test]
    fn hexists_and_hlen() {
        let mut s = store();
        s.hset("h", "f", "v").unwrap();
        assert_eq!(s.hexists("h", "f").unwrap(), RedisValue::from_integer(1));
        assert_eq!(s.hexists("h", "x").unwrap(), RedisValue::from_integer(0));
        assert_eq!(s.hlen("h").unwrap(), RedisValue::from_integer(1));
    }

    // ── Set commands ──────────────────────────────────────────────────────

    #[test]
    fn sadd_and_scard() {
        let mut s = store();
        s.sadd("s", &["a", "b", "c"]).unwrap();
        assert_eq!(s.scard("s").unwrap(), RedisValue::from_integer(3));
    }

    #[test]
    fn sadd_duplicate_not_counted() {
        let mut s = store();
        s.sadd("s", &["a", "b"]).unwrap();
        let r = s.sadd("s", &["a", "c"]).unwrap();
        assert_eq!(r, RedisValue::from_integer(1));
    }

    #[test]
    fn sismember_returns_correctly() {
        let mut s = store();
        s.sadd("s", &["x"]).unwrap();
        assert_eq!(s.sismember("s", "x").unwrap(), RedisValue::from_integer(1));
        assert_eq!(s.sismember("s", "y").unwrap(), RedisValue::from_integer(0));
    }

    // ── Key commands ──────────────────────────────────────────────────────

    #[test]
    fn del_deletes_keys() {
        let mut s = store();
        s.set("a", "1", None, false, false).unwrap();
        s.set("b", "2", None, false, false).unwrap();
        assert_eq!(s.del(&["a", "b"]).unwrap(), RedisValue::from_integer(2));
        assert!(s.get("a").unwrap().is_null());
    }

    #[test]
    fn exists_returns_one_or_zero() {
        let mut s = store();
        s.set("k", "v", None, false, false).unwrap();
        assert_eq!(s.exists("k").unwrap(), RedisValue::from_integer(1));
        assert_eq!(s.exists("missing").unwrap(), RedisValue::from_integer(0));
    }

    #[test]
    fn ttl_no_expiry_returns_minus_one() {
        let mut s = store();
        s.set("k", "v", None, false, false).unwrap();
        assert_eq!(s.ttl("k").unwrap(), RedisValue::from_integer(-1));
    }

    #[test]
    fn ttl_missing_key_returns_minus_two() {
        let mut s = store();
        assert_eq!(s.ttl("missing").unwrap(), RedisValue::from_integer(-2));
    }

    #[test]
    fn rename_moves_key() {
        let mut s = store();
        s.set("old", "v", None, false, false).unwrap();
        s.rename("old", "new").unwrap();
        assert!(s.get("old").unwrap().is_null());
        assert_eq!(s.get("new").unwrap(), RedisValue::from_string("v"));
    }

    #[test]
    fn dbsize_counts_live_keys() {
        let mut s = store();
        s.set("a", "1", None, false, false).unwrap();
        s.set("b", "2", None, false, false).unwrap();
        assert_eq!(s.dbsize().unwrap(), RedisValue::from_integer(2));
    }

    #[test]
    fn flushdb_clears_all() {
        let mut s = store();
        s.set("a", "1", None, false, false).unwrap();
        s.flushdb().unwrap();
        assert_eq!(s.dbsize().unwrap(), RedisValue::from_integer(0));
    }

    #[test]
    fn type_of_returns_correct_type() {
        let mut s = store();
        s.set("str", "v", None, false, false).unwrap();
        s.lpush("lst", &["v"]).unwrap();
        s.hset("hash", "f", "v").unwrap();
        s.sadd("set", &["v"]).unwrap();
        assert_eq!(s.type_of("str").unwrap(), RedisValue::from_string("string"));
        assert_eq!(s.type_of("lst").unwrap(), RedisValue::from_string("list"));
        assert_eq!(s.type_of("hash").unwrap(), RedisValue::from_string("hash"));
        assert_eq!(s.type_of("set").unwrap(), RedisValue::from_string("set"));
        assert_eq!(s.type_of("missing").unwrap(), RedisValue::from_string("none"));
    }
}

fn glob_match_inner(text: &[u8], pattern: &[u8]) -> bool {
    let mut ti = 0;
    let mut pi = 0;
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'.' && (pi + 1 < pattern.len() && pattern[pi + 1] == b'*')) {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 2;
        } else if pi < pattern.len() && pattern[pi] == b'.' {
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'\\' && pi + 1 < pattern.len() {
            if text[ti] == pattern[pi + 1] {
                ti += 1;
                pi += 2;
            } else if let Some(sp) = star_pi {
                pi = sp + 2;
                star_ti += 1;
                ti = star_ti;
            } else {
                return false;
            }
        } else if pi < pattern.len() && text[ti] == pattern[pi] {
            ti += 1;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 2;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi + 1 < pattern.len() && pattern[pi] == b'.' && pattern[pi + 1] == b'*' {
        pi += 2;
    }

    pi >= pattern.len()
}
