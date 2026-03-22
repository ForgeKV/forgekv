use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    Null,
    Ok,
    String(String),
    Integer(i64),
    Float(f64),
    List(Vec<Option<String>>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
    Error(String),
}

impl RedisValue {
    pub fn null() -> Self {
        RedisValue::Null
    }

    pub fn ok() -> Self {
        RedisValue::Ok
    }

    pub fn from_string(s: impl Into<String>) -> Self {
        RedisValue::String(s.into())
    }

    pub fn from_integer(n: i64) -> Self {
        RedisValue::Integer(n)
    }

    pub fn from_float(f: f64) -> Self {
        RedisValue::Float(f)
    }

    pub fn from_list(list: Vec<Option<String>>) -> Self {
        RedisValue::List(list)
    }

    pub fn from_set(set: HashSet<String>) -> Self {
        RedisValue::Set(set)
    }

    pub fn from_hash(map: HashMap<String, String>) -> Self {
        RedisValue::Hash(map)
    }

    pub fn from_error(msg: impl Into<String>) -> Self {
        RedisValue::Error(msg.into())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, RedisValue::Null)
    }
}
