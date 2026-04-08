/// JSON commands — compatible with RedisJSON / Dragonfly.
///
/// Commands: JSON.SET JSON.GET JSON.DEL JSON.TYPE JSON.NUMINCRBY JSON.NUMMULTBY
///           JSON.STRLEN JSON.STRAPPEND JSON.ARRAPPEND JSON.ARRINSERT
///           JSON.ARRLEN JSON.ARRPOP JSON.ARRTRIM JSON.ARRINDEX
///           JSON.OBJKEYS JSON.OBJLEN JSON.CLEAR JSON.TOGGLE JSON.MGET
///           JSON.MSET JSON.MERGE JSON.RESP JSON.FORGET JSON.DEBUG JSON.DUMP
///
/// Storage: JSON values are stored as UTF-8 strings in the database under the
/// normal key namespace with a type tag. For simplicity this implementation
/// stores the entire JSON document as a raw string and uses a minimal
/// JSONPath evaluator for dot-notation paths ($, .field, [index]).
use std::collections::HashMap;

use parking_lot::Mutex;

use super::CommandHandler;
use crate::ext_type_registry;
use crate::resp::RespValue;

// ── JSON store ────────────────────────────────────────────────────────────────

lazy_static::lazy_static! {
    /// db_index → key → json_string
    static ref JSON_STORE: Mutex<HashMap<usize, HashMap<Vec<u8>, serde_json::Value>>> =
        Mutex::new(HashMap::new());
}

fn store_get(db: usize, key: &[u8]) -> Option<serde_json::Value> {
    JSON_STORE.lock().get(&db).and_then(|m| m.get(key)).cloned()
}

fn store_set(db: usize, key: Vec<u8>, val: serde_json::Value) {
    let is_new = JSON_STORE
        .lock()
        .get(&db)
        .map_or(true, |m| !m.contains_key(&key));
    JSON_STORE
        .lock()
        .entry(db)
        .or_default()
        .insert(key.clone(), val);
    if is_new {
        ext_type_registry::register(db, &key, "ReJSON-RL");
    }
}

fn store_del(db: usize, key: &[u8]) -> bool {
    let removed = JSON_STORE
        .lock()
        .get_mut(&db)
        .map(|m| m.remove(key).is_some())
        .unwrap_or(false);
    if removed {
        ext_type_registry::unregister(db, key);
    }
    removed
}

// ── JSONPath ──────────────────────────────────────────────────────────────────

/// Apply a simple JSONPath to a value, returning a mutable reference.
/// Supported: `$` (root), `.field`, `[n]`, `..field` (recursive).
/// Returns None if path not found.
fn path_get<'a>(root: &'a serde_json::Value, path: &str) -> Vec<serde_json::Value> {
    if path == "$" || path == "." {
        return vec![root.clone()];
    }
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        &path[1..]
    } else {
        path
    };
    navigate(root, path)
}

fn navigate(val: &serde_json::Value, path: &str) -> Vec<serde_json::Value> {
    if path.is_empty() {
        return vec![val.clone()];
    }
    if let Some(rest) = path.strip_prefix('[') {
        // Array index
        if let Some(end) = rest.find(']') {
            let idx_str = &rest[..end];
            let after = &rest[end + 1..];
            let after = if after.starts_with('.') {
                &after[1..]
            } else {
                after
            };
            if let (Ok(idx), Some(arr)) = (idx_str.parse::<usize>(), val.as_array()) {
                if let Some(elem) = arr.get(idx) {
                    return navigate(elem, after);
                }
            }
        }
        return vec![];
    }
    // Dot notation
    let (field, rest) = if let Some(dot) = path.find('.') {
        if let Some(bracket) = path.find('[') {
            if bracket < dot {
                (&path[..bracket], &path[bracket..])
            } else {
                (&path[..dot], &path[dot + 1..])
            }
        } else {
            (&path[..dot], &path[dot + 1..])
        }
    } else if let Some(bracket) = path.find('[') {
        (&path[..bracket], &path[bracket..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object() {
        if let Some(child) = obj.get(field) {
            return navigate(child, rest);
        }
    }
    vec![]
}

/// Set value at path. Returns (root_after_set, was_created).
fn path_set(
    root: &mut serde_json::Value,
    path: &str,
    new_val: serde_json::Value,
    nx: bool, // only set if not exists
    xx: bool, // only set if exists
) -> Option<bool> {
    if path == "$" || path == "." {
        let existed = !root.is_null();
        if nx && existed {
            return Some(false);
        }
        if xx && !existed {
            return Some(false);
        }
        *root = new_val;
        return Some(!existed);
    }
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        &path[1..]
    } else {
        path
    };
    path_set_inner(root, path, new_val, nx, xx)
}

fn path_set_inner(
    val: &mut serde_json::Value,
    path: &str,
    new_val: serde_json::Value,
    nx: bool,
    xx: bool,
) -> Option<bool> {
    if path.is_empty() {
        let existed = !val.is_null();
        if nx && existed {
            return Some(false);
        }
        if xx && !existed {
            return Some(false);
        }
        *val = new_val;
        return Some(!existed);
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        (&path[..dot], &path[dot + 1..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object_mut() {
        if rest.is_empty() {
            let existed = obj.contains_key(field);
            if nx && existed {
                return Some(false);
            }
            if xx && !existed {
                return Some(false);
            }
            obj.insert(field.to_string(), new_val);
            return Some(!existed);
        }
        if let Some(child) = obj.get_mut(field) {
            return path_set_inner(child, rest, new_val, nx, xx);
        } else if !xx {
            // Create intermediate object
            let mut child = serde_json::Value::Object(serde_json::Map::new());
            path_set_inner(&mut child, rest, new_val, nx, xx)?;
            obj.insert(field.to_string(), child);
            return Some(true);
        }
    }
    None
}

/// Delete at path. Returns number deleted.
fn path_del(root: &mut serde_json::Value, path: &str) -> usize {
    if path == "$" || path == "." {
        *root = serde_json::Value::Null;
        return 1;
    }
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        &path[1..]
    } else {
        path
    };
    path_del_inner(root, path)
}

fn path_del_inner(val: &mut serde_json::Value, path: &str) -> usize {
    if path.is_empty() {
        return 0;
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        (&path[..dot], &path[dot + 1..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object_mut() {
        if rest.is_empty() {
            return if obj.remove(field).is_some() { 1 } else { 0 };
        }
        if let Some(child) = obj.get_mut(field) {
            return path_del_inner(child, rest);
        }
    }
    0
}

// ── JSON.SET ──────────────────────────────────────────────────────────────────

pub struct JsonSetCommand;
impl CommandHandler for JsonSetCommand {
    fn name(&self) -> &str {
        "JSON.SET"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // JSON.SET key path value [NX|XX]
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.SET'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid path"),
        };
        let json_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR value must be a string"),
        };
        let new_val: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(e) => return RespValue::error(&format!("ERR invalid JSON: {}", e)),
        };
        let mut nx = false;
        let mut xx = false;
        for arg in &args[4..] {
            match arg.as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("NX") => nx = true,
                Some("XX") => xx = true,
                _ => {}
            }
        }
        let db = *db_index;
        let mut root = store_get(db, &key).unwrap_or(serde_json::Value::Null);
        match path_set(&mut root, &path, new_val, nx, xx) {
            None => RespValue::null_bulk(),
            Some(_) => {
                store_set(db, key, root);
                RespValue::ok()
            }
        }
    }
}

// ── JSON.GET ──────────────────────────────────────────────────────────────────

pub struct JsonGetCommand;
impl CommandHandler for JsonGetCommand {
    fn name(&self) -> &str {
        "JSON.GET"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path [path ...]]
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.GET'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let db = *db_index;
        let root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        // Collect paths, skipping options
        let mut paths: Vec<String> = Vec::new();
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("INDENT") | Some("NEWLINE") | Some("SPACE") => {
                    i += 2;
                    continue;
                }
                _ => {}
            }
            if let Some(p) = args[i].as_str() {
                paths.push(p.to_string());
            }
            i += 1;
        }
        if paths.is_empty() {
            paths.push("$".to_string());
        }
        if paths.len() == 1 {
            let results = path_get(&root, &paths[0]);
            if results.is_empty() {
                return RespValue::null_bulk();
            }
            if paths[0] == "$" || paths[0] == "." {
                return RespValue::bulk_str(
                    &serde_json::to_string(&results[0]).unwrap_or_default(),
                );
            }
            // JSONPath style returns array
            let json =
                serde_json::to_string(&serde_json::Value::Array(results)).unwrap_or_default();
            RespValue::bulk_str(&json)
        } else {
            // Multiple paths → object {path: value}
            let mut obj = serde_json::Map::new();
            for p in &paths {
                let results = path_get(&root, p);
                obj.insert(p.clone(), serde_json::Value::Array(results));
            }
            RespValue::bulk_str(
                &serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default(),
            )
        }
    }
}

// ── JSON.MGET ─────────────────────────────────────────────────────────────────

pub struct JsonMGetCommand;
impl CommandHandler for JsonMGetCommand {
    fn name(&self) -> &str {
        "JSON.MGET"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // JSON.MGET key [key ...] path
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.MGET'");
        }
        let path = match args.last().and_then(|a| a.as_str()) {
            Some(p) => p.to_string(),
            None => return RespValue::error("ERR path must be a string"),
        };
        let db = *db_index;
        let results: Vec<RespValue> = args[1..args.len() - 1]
            .iter()
            .map(|a| {
                let key = match a.as_bytes() {
                    Some(b) => b.to_vec(),
                    None => return RespValue::null_bulk(),
                };
                match store_get(db, &key) {
                    None => RespValue::null_bulk(),
                    Some(root) => {
                        let vals = path_get(&root, &path);
                        if vals.is_empty() {
                            RespValue::null_bulk()
                        } else {
                            let json = serde_json::to_string(&serde_json::Value::Array(vals))
                                .unwrap_or_default();
                            RespValue::bulk_str(&json)
                        }
                    }
                }
            })
            .collect();
        RespValue::Array(Some(results))
    }
}

// ── JSON.DEL / JSON.FORGET ────────────────────────────────────────────────────

pub struct JsonDelCommand {
    pub name: &'static str,
}
impl CommandHandler for JsonDelCommand {
    fn name(&self) -> &str {
        self.name
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error(&format!(
                "ERR wrong number of arguments for '{}'",
                self.name
            ));
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        if path == "$" || path == "." {
            let deleted = store_del(db, &key);
            return RespValue::Integer(if deleted { 1 } else { 0 });
        }
        let mut root = match store_get(db, &key) {
            None => return RespValue::Integer(0),
            Some(v) => v,
        };
        let count = path_del(&mut root, path) as i64;
        store_set(db, key, root);
        RespValue::Integer(count)
    }
}

// ── JSON.TYPE ─────────────────────────────────────────────────────────────────

pub struct JsonTypeCommand;
impl CommandHandler for JsonTypeCommand {
    fn name(&self) -> &str {
        "JSON.TYPE"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.TYPE'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::null_bulk(),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        match store_get(db, &key) {
            None => RespValue::null_bulk(),
            Some(root) => {
                let vals = path_get(&root, path);
                let types: Vec<RespValue> = vals
                    .iter()
                    .map(|v| RespValue::bulk_str(json_type_name(v)))
                    .collect();
                if types.is_empty() {
                    RespValue::null_bulk()
                } else if path == "$" || path == "." {
                    RespValue::bulk_str(json_type_name(&vals[0]))
                } else {
                    RespValue::Array(Some(types))
                }
            }
        }
    }
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) => {
            if n.is_f64() {
                "number"
            } else {
                "integer"
            }
        }
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ── JSON.NUMINCRBY ────────────────────────────────────────────────────────────

pub struct JsonNumIncrByCommand;
impl CommandHandler for JsonNumIncrByCommand {
    fn name(&self) -> &str {
        "JSON.NUMINCRBY"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.NUMINCRBY'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid path"),
        };
        let incr: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let db = *db_index;
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let result = num_op_path(&mut root, &path, |n| n + incr);
        store_set(db, key, root);
        match result {
            Some(vals) => {
                let json =
                    serde_json::to_string(&serde_json::Value::Array(vals)).unwrap_or_default();
                RespValue::bulk_str(&json)
            }
            None => RespValue::null_bulk(),
        }
    }
}

pub struct JsonNumMultByCommand;
impl CommandHandler for JsonNumMultByCommand {
    fn name(&self) -> &str {
        "JSON.NUMMULTBY"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.NUMMULTBY'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid path"),
        };
        let mult: f64 = match args[3].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not a valid float"),
        };
        let db = *db_index;
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let result = num_op_path(&mut root, &path, |n| n * mult);
        store_set(db, key, root);
        match result {
            Some(vals) => {
                let json =
                    serde_json::to_string(&serde_json::Value::Array(vals)).unwrap_or_default();
                RespValue::bulk_str(&json)
            }
            None => RespValue::null_bulk(),
        }
    }
}

fn num_op_path<F: Fn(f64) -> f64>(
    root: &mut serde_json::Value,
    path: &str,
    op: F,
) -> Option<Vec<serde_json::Value>> {
    let canonical = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        &path[1..]
    } else {
        path
    };
    num_op_inner(root, canonical, &op).map(|v| vec![v])
}

fn num_op_inner<F: Fn(f64) -> f64>(
    val: &mut serde_json::Value,
    path: &str,
    op: &F,
) -> Option<serde_json::Value> {
    if path.is_empty() {
        if let Some(n) = val.as_f64() {
            let result = op(n);
            if result.fract() == 0.0 && result.abs() < 1e15 {
                *val = serde_json::Value::Number(serde_json::Number::from(result as i64));
            } else {
                *val = serde_json::Value::Number(serde_json::Number::from_f64(result)?);
            }
            return Some(val.clone());
        }
        return None;
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        (&path[..dot], &path[dot + 1..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object_mut() {
        if let Some(child) = obj.get_mut(field) {
            return num_op_inner(child, rest, op);
        }
    }
    None
}

// ── JSON.STRLEN ───────────────────────────────────────────────────────────────

pub struct JsonStrLenCommand;
impl CommandHandler for JsonStrLenCommand {
    fn name(&self) -> &str {
        "JSON.STRLEN"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.STRLEN'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::null_bulk(),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        match store_get(db, &key) {
            None => RespValue::null_bulk(),
            Some(root) => {
                let vals = path_get(&root, path);
                let results: Vec<RespValue> = vals
                    .iter()
                    .map(|v| match v.as_str() {
                        Some(s) => RespValue::Integer(s.len() as i64),
                        None => RespValue::null_bulk(),
                    })
                    .collect();
                if results.len() == 1 {
                    results.into_iter().next().unwrap()
                } else {
                    RespValue::Array(Some(results))
                }
            }
        }
    }
}

// ── JSON.ARRLEN ───────────────────────────────────────────────────────────────

pub struct JsonArrLenCommand;
impl CommandHandler for JsonArrLenCommand {
    fn name(&self) -> &str {
        "JSON.ARRLEN"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.ARRLEN'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::null_bulk(),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        match store_get(db, &key) {
            None => RespValue::null_bulk(),
            Some(root) => {
                let vals = path_get(&root, path);
                let results: Vec<RespValue> = vals
                    .iter()
                    .map(|v| match v.as_array() {
                        Some(arr) => RespValue::Integer(arr.len() as i64),
                        None => RespValue::null_bulk(),
                    })
                    .collect();
                if results.len() == 1 {
                    results.into_iter().next().unwrap()
                } else {
                    RespValue::Array(Some(results))
                }
            }
        }
    }
}

// ── JSON.ARRAPPEND ────────────────────────────────────────────────────────────

pub struct JsonArrAppendCommand;
impl CommandHandler for JsonArrAppendCommand {
    fn name(&self) -> &str {
        "JSON.ARRAPPEND"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.ARRAPPEND'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid path"),
        };
        let new_items: Vec<serde_json::Value> = args[3..]
            .iter()
            .filter_map(|a| a.as_str().and_then(|s| serde_json::from_str(s).ok()))
            .collect();
        let db = *db_index;
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let canonical = if path.starts_with("$.") {
            path[2..].to_string()
        } else if path.starts_with('$') {
            path[1..].to_string()
        } else if path.starts_with('.') {
            path[1..].to_string()
        } else {
            path.clone()
        };
        let result = arr_append_inner(&mut root, &canonical, &new_items);
        store_set(db, key, root);
        RespValue::Integer(result as i64)
    }
}

fn arr_append_inner(val: &mut serde_json::Value, path: &str, items: &[serde_json::Value]) -> usize {
    if path.is_empty() {
        if let Some(arr) = val.as_array_mut() {
            arr.extend_from_slice(items);
            return arr.len();
        }
        return 0;
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        (&path[..dot], &path[dot + 1..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object_mut() {
        if let Some(child) = obj.get_mut(field) {
            return arr_append_inner(child, rest, items);
        }
    }
    0
}

// ── JSON.OBJKEYS ──────────────────────────────────────────────────────────────

pub struct JsonObjKeysCommand;
impl CommandHandler for JsonObjKeysCommand {
    fn name(&self) -> &str {
        "JSON.OBJKEYS"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.OBJKEYS'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::null_bulk(),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        match store_get(db, &key) {
            None => RespValue::null_bulk(),
            Some(root) => {
                let vals = path_get(&root, path);
                let results: Vec<RespValue> = vals
                    .iter()
                    .map(|v| match v.as_object() {
                        Some(obj) => RespValue::Array(Some(
                            obj.keys().map(|k| RespValue::bulk_str(k)).collect(),
                        )),
                        None => RespValue::null_bulk(),
                    })
                    .collect();
                if results.len() == 1 {
                    results.into_iter().next().unwrap()
                } else {
                    RespValue::Array(Some(results))
                }
            }
        }
    }
}

// ── JSON.OBJLEN ───────────────────────────────────────────────────────────────

pub struct JsonObjLenCommand;
impl CommandHandler for JsonObjLenCommand {
    fn name(&self) -> &str {
        "JSON.OBJLEN"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.OBJLEN'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::null_bulk(),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        match store_get(db, &key) {
            None => RespValue::null_bulk(),
            Some(root) => {
                let vals = path_get(&root, path);
                let results: Vec<RespValue> = vals
                    .iter()
                    .map(|v| match v.as_object() {
                        Some(obj) => RespValue::Integer(obj.len() as i64),
                        None => RespValue::null_bulk(),
                    })
                    .collect();
                if results.len() == 1 {
                    results.into_iter().next().unwrap()
                } else {
                    RespValue::Array(Some(results))
                }
            }
        }
    }
}

// ── JSON.CLEAR ────────────────────────────────────────────────────────────────

pub struct JsonClearCommand;
impl CommandHandler for JsonClearCommand {
    fn name(&self) -> &str {
        "JSON.CLEAR"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.CLEAR'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::Integer(0),
        };
        let count = json_clear_path(&mut root, path);
        store_set(db, key, root);
        RespValue::Integer(count as i64)
    }
}

fn json_clear_path(root: &mut serde_json::Value, path: &str) -> usize {
    if path == "$" || path == "." {
        let mut count = 0;
        if let Some(arr) = root.as_array_mut() {
            arr.clear();
            count += 1;
        } else if let Some(obj) = root.as_object_mut() {
            obj.clear();
            count += 1;
        } else if root.is_number() {
            *root = serde_json::Value::Number(0.into());
            count += 1;
        }
        return count;
    }
    let vals = path_get(root, path);
    vals.len()
}

// ── JSON.TOGGLE ───────────────────────────────────────────────────────────────

pub struct JsonToggleCommand;
impl CommandHandler for JsonToggleCommand {
    fn name(&self) -> &str {
        "JSON.TOGGLE"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.TOGGLE'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("WRONGTYPE"),
        };
        let path = match args[2].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid path"),
        };
        let db = *db_index;
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let canonical = if path.starts_with("$.") {
            path[2..].to_string()
        } else if path.starts_with('$') {
            path[1..].to_string()
        } else if path.starts_with('.') {
            path[1..].to_string()
        } else {
            path.clone()
        };
        let result = toggle_inner(&mut root, &canonical);
        store_set(db, key, root);
        match result {
            Some(b) => RespValue::Integer(if b { 1 } else { 0 }),
            None => RespValue::null_bulk(),
        }
    }
}

fn toggle_inner(val: &mut serde_json::Value, path: &str) -> Option<bool> {
    if path.is_empty() {
        if let Some(b) = val.as_bool() {
            *val = serde_json::Value::Bool(!b);
            return Some(!b);
        }
        return None;
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        (&path[..dot], &path[dot + 1..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object_mut() {
        if let Some(child) = obj.get_mut(field) {
            return toggle_inner(child, rest);
        }
    }
    None
}

// ── JSON.RESP ─────────────────────────────────────────────────────────────────

pub struct JsonRespCommand;
impl CommandHandler for JsonRespCommand {
    fn name(&self) -> &str {
        "JSON.RESP"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.RESP'");
        }
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::null_bulk(),
        };
        let path = args.get(2).and_then(|a| a.as_str()).unwrap_or("$");
        let db = *db_index;
        match store_get(db, &key) {
            None => RespValue::null_bulk(),
            Some(root) => {
                let vals = path_get(&root, path);
                if vals.is_empty() {
                    return RespValue::null_bulk();
                }
                json_to_resp(&vals[0])
            }
        }
    }
}

fn json_to_resp(val: &serde_json::Value) -> RespValue {
    match val {
        serde_json::Value::Null => RespValue::null_bulk(),
        serde_json::Value::Bool(b) => RespValue::bulk_str(if *b { "true" } else { "false" }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                RespValue::Integer(i)
            } else {
                RespValue::bulk_str(&n.to_string())
            }
        }
        serde_json::Value::String(s) => RespValue::bulk_str(s),
        serde_json::Value::Array(arr) => {
            let mut items = vec![RespValue::bulk_str("[")];
            items.extend(arr.iter().map(json_to_resp));
            RespValue::Array(Some(items))
        }
        serde_json::Value::Object(obj) => {
            let mut items = vec![RespValue::bulk_str("{")];
            for (k, v) in obj {
                items.push(RespValue::bulk_str(k));
                items.push(json_to_resp(v));
            }
            RespValue::Array(Some(items))
        }
    }
}

// ── JSON.DEBUG ────────────────────────────────────────────────────────────────

pub struct JsonDebugCommand;
impl CommandHandler for JsonDebugCommand {
    fn name(&self) -> &str {
        "JSON.DEBUG"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("MEMORY") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments");
                }
                let key = match args[2].as_bytes() {
                    Some(b) => b.to_vec(),
                    None => return RespValue::Integer(0),
                };
                let db = *db_index;
                match store_get(db, &key) {
                    None => RespValue::Integer(0),
                    Some(root) => {
                        let size = serde_json::to_string(&root).map(|s| s.len()).unwrap_or(0);
                        RespValue::Integer(size as i64)
                    }
                }
            }
            Some("HELP") => RespValue::Array(Some(vec![RespValue::bulk_str(
                "JSON.DEBUG MEMORY <key> [path] - Report memory usage",
            )])),
            _ => RespValue::error("ERR unknown subcommand"),
        }
    }
}

// ── JSON.MSET ─────────────────────────────────────────────────────────────────

pub struct JsonMSetCommand;
impl CommandHandler for JsonMSetCommand {
    fn name(&self) -> &str {
        "JSON.MSET"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // JSON.MSET key path value [key path value ...]
        if args.len() < 4 || (args.len() - 1) % 3 != 0 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.MSET'");
        }
        let db = *db_index;
        let mut i = 1;
        while i + 2 < args.len() {
            let key = match args[i].as_bytes() {
                Some(b) => b.to_vec(),
                None => return RespValue::error("ERR key must be a string"),
            };
            let path = match args[i + 1].as_str() {
                Some(s) => s.to_string(),
                None => return RespValue::error("ERR path must be a string"),
            };
            let json_str = match args[i + 2].as_str() {
                Some(s) => s,
                None => return RespValue::error("ERR value must be a string"),
            };
            let new_val: serde_json::Value = match serde_json::from_str(json_str) {
                Ok(v) => v,
                Err(e) => return RespValue::error(&format!("ERR invalid JSON: {}", e)),
            };
            let mut root = store_get(db, &key).unwrap_or(serde_json::Value::Null);
            path_set(&mut root, &path, new_val, false, false);
            store_set(db, key, root);
            i += 3;
        }
        RespValue::ok()
    }
}

// ── JSON.STRAPPEND ────────────────────────────────────────────────────────────

pub struct JsonStrAppendCommand;
impl CommandHandler for JsonStrAppendCommand {
    fn name(&self) -> &str {
        "JSON.STRAPPEND"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.STRAPPEND'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR key must be a string"),
        };
        let (path, json_str) = if args.len() == 3 {
            ("$".to_string(), args[2].as_str().unwrap_or("").to_string())
        } else {
            (
                args[2].as_str().unwrap_or("$").to_string(),
                args[3].as_str().unwrap_or("").to_string(),
            )
        };
        let append_str: serde_json::Value = match serde_json::from_str(&json_str) {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not a valid JSON string"),
        };
        let append = match append_str.as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR value is not a string"),
        };
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        if path == "$" || path == "." {
            if let Some(s) = root.as_str() {
                let new_str = format!("{}{}", s, append);
                let len = new_str.len() as i64;
                store_set(db, key, serde_json::Value::String(new_str));
                return RespValue::integer(len);
            }
            return RespValue::null_bulk();
        }
        // Apply to path
        let results = path_get(&root, &path);
        let mut lengths = Vec::new();
        for v in &results {
            if let Some(s) = v.as_str() {
                lengths.push(RespValue::integer((s.len() + append.len()) as i64));
            } else {
                lengths.push(RespValue::null_bulk());
            }
        }
        // Actually mutate
        json_str_append_at(&mut root, &path, &append);
        store_set(db, key, root);
        if lengths.len() == 1 {
            lengths.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(lengths))
        }
    }
}

fn json_str_append_at(val: &mut serde_json::Value, path: &str, append: &str) {
    if path == "$" || path == "." {
        if let Some(s) = val.as_str() {
            *val = serde_json::Value::String(format!("{}{}", s, append));
        }
        return;
    }
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        &path[1..]
    } else {
        path
    };
    json_str_append_inner(val, path, append);
}

fn json_str_append_inner(val: &mut serde_json::Value, path: &str, append: &str) {
    if path.is_empty() {
        if let Some(s) = val.as_str() {
            *val = serde_json::Value::String(format!("{}{}", s, append));
        }
        return;
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        (&path[..dot], &path[dot + 1..])
    } else {
        (path, "")
    };
    if let Some(obj) = val.as_object_mut() {
        if let Some(child) = obj.get_mut(field) {
            json_str_append_inner(child, rest, append);
        }
    }
}

// ── JSON.ARRPOP ───────────────────────────────────────────────────────────────

pub struct JsonArrPopCommand;
impl CommandHandler for JsonArrPopCommand {
    fn name(&self) -> &str {
        "JSON.ARRPOP"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.ARRPOP'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let path = if args.len() >= 3 {
            args[2].as_str().unwrap_or("$").to_string()
        } else {
            "$".to_string()
        };
        let index: i64 = if args.len() >= 4 {
            args[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1)
        } else {
            -1
        };
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let popped = json_arr_pop(&mut root, &path, index);
        store_set(db, key, root);
        match popped {
            Some(v) => RespValue::bulk_str(&v.to_string()),
            None => RespValue::null_bulk(),
        }
    }
}

fn json_arr_pop(root: &mut serde_json::Value, path: &str, index: i64) -> Option<serde_json::Value> {
    let target = json_get_mut(root, path)?;
    if let Some(arr) = target.as_array_mut() {
        if arr.is_empty() {
            return None;
        }
        let idx = if index < 0 {
            (arr.len() as i64 + index).max(0) as usize
        } else {
            (index as usize).min(arr.len() - 1)
        };
        Some(arr.remove(idx))
    } else {
        None
    }
}

fn json_get_mut<'a>(
    root: &'a mut serde_json::Value,
    path: &str,
) -> Option<&'a mut serde_json::Value> {
    if path == "$" || path == "." {
        return Some(root);
    }
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        &path[1..]
    } else {
        path
    };
    json_get_mut_inner(root, path)
}

fn json_get_mut_inner<'a>(
    val: &'a mut serde_json::Value,
    path: &str,
) -> Option<&'a mut serde_json::Value> {
    if path.is_empty() {
        return Some(val);
    }
    if let Some(rest) = path.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            let idx: usize = rest[..end].parse().ok()?;
            let after = &rest[end + 1..];
            let after = if after.starts_with('.') {
                &after[1..]
            } else {
                after
            };
            let arr = val.as_array_mut()?;
            let elem = arr.get_mut(idx)?;
            return json_get_mut_inner(elem, after);
        }
        return None;
    }
    let (field, rest) = if let Some(dot) = path.find('.') {
        if let Some(bracket) = path.find('[') {
            if bracket < dot {
                (&path[..bracket], &path[bracket..])
            } else {
                (&path[..dot], &path[dot + 1..])
            }
        } else {
            (&path[..dot], &path[dot + 1..])
        }
    } else if let Some(bracket) = path.find('[') {
        (&path[..bracket], &path[bracket..])
    } else {
        (path, "")
    };
    let obj = val.as_object_mut()?;
    let child = obj.get_mut(field)?;
    json_get_mut_inner(child, rest)
}

// ── JSON.ARRTRIM ──────────────────────────────────────────────────────────────

pub struct JsonArrTrimCommand;
impl CommandHandler for JsonArrTrimCommand {
    fn name(&self) -> &str {
        "JSON.ARRTRIM"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.ARRTRIM'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let path = args[2].as_str().unwrap_or("$").to_string();
        let start: i64 = args[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
        let stop: i64 = args[4].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let result = if let Some(target) = json_get_mut(&mut root, &path) {
            if let Some(arr) = target.as_array_mut() {
                let len = arr.len() as i64;
                let s = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len) as usize
                };
                let e = if stop < 0 {
                    (len + stop + 1).max(0) as usize
                } else {
                    (stop + 1).min(len) as usize
                };
                if s >= e || s >= arr.len() {
                    arr.clear();
                    RespValue::integer(0)
                } else {
                    let end = e.min(arr.len());
                    *arr = arr[s..end].to_vec();
                    RespValue::integer(arr.len() as i64)
                }
            } else {
                RespValue::null_bulk()
            }
        } else {
            RespValue::null_bulk()
        };
        store_set(db, key, root);
        result
    }
}

// ── JSON.ARRINDEX ─────────────────────────────────────────────────────────────

pub struct JsonArrIndexCommand;
impl CommandHandler for JsonArrIndexCommand {
    fn name(&self) -> &str {
        "JSON.ARRINDEX"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.ARRINDEX'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let path = args[2].as_str().unwrap_or("$").to_string();
        let needle_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR"),
        };
        let needle: serde_json::Value = match serde_json::from_str(needle_str) {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR invalid JSON"),
        };
        let start: usize = args
            .get(4)
            .and_then(|a| a.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let stop: i64 = args
            .get(5)
            .and_then(|a| a.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let targets = path_get(&root, &path);
        let results: Vec<RespValue> = targets
            .iter()
            .map(|v| {
                if let Some(arr) = v.as_array() {
                    let end = if stop <= 0 {
                        arr.len()
                    } else {
                        (stop as usize).min(arr.len())
                    };
                    for (i, elem) in arr.iter().enumerate().skip(start) {
                        if i >= end {
                            break;
                        }
                        if elem == &needle {
                            return RespValue::integer(i as i64);
                        }
                    }
                    RespValue::integer(-1)
                } else {
                    RespValue::null_bulk()
                }
            })
            .collect();
        if results.len() == 1 {
            results.into_iter().next().unwrap()
        } else {
            RespValue::Array(Some(results))
        }
    }
}

// ── JSON.ARRINSERT ────────────────────────────────────────────────────────────

pub struct JsonArrInsertCommand;
impl CommandHandler for JsonArrInsertCommand {
    fn name(&self) -> &str {
        "JSON.ARRINSERT"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 5 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.ARRINSERT'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let path = args[2].as_str().unwrap_or("$").to_string();
        let index: i64 = args[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
        let values: Vec<serde_json::Value> = args[4..]
            .iter()
            .filter_map(|a| a.as_str().and_then(|s| serde_json::from_str(s).ok()))
            .collect();
        if values.is_empty() {
            return RespValue::error("ERR no values to insert");
        }
        let mut root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let result = if let Some(target) = json_get_mut(&mut root, &path) {
            if let Some(arr) = target.as_array_mut() {
                let len = arr.len() as i64;
                let idx = if index < 0 {
                    (len + index + 1).max(0) as usize
                } else {
                    (index as usize).min(arr.len())
                };
                for (i, v) in values.into_iter().enumerate() {
                    arr.insert(idx + i, v);
                }
                RespValue::integer(arr.len() as i64)
            } else {
                RespValue::null_bulk()
            }
        } else {
            RespValue::null_bulk()
        };
        store_set(db, key, root);
        result
    }
}

// ── JSON.MERGE ────────────────────────────────────────────────────────────────

pub struct JsonMergeCommand;
impl CommandHandler for JsonMergeCommand {
    fn name(&self) -> &str {
        "JSON.MERGE"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 4 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.MERGE'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let path = args[2].as_str().unwrap_or("$").to_string();
        let json_str = match args[3].as_str() {
            Some(s) => s,
            None => return RespValue::error("ERR"),
        };
        let patch: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(e) => return RespValue::error(&format!("ERR invalid JSON: {}", e)),
        };
        let mut root = store_get(db, &key).unwrap_or(serde_json::Value::Null);
        json_merge_at(&mut root, &path, patch);
        store_set(db, key, root);
        RespValue::ok()
    }
}

fn json_merge_at(root: &mut serde_json::Value, path: &str, patch: serde_json::Value) {
    if path == "$" || path == "." {
        json_merge_values(root, patch);
        return;
    }
    if let Some(target) = json_get_mut(root, path) {
        json_merge_values(target, patch);
    }
}

fn json_merge_values(target: &mut serde_json::Value, patch: serde_json::Value) {
    match (target.as_object_mut(), patch.as_object()) {
        (Some(t), Some(p)) => {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else if let Some(existing) = t.get_mut(k) {
                    json_merge_values(existing, v.clone());
                } else {
                    t.insert(k.clone(), v.clone());
                }
            }
        }
        _ => {
            *target = patch;
        }
    }
}

// ── JSON.DUMP ─────────────────────────────────────────────────────────────────

pub struct JsonDumpCommand;
impl CommandHandler for JsonDumpCommand {
    fn name(&self) -> &str {
        "JSON.DUMP"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'JSON.DUMP'");
        }
        let db = *db_index;
        let key = match args[1].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR"),
        };
        let path = if args.len() >= 3 {
            args[2].as_str().unwrap_or("$").to_string()
        } else {
            "$".to_string()
        };
        let root = match store_get(db, &key) {
            Some(v) => v,
            None => return RespValue::null_bulk(),
        };
        let vals = path_get(&root, &path);
        if vals.is_empty() {
            return RespValue::null_bulk();
        }
        RespValue::bulk_str(&vals[0].to_string())
    }
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

pub fn flush_db(db_index: usize) {
    JSON_STORE.lock().remove(&db_index);
    ext_type_registry::flush_db(db_index);
}

pub fn flush_all() {
    JSON_STORE.lock().clear();
    // Note: ext_type_registry is cleared by the caller (FlushAllCommand)
}
