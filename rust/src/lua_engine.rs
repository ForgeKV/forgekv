/// Lua scripting engine for EVAL / EVALSHA.
///
/// Enable full Lua support by building with: `cargo build --features lua`
///
/// Without the `lua` feature, EVAL/EVALSHA return an error indicating Lua
/// support must be compiled in (same as how Redis behaves when built without
/// Lua). The Docker / Linux build enables this feature automatically.
///
/// With the `lua` feature enabled (requires a C compiler, e.g. gcc/clang):
///   - redis.call(cmd, ...)  — dispatch a Redis command; errors raise a Lua error
///   - redis.pcall(cmd, ...) — dispatch; errors return {err = "..."} table
///   - redis.status_reply(s) — return {ok = s}
///   - redis.error_reply(s)  — return {err = s}
///   - redis.log(level, msg) — logs to stderr
///   - KEYS / ARGV global tables (1-indexed, matching Redis semantics)

use std::sync::Arc;

use crate::commands::CommandRegistry;
use crate::resp::RespValue;

// ── Feature-gated implementation ──────────────────────────────────────────────

#[cfg(feature = "lua")]
pub use lua_impl::eval_lua;

#[cfg(not(feature = "lua"))]
pub fn eval_lua(
    _script: &str,
    _keys: &[RespValue],
    _argv: &[RespValue],
    _db_index: usize,
    _registry: &Arc<CommandRegistry>,
) -> RespValue {
    RespValue::error(
        "ERR This Redis instance is not configured to use Lua scripting. \
         Rebuild with --features lua to enable EVAL support.",
    )
}

// ── Full Lua implementation (requires C compiler / gcc) ───────────────────────

#[cfg(feature = "lua")]
mod lua_impl {
    use std::sync::Arc;

    use mlua::{Error as LuaError, Lua, MultiValue, Value};

    use crate::commands::CommandRegistry;
    use crate::resp::RespValue;

    pub fn eval_lua(
        script: &str,
        keys: &[RespValue],
        argv: &[RespValue],
        db_index: usize,
        registry: &Arc<CommandRegistry>,
    ) -> RespValue {
        let lua = Lua::new();

        let result: Result<RespValue, LuaError> = (|| {
            // ── KEYS table ────────────────────────────────────────────────────
            let lua_keys = lua.create_table()?;
            for (i, key) in keys.iter().enumerate() {
                if let Some(b) = key.as_bytes() {
                    lua_keys.set(i + 1, lua.create_string(b)?)?;
                }
            }
            lua.globals().set("KEYS", lua_keys)?;

            // ── ARGV table ────────────────────────────────────────────────────
            let lua_argv = lua.create_table()?;
            for (i, arg) in argv.iter().enumerate() {
                if let Some(b) = arg.as_bytes() {
                    lua_argv.set(i + 1, lua.create_string(b)?)?;
                }
            }
            lua.globals().set("ARGV", lua_argv)?;

            // ── redis.* table ─────────────────────────────────────────────────
            let redis_tbl = lua.create_table()?;

            // redis.call — errors propagate as Lua errors
            {
                let reg = registry.clone();
                redis_tbl.set(
                    "call",
                    lua.create_function(move |lua_ctx, args: MultiValue| {
                        let resp_args = multi_to_resp(&args)?;
                        let mut idx = db_index;
                        let res = reg.execute(&mut idx, &resp_args);
                        match res {
                            RespValue::Error(e) => Err(LuaError::RuntimeError(
                                String::from_utf8_lossy(&e).into_owned(),
                            )),
                            other => resp_to_lua(lua_ctx, other),
                        }
                    })?,
                )?;
            }

            // redis.pcall — errors returned as {err = "..."} table
            {
                let reg = registry.clone();
                redis_tbl.set(
                    "pcall",
                    lua.create_function(move |lua_ctx, args: MultiValue| {
                        let resp_args = multi_to_resp(&args)?;
                        let mut idx = db_index;
                        let res = reg.execute(&mut idx, &resp_args);
                        resp_to_lua(lua_ctx, res)
                    })?,
                )?;
            }

            // redis.status_reply(s) → {ok = s}
            redis_tbl.set(
                "status_reply",
                lua.create_function(|lua_ctx, status: String| {
                    let t = lua_ctx.create_table()?;
                    t.set("ok", status)?;
                    Ok(t)
                })?,
            )?;

            // redis.error_reply(s) → {err = s}
            redis_tbl.set(
                "error_reply",
                lua.create_function(|lua_ctx, err: String| {
                    let t = lua_ctx.create_table()?;
                    t.set("err", err)?;
                    Ok(t)
                })?,
            )?;

            // redis.log(level, msg) — emit to stderr
            redis_tbl.set(
                "log",
                lua.create_function(|_, args: MultiValue| {
                    let msg = args
                        .iter()
                        .rev()
                        .find_map(|v| {
                            if let Value::String(s) = v {
                                Some(s.to_str().map(|s| s.to_string()).unwrap_or_default())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();
                    eprintln!("[Lua] {}", msg);
                    Ok(())
                })?,
            )?;

            // Expose log level constants
            redis_tbl.set("LOG_DEBUG", 0i64)?;
            redis_tbl.set("LOG_VERBOSE", 1i64)?;
            redis_tbl.set("LOG_NOTICE", 2i64)?;
            redis_tbl.set("LOG_WARNING", 3i64)?;

            lua.globals().set("redis", redis_tbl)?;

            // ── Execute ───────────────────────────────────────────────────────
            let results = lua.load(script).eval::<MultiValue>()?;
            Ok(match results.into_iter().next() {
                Some(v) => lua_to_resp(v),
                None => RespValue::null_bulk(),
            })
        })();

        match result {
            Ok(v) => v,
            Err(LuaError::RuntimeError(e)) => {
                RespValue::error(&format!("ERR Error running script (error): {}", e))
            }
            Err(LuaError::SyntaxError { message, .. }) => RespValue::error(&format!(
                "ERR Error compiling script (new function): {}",
                message
            )),
            Err(e) => RespValue::error(&format!("ERR Lua error: {}", e)),
        }
    }

    fn multi_to_resp(args: &MultiValue) -> Result<Vec<RespValue>, LuaError> {
        let mut out = Vec::with_capacity(args.len());
        for val in args.iter() {
            match val {
                Value::String(s) => {
                    out.push(RespValue::BulkString(Some(s.as_bytes().to_vec())));
                }
                Value::Integer(n) => {
                    out.push(RespValue::BulkString(Some(n.to_string().into_bytes())));
                }
                Value::Number(f) => {
                    out.push(RespValue::BulkString(Some((*f as i64).to_string().into_bytes())));
                }
                _ => {
                    return Err(LuaError::RuntimeError(
                        "Lua redis() command arguments must be strings or integers".to_string(),
                    ))
                }
            }
        }
        Ok(out)
    }

    fn resp_to_lua(lua: &Lua, resp: RespValue) -> mlua::Result<Value> {
        match resp {
            RespValue::SimpleString(s) => {
                let t = lua.create_table()?;
                t.set("ok", String::from_utf8_lossy(&s).into_owned())?;
                Ok(Value::Table(t))
            }
            RespValue::Error(e) => {
                let t = lua.create_table()?;
                t.set("err", String::from_utf8_lossy(&e).into_owned())?;
                Ok(Value::Table(t))
            }
            RespValue::Integer(n) => Ok(Value::Integer(n)),
            RespValue::BulkString(None) => Ok(Value::Boolean(false)),
            RespValue::BulkString(Some(b)) => Ok(Value::String(lua.create_string(&b)?)),
            RespValue::Array(None) => Ok(Value::Boolean(false)),
            RespValue::Array(Some(items)) => {
                let t = lua.create_table()?;
                for (i, item) in items.into_iter().enumerate() {
                    t.set(i + 1, resp_to_lua(lua, item)?)?;
                }
                Ok(Value::Table(t))
            }
        }
    }

    pub fn lua_to_resp(val: Value) -> RespValue {
        match val {
            Value::Nil => RespValue::null_bulk(),
            Value::Boolean(false) => RespValue::null_bulk(),
            Value::Boolean(true) => RespValue::Integer(1),
            Value::Integer(n) => RespValue::Integer(n),
            Value::Number(f) => RespValue::Integer(f as i64),
            Value::String(s) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
            Value::Table(t) => {
                if let Ok(Value::String(e)) = t.get::<Value>("err") {
                    return RespValue::Error(e.as_bytes().to_vec());
                }
                if let Ok(Value::String(ok)) = t.get::<Value>("ok") {
                    return RespValue::SimpleString(ok.as_bytes().to_vec());
                }
                let mut items = Vec::new();
                for i in 1i64.. {
                    match t.get::<Value>(i) {
                        Ok(Value::Nil) => break,
                        Ok(v) => items.push(lua_to_resp(v)),
                        Err(_) => break,
                    }
                }
                RespValue::Array(Some(items))
            }
            _ => RespValue::null_bulk(),
        }
    }
}
