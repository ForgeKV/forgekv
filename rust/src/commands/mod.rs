pub mod bf_cmds;
pub mod cf_cmds;
pub mod geo_cmds;
pub mod json_cmds;
pub mod probabilistic_cmds;
pub mod search_cmds;
pub mod stream_cmds;
pub mod tdigest_cmds;
pub mod hash_cmds;
pub mod hll_cmds;
pub mod key_cmds;
pub mod list_cmds;
pub mod server_cmds;
pub mod set_cmds;
pub mod string_cmds;
pub mod zset_cmds;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::resp::RespValue;

pub trait CommandHandler: Send + Sync {
    fn name(&self) -> &str;
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue;
}

pub struct CommandRegistry {
    handlers: RwLock<HashMap<String, Arc<dyn CommandHandler>>>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        CommandRegistry {
            handlers: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, handler: Arc<dyn CommandHandler>) {
        let mut handlers = self.handlers.write();
        handlers.insert(handler.name().to_uppercase(), handler);
    }

    pub fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.is_empty() {
            return RespValue::error("ERR empty command");
        }

        let cmd_name = match args[0].as_str() {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR command name must be a string"),
        };

        let handlers = self.handlers.read();
        match handlers.get(&cmd_name) {
            Some(handler) => handler.execute(db_index, args),
            None => RespValue::error(&format!("ERR unknown command '{}'", cmd_name)),
        }
    }
}
