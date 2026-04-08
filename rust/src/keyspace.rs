/// Redis-compatible keyspace notifications.
///
/// Publishes to:
///   __keyspace@<db>__:<key>   → event name (e.g. "set", "del", "lpush")
///   __keyevent@<db>__:<event> → key name
///
/// Controlled by the `notify-keyspace-events` config flag.
use lazy_static::lazy_static;
use parking_lot::RwLock;
use tokio::sync::broadcast;

/// Notification tuple: (db_index, key_bytes, event_name)
pub type KsNotif = (usize, Vec<u8>, String);

/// Parsed flags from the notify-keyspace-events config string.
#[derive(Default, Clone)]
pub struct KsConfig {
    pub enabled: bool,  // any output enabled?
    pub keyspace: bool, // K
    pub keyevent: bool, // E
    pub generic: bool,  // g  (DEL EXPIRE RENAME COPY MOVE RESTORE …)
    pub string: bool,   // $  (SET GET INCR …)
    pub list: bool,     // l
    pub set: bool,      // s
    pub zset: bool,     // z
    pub hash: bool,     // h
    pub expired: bool,  // x
    pub stream: bool,   // t
}

impl KsConfig {
    fn update_enabled(&mut self) {
        let any_type = self.generic
            || self.string
            || self.list
            || self.set
            || self.zset
            || self.hash
            || self.expired
            || self.stream;
        self.enabled = (self.keyspace || self.keyevent) && any_type;
    }
}

pub struct KeyspaceNotifier {
    tx: broadcast::Sender<KsNotif>,
    pub cfg: RwLock<KsConfig>,
}

impl KeyspaceNotifier {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(8192);
        KeyspaceNotifier {
            tx,
            cfg: RwLock::new(KsConfig::default()),
        }
    }

    /// Apply a notify-keyspace-events string (e.g. "KEA", "Klx", "").
    pub fn configure(&self, s: &str) {
        let mut c = self.cfg.write();
        let all = s.contains('A');
        c.keyspace = s.contains('K');
        c.keyevent = s.contains('E');
        c.generic = all || s.contains('g');
        c.string = all || s.contains('$');
        c.list = all || s.contains('l');
        c.set = all || s.contains('s');
        c.zset = all || s.contains('z');
        c.hash = all || s.contains('h');
        c.expired = all || s.contains('x');
        c.stream = all || s.contains('t');
        c.update_enabled();
    }

    /// Emit a notification. `event_type` is a single char: '$', 'l', 's', etc.
    /// Or use '\0' for "always" (e.g. DEL).
    pub fn notify(&self, db: usize, key: &[u8], event: &str) {
        if !self.cfg.read().enabled {
            return;
        }
        let _ = self.tx.send((db, key.to_vec(), event.to_string()));
    }

    pub fn subscribe(&self) -> broadcast::Receiver<KsNotif> {
        self.tx.subscribe()
    }
}

lazy_static! {
    pub static ref KS_NOTIFIER: KeyspaceNotifier = KeyspaceNotifier::new();
}

// ── Command → event mapping ────────────────────────────────────────────────────

/// Return the keyspace event name for a command, or "" if the command
/// doesn't generate notifications (e.g. read-only commands).
/// Also returns a hint about which category gate to check ('g','$','l','s','z','h','t','x').
pub fn cmd_event(cmd: &str) -> (&'static str, char) {
    match cmd {
        // Generic
        "DEL" | "UNLINK" => ("del", 'g'),
        "EXPIRE" => ("expire", 'g'),
        "EXPIREAT" => ("expire", 'g'),
        "PEXPIRE" => ("pexpire", 'g'),
        "PEXPIREAT" => ("pexpire", 'g'),
        "PERSIST" => ("persist", 'g'),
        "RENAME" => ("rename_from", 'g'), // special: also rename_to on dest
        "RENAMENX" => ("rename_from", 'g'),
        "COPY" => ("copy_to", 'g'),
        "MOVE" => ("move_from", 'g'),
        "RESTORE" => ("restore", 'g'),
        "OBJECT" => ("object", 'g'),
        "SORT" => ("sortstore", 'g'),
        "WAIT" => ("", '\0'),

        // Strings  ($)
        "SET" => ("set", '$'),
        "SETEX" => ("set", '$'),
        "PSETEX" => ("set", '$'),
        "SETNX" => ("set", '$'),
        "GETSET" => ("set", '$'),
        "GETDEL" => ("del", '$'),
        "GETEX" => ("getex", '$'),
        "MSET" => ("set", '$'), // key = args[i*2+1]
        "MSETNX" => ("set", '$'),
        "SETRANGE" => ("setrange", '$'),
        "INCR" => ("incrby", '$'),
        "INCRBY" => ("incrby", '$'),
        "INCRBYFLOAT" => ("incrbyfloat", '$'),
        "DECR" => ("decrby", '$'),
        "DECRBY" => ("decrby", '$'),
        "APPEND" => ("append", '$'),

        // Lists (l)
        "LPUSH" | "LPUSHX" => ("lpush", 'l'),
        "RPUSH" | "RPUSHX" => ("rpush", 'l'),
        "LPOP" => ("lpop", 'l'),
        "RPOP" => ("rpop", 'l'),
        "LINSERT" => ("linsert", 'l'),
        "LSET" => ("lset", 'l'),
        "LTRIM" => ("ltrim", 'l'),
        "LREM" => ("lrem", 'l'),
        "LMOVE" => ("lmove", 'l'),
        "RPOPLPUSH" => ("rpoplpush", 'l'),
        "LMPOP" => ("lpop", 'l'),

        // Sets (s)
        "SADD" => ("sadd", 's'),
        "SREM" => ("srem", 's'),
        "SPOP" => ("spop", 's'),
        "SMOVE" => ("srem", 's'), // special
        "SINTERSTORE" => ("sinterstore", 's'),
        "SUNIONSTORE" => ("sunionstore", 's'),
        "SDIFFSTORE" => ("sdiffstore", 's'),

        // Sorted sets (z)
        "ZADD" => ("zadd", 'z'),
        "ZREM" => ("zrem", 'z'),
        "ZINCRBY" => ("zincrby", 'z'),
        "ZPOPMIN" => ("zpopmin", 'z'),
        "ZPOPMAX" => ("zpopmax", 'z'),
        "ZRANGESTORE" => ("zrangestore", 'z'),
        "ZUNIONSTORE" => ("zunionstore", 'z'),
        "ZINTERSTORE" => ("zinterstore", 'z'),
        "ZDIFFSTORE" => ("zdiffstore", 'z'),
        "BZPOPMIN" => ("zpopmin", 'z'),
        "BZPOPMAX" => ("zpopmax", 'z'),

        // Hashes (h)
        "HSET" | "HMSET" => ("hset", 'h'),
        "HSETNX" => ("hset", 'h'),
        "HDEL" => ("hdel", 'h'),
        "HINCRBY" => ("hincrby", 'h'),
        "HINCRBYFLOAT" => ("hincrbyfloat", 'h'),

        // Streams (t)
        "XADD" => ("xadd", 't'),
        "XDEL" => ("xdel", 't'),
        "XTRIM" => ("xtrim", 't'),
        "XSETID" => ("xsetid", 't'),
        "XGROUP" => ("xgroup-create", 't'),
        "XACK" => ("xack", 't'),
        "XCLAIM" => ("xclaim", 't'),
        "XAUTOCLAIM" => ("xautoclaim", 't'),

        // Expired (x) — handled internally when key expires
        // Generic: FLUSHDB / FLUSHALL don't generate per-key events
        _ => ("", '\0'),
    }
}

/// Check if the event passes the category filter.
pub fn event_allowed(cfg: &KsConfig, cat: char) -> bool {
    match cat {
        'g' => cfg.generic,
        '$' => cfg.string,
        'l' => cfg.list,
        's' => cfg.set,
        'z' => cfg.zset,
        'h' => cfg.hash,
        't' => cfg.stream,
        'x' => cfg.expired,
        _ => true, // '\0' or unknown: always allow if any type is enabled
    }
}
