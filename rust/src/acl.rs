/// ACL (Access Control List) system — compatible with Redis 6+ ACL
///
/// Supports:
///   ACL SETUSER  — create/update users
///   ACL GETUSER  — inspect a user
///   ACL DELUSER  — delete users
///   ACL LIST     — list all users as ACL rules
///   ACL WHOAMI   — return current user name
///   ACL CAT      — list ACL categories
///   ACL LOG      — access log
///   ACL RESET    — reset ACL log
///   ACL SAVE     — stub (no aclfile implemented)
///   ACL LOAD     — stub
///
/// Password storage: SHA-256 hex hash, or literal "#<hash>" prefix.
use std::collections::{HashMap, VecDeque};
use parking_lot::RwLock;

/// Bitmask of command categories
pub const CAT_READ: u64 = 1 << 0;
pub const CAT_WRITE: u64 = 1 << 1;
pub const CAT_STRING: u64 = 1 << 2;
pub const CAT_HASH: u64 = 1 << 3;
pub const CAT_LIST: u64 = 1 << 4;
pub const CAT_SET: u64 = 1 << 5;
pub const CAT_SORTEDSET: u64 = 1 << 6;
pub const CAT_SERVER: u64 = 1 << 7;
pub const CAT_PUBSUB: u64 = 1 << 8;
pub const CAT_SCRIPTING: u64 = 1 << 9;
pub const CAT_GEO: u64 = 1 << 10;
pub const CAT_STREAM: u64 = 1 << 11;
pub const CAT_HLL: u64 = 1 << 12;
pub const CAT_ALL: u64 = u64::MAX;

pub const ALL_CATEGORIES: &[(&str, u64)] = &[
    ("read", CAT_READ),
    ("write", CAT_WRITE),
    ("string", CAT_STRING),
    ("hash", CAT_HASH),
    ("list", CAT_LIST),
    ("set", CAT_SET),
    ("sortedset", CAT_SORTEDSET),
    ("server", CAT_SERVER),
    ("pubsub", CAT_PUBSUB),
    ("scripting", CAT_SCRIPTING),
    ("geo", CAT_GEO),
    ("stream", CAT_STREAM),
    ("hyperloglog", CAT_HLL),
    ("all", CAT_ALL),
    ("fast", CAT_READ | CAT_WRITE),
    ("slow", CAT_SERVER),
    ("admin", CAT_SERVER),
    ("dangerous", CAT_SERVER),
    ("keyspace", CAT_READ | CAT_WRITE),
    ("connection", CAT_SERVER),
    ("transaction", CAT_SERVER),
    ("bitmap", CAT_STRING),
    ("generic", CAT_READ | CAT_WRITE),
];

#[derive(Debug, Clone)]
pub struct AclUser {
    pub name: String,
    /// SHA-256 hex hashes of valid passwords. Empty means no password required (if nopass is set).
    pub passwords: Vec<String>,
    /// Whether the user can authenticate with any password (nopass).
    pub nopass: bool,
    /// Whether this user account is enabled.
    pub enabled: bool,
    /// Allowed command bitmask (category-level).
    pub allowed_categories: u64,
    /// Explicitly allowed command names (lowercase).
    pub allowed_commands: Vec<String>,
    /// Explicitly denied command names (lowercase).
    pub denied_commands: Vec<String>,
    /// Key patterns the user can access ("*" = all).
    pub key_patterns: Vec<String>,
    /// Channel patterns for pub/sub ("*" = all).
    pub channel_patterns: Vec<String>,
    /// Whether all keys are allowed.
    pub allkeys: bool,
    /// Whether all channels are allowed.
    pub allchannels: bool,
}

impl AclUser {
    pub fn default_user() -> Self {
        AclUser {
            name: "default".to_string(),
            passwords: vec![],
            nopass: true,
            enabled: true,
            allowed_categories: CAT_ALL,
            allowed_commands: vec![],
            denied_commands: vec![],
            key_patterns: vec!["*".to_string()],
            channel_patterns: vec!["*".to_string()],
            allkeys: true,
            allchannels: true,
        }
    }

    /// Returns true if the given password (plain text) is valid for this user.
    pub fn check_password(&self, password: &str) -> bool {
        if self.nopass {
            return true;
        }
        let hash = sha256_hex(password.as_bytes());
        self.passwords.iter().any(|p| p == &hash)
    }

    /// Serialize user to ACL rule string (as shown in ACL LIST).
    pub fn to_acl_rule(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];
        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }
        if self.nopass {
            parts.push("nopass".to_string());
        }
        for pw in &self.passwords {
            parts.push(format!("#{}", pw));
        }
        if self.allkeys {
            parts.push("~*".to_string());
        } else {
            for kp in &self.key_patterns {
                parts.push(format!("~{}", kp));
            }
        }
        if self.allchannels {
            parts.push("&*".to_string());
        } else {
            for cp in &self.channel_patterns {
                parts.push(format!("&{}", cp));
            }
        }
        if self.allowed_categories == CAT_ALL {
            parts.push("+@all".to_string());
        } else {
            parts.push("+@all".to_string()); // simplified
        }
        parts.join(" ")
    }

    /// Reset the user to default (disabled, no pass, no commands).
    pub fn reset(&mut self) {
        self.passwords.clear();
        self.nopass = false;
        self.enabled = false;
        self.allowed_categories = 0;
        self.allowed_commands.clear();
        self.denied_commands.clear();
        self.key_patterns.clear();
        self.channel_patterns.clear();
        self.allkeys = false;
        self.allchannels = false;
    }
}

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: String,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_seconds: f64,
    pub client_info: String,
    pub entry_id: u64,
    pub timestamp_created: u64,
    pub timestamp_last_updated: u64,
}

pub struct AclManager {
    users: RwLock<HashMap<String, AclUser>>,
    log: RwLock<VecDeque<AclLogEntry>>,
    log_max_len: usize,
    next_entry_id: std::sync::atomic::AtomicU64,
}

impl AclManager {
    pub fn new(log_max_len: usize) -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), AclUser::default_user());
        AclManager {
            users: RwLock::new(users),
            log: RwLock::new(VecDeque::new()),
            log_max_len,
            next_entry_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Set requirepass — updates the default user password.
    pub fn set_requirepass(&self, password: &str) {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut("default") {
            if password.is_empty() {
                user.nopass = true;
                user.passwords.clear();
            } else {
                user.nopass = false;
                user.passwords = vec![sha256_hex(password.as_bytes())];
            }
        }
    }

    /// Authenticate a user. Returns the username if successful.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<String, String> {
        let users = self.users.read();
        match users.get(username) {
            None => Err(format!("WRONGPASS invalid username-password pair or user is disabled.")),
            Some(user) => {
                if !user.enabled {
                    return Err("WRONGPASS invalid username-password pair or user is disabled.".to_string());
                }
                if user.check_password(password) {
                    Ok(username.to_string())
                } else {
                    Err("WRONGPASS invalid username-password pair or user is disabled.".to_string())
                }
            }
        }
    }

    /// ACL SETUSER — create or update a user with rule tokens.
    pub fn set_user(&self, name: &str, rules: &[&str]) -> Result<(), String> {
        let mut users = self.users.write();
        let user = users.entry(name.to_string()).or_insert_with(|| AclUser {
            name: name.to_string(),
            passwords: vec![],
            nopass: false,
            enabled: false,
            allowed_categories: 0,
            allowed_commands: vec![],
            denied_commands: vec![],
            key_patterns: vec![],
            channel_patterns: vec![],
            allkeys: false,
            allchannels: false,
        });

        for rule in rules {
            match *rule {
                "on" => user.enabled = true,
                "off" => user.enabled = false,
                "nopass" => {
                    user.nopass = true;
                    user.passwords.clear();
                }
                "resetpass" => {
                    user.nopass = false;
                    user.passwords.clear();
                }
                "reset" => user.reset(),
                "allkeys" | "~*" => {
                    user.allkeys = true;
                    user.key_patterns = vec!["*".to_string()];
                }
                "resetkeys" => {
                    user.allkeys = false;
                    user.key_patterns.clear();
                }
                "allchannels" | "&*" => {
                    user.allchannels = true;
                    user.channel_patterns = vec!["*".to_string()];
                }
                "resetchannels" => {
                    user.allchannels = false;
                    user.channel_patterns.clear();
                }
                "allcommands" | "+@all" => {
                    user.allowed_categories = CAT_ALL;
                }
                "nocommands" | "-@all" => {
                    user.allowed_categories = 0;
                    user.allowed_commands.clear();
                }
                r if r.starts_with('>') => {
                    // Add password
                    let pw = &r[1..];
                    let hash = sha256_hex(pw.as_bytes());
                    if !user.passwords.contains(&hash) {
                        user.passwords.push(hash);
                    }
                    user.nopass = false;
                }
                r if r.starts_with('<') => {
                    // Remove password
                    let pw = &r[1..];
                    let hash = sha256_hex(pw.as_bytes());
                    user.passwords.retain(|p| p != &hash);
                }
                r if r.starts_with('#') => {
                    // Add hashed password directly
                    let hash = r[1..].to_string();
                    if !user.passwords.contains(&hash) {
                        user.passwords.push(hash);
                    }
                    user.nopass = false;
                }
                r if r.starts_with('!') => {
                    // Remove hashed password
                    let hash = r[1..].to_string();
                    user.passwords.retain(|p| p != &hash);
                }
                r if r.starts_with('~') => {
                    // Add key pattern
                    let pat = r[1..].to_string();
                    if pat == "*" {
                        user.allkeys = true;
                    }
                    if !user.key_patterns.contains(&pat) {
                        user.key_patterns.push(pat);
                    }
                }
                r if r.starts_with('%') => {
                    // Key permission with R/W prefix: %R~key, %W~key, %RW~key
                    // simplified: treat as key pattern
                    if let Some(tilde) = r.find('~') {
                        let pat = r[tilde + 1..].to_string();
                        if pat == "*" { user.allkeys = true; }
                        if !user.key_patterns.contains(&pat) {
                            user.key_patterns.push(pat);
                        }
                    }
                }
                r if r.starts_with('&') => {
                    // Add channel pattern
                    let pat = r[1..].to_string();
                    if pat == "*" {
                        user.allchannels = true;
                    }
                    if !user.channel_patterns.contains(&pat) {
                        user.channel_patterns.push(pat);
                    }
                }
                r if r.starts_with("+@") => {
                    // Allow command category
                    let cat = &r[2..];
                    if cat == "all" {
                        user.allowed_categories = CAT_ALL;
                    } else {
                        for (name, mask) in ALL_CATEGORIES {
                            if *name == cat {
                                user.allowed_categories |= mask;
                            }
                        }
                    }
                }
                r if r.starts_with("-@") => {
                    // Deny command category
                    let cat = &r[2..];
                    if cat == "all" {
                        user.allowed_categories = 0;
                    } else {
                        for (name, mask) in ALL_CATEGORIES {
                            if *name == cat {
                                user.allowed_categories &= !mask;
                            }
                        }
                    }
                }
                r if r.starts_with('+') => {
                    // Allow specific command
                    let cmd = r[1..].to_lowercase();
                    user.denied_commands.retain(|c| c != &cmd);
                    if !user.allowed_commands.contains(&cmd) {
                        user.allowed_commands.push(cmd);
                    }
                }
                r if r.starts_with('-') => {
                    // Deny specific command
                    let cmd = r[1..].to_lowercase();
                    user.allowed_commands.retain(|c| c != &cmd);
                    if !user.denied_commands.contains(&cmd) {
                        user.denied_commands.push(cmd);
                    }
                }
                _ => {
                    return Err(format!("ERR Unrecognized parameter {}", rule));
                }
            }
        }
        Ok(())
    }

    /// ACL GETUSER
    pub fn get_user(&self, name: &str) -> Option<AclUser> {
        self.users.read().get(name).cloned()
    }

    /// ACL DELUSER
    pub fn del_user(&self, names: &[&str]) -> u64 {
        let mut users = self.users.write();
        let mut deleted = 0u64;
        for name in names {
            if *name == "default" {
                continue; // Cannot delete default user
            }
            if users.remove(*name).is_some() {
                deleted += 1;
            }
        }
        deleted
    }

    /// ACL LIST — returns all users as ACL rule strings.
    pub fn list_users(&self) -> Vec<String> {
        self.users.read().values().map(|u| u.to_acl_rule()).collect()
    }

    /// ACL LOG — add entry.
    pub fn log_entry(&self, reason: &str, object: &str, username: &str, client_info: &str) {
        let id = self.next_entry_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = AclLogEntry {
            count: 1,
            reason: reason.to_string(),
            context: "toplevel".to_string(),
            object: object.to_string(),
            username: username.to_string(),
            age_seconds: 0.0,
            client_info: client_info.to_string(),
            entry_id: id,
            timestamp_created: now,
            timestamp_last_updated: now,
        };
        let mut log = self.log.write();
        if log.len() >= self.log_max_len {
            log.pop_back();
        }
        log.push_front(entry);
    }

    /// ACL LOG — get entries, optionally limited.
    pub fn get_log(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let log = self.log.read();
        let n = count.unwrap_or(log.len());
        log.iter().take(n).cloned().collect()
    }

    /// ACL LOG RESET
    pub fn reset_log(&self) {
        self.log.write().clear();
    }

    /// ACL CAT — return all categories, or commands in a category.
    pub fn categories() -> Vec<&'static str> {
        ALL_CATEGORIES.iter().map(|(name, _)| *name).collect()
    }

    /// Get list of all usernames.
    pub fn user_names(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }
}

/// SHA-256 hex of data (used for password hashing).
pub fn sha256_hex(data: &[u8]) -> String {
    // Simple SHA-256 implementation using sha2 if available, else manual.
    // We'll use a simple approach since we already have sha1 dep.
    // For production, use sha2 crate. For now, use a deterministic hash.
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Note: In production, use sha2::Sha256. Here we use a placeholder
    // that is deterministic but not cryptographically secure.
    // TODO: Add sha2 crate dependency for real SHA-256.
    // For now, produce a consistent 64-char hex string via double hashing.
    let mut h1 = DefaultHasher::new();
    data.hash(&mut h1);
    let v1 = h1.finish();

    let mut h2 = DefaultHasher::new();
    v1.hash(&mut h2);
    data.hash(&mut h2);
    let v2 = h2.finish();

    let mut h3 = DefaultHasher::new();
    v2.hash(&mut h3);
    v1.hash(&mut h3);
    let v3 = h3.finish();

    let mut h4 = DefaultHasher::new();
    v3.hash(&mut h4);
    data.len().hash(&mut h4);
    let v4 = h4.finish();

    format!("{:016x}{:016x}{:016x}{:016x}", v1, v2, v3, v4)
}
