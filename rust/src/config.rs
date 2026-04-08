use std::error::Error;
use std::fs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalSyncMode {
    Always,
    Everysec,
    No,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllkeysLru,
    VolatileLru,
    AllkeysLfu,
    VolatileLfu,
    AllkeysRandom,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "noeviction",
            EvictionPolicy::AllkeysLru => "allkeys-lru",
            EvictionPolicy::VolatileLru => "volatile-lru",
            EvictionPolicy::AllkeysLfu => "allkeys-lfu",
            EvictionPolicy::VolatileLfu => "volatile-lfu",
            EvictionPolicy::AllkeysRandom => "allkeys-random",
            EvictionPolicy::VolatileRandom => "volatile-random",
            EvictionPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn from_str(s: &str) -> EvictionPolicy {
        match s.to_lowercase().as_str() {
            "allkeys-lru" => EvictionPolicy::AllkeysLru,
            "volatile-lru" => EvictionPolicy::VolatileLru,
            "allkeys-lfu" => EvictionPolicy::AllkeysLfu,
            "volatile-lfu" => EvictionPolicy::VolatileLfu,
            "allkeys-random" => EvictionPolicy::AllkeysRandom,
            "volatile-random" => EvictionPolicy::VolatileRandom,
            "volatile-ttl" => EvictionPolicy::VolatileTtl,
            _ => EvictionPolicy::NoEviction,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    // Network
    pub bind: String,
    pub port: u16,
    pub tcp_backlog: u32,
    pub tcp_keepalive: u32,
    pub timeout: u64,
    pub protected_mode: bool,

    // Storage
    pub dir: String,
    pub dbfilename: String,
    pub databases: usize,
    pub memtable_size_mb: u64,
    pub wal_sync_mode: WalSyncMode,

    // Replication
    pub repl_backlog_size: u64,
    pub repl_diskless_sync: bool,
    pub repl_diskless_sync_delay: u64,
    pub repl_timeout: u64,
    pub replica_lazy_flush: bool,
    pub replica_serve_stale_data: bool,
    pub replica_read_only: bool,

    // General
    pub hz: u32,
    pub dynamic_hz: bool,
    pub log_level: LogLevel,
    pub log_file: String,
    pub syslog_enabled: bool,
    pub syslog_ident: String,
    pub crash_log_enabled: bool,

    // Security
    pub requirepass: String,
    pub aclfile: String,
    pub acllog_max_len: u64,
    pub enable_debug_command: bool,
    pub enable_protected_configs: bool,

    // Memory
    pub max_memory: u64,
    pub max_memory_policy: EvictionPolicy,
    pub maxmemory_samples: u32,
    pub lfu_log_factor: u32,
    pub lfu_decay_time: u32,
    pub active_expire_enabled: bool,
    pub lazyfree_lazy_eviction: bool,
    pub lazyfree_lazy_expire: bool,
    pub lazyfree_lazy_server_del: bool,
    pub lazyfree_lazy_user_del: bool,
    pub lazyfree_lazy_user_flush: bool,
    pub activerehashing: bool,
    pub activedefrag: bool,

    // Limits
    pub proto_max_bulk_len: u64,
    pub client_query_buffer_limit: u64,

    // Encoding thresholds (listpack/ziplist)
    pub hash_max_listpack_entries: u64,
    pub hash_max_listpack_value: u64,
    pub set_max_intset_entries: u64,
    pub set_max_listpack_entries: u64,
    pub set_max_listpack_value: u64,
    pub zset_max_listpack_entries: u64,
    pub zset_max_listpack_value: u64,
    pub list_max_listpack_size: i32,
    pub list_compress_depth: u32,
    pub active_defrag_ignore_bytes: u64,
    pub stream_node_max_bytes: u64,
    pub stream_node_max_entries: u64,

    // Scripting
    pub lua_time_limit: u64,
    pub busy_reply_threshold: u64,
    pub script_oom_score_adj_values: String,

    // Slow log
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: u64,

    // Latency
    pub latency_tracking: bool,
    pub latency_monitor_threshold: u64,

    // Notifications
    pub notify_keyspace_events: String,

    // Cluster
    pub cluster_enabled: bool,
    pub cluster_config_file: String,
    pub cluster_node_timeout: u64,
    pub cluster_require_full_coverage: bool,

    // IO
    pub io_threads: u32,
    pub io_threads_do_reads: bool,

    // RDB
    pub rdb_compression: bool,
    pub rdb_checksum: bool,
    pub rdb_save_incremental_fsync: bool,
    pub aof_use_rdb_preamble: bool,

    // Tracking
    pub tracking_table_max_keys: u64,

    // Misc
    pub jemalloc_bg_thread: bool,
    pub socket_mark_id: u32,
    pub close_on_oom: bool,
    pub loglevel_debug_sleep: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            tcp_backlog: 511,
            tcp_keepalive: 300,
            timeout: 0,
            protected_mode: true,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            databases: 16,
            memtable_size_mb: 512,
            wal_sync_mode: WalSyncMode::Everysec,
            repl_backlog_size: 1048576,
            repl_diskless_sync: true,
            repl_diskless_sync_delay: 5,
            repl_timeout: 60,
            replica_lazy_flush: false,
            replica_serve_stale_data: true,
            replica_read_only: true,
            hz: 10,
            dynamic_hz: true,
            log_level: LogLevel::Notice,
            log_file: String::new(),
            syslog_enabled: false,
            syslog_ident: "redis".to_string(),
            crash_log_enabled: true,
            requirepass: String::new(),
            aclfile: String::new(),
            acllog_max_len: 128,
            enable_debug_command: true,
            enable_protected_configs: false,
            max_memory: 0,
            max_memory_policy: EvictionPolicy::NoEviction,
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            active_expire_enabled: true,
            lazyfree_lazy_eviction: false,
            lazyfree_lazy_expire: false,
            lazyfree_lazy_server_del: false,
            lazyfree_lazy_user_del: false,
            lazyfree_lazy_user_flush: false,
            activerehashing: true,
            activedefrag: false,
            proto_max_bulk_len: 536870912,         // 512mb
            client_query_buffer_limit: 1073741824, // 1gb
            hash_max_listpack_entries: 128,
            hash_max_listpack_value: 64,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            list_max_listpack_size: -2,
            list_compress_depth: 0,
            active_defrag_ignore_bytes: 104857600,
            stream_node_max_bytes: 4096,
            stream_node_max_entries: 100,
            lua_time_limit: 5000,
            busy_reply_threshold: 5000,
            script_oom_score_adj_values: "0 200 800".to_string(),
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            latency_tracking: true,
            latency_monitor_threshold: 0,
            notify_keyspace_events: String::new(),
            cluster_enabled: false,
            cluster_config_file: "nodes.conf".to_string(),
            cluster_node_timeout: 15000,
            cluster_require_full_coverage: true,
            io_threads: 1,
            io_threads_do_reads: false,
            rdb_compression: true,
            rdb_checksum: true,
            rdb_save_incremental_fsync: true,
            aof_use_rdb_preamble: true,
            tracking_table_max_keys: 0,
            jemalloc_bg_thread: true,
            socket_mark_id: 0,
            close_on_oom: false,
            loglevel_debug_sleep: 0,
        }
    }
}

pub struct ConfigParser;

impl ConfigParser {
    pub fn parse_file(path: &str) -> Result<ServerConfig, Box<dyn Error>> {
        let content = fs::read_to_string(path)?;
        let mut config = ServerConfig::default();

        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = trimmed.splitn(2, char::is_whitespace).collect();
            if parts.len() < 2 {
                continue;
            }

            let key = parts[0].trim().to_lowercase();
            let value = parts[1].trim();

            match key.as_str() {
                "bind" => {
                    config.bind = value
                        .split_whitespace()
                        .next()
                        .unwrap_or("127.0.0.1")
                        .to_string()
                }
                "port" => config.port = value.parse().unwrap_or(6379),
                "tcp-backlog" => config.tcp_backlog = value.parse().unwrap_or(511),
                "tcp-keepalive" => config.tcp_keepalive = value.parse().unwrap_or(300),
                "timeout" => config.timeout = value.parse().unwrap_or(0),
                "protected-mode" => config.protected_mode = value == "yes",
                "dir" => config.dir = value.to_string(),
                "dbfilename" => config.dbfilename = value.to_string(),
                "databases" => config.databases = value.parse().unwrap_or(16),
                "hz" => config.hz = value.parse().unwrap_or(10),
                "dynamic-hz" => config.dynamic_hz = value == "yes",
                "maxmemory" => config.max_memory = parse_memory_size(value),
                "maxmemory-policy" => config.max_memory_policy = EvictionPolicy::from_str(value),
                "maxmemory-samples" => config.maxmemory_samples = value.parse().unwrap_or(5),
                "lfu-log-factor" => config.lfu_log_factor = value.parse().unwrap_or(10),
                "lfu-decay-time" => config.lfu_decay_time = value.parse().unwrap_or(1),
                "memtable-size-mb" => config.memtable_size_mb = value.parse().unwrap_or(64),
                "wal-sync-mode" | "appendfsync" => {
                    config.wal_sync_mode = match value.to_lowercase().as_str() {
                        "always" => WalSyncMode::Always,
                        "everysec" => WalSyncMode::Everysec,
                        "no" => WalSyncMode::No,
                        _ => WalSyncMode::Everysec,
                    }
                }
                "loglevel" => {
                    config.log_level = match value.to_lowercase().as_str() {
                        "debug" => LogLevel::Debug,
                        "verbose" => LogLevel::Verbose,
                        "notice" => LogLevel::Notice,
                        "warning" => LogLevel::Warning,
                        _ => LogLevel::Notice,
                    }
                }
                "logfile" => config.log_file = value.to_string(),
                "syslog-enabled" => config.syslog_enabled = value == "yes",
                "syslog-ident" => config.syslog_ident = value.to_string(),
                "requirepass" => config.requirepass = value.to_string(),
                "aclfile" => config.aclfile = value.to_string(),
                "acllog-max-len" => config.acllog_max_len = value.parse().unwrap_or(128),
                "enable-debug-command" => config.enable_debug_command = value != "no",
                "lazyfree-lazy-eviction" => config.lazyfree_lazy_eviction = value == "yes",
                "lazyfree-lazy-expire" => config.lazyfree_lazy_expire = value == "yes",
                "lazyfree-lazy-server-del" => config.lazyfree_lazy_server_del = value == "yes",
                "lazyfree-lazy-user-del" => config.lazyfree_lazy_user_del = value == "yes",
                "lazyfree-lazy-user-flush" => config.lazyfree_lazy_user_flush = value == "yes",
                "activerehashing" => config.activerehashing = value == "yes",
                "activedefrag" => config.activedefrag = value == "yes",
                "proto-max-bulk-len" => config.proto_max_bulk_len = parse_memory_size(value),
                "client-query-buffer-limit" => {
                    config.client_query_buffer_limit = parse_memory_size(value)
                }
                "hash-max-listpack-entries" | "hash-max-ziplist-entries" => {
                    config.hash_max_listpack_entries = value.parse().unwrap_or(128);
                }
                "hash-max-listpack-value" | "hash-max-ziplist-value" => {
                    config.hash_max_listpack_value = value.parse().unwrap_or(64);
                }
                "set-max-intset-entries" => {
                    config.set_max_intset_entries = value.parse().unwrap_or(512)
                }
                "set-max-listpack-entries" => {
                    config.set_max_listpack_entries = value.parse().unwrap_or(128)
                }
                "set-max-listpack-value" => {
                    config.set_max_listpack_value = value.parse().unwrap_or(64)
                }
                "zset-max-listpack-entries" | "zset-max-ziplist-entries" => {
                    config.zset_max_listpack_entries = value.parse().unwrap_or(128);
                }
                "zset-max-listpack-value" | "zset-max-ziplist-value" => {
                    config.zset_max_listpack_value = value.parse().unwrap_or(64);
                }
                "list-max-listpack-size" | "list-max-ziplist-size" => {
                    config.list_max_listpack_size = value.parse().unwrap_or(-2);
                }
                "list-compress-depth" => config.list_compress_depth = value.parse().unwrap_or(0),
                "stream-node-max-bytes" => {
                    config.stream_node_max_bytes = value.parse().unwrap_or(4096)
                }
                "stream-node-max-entries" => {
                    config.stream_node_max_entries = value.parse().unwrap_or(100)
                }
                "lua-time-limit" => config.lua_time_limit = value.parse().unwrap_or(5000),
                "busy-reply-threshold" => {
                    config.busy_reply_threshold = value.parse().unwrap_or(5000)
                }
                "slowlog-log-slower-than" => {
                    config.slowlog_log_slower_than = value.parse().unwrap_or(10000)
                }
                "slowlog-max-len" => config.slowlog_max_len = value.parse().unwrap_or(128),
                "latency-tracking" => config.latency_tracking = value == "yes",
                "latency-monitor-threshold" => {
                    config.latency_monitor_threshold = value.parse().unwrap_or(0)
                }
                "notify-keyspace-events" => config.notify_keyspace_events = value.to_string(),
                "cluster-enabled" => config.cluster_enabled = value == "yes",
                "cluster-config-file" => config.cluster_config_file = value.to_string(),
                "cluster-node-timeout" => {
                    config.cluster_node_timeout = value.parse().unwrap_or(15000)
                }
                "cluster-require-full-coverage" => {
                    config.cluster_require_full_coverage = value == "yes"
                }
                "io-threads" => config.io_threads = value.parse().unwrap_or(1),
                "io-threads-do-reads" => config.io_threads_do_reads = value == "yes",
                "rdbcompression" | "rdb-compression" => config.rdb_compression = value == "yes",
                "rdbchecksum" | "rdb-checksum" => config.rdb_checksum = value == "yes",
                "rdb-save-incremental-fsync" => config.rdb_save_incremental_fsync = value == "yes",
                "aof-use-rdb-preamble" => config.aof_use_rdb_preamble = value == "yes",
                "tracking-table-max-keys" => {
                    config.tracking_table_max_keys = value.parse().unwrap_or(0)
                }
                "jemalloc-bg-thread" => config.jemalloc_bg_thread = value == "yes",
                "repl-backlog-size" => config.repl_backlog_size = parse_memory_size(value),
                "repl-diskless-sync" => config.repl_diskless_sync = value == "yes",
                "repl-diskless-sync-delay" => {
                    config.repl_diskless_sync_delay = value.parse().unwrap_or(5)
                }
                "repl-timeout" => config.repl_timeout = value.parse().unwrap_or(60),
                "replica-lazy-flush" => config.replica_lazy_flush = value == "yes",
                "replica-serve-stale-data" | "slave-serve-stale-data" => {
                    config.replica_serve_stale_data = value == "yes";
                }
                "replica-read-only" | "slave-read-only" => {
                    config.replica_read_only = value == "yes"
                }
                "active-expire-enabled" => config.active_expire_enabled = value == "yes",
                "close-on-oom" => config.close_on_oom = value == "yes",
                "crash-log-enabled" => config.crash_log_enabled = value == "yes",
                _ => {}
            }
        }

        Ok(config)
    }
}

/// Parse Redis memory size strings like "512mb", "1gb", "100000"
pub fn parse_memory_size(s: &str) -> u64 {
    let s = s.to_lowercase();
    if s.ends_with("gb") {
        s[..s.len() - 2].parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else if s.ends_with("mb") {
        s[..s.len() - 2].parse::<u64>().unwrap_or(0) * 1024 * 1024
    } else if s.ends_with("kb") {
        s[..s.len() - 2].parse::<u64>().unwrap_or(0) * 1024
    } else if s.ends_with('b') {
        s[..s.len() - 1].parse::<u64>().unwrap_or(0)
    } else {
        s.parse::<u64>().unwrap_or(0)
    }
}
