use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::acl::AclManager;
use crate::config::ServerConfig;
use crate::database::RedisDatabase;
use crate::pubsub::PubSubHub;
use crate::resp::RespValue;

use super::{CommandHandler, CommandRegistry};

// ─── Server Info ──────────────────────────────────────────────────────────────

pub struct ServerInfo {
    pub num_dbs: usize,
    pub start_time: Instant,
    pub port: u16,
    pub total_connections: Arc<AtomicU64>,
    pub total_commands: Arc<AtomicU64>,
}

// ─── SLOWLOG ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SlowlogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub duration_us: u64,
    pub args: Vec<String>,
    pub client_addr: String,
    pub client_name: String,
}

pub struct SlowlogStore {
    entries: Mutex<VecDeque<SlowlogEntry>>,
    max_len: Mutex<usize>,
    next_id: AtomicU64,
    threshold_us: Mutex<i64>,
}

impl SlowlogStore {
    pub fn new(max_len: usize, threshold_us: i64) -> Self {
        SlowlogStore {
            entries: Mutex::new(VecDeque::new()),
            max_len: Mutex::new(max_len),
            next_id: AtomicU64::new(0),
            threshold_us: Mutex::new(threshold_us),
        }
    }

    pub fn maybe_add(&self, duration_us: u64, args: Vec<String>, addr: &str, name: &str) {
        let threshold = *self.threshold_us.lock();
        if threshold >= 0 && duration_us < threshold as u64 {
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = SlowlogEntry {
            id,
            timestamp: ts,
            duration_us,
            args,
            client_addr: addr.to_string(),
            client_name: name.to_string(),
        };
        let mut entries = self.entries.lock();
        let max = *self.max_len.lock();
        if entries.len() >= max {
            entries.pop_back();
        }
        entries.push_front(entry);
    }

    pub fn get(&self, count: usize) -> Vec<SlowlogEntry> {
        let entries = self.entries.lock();
        entries.iter().take(count).cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn reset(&self) {
        self.entries.lock().clear();
    }

    pub fn set_max_len(&self, n: usize) {
        *self.max_len.lock() = n;
    }

    pub fn set_threshold(&self, us: i64) {
        *self.threshold_us.lock() = us;
    }
}

// ─── LATENCY ──────────────────────────────────────────────────────────────────

pub struct LatencyStore {
    events: Mutex<HashMap<String, Vec<(u64, u64)>>>, // name -> [(timestamp, latency_ms)]
    max_samples: usize,
}

impl LatencyStore {
    pub fn new() -> Self {
        LatencyStore {
            events: Mutex::new(HashMap::new()),
            max_samples: 181,
        }
    }

    pub fn add(&self, event: &str, latency_ms: u64) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut events = self.events.lock();
        let history = events.entry(event.to_string()).or_insert_with(Vec::new);
        history.push((ts, latency_ms));
        if history.len() > self.max_samples {
            history.remove(0);
        }
    }

    pub fn history(&self, event: &str) -> Vec<(u64, u64)> {
        self.events.lock().get(event).cloned().unwrap_or_default()
    }

    pub fn latest(&self) -> Vec<(String, u64, u64)> {
        let events = self.events.lock();
        events
            .iter()
            .filter_map(|(name, history)| history.last().map(|(ts, lat)| (name.clone(), *ts, *lat)))
            .collect()
    }

    pub fn event_names(&self) -> Vec<String> {
        self.events.lock().keys().cloned().collect()
    }

    pub fn reset(&self, event: Option<&str>) {
        let mut events = self.events.lock();
        match event {
            Some(e) => {
                events.remove(e);
            }
            None => events.clear(),
        }
    }
}

// ─── PING ─────────────────────────────────────────────────────────────────────

pub struct PingCommand;

impl CommandHandler for PingCommand {
    fn name(&self) -> &str {
        "PING"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() > 1 {
            if let Some(msg) = args[1].as_bytes() {
                return RespValue::bulk_bytes(msg.to_vec());
            }
        }
        RespValue::pong()
    }
}

// ─── SELECT ───────────────────────────────────────────────────────────────────

pub struct SelectCommand {
    pub num_dbs: usize,
}

impl CommandHandler for SelectCommand {
    fn name(&self) -> &str {
        "SELECT"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'select' command");
        }
        let idx: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if idx >= self.num_dbs {
            return RespValue::error("ERR DB index is out of range");
        }
        *db_index = idx;
        RespValue::ok()
    }
}

// ─── FLUSHALL / FLUSHDB ───────────────────────────────────────────────────────

pub struct FlushAllCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for FlushAllCommand {
    fn name(&self) -> &str {
        "FLUSHALL"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        super::stream_cmds::flush_all();
        super::json_cmds::flush_all();
        super::bf_cmds::flush_all();
        super::cf_cmds::flush_all();
        super::probabilistic_cmds::flush_all();
        super::tdigest_cmds::flush_all();
        super::hash_cmds::hfield_ttl_flush_all();
        crate::ext_type_registry::flush_all();
        match self.db.flush_all() {
            Ok(()) => RespValue::ok(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

pub struct FlushDbCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for FlushDbCommand {
    fn name(&self) -> &str {
        "FLUSHDB"
    }
    fn execute(&self, db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        let db = *db_index;
        super::stream_cmds::flush_db(db);
        super::json_cmds::flush_db(db);
        super::bf_cmds::flush_db(db);
        super::cf_cmds::flush_db(db);
        super::probabilistic_cmds::flush_db(db);
        super::tdigest_cmds::flush_db(db);
        super::hash_cmds::hfield_ttl_flush_db(db);
        // ext_type_registry is flushed by the per-module flush_db calls above
        match self.db.flush_db(db) {
            Ok(()) => RespValue::ok(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

// ─── INFO ─────────────────────────────────────────────────────────────────────

pub struct InfoCommand {
    pub db: Arc<RedisDatabase>,
    pub info: Arc<ServerInfo>,
    pub config: Arc<ServerConfig>,
}

impl CommandHandler for InfoCommand {
    fn name(&self) -> &str {
        "INFO"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        let section = args
            .get(1)
            .and_then(|a| a.as_str())
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|| "default".to_string());

        let uptime_secs = self.info.start_time.elapsed().as_secs();
        let total_conns = self.info.total_connections.load(Ordering::Relaxed);
        let total_cmds = self.info.total_commands.load(Ordering::Relaxed);
        let start_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(uptime_secs);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let include_all = matches!(section.as_str(), "all" | "everything" | "default");

        let mut output = String::new();

        // Server section
        if include_all || section == "server" {
            output.push_str(&format!(
                "# Server\r\n\
                redis_version:7.2.0\r\n\
                redis_git_sha1:00000000\r\n\
                redis_git_dirty:0\r\n\
                redis_build_id:0\r\n\
                redis_mode:standalone\r\n\
                os:Linux 5.10.0 x86_64\r\n\
                arch_bits:64\r\n\
                monotonic_clock:POSIX clock_gettime\r\n\
                multiplexing_api:epoll\r\n\
                atomicvar_api:c11-builtin\r\n\
                gcc_version:0.0.0\r\n\
                process_id:{pid}\r\n\
                run_id:{run_id}\r\n\
                tcp_port:{port}\r\n\
                server_time_usec:{ts_us}\r\n\
                uptime_in_seconds:{uptime}\r\n\
                uptime_in_days:{uptime_days}\r\n\
                hz:{hz}\r\n\
                configured_hz:{hz}\r\n\
                aof_enabled:0\r\n\
                rdb_enabled:0\r\n\
                lsm_enabled:1\r\n\
                executable:forgekv\r\n\
                config_file:\r\n\
                io_threads_active:0\r\n",
                pid = std::process::id(),
                run_id = "0000000000000000000000000000000000000000",
                port = self.info.port,
                ts_us = now_ms * 1000,
                uptime = uptime_secs,
                uptime_days = uptime_secs / 86400,
                hz = self.config.hz,
            ));
        }

        // Clients section
        if include_all || section == "clients" {
            output.push_str(&format!(
                "# Clients\r\n\
                connected_clients:{conns}\r\n\
                cluster_connections:0\r\n\
                maxclients:10000\r\n\
                client_recent_max_input_buffer:0\r\n\
                client_recent_max_output_buffer:0\r\n\
                blocked_clients:0\r\n\
                tracking_clients:0\r\n\
                clients_in_timeout_table:0\r\n\
                total_blocking_keys:0\r\n\
                total_blocking_keys_on_nokey:0\r\n",
                conns = total_conns,
            ));
        }

        // Memory section
        if include_all || section == "memory" {
            output.push_str(&format!(
                "# Memory\r\n\
                used_memory:1000000\r\n\
                used_memory_human:976.56K\r\n\
                used_memory_rss:2000000\r\n\
                used_memory_rss_human:1.91M\r\n\
                used_memory_peak:1000000\r\n\
                used_memory_peak_human:976.56K\r\n\
                used_memory_peak_perc:100.00%\r\n\
                used_memory_overhead:500000\r\n\
                used_memory_startup:500000\r\n\
                used_memory_dataset:500000\r\n\
                used_memory_dataset_perc:50.00%\r\n\
                allocator_allocated:1000000\r\n\
                allocator_active:1200000\r\n\
                allocator_resident:2000000\r\n\
                total_system_memory:8000000000\r\n\
                total_system_memory_human:7.45G\r\n\
                used_memory_lua:37888\r\n\
                used_memory_vm_eval:37888\r\n\
                used_memory_lua_human:37.00K\r\n\
                used_memory_scripts_eval:0\r\n\
                number_of_cached_scripts:0\r\n\
                number_of_functions:0\r\n\
                number_of_libraries:0\r\n\
                used_memory_vm_functions:32768\r\n\
                used_memory_vm_total:70656\r\n\
                used_memory_vm_total_human:69.00K\r\n\
                used_memory_functions:216\r\n\
                used_memory_scripts:216\r\n\
                used_memory_scripts_human:216B\r\n\
                maxmemory:{maxmem}\r\n\
                maxmemory_human:{maxmem_h}\r\n\
                maxmemory_policy:{policy}\r\n\
                allocator_frag_ratio:1.00\r\n\
                allocator_frag_bytes:0\r\n\
                allocator_rss_ratio:1.00\r\n\
                allocator_rss_bytes:0\r\n\
                rss_overhead_ratio:1.00\r\n\
                rss_overhead_bytes:0\r\n\
                mem_fragmentation_ratio:1.00\r\n\
                mem_fragmentation_bytes:0\r\n\
                mem_not_counted_for_evict:0\r\n\
                mem_replication_backlog:0\r\n\
                mem_total_replication_buffers:0\r\n\
                mem_clients_slaves:0\r\n\
                mem_clients_normal:0\r\n\
                mem_cluster_links:0\r\n\
                mem_aof_buffer:0\r\n\
                active_defrag_running:0\r\n\
                lazyfree_pending_objects:0\r\n\
                lazyfreed_objects:0\r\n",
                maxmem = self.config.max_memory,
                maxmem_h = if self.config.max_memory == 0 {
                    "0B".to_string()
                } else {
                    format!("{}B", self.config.max_memory)
                },
                policy = self.config.max_memory_policy.as_str(),
            ));
        }

        // Persistence section
        if include_all || section == "persistence" {
            output.push_str(&format!(
                "# Persistence\r\n\
                loading:0\r\n\
                async_loading:0\r\n\
                current_cow_peak:0\r\n\
                current_cow_size:0\r\n\
                current_cow_size_age:0\r\n\
                current_fork_perc:0.00\r\n\
                current_save_keys_processed:0\r\n\
                current_save_keys_total:0\r\n\
                rdb_changes_since_last_save:0\r\n\
                rdb_bgsave_in_progress:0\r\n\
                rdb_last_save_time:{ts}\r\n\
                rdb_last_bgsave_status:ok\r\n\
                rdb_last_bgsave_time_sec:-1\r\n\
                rdb_current_bgsave_time_sec:-1\r\n\
                rdb_saves:0\r\n\
                rdb_last_cow_size:0\r\n\
                aof_enabled:0\r\n\
                aof_rewrite_in_progress:0\r\n\
                aof_rewrite_scheduled:0\r\n\
                aof_last_rewrite_time_sec:-1\r\n\
                aof_current_rewrite_time_sec:-1\r\n\
                aof_last_bgrewrite_status:ok\r\n\
                aof_last_write_status:ok\r\n\
                aof_last_cow_size:0\r\n\
                module_fork_in_progress:0\r\n\
                module_fork_last_cow_size:0\r\n\
                lsm_wal_sync_mode:{wal}\r\n",
                ts = start_ts,
                wal = match self.config.wal_sync_mode {
                    crate::config::WalSyncMode::Always => "always",
                    crate::config::WalSyncMode::Everysec => "everysec",
                    crate::config::WalSyncMode::No => "no",
                },
            ));
        }

        // Stats section
        if include_all || section == "stats" {
            output.push_str(&format!(
                "# Stats\r\n\
                total_connections_received:{conns}\r\n\
                total_commands_processed:{cmds}\r\n\
                instantaneous_ops_per_sec:0\r\n\
                total_net_input_bytes:0\r\n\
                total_net_output_bytes:0\r\n\
                total_net_repl_input_bytes:0\r\n\
                total_net_repl_output_bytes:0\r\n\
                instantaneous_input_kbps:0.00\r\n\
                instantaneous_output_kbps:0.00\r\n\
                rejected_connections:0\r\n\
                sync_full:0\r\n\
                sync_partial_ok:0\r\n\
                sync_partial_err:0\r\n\
                expired_keys:0\r\n\
                expired_stale_perc:0.00\r\n\
                expired_time_cap_reached_count:0\r\n\
                expire_cycle_cpu_milliseconds:0\r\n\
                evicted_keys:0\r\n\
                evicted_clients:0\r\n\
                total_eviction_exceeded_time:0\r\n\
                current_eviction_exceeded_time:0\r\n\
                keyspace_hits:0\r\n\
                keyspace_misses:0\r\n\
                pubsub_channels:0\r\n\
                pubsub_patterns:0\r\n\
                pubsub_shardchannels:0\r\n\
                latest_fork_usec:0\r\n\
                migrate_cached_sockets:0\r\n\
                slave_expires_tracked_keys:0\r\n\
                active_defrag_hits:0\r\n\
                active_defrag_misses:0\r\n\
                active_defrag_key_hits:0\r\n\
                active_defrag_key_misses:0\r\n\
                active_defrag_scanned:0\r\n\
                total_active_defrag_time:0\r\n\
                current_active_defrag_time:0\r\n\
                tracking_table_size:0\r\n\
                tracking_table_entries:0\r\n\
                unexpected_error_replies:0\r\n\
                total_reads_processed:0\r\n\
                total_writes_processed:0\r\n\
                io_threaded_reads_processed:0\r\n\
                io_threaded_writes_processed:0\r\n\
                reply_buffer_shrinks:0\r\n\
                reply_buffer_expands:0\r\n\
                eventloop_cycles:0\r\n\
                eventloop_duration_sum:0\r\n\
                eventloop_duration_cmd_sum:0\r\n\
                instantaneous_eventloop_cycles_per_sec:0\r\n\
                instantaneous_eventloop_duration_usec:0\r\n\
                acl_access_denied_auth:0\r\n\
                acl_access_denied_cmd:0\r\n\
                acl_access_denied_key:0\r\n\
                acl_access_denied_channel:0\r\n",
                conns = total_conns,
                cmds = total_cmds,
            ));
        }

        // Replication section
        if include_all || section == "replication" {
            output.push_str(
                "# Replication\r\n\
                role:master\r\n\
                connected_slaves:0\r\n\
                master_failover_state:no-failover\r\n\
                master_replid:0000000000000000000000000000000000000000\r\n\
                master_replid2:0000000000000000000000000000000000000000\r\n\
                master_repl_offset:0\r\n\
                second_repl_offset:-1\r\n\
                repl_backlog_active:0\r\n\
                repl_backlog_size:1048576\r\n\
                repl_backlog_first_byte_offset:0\r\n\
                repl_backlog_histlen:0\r\n",
            );
        }

        // CPU section
        if include_all || section == "cpu" {
            output.push_str(
                "# CPU\r\n\
                used_cpu_sys:0.000000\r\n\
                used_cpu_user:0.000000\r\n\
                used_cpu_sys_children:0.000000\r\n\
                used_cpu_user_children:0.000000\r\n\
                used_cpu_sys_main_thread:0.000000\r\n\
                used_cpu_user_main_thread:0.000000\r\n",
            );
        }

        // Modules section
        if include_all || section == "modules" {
            output.push_str(
                "# Modules\r\n\
                module:name=ReJSON,ver=20007,api=1,filters=0,usedby=[],using=[],options=[handle-io-errors]\r\n\
                module:name=bf,ver=20605,api=1,filters=0,usedby=[],using=[],options=[]\r\n\
                module:name=search,ver=20816,api=1,filters=0,usedby=[],using=[],options=[]\r\n"
            );
        }

        // Commandstats section
        if include_all || section == "commandstats" {
            output.push_str("# Commandstats\r\n");
        }

        // Errorstats section
        if include_all || section == "errorstats" {
            output.push_str("# Errorstats\r\n");
        }

        // Cluster section
        if include_all || section == "cluster" {
            output.push_str(&format!(
                "# Cluster\r\n\
                cluster_enabled:{}\r\n",
                if self.config.cluster_enabled { 1 } else { 0 },
            ));
        }

        // Keyspace section
        if include_all || section == "keyspace" {
            let mut db_section = String::new();
            for i in 0..self.info.num_dbs {
                if let Ok((keys, expires)) = self.db.db_info(i) {
                    if keys > 0 {
                        db_section.push_str(&format!(
                            "db{}:keys={},expires={},avg_ttl=0\r\n",
                            i, keys, expires
                        ));
                    }
                }
            }
            output.push_str(&format!("# Keyspace\r\n{}", db_section));
        }

        let _ = db_index;
        RespValue::bulk_str(&output)
    }
}

// ─── DBSIZE ───────────────────────────────────────────────────────────────────

pub struct DbSizeCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for DbSizeCommand {
    fn name(&self) -> &str {
        "DBSIZE"
    }
    fn execute(&self, db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        match self.db.db_info(*db_index) {
            Ok((keys, _)) => RespValue::integer(keys),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

// ─── COMMAND ──────────────────────────────────────────────────────────────────

pub struct CommandCommand;

impl CommandHandler for CommandCommand {
    fn name(&self) -> &str {
        "COMMAND"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // Full list of supported commands
        const CMD_NAMES: &[&str] = &[
            "ACL",
            "AUTH",
            "APPEND",
            "BGREWRITEAOF",
            "BGSAVE",
            "BF.ADD",
            "BF.CARD",
            "BF.EXISTS",
            "BF.INFO",
            "BF.INSERT",
            "BF.LOADCHUNK",
            "BF.MADD",
            "BF.MEXISTS",
            "BF.RESERVE",
            "BF.SCANDUMP",
            "BITCOUNT",
            "BITFIELD",
            "BITFIELD_RO",
            "BITOP",
            "BITPOS",
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "CF.ADD",
            "CF.ADDNX",
            "CF.COMPACT",
            "CF.COUNT",
            "CF.DEL",
            "CF.EXISTS",
            "CF.INFO",
            "CF.INSERT",
            "CF.INSERTNX",
            "CF.LOADCHUNK",
            "CF.MEXISTS",
            "CF.RESERVE",
            "CF.SCANDUMP",
            "CL.THROTTLE",
            "CLIENT",
            "CLUSTER",
            "CMS.INCRBY",
            "CMS.INFO",
            "CMS.INITBYDIM",
            "CMS.INITBYPROB",
            "CMS.MERGE",
            "CMS.QUERY",
            "COMMAND",
            "CONFIG",
            "COPY",
            "DBSIZE",
            "DEBUG",
            "DECR",
            "DECRBY",
            "DEL",
            "DISCARD",
            "DFLY",
            "DUMP",
            "ECHO",
            "EVAL",
            "EVALSHA",
            "EVALSHA_RO",
            "EVAL_RO",
            "EXEC",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "EXPIRETIME",
            "FAILOVER",
            "FCALL",
            "FCALL_RO",
            "FLUSHALL",
            "FLUSHDB",
            "FUNCTION",
            "GEOADD",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS",
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER",
            "GEORADIUSBYMEMBER_RO",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
            "GET",
            "GETBIT",
            "GETDEL",
            "GETEX",
            "GETRANGE",
            "GETSET",
            "HDEL",
            "HEXISTS",
            "HGET",
            "HGETALL",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HKEYS",
            "HLEN",
            "HMGET",
            "HMSET",
            "HRANDFIELD",
            "HSCAN",
            "HSET",
            "HSETNX",
            "HSTRLEN",
            "HVALS",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "INFO",
            "JSON.ARRAPPEND",
            "JSON.ARRINDEX",
            "JSON.ARRINSERT",
            "JSON.ARRLEN",
            "JSON.ARRPOP",
            "JSON.ARRTRIM",
            "JSON.CLEAR",
            "JSON.DEBUG",
            "JSON.DEL",
            "JSON.DUMP",
            "JSON.FORGET",
            "JSON.GET",
            "JSON.MERGE",
            "JSON.MGET",
            "JSON.MSET",
            "JSON.NUMINCRBY",
            "JSON.NUMMULTBY",
            "JSON.OBJKEYS",
            "JSON.OBJLEN",
            "JSON.RESP",
            "JSON.SET",
            "JSON.STRAPPEND",
            "JSON.STRLEN",
            "JSON.TOGGLE",
            "JSON.TYPE",
            "KEYS",
            "LASTSAVE",
            "LATENCY",
            "LCS",
            "LINDEX",
            "LINSERT",
            "LLEN",
            "LMOVE",
            "LMPOP",
            "LOLWUT",
            "LPOP",
            "LPOS",
            "LPUSH",
            "LPUSHX",
            "LRANGE",
            "LREM",
            "LSET",
            "LTRIM",
            "MEMORY",
            "MGET",
            "MOVE",
            "MSET",
            "MSETNX",
            "MULTI",
            "OBJECT",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PEXPIRETIME",
            "PFADD",
            "PFCOUNT",
            "PFMERGE",
            "PING",
            "PSETEX",
            "PSYNC",
            "PTTL",
            "PUBLISH",
            "PUBSUB",
            "QUIT",
            "RANDOMKEY",
            "REPLCONF",
            "REPLICAOF",
            "RESET",
            "RESTORE",
            "RPOP",
            "RPOPLPUSH",
            "RPUSH",
            "RPUSHX",
            "SADD",
            "SAVE",
            "SCAN",
            "SCARD",
            "SCRIPT",
            "SELECT",
            "SET",
            "SETBIT",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "SINTERCARD",
            "SINTER",
            "SINTERSTORE",
            "SISMEMBER",
            "SLAVEOF",
            "SLOWLOG",
            "SMEMBERS",
            "SMISMEMBER",
            "SMOVE",
            "SORT",
            "SORT_RO",
            "SPOP",
            "SPUBLISH",
            "SRANDMEMBER",
            "SREM",
            "SSCAN",
            "SSUBSCRIBE",
            "STRLEN",
            "SUBSTR",
            "SUNION",
            "SUNIONSTORE",
            "SUNSUBSCRIBE",
            "SWAPDB",
            "TDIGEST.ADD",
            "TDIGEST.BYRANK",
            "TDIGEST.BYREVRANK",
            "TDIGEST.CDF",
            "TDIGEST.CREATE",
            "TDIGEST.INFO",
            "TDIGEST.MAX",
            "TDIGEST.MERGE",
            "TDIGEST.MIN",
            "TDIGEST.QUANTILE",
            "TDIGEST.RANK",
            "TDIGEST.RESET",
            "TDIGEST.REVRANK",
            "TDIGEST.TRIMMED_MEAN",
            "TIME",
            "TOPK.ADD",
            "TOPK.COUNT",
            "TOPK.INCRBY",
            "TOPK.INFO",
            "TOPK.LIST",
            "TOPK.QUERY",
            "TOPK.RESERVE",
            "TOUCH",
            "TTL",
            "TYPE",
            "UNLINK",
            "UNWATCH",
            "WAIT",
            "WAITAOF",
            "WATCH",
            "XACK",
            "XADD",
            "XAUTOCLAIM",
            "XCLAIM",
            "XDEL",
            "XGROUP",
            "XINFO",
            "XLEN",
            "XPENDING",
            "XRANGE",
            "XREAD",
            "XREADGROUP",
            "XREVRANGE",
            "XTRIM",
            "ZADD",
            "ZCARD",
            "ZCOUNT",
            "ZDIFF",
            "ZDIFFSTORE",
            "ZINCRBY",
            "ZINTER",
            "ZINTERCARD",
            "ZINTERSTORE",
            "ZLEXCOUNT",
            "ZMPOP",
            "ZMSCORE",
            "ZPOPMAX",
            "ZPOPMIN",
            "ZRANDMEMBER",
            "ZRANGE",
            "ZRANGEBYLEX",
            "ZRANGEBYSCORE",
            "ZRANGESTORE",
            "ZRANK",
            "ZREM",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
            "ZREVRANGE",
            "ZREVRANGEBYLEX",
            "ZREVRANGEBYSCORE",
            "ZREVRANK",
            "ZSCAN",
            "ZSCORE",
            "ZUNION",
            "ZUNIONSTORE",
        ];

        if args.len() >= 2 {
            match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
                Some("COUNT") => return RespValue::integer(CMD_NAMES.len() as i64),
                Some("DOCS") => return RespValue::Array(Some(vec![])),
                Some("GETKEYS") => {
                    // Basic key extraction: return args[2..] minus option args
                    if args.len() >= 3 {
                        let keys: Vec<RespValue> = args[3..]
                            .iter()
                            .take(1)
                            .filter_map(|a| a.as_bytes().map(|b| RespValue::bulk_bytes(b.to_vec())))
                            .collect();
                        return RespValue::Array(Some(keys));
                    }
                    return RespValue::Array(Some(vec![]));
                }
                Some("INFO") => return RespValue::Array(Some(vec![])),
                Some("LIST") => {
                    // Support FILTERBY PATTERN <glob>
                    let pattern = if args.len() >= 5
                        && args[2].as_str().map(|s| s.to_uppercase())
                            == Some("FILTERBY".to_string())
                        && args[3].as_str().map(|s| s.to_uppercase()) == Some("PATTERN".to_string())
                    {
                        args[4].as_str().map(|s| s.to_uppercase())
                    } else {
                        None
                    };
                    let names: Vec<RespValue> = CMD_NAMES
                        .iter()
                        .filter(|&&name| {
                            if let Some(ref pat) = pattern {
                                crate::pubsub::pattern_matches(pat, name)
                            } else {
                                true
                            }
                        })
                        .map(|&n| RespValue::bulk_str(n))
                        .collect();
                    return RespValue::Array(Some(names));
                }
                Some("HELP") => {
                    return RespValue::Array(Some(vec![
                        RespValue::bulk_str("COMMAND <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                        RespValue::bulk_str("COUNT -- Return number of commands in server."),
                        RespValue::bulk_str("DOCS [<command-name> ...] -- Return details about commands."),
                        RespValue::bulk_str("GETKEYS <full-command> -- Return keys from a full Redis command."),
                        RespValue::bulk_str("INFO [<command-name> ...] -- Return details about commands."),
                        RespValue::bulk_str("LIST [FILTERBY MODULE <module-name>|ACLCAT <category>|PATTERN <pattern>] -- List all commands."),
                    ]));
                }
                _ => {}
            }
        }
        // COMMAND with no args: return count (avoid dumping all command info which is huge)
        RespValue::integer(CMD_NAMES.len() as i64)
    }
}

// ─── ECHO ─────────────────────────────────────────────────────────────────────

pub struct EchoCommand;

impl CommandHandler for EchoCommand {
    fn name(&self) -> &str {
        "ECHO"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 2 {
            return RespValue::error("ERR wrong number of arguments for 'echo' command");
        }
        match args[1].as_bytes() {
            Some(b) => RespValue::bulk_bytes(b.to_vec()),
            None => RespValue::null_bulk(),
        }
    }
}

// ─── CONFIG ───────────────────────────────────────────────────────────────────

pub struct ConfigCommand {
    pub config: Arc<parking_lot::RwLock<ServerConfig>>,
}

impl CommandHandler for ConfigCommand {
    fn name(&self) -> &str {
        "CONFIG"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'config' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("RESETSTAT") => RespValue::ok(),
            Some("REWRITE") => RespValue::ok(),
            Some("SET") => {
                if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                    return RespValue::error("ERR wrong number of arguments for 'config|set' command");
                }
                let mut cfg = self.config.write();
                let mut i = 2;
                while i + 1 < args.len() {
                    let key = args[i].as_str().unwrap_or("").to_lowercase();
                    let val = args[i + 1].as_str().unwrap_or("");
                    apply_config_set(&mut cfg, &key, val);
                    i += 2;
                }
                RespValue::ok()
            }
            Some("GET") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'config|get' command");
                }
                let cfg = self.config.read();
                let mut result = Vec::new();
                // Support glob patterns and multiple params
                for i in 2..args.len() {
                    let pattern = args[i].as_str().unwrap_or("*");
                    let pairs = get_config_pairs(&cfg, pattern);
                    for (k, v) in pairs {
                        result.push(RespValue::bulk_str(&k));
                        result.push(RespValue::bulk_str(&v));
                    }
                }
                RespValue::Array(Some(result))
            }
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("CONFIG <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("GET <pattern> -- Return parameters matching the glob-like <pattern> and their values."),
                    RespValue::bulk_str("SET <directive> <value> -- Set the configuration directives."),
                    RespValue::bulk_str("RESETSTAT -- Reset the stats counters."),
                    RespValue::bulk_str("REWRITE -- Rewrite the configuration file."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand. Try CONFIG HELP."),
        }
    }
}

fn apply_config_set(cfg: &mut ServerConfig, key: &str, val: &str) {
    use crate::config::{parse_memory_size, EvictionPolicy, WalSyncMode};
    match key {
        "maxmemory" => cfg.max_memory = parse_memory_size(val),
        "maxmemory-policy" => cfg.max_memory_policy = EvictionPolicy::from_str(val),
        "maxmemory-samples" => cfg.maxmemory_samples = val.parse().unwrap_or(5),
        "hz" => cfg.hz = val.parse().unwrap_or(10),
        "loglevel" => {}
        "requirepass" => cfg.requirepass = val.to_string(),
        "slowlog-log-slower-than" => cfg.slowlog_log_slower_than = val.parse().unwrap_or(10000),
        "slowlog-max-len" => cfg.slowlog_max_len = val.parse().unwrap_or(128),
        "hash-max-listpack-entries" | "hash-max-ziplist-entries" => {
            cfg.hash_max_listpack_entries = val.parse().unwrap_or(128);
        }
        "hash-max-listpack-value" | "hash-max-ziplist-value" => {
            cfg.hash_max_listpack_value = val.parse().unwrap_or(64);
        }
        "set-max-intset-entries" => cfg.set_max_intset_entries = val.parse().unwrap_or(512),
        "set-max-listpack-entries" => cfg.set_max_listpack_entries = val.parse().unwrap_or(128),
        "set-max-listpack-value" => cfg.set_max_listpack_value = val.parse().unwrap_or(64),
        "zset-max-listpack-entries" | "zset-max-ziplist-entries" => {
            cfg.zset_max_listpack_entries = val.parse().unwrap_or(128);
        }
        "zset-max-listpack-value" | "zset-max-ziplist-value" => {
            cfg.zset_max_listpack_value = val.parse().unwrap_or(64);
        }
        "list-max-listpack-size" | "list-max-ziplist-size" => {
            cfg.list_max_listpack_size = val.parse().unwrap_or(-2);
        }
        "activerehashing" => cfg.activerehashing = val == "yes",
        "lazyfree-lazy-eviction" => cfg.lazyfree_lazy_eviction = val == "yes",
        "lazyfree-lazy-expire" => cfg.lazyfree_lazy_expire = val == "yes",
        "notify-keyspace-events" => {
            cfg.notify_keyspace_events = val.to_string();
            crate::keyspace::KS_NOTIFIER.configure(val);
        }
        "lua-time-limit" => cfg.lua_time_limit = val.parse().unwrap_or(5000),
        "lfu-log-factor" => cfg.lfu_log_factor = val.parse().unwrap_or(10),
        "lfu-decay-time" => cfg.lfu_decay_time = val.parse().unwrap_or(1),
        "proto-max-bulk-len" => cfg.proto_max_bulk_len = parse_memory_size(val),
        "client-query-buffer-limit" => cfg.client_query_buffer_limit = parse_memory_size(val),
        "appendfsync" => {
            cfg.wal_sync_mode = match val {
                "always" => WalSyncMode::Always,
                "no" => WalSyncMode::No,
                _ => WalSyncMode::Everysec,
            };
        }
        "dynamic-hz" => cfg.dynamic_hz = val == "yes",
        "latency-tracking" => cfg.latency_tracking = val == "yes",
        "latency-monitor-threshold" => cfg.latency_monitor_threshold = val.parse().unwrap_or(0),
        "tracking-table-max-keys" => cfg.tracking_table_max_keys = val.parse().unwrap_or(0),
        "stream-node-max-bytes" => cfg.stream_node_max_bytes = val.parse().unwrap_or(4096),
        "stream-node-max-entries" => cfg.stream_node_max_entries = val.parse().unwrap_or(100),
        "tcp-keepalive" => cfg.tcp_keepalive = val.parse().unwrap_or(300),
        "timeout" => cfg.timeout = val.parse().unwrap_or(0),
        "active-expire-enabled" => cfg.active_expire_enabled = val == "yes",
        _ => {}
    }
}

fn get_config_pairs(cfg: &ServerConfig, pattern: &str) -> Vec<(String, String)> {
    use crate::config::WalSyncMode;

    let all: Vec<(String, String)> = vec![
        ("bind".to_string(), cfg.bind.clone()),
        ("port".to_string(), cfg.port.to_string()),
        ("tcp-backlog".to_string(), cfg.tcp_backlog.to_string()),
        ("tcp-keepalive".to_string(), cfg.tcp_keepalive.to_string()),
        ("timeout".to_string(), cfg.timeout.to_string()),
        (
            "protected-mode".to_string(),
            if cfg.protected_mode {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("databases".to_string(), cfg.databases.to_string()),
        ("hz".to_string(), cfg.hz.to_string()),
        (
            "dynamic-hz".to_string(),
            if cfg.dynamic_hz {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("maxmemory".to_string(), cfg.max_memory.to_string()),
        (
            "maxmemory-policy".to_string(),
            cfg.max_memory_policy.as_str().to_string(),
        ),
        (
            "maxmemory-samples".to_string(),
            cfg.maxmemory_samples.to_string(),
        ),
        ("lfu-log-factor".to_string(), cfg.lfu_log_factor.to_string()),
        ("lfu-decay-time".to_string(), cfg.lfu_decay_time.to_string()),
        (
            "activerehashing".to_string(),
            if cfg.activerehashing {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "lazyfree-lazy-eviction".to_string(),
            if cfg.lazyfree_lazy_eviction {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "lazyfree-lazy-expire".to_string(),
            if cfg.lazyfree_lazy_expire {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "lazyfree-lazy-server-del".to_string(),
            if cfg.lazyfree_lazy_server_del {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "lazyfree-lazy-user-del".to_string(),
            if cfg.lazyfree_lazy_user_del {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "lazyfree-lazy-user-flush".to_string(),
            if cfg.lazyfree_lazy_user_flush {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "activedefrag".to_string(),
            if cfg.activedefrag {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "proto-max-bulk-len".to_string(),
            cfg.proto_max_bulk_len.to_string(),
        ),
        (
            "client-query-buffer-limit".to_string(),
            cfg.client_query_buffer_limit.to_string(),
        ),
        (
            "hash-max-listpack-entries".to_string(),
            cfg.hash_max_listpack_entries.to_string(),
        ),
        (
            "hash-max-listpack-value".to_string(),
            cfg.hash_max_listpack_value.to_string(),
        ),
        (
            "hash-max-ziplist-entries".to_string(),
            cfg.hash_max_listpack_entries.to_string(),
        ),
        (
            "hash-max-ziplist-value".to_string(),
            cfg.hash_max_listpack_value.to_string(),
        ),
        (
            "set-max-intset-entries".to_string(),
            cfg.set_max_intset_entries.to_string(),
        ),
        (
            "set-max-listpack-entries".to_string(),
            cfg.set_max_listpack_entries.to_string(),
        ),
        (
            "set-max-listpack-value".to_string(),
            cfg.set_max_listpack_value.to_string(),
        ),
        (
            "zset-max-listpack-entries".to_string(),
            cfg.zset_max_listpack_entries.to_string(),
        ),
        (
            "zset-max-listpack-value".to_string(),
            cfg.zset_max_listpack_value.to_string(),
        ),
        (
            "zset-max-ziplist-entries".to_string(),
            cfg.zset_max_listpack_entries.to_string(),
        ),
        (
            "zset-max-ziplist-value".to_string(),
            cfg.zset_max_listpack_value.to_string(),
        ),
        (
            "list-max-listpack-size".to_string(),
            cfg.list_max_listpack_size.to_string(),
        ),
        (
            "list-max-ziplist-size".to_string(),
            cfg.list_max_listpack_size.to_string(),
        ),
        (
            "list-compress-depth".to_string(),
            cfg.list_compress_depth.to_string(),
        ),
        (
            "stream-node-max-bytes".to_string(),
            cfg.stream_node_max_bytes.to_string(),
        ),
        (
            "stream-node-max-entries".to_string(),
            cfg.stream_node_max_entries.to_string(),
        ),
        ("lua-time-limit".to_string(), cfg.lua_time_limit.to_string()),
        (
            "busy-reply-threshold".to_string(),
            cfg.busy_reply_threshold.to_string(),
        ),
        (
            "slowlog-log-slower-than".to_string(),
            cfg.slowlog_log_slower_than.to_string(),
        ),
        (
            "slowlog-max-len".to_string(),
            cfg.slowlog_max_len.to_string(),
        ),
        (
            "latency-tracking".to_string(),
            if cfg.latency_tracking {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "latency-monitor-threshold".to_string(),
            cfg.latency_monitor_threshold.to_string(),
        ),
        (
            "notify-keyspace-events".to_string(),
            cfg.notify_keyspace_events.clone(),
        ),
        (
            "appendfsync".to_string(),
            match cfg.wal_sync_mode {
                WalSyncMode::Always => "always".to_string(),
                WalSyncMode::Everysec => "everysec".to_string(),
                WalSyncMode::No => "no".to_string(),
            },
        ),
        ("requirepass".to_string(), cfg.requirepass.clone()),
        ("aclfile".to_string(), cfg.aclfile.clone()),
        ("acllog-max-len".to_string(), cfg.acllog_max_len.to_string()),
        (
            "cluster-enabled".to_string(),
            if cfg.cluster_enabled {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "cluster-config-file".to_string(),
            cfg.cluster_config_file.clone(),
        ),
        (
            "cluster-node-timeout".to_string(),
            cfg.cluster_node_timeout.to_string(),
        ),
        (
            "repl-backlog-size".to_string(),
            cfg.repl_backlog_size.to_string(),
        ),
        (
            "repl-diskless-sync".to_string(),
            if cfg.repl_diskless_sync {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "repl-diskless-sync-delay".to_string(),
            cfg.repl_diskless_sync_delay.to_string(),
        ),
        ("repl-timeout".to_string(), cfg.repl_timeout.to_string()),
        ("io-threads".to_string(), cfg.io_threads.to_string()),
        (
            "io-threads-do-reads".to_string(),
            if cfg.io_threads_do_reads {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "rdbcompression".to_string(),
            if cfg.rdb_compression {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "rdbchecksum".to_string(),
            if cfg.rdb_checksum {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("dbfilename".to_string(), cfg.dbfilename.clone()),
        ("dir".to_string(), cfg.dir.clone()),
        (
            "tracking-table-max-keys".to_string(),
            cfg.tracking_table_max_keys.to_string(),
        ),
        (
            "jemalloc-bg-thread".to_string(),
            if cfg.jemalloc_bg_thread {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "active-expire-enabled".to_string(),
            if cfg.active_expire_enabled {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("loglevel".to_string(), "notice".to_string()),
        ("logfile".to_string(), cfg.log_file.clone()),
        ("save".to_string(), "3600 1 300 100 60 10000".to_string()),
        (
            "aof-use-rdb-preamble".to_string(),
            if cfg.aof_use_rdb_preamble {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "replica-lazy-flush".to_string(),
            if cfg.replica_lazy_flush {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "replica-serve-stale-data".to_string(),
            if cfg.replica_serve_stale_data {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "replica-read-only".to_string(),
            if cfg.replica_read_only {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "memtable-size-mb".to_string(),
            cfg.memtable_size_mb.to_string(),
        ),
        (
            "enable-debug-command".to_string(),
            if cfg.enable_debug_command {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "close-on-oom".to_string(),
            if cfg.close_on_oom {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "crash-log-enabled".to_string(),
            if cfg.crash_log_enabled {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "syslog-enabled".to_string(),
            if cfg.syslog_enabled {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("syslog-ident".to_string(), cfg.syslog_ident.clone()),
        ("tcp-backlog".to_string(), cfg.tcp_backlog.to_string()),
        (
            "enable-protected-configs".to_string(),
            if cfg.enable_protected_configs {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("bind-source-addr".to_string(), "".to_string()),
        ("oom-score-adj".to_string(), "no".to_string()),
        ("oom-score-adj-values".to_string(), "0 200 800".to_string()),
        (
            "acl-pubsub-default".to_string(),
            "resetchannels".to_string(),
        ),
        ("socket-mark-id".to_string(), "0".to_string()),
        ("tls-port".to_string(), "0".to_string()),
        ("tls-replication".to_string(), "no".to_string()),
        ("tls-cluster".to_string(), "no".to_string()),
        ("repl-min-slaves-to-write".to_string(), "0".to_string()),
        ("repl-min-replicas-to-write".to_string(), "0".to_string()),
        ("repl-min-slaves-max-lag".to_string(), "10".to_string()),
        ("repl-min-replicas-max-lag".to_string(), "10".to_string()),
        (
            "repl-diskless-sync-max-replicas".to_string(),
            "0".to_string(),
        ),
        ("no-appendfsync-on-rewrite".to_string(), "no".to_string()),
        ("auto-aof-rewrite-percentage".to_string(), "100".to_string()),
        (
            "auto-aof-rewrite-min-size".to_string(),
            "67108864".to_string(),
        ),
        (
            "aof-rewrite-incremental-fsync".to_string(),
            "yes".to_string(),
        ),
        (
            "rdb-save-incremental-fsync".to_string(),
            if cfg.rdb_save_incremental_fsync {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("crash-memlog-enabled".to_string(), "yes".to_string()),
        ("use-exit-on-panic".to_string(), "no".to_string()),
        ("disable-thp".to_string(), "no".to_string()),
        (
            "cluster-require-full-coverage".to_string(),
            if cfg.cluster_require_full_coverage {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("cluster-slave-no-failover".to_string(), "no".to_string()),
        (
            "cluster-allow-reads-when-down".to_string(),
            "no".to_string(),
        ),
        (
            "cluster-allow-pubsubshard-when-down".to_string(),
            "yes".to_string(),
        ),
        ("cluster-migration-barrier".to_string(), "1".to_string()),
        ("cluster-announce-ip".to_string(), "".to_string()),
        ("cluster-announce-port".to_string(), "0".to_string()),
        ("cluster-announce-bus-port".to_string(), "0".to_string()),
        ("cluster-link-sendbuf-limit".to_string(), "0".to_string()),
        ("cluster-announce-hostname".to_string(), "".to_string()),
        (
            "cluster-announce-human-nodename".to_string(),
            "".to_string(),
        ),
        (
            "cluster-slave-validity-factor".to_string(),
            "10".to_string(),
        ),
        ("slave-lazy-flush".to_string(), "no".to_string()),
        ("slave-serve-stale-data".to_string(), "yes".to_string()),
        ("slave-read-only".to_string(), "yes".to_string()),
        (
            "active-defrag-ignore-bytes".to_string(),
            cfg.active_defrag_ignore_bytes.to_string(),
        ),
        (
            "active-defrag-enabled".to_string(),
            if cfg.activedefrag {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("active-defrag-cycle-min".to_string(), "1".to_string()),
        ("active-defrag-cycle-max".to_string(), "25".to_string()),
        (
            "active-defrag-max-scan-fields".to_string(),
            "1000".to_string(),
        ),
    ];

    // Filter by pattern (glob match)
    all.into_iter()
        .filter(|(k, _)| glob_match(pattern, k))
        .collect()
}

fn glob_match(pattern: &str, s: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let pat: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = s.chars().collect();
    glob_match_chars(&pat, &s)
}

fn glob_match_chars(pat: &[char], s: &[char]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (Some(&'*'), _) => {
            // Try matching zero chars, then one char, etc.
            glob_match_chars(&pat[1..], s) || (!s.is_empty() && glob_match_chars(pat, &s[1..]))
        }
        (Some(&'?'), Some(_)) => glob_match_chars(&pat[1..], &s[1..]),
        (Some(p), Some(c)) if p == c => glob_match_chars(&pat[1..], &s[1..]),
        _ => false,
    }
}

// ─── AUTH (command, for registry fallback) ───────────────────────────────────

pub struct AuthCommand {
    pub acl: Arc<AclManager>,
    pub requirepass: String,
}

impl CommandHandler for AuthCommand {
    fn name(&self) -> &str {
        "AUTH"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        // Real AUTH handling is done in server.rs; this is the fallback.
        RespValue::ok()
    }
}

// ─── CLIENT (fallback) ───────────────────────────────────────────────────────

pub struct ClientCommand;

impl CommandHandler for ClientCommand {
    fn name(&self) -> &str {
        "CLIENT"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        // Real CLIENT handling is done in server.rs; this is the fallback.
        RespValue::ok()
    }
}

// ─── QUIT ─────────────────────────────────────────────────────────────────────

pub struct QuitCommand;

impl CommandHandler for QuitCommand {
    fn name(&self) -> &str {
        "QUIT"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

// ─── DEBUG ────────────────────────────────────────────────────────────────────

pub struct DebugCommand;

impl CommandHandler for DebugCommand {
    fn name(&self) -> &str {
        "DEBUG"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::ok();
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("SLEEP") => {
                if let Some(secs) = args
                    .get(2)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    let ms = (secs * 1000.0) as u64;
                    std::thread::sleep(std::time::Duration::from_millis(ms.min(5000)));
                }
                RespValue::ok()
            }
            Some("RELOAD") | Some("LOADAOF") => RespValue::ok(),
            Some("FLUSHALL") => RespValue::ok(),
            Some("OBJECT") => {
                // DEBUG OBJECT key
                RespValue::bulk_str("Value at: 0x0 refcount:1 encoding:embstr serializedlength:0 lru:0 lru_seconds_idle:0")
            }
            Some("QUICKLIST-PACKED-THRESHOLD") => RespValue::ok(),
            Some("JMAP") => RespValue::ok(),
            Some("CHANGE-REPL-ID") => RespValue::ok(),
            Some("AOFSTATS") => RespValue::ok(),
            Some("DICTRESIZE") => RespValue::ok(),
            Some("SFLAGS") => RespValue::bulk_str("0"),
            Some("GETANDPROPAGATEEXPIRE") => RespValue::integer(0),
            Some("STRINGMATCH-LEN") => RespValue::integer(0),
            Some("ERROR") => {
                let msg = args.get(2).and_then(|a| a.as_str()).unwrap_or("ERR");
                RespValue::error(msg)
            }
            Some("OOM") => RespValue::ok(),
            Some("PANIC") => RespValue::ok(),
            Some("SEGFAULT") => RespValue::ok(),
            Some("DISABLE-NEXT-AOF-FSYNC") => RespValue::ok(),
            Some("CLOSE-LISTENERS-ASA") => RespValue::ok(),
            Some("CRASH-AND-RECOVER") => RespValue::ok(),
            Some("LISTPACK-ENTRIES") => RespValue::integer(0),
            Some("DISABLE-REPLICATION-CACHING") => RespValue::ok(),
            Some("LEAK") => RespValue::ok(),
            _ => RespValue::ok(),
        }
    }
}

// ─── SLOWLOG ──────────────────────────────────────────────────────────────────

pub struct SlowlogCommand {
    pub store: Arc<SlowlogStore>,
}

impl CommandHandler for SlowlogCommand {
    fn name(&self) -> &str {
        "SLOWLOG"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'slowlog' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("GET") => {
                let count = args.get(2)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(128usize);
                let entries = self.store.get(count);
                RespValue::Array(Some(entries.into_iter().map(|e| {
                    RespValue::Array(Some(vec![
                        RespValue::integer(e.id as i64),
                        RespValue::integer(e.timestamp as i64),
                        RespValue::integer(e.duration_us as i64),
                        RespValue::Array(Some(e.args.iter().map(|a| RespValue::bulk_str(a)).collect())),
                        RespValue::bulk_str(&e.client_addr),
                        RespValue::bulk_str(&e.client_name),
                    ]))
                }).collect()))
            }
            Some("LEN") => RespValue::integer(self.store.len() as i64),
            Some("RESET") => {
                self.store.reset();
                RespValue::ok()
            }
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("SLOWLOG <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("GET [<count>] -- Return top <count> entries from the slowlog (default: 128). Entries are made of:"),
                    RespValue::bulk_str("    id, timestamp, time in microseconds, arguments array, client IP and port,"),
                    RespValue::bulk_str("    client name"),
                    RespValue::bulk_str("LEN -- Return the number of entries in the slowlog."),
                    RespValue::bulk_str("RESET -- Reset the slowlog."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand. Try SLOWLOG HELP."),
        }
    }
}

// ─── LATENCY ──────────────────────────────────────────────────────────────────

pub struct LatencyCommand {
    pub store: Arc<LatencyStore>,
}

impl CommandHandler for LatencyCommand {
    fn name(&self) -> &str {
        "LATENCY"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'latency' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("HISTORY") => {
                let event = args.get(2).and_then(|a| a.as_str()).unwrap_or("");
                let history = self.store.history(event);
                RespValue::Array(Some(
                    history
                        .into_iter()
                        .map(|(ts, lat)| {
                            RespValue::Array(Some(vec![
                                RespValue::integer(ts as i64),
                                RespValue::integer(lat as i64),
                            ]))
                        })
                        .collect(),
                ))
            }
            Some("LATEST") => {
                let latest = self.store.latest();
                RespValue::Array(Some(
                    latest
                        .into_iter()
                        .map(|(name, ts, lat)| {
                            RespValue::Array(Some(vec![
                                RespValue::bulk_str(&name),
                                RespValue::integer(ts as i64),
                                RespValue::integer(lat as i64),
                            ]))
                        })
                        .collect(),
                ))
            }
            Some("RESET") => {
                let event = args.get(2).and_then(|a| a.as_str());
                self.store.reset(event);
                RespValue::integer(1)
            }
            Some("GRAPH") => RespValue::bulk_str("No data."),
            Some("HELP") => RespValue::Array(Some(vec![
                RespValue::bulk_str(
                    "LATENCY <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
                ),
                RespValue::bulk_str("HISTORY <event> -- Return latency history of <event>."),
                RespValue::bulk_str("LATEST -- Return the latest latency samples for all events."),
                RespValue::bulk_str(
                    "RESET [<event> ...] -- Reset latency data of one or more events.",
                ),
            ])),
            _ => RespValue::empty_array(),
        }
    }
}

// ─── MEMORY ───────────────────────────────────────────────────────────────────

pub struct MemoryCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for MemoryCommand {
    fn name(&self) -> &str {
        "MEMORY"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'memory' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("USAGE") => {
                // MEMORY USAGE key [SAMPLES count]
                // Return estimated memory usage in bytes
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'memory|usage' command");
                }
                let key = args[2].as_str().unwrap_or("");
                match self.db.key_type(*db_index, key.as_bytes()) {
                    Ok(t) if t != "none" => RespValue::integer(100),
                    _ => RespValue::null_bulk(),
                }
            }
            Some("DOCTOR") => RespValue::bulk_str(
                "Sam, I detected a few issues in this Redis instance memory implants:\n\n\
                * Persistence is disabled. Please check whether this makes sense for your use case.\n\n\
                I'm here to help you make the right Redis memory configuration decisions.\n\
                Memory related issues can be subtle, and the diagnosis might be affected by the\n\
                volume of data currently present in this instance. Always use the MEMORY DOCTOR\n\
                command referring to the documentation at http://redis.io/topics/memory-optimization."
            ),
            Some("MALLOC-STATS") => RespValue::bulk_str("jemalloc stats: N/A"),
            Some("PURGE") => RespValue::ok(),
            Some("STATS") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("peak.allocated"),
                    RespValue::integer(1000000),
                    RespValue::bulk_str("total.allocated"),
                    RespValue::integer(1000000),
                    RespValue::bulk_str("startup.allocated"),
                    RespValue::integer(500000),
                    RespValue::bulk_str("replication.backlog"),
                    RespValue::integer(0),
                    RespValue::bulk_str("clients.slaves"),
                    RespValue::integer(0),
                    RespValue::bulk_str("clients.normal"),
                    RespValue::integer(0),
                    RespValue::bulk_str("cluster.links"),
                    RespValue::integer(0),
                    RespValue::bulk_str("aof.buffer"),
                    RespValue::integer(0),
                    RespValue::bulk_str("modules.overhead"),
                    RespValue::integer(0),
                    RespValue::bulk_str("overhead.total"),
                    RespValue::integer(500000),
                    RespValue::bulk_str("keys.count"),
                    RespValue::integer(0),
                    RespValue::bulk_str("keys.bytes-per-key"),
                    RespValue::integer(0),
                    RespValue::bulk_str("dataset.bytes"),
                    RespValue::integer(500000),
                    RespValue::bulk_str("dataset.percentage"),
                    RespValue::bulk_str("50.00"),
                    RespValue::bulk_str("peak.percentage"),
                    RespValue::bulk_str("100.00"),
                    RespValue::bulk_str("allocator.frag"),
                    RespValue::bulk_str("1.00"),
                    RespValue::bulk_str("allocator.rss"),
                    RespValue::bulk_str("1.00"),
                    RespValue::bulk_str("rss-overhead.ratio"),
                    RespValue::bulk_str("1.00"),
                    RespValue::bulk_str("mem_fragmentation_ratio"),
                    RespValue::bulk_str("1.00"),
                    RespValue::bulk_str("mem_fragmentation_bytes"),
                    RespValue::integer(0),
                    RespValue::bulk_str("mem_not_counted_for_evict"),
                    RespValue::integer(0),
                    RespValue::bulk_str("active_defrag_running"),
                    RespValue::integer(0),
                    RespValue::bulk_str("lazyfree_pending_objects"),
                    RespValue::integer(0),
                    RespValue::bulk_str("lazyfreed_objects"),
                    RespValue::integer(0),
                ]))
            }
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("MEMORY <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("DOCTOR -- Outputs memory problems report."),
                    RespValue::bulk_str("MALLOC-STATS -- Show allocator internal stats."),
                    RespValue::bulk_str("PURGE -- Ask the allocator to release memory."),
                    RespValue::bulk_str("STATS -- Show memory usage details."),
                    RespValue::bulk_str("USAGE <key> [SAMPLES <count>] -- Estimate memory usage of key."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand. Try MEMORY HELP."),
        }
    }
}

// ─── SAVE / BGSAVE / LASTSAVE ─────────────────────────────────────────────────

pub struct SaveCommand;
impl CommandHandler for SaveCommand {
    fn name(&self) -> &str {
        "SAVE"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

pub struct BgSaveCommand;
impl CommandHandler for BgSaveCommand {
    fn name(&self) -> &str {
        "BGSAVE"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::simple("Background saving started")
    }
}

pub struct BgRewriteAofCommand;
impl CommandHandler for BgRewriteAofCommand {
    fn name(&self) -> &str {
        "BGREWRITEAOF"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::simple("Background append only file rewriting started")
    }
}

lazy_static::lazy_static! {
    static ref LAST_SAVE_TIME: std::sync::atomic::AtomicU64 = {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        std::sync::atomic::AtomicU64::new(ts)
    };
}

pub struct LastSaveCommand;
impl CommandHandler for LastSaveCommand {
    fn name(&self) -> &str {
        "LASTSAVE"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::integer(LAST_SAVE_TIME.load(Ordering::Relaxed) as i64)
    }
}

// ─── TIME ─────────────────────────────────────────────────────────────────────

pub struct TimeCommand;
impl CommandHandler for TimeCommand {
    fn name(&self) -> &str {
        "TIME"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let micros = now.subsec_micros();
        RespValue::Array(Some(vec![
            RespValue::bulk_str(&secs.to_string()),
            RespValue::bulk_str(&micros.to_string()),
        ]))
    }
}

// ─── MULTI/EXEC/DISCARD/WATCH stubs ──────────────────────────────────────────

pub struct MultiCommand;
impl CommandHandler for MultiCommand {
    fn name(&self) -> &str {
        "MULTI"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

pub struct ExecCommand;
impl CommandHandler for ExecCommand {
    fn name(&self) -> &str {
        "EXEC"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

pub struct DiscardCommand;
impl CommandHandler for DiscardCommand {
    fn name(&self) -> &str {
        "DISCARD"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

pub struct WatchCommand;
impl CommandHandler for WatchCommand {
    fn name(&self) -> &str {
        "WATCH"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

pub struct UnwatchCommand;
impl CommandHandler for UnwatchCommand {
    fn name(&self) -> &str {
        "UNWATCH"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

// ─── PUBLISH / PUBSUB ─────────────────────────────────────────────────────────

pub struct PublishCommand {
    pub hub: Arc<PubSubHub>,
}

impl CommandHandler for PublishCommand {
    fn name(&self) -> &str {
        "PUBLISH"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'publish' command");
        }
        let channel = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid channel name"),
        };
        let data = match args[2].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR invalid message"),
        };
        RespValue::integer(self.hub.publish(&channel, data) as i64)
    }
}

pub struct PubSubCommand {
    pub hub: Arc<PubSubHub>,
}

impl CommandHandler for PubSubCommand {
    fn name(&self) -> &str {
        "PUBSUB"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'pubsub' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("CHANNELS") => {
                let pattern = args.get(2).and_then(|a| a.as_str()).map(|s| s.to_string());
                let channels = self.hub.active_channels(pattern.as_deref());
                RespValue::Array(Some(channels.into_iter().map(|c| RespValue::bulk_bytes(c.into_bytes())).collect()))
            }
            Some("NUMPAT") => RespValue::integer(self.hub.num_patterns() as i64),
            Some("NUMSUB") => {
                let mut result = Vec::new();
                for i in 2..args.len() {
                    if let Some(ch) = args[i].as_str() {
                        result.push(RespValue::bulk_str(ch));
                        result.push(RespValue::integer(self.hub.num_subscribers(ch) as i64));
                    }
                }
                RespValue::Array(Some(result))
            }
            Some("SHARDCHANNELS") => {
                let pattern = args.get(2).and_then(|a| a.as_str()).map(|s| s.to_string());
                let channels = self.hub.active_channels(pattern.as_deref());
                RespValue::Array(Some(channels.into_iter().map(|c| RespValue::bulk_bytes(c.into_bytes())).collect()))
            }
            Some("SHARDNUMSUB") => {
                let mut result = Vec::new();
                for i in 2..args.len() {
                    if let Some(ch) = args[i].as_str() {
                        result.push(RespValue::bulk_str(ch));
                        result.push(RespValue::integer(self.hub.num_subscribers(ch) as i64));
                    }
                }
                RespValue::Array(Some(result))
            }
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("PUBSUB <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("CHANNELS [<pattern>] -- Return the currently active channels."),
                    RespValue::bulk_str("NUMPAT -- Return number of subscriptions to patterns."),
                    RespValue::bulk_str("NUMSUB [<channel> ...] -- Return the number of subscribers for the specified channels."),
                    RespValue::bulk_str("SHARDCHANNELS [<pattern>] -- Return the currently active shard channels."),
                    RespValue::bulk_str("SHARDNUMSUB [<shardchannel> ...] -- Return the number of subscribers for the specified shard channels."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand or wrong number of arguments for 'pubsub' command"),
        }
    }
}

// ─── SWAPDB ───────────────────────────────────────────────────────────────────

pub struct SwapDbCommand {
    pub db: Arc<RedisDatabase>,
}

impl CommandHandler for SwapDbCommand {
    fn name(&self) -> &str {
        "SWAPDB"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'swapdb' command");
        }
        let db1: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let db2: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        match self.db.swapdb(db1, db2) {
            Ok(()) => RespValue::ok(),
            Err(e) => RespValue::error(&e.to_string()),
        }
    }
}

// ─── WAIT ─────────────────────────────────────────────────────────────────────

pub struct WaitCommand;
impl CommandHandler for WaitCommand {
    fn name(&self) -> &str {
        "WAIT"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::integer(0)
    }
}

// ─── WAITAOF ──────────────────────────────────────────────────────────────────

pub struct WaitAofCommand;
impl CommandHandler for WaitAofCommand {
    fn name(&self) -> &str {
        "WAITAOF"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![RespValue::integer(0), RespValue::integer(0)]))
    }
}

// ─── OBJECT ───────────────────────────────────────────────────────────────────

pub struct ObjectCommand {
    pub db: Arc<RedisDatabase>,
    pub config: Arc<parking_lot::RwLock<crate::config::ServerConfig>>,
}

impl CommandHandler for ObjectCommand {
    fn name(&self) -> &str {
        "OBJECT"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'object' command");
        }
        let sub = args[1]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match sub.as_str() {
            "ENCODING" => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'object|encoding' command");
                }
                let key = match args[2].as_bytes() {
                    Some(b) => b.to_vec(),
                    None => return RespValue::error("ERR invalid key"),
                };
                match self.db.key_type(*db_index, &key) {
                    Ok(t) if t != "none" => {
                        let count = self.db.key_count(*db_index, &key).unwrap_or(0);
                        let cfg = self.config.read();
                        let encoding = match t {
                            "string" => {
                                if let Ok(Some(val)) = self.db.string_get(*db_index, &key) {
                                    if std::str::from_utf8(&val).ok().and_then(|s| s.parse::<i64>().ok()).is_some() {
                                        "int"
                                    } else if val.len() <= 44 {
                                        "embstr"
                                    } else {
                                        "raw"
                                    }
                                } else {
                                    "embstr"
                                }
                            }
                            "hash" => {
                                // listpack if entry count AND all values fit within size threshold
                                if count > cfg.hash_max_listpack_entries as i64 {
                                    "hashtable"
                                } else {
                                    // Check if any field/value exceeds the size limit
                                    let max_val = cfg.hash_max_listpack_value as usize;
                                    let exceeds = self.db.hgetall(*db_index, &key)
                                        .unwrap_or_default()
                                        .iter()
                                        .any(|(f, v)| f.len() > max_val || v.len() > max_val);
                                    if exceeds { "hashtable" } else { "listpack" }
                                }
                            }
                            "list" => {
                                // list-max-listpack-size: positive = max entries, negative = max size bytes
                                // -1=4KB, -2=8KB, -3=16KB, -4=32KB, -5=64KB
                                let is_listpack = if cfg.list_max_listpack_size > 0 {
                                    count <= cfg.list_max_listpack_size as i64
                                } else {
                                    let max_bytes: i64 = match cfg.list_max_listpack_size {
                                        -1 => 4096,
                                        -2 => 8192,
                                        -3 => 16384,
                                        -4 => 32768,
                                        -5 => 65536,
                                        _ => 8192,
                                    };
                                    // Estimate: fetch elements and sum byte sizes (only for small-ish lists)
                                    if count > 1024 {
                                        false // definitely quicklist
                                    } else {
                                        let total_bytes: i64 = self.db.lrange(*db_index, &key, 0, count)
                                            .unwrap_or_default()
                                            .iter()
                                            .map(|v| v.len() as i64 + 11) // 11 bytes overhead per entry estimate
                                            .sum();
                                        total_bytes <= max_bytes
                                    }
                                };
                                if is_listpack { "listpack" } else { "quicklist" }
                            }
                            "set" => {
                                // Check if all members are integers for intset encoding
                                let members = self.db.smembers(*db_index, &key).unwrap_or_default();
                                let all_integers = members.iter().all(|m| {
                                    std::str::from_utf8(m).ok()
                                        .and_then(|s| s.parse::<i64>().ok())
                                        .is_some()
                                });
                                if all_integers && count <= cfg.set_max_intset_entries as i64 {
                                    "intset"
                                } else if count <= cfg.set_max_listpack_entries as i64 {
                                    // Also check member size limit
                                    let max_val = cfg.set_max_listpack_value as usize;
                                    let any_large = members.iter().any(|m| m.len() > max_val);
                                    if any_large { "hashtable" } else { "listpack" }
                                } else {
                                    "hashtable"
                                }
                            }
                            "zset" => {
                                if count > cfg.zset_max_listpack_entries as i64 {
                                    "skiplist"
                                } else {
                                    // Check if any member exceeds size threshold
                                    let max_val = cfg.zset_max_listpack_value as usize;
                                    let exceeds = self.db.zrange(*db_index, &key, 0, -1)
                                        .unwrap_or_default()
                                        .iter()
                                        .any(|(m, _)| m.len() > max_val);
                                    if exceeds { "skiplist" } else { "listpack" }
                                }
                            }
                            "stream" | "Stream" => "stream",
                            "ReJSON-RL" => "embstr",
                            "MBbloom--" => "raw",
                            _ => "raw",
                        };
                        RespValue::bulk_str(encoding)
                    }
                    Ok(_) => RespValue::null_bulk(), // key doesn't exist → nil (Redis 7 behavior)
                    Err(e) => RespValue::error(&e.to_string()),
                }
            }
            "REFCOUNT" => {
                if args.len() < 3 { return RespValue::error("ERR wrong number of arguments"); }
                let key = args[2].as_bytes().map(|b| b.to_vec()).unwrap_or_default();
                match self.db.key_type(*db_index, &key) {
                    Ok(t) if t != "none" => RespValue::integer(1),
                    _ => RespValue::null_bulk(),
                }
            }
            "IDLETIME" => {
                if args.len() < 3 { return RespValue::error("ERR wrong number of arguments"); }
                let key = args[2].as_bytes().map(|b| b.to_vec()).unwrap_or_default();
                match self.db.key_type(*db_index, &key) {
                    Ok(t) if t != "none" => RespValue::integer(0),
                    _ => RespValue::null_bulk(),
                }
            }
            "FREQ" => {
                if args.len() < 3 { return RespValue::error("ERR wrong number of arguments"); }
                let key = args[2].as_bytes().map(|b| b.to_vec()).unwrap_or_default();
                match self.db.key_type(*db_index, &key) {
                    Ok(t) if t != "none" => RespValue::integer(0),
                    _ => RespValue::null_bulk(),
                }
            }
            "VERSION" => {
                // Dragonfly-specific: returns a logical monotonic version counter for the key
                if args.len() < 3 { return RespValue::error("ERR wrong number of arguments"); }
                let key = args[2].as_bytes().map(|b| b.to_vec()).unwrap_or_default();
                match self.db.key_version(*db_index, &key) {
                    Some(v) => RespValue::integer(v as i64),
                    None => RespValue::null_bulk(),
                }
            }
            "SET-ENCODING" => {
                // Dragonfly-specific: hint to change encoding, we acknowledge but take no action
                RespValue::ok()
            }
            "HELP" => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("OBJECT <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("ENCODING <key> -- Return the kind of internal representation the Redis object stored at <key> is using."),
                    RespValue::bulk_str("FREQ <key> -- Return the access frequency index of the key <key>."),
                    RespValue::bulk_str("HELP -- Return subcommand help summary."),
                    RespValue::bulk_str("IDLETIME <key> -- Return the idle time of the key <key>, that is the approximated number of seconds elapsed since the last access to the key."),
                    RespValue::bulk_str("REFCOUNT <key> -- Return the reference count of the object stored at <key>."),
                    RespValue::bulk_str("VERSION <key> -- Return the logical clock version of the object stored at <key> (Dragonfly)."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand or wrong number of arguments for 'object' command"),
        }
    }
}

// ─── RESET ────────────────────────────────────────────────────────────────────

pub struct ResetCommand;
impl CommandHandler for ResetCommand {
    fn name(&self) -> &str {
        "RESET"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::simple("RESET")
    }
}

// ─── LOLWUT ───────────────────────────────────────────────────────────────────

pub struct LolwutCommand;
impl CommandHandler for LolwutCommand {
    fn name(&self) -> &str {
        "LOLWUT"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        let art = "\
            \r\n\
            ██████╗ ███████╗██████╗     ██╗      ███████╗███╗   ███╗\r\n\
            ██╔══██╗██╔════╝██╔══██╗    ██║  ██╗██╔════╝████╗ ████║\r\n\
            ██████╔╝█████╗  ██║  ██║    ██║  ██║╚█████╗ ██╔████╔██║\r\n\
            ██╔══██╗██╔══╝  ██║  ██║    ╚██╗██╔╝ ╚═══██╗██║╚██╔╝██║\r\n\
            ██║  ██║███████╗██████╔╝     ╚████╔╝██████╔╝██║ ╚═╝ ██║\r\n\
            ╚═╝  ╚═╝╚══════╝╚═════╝       ╚═══╝ ╚═════╝ ╚═╝     ╚═╝\r\n\
            \r\n\
            ForgeKV 7.2.0 — Redis-compatible, LSM-tree backed, SSD-first\r\n\
            Dragon Mode: ON 🐉 (SSD is fast, RAM is expensive)\r\n\
            \r\n\
            Dr. Redis was here.\r\n";
        RespValue::bulk_str(art)
    }
}

// ─── SCRIPT ───────────────────────────────────────────────────────────────────

use sha1::{Digest, Sha1};

lazy_static::lazy_static! {
    static ref SCRIPT_STORE: Mutex<HashMap<String, String>> = Mutex::new(HashMap::new());
    static ref FUNCTION_STORE: Mutex<HashMap<String, String>> = Mutex::new(HashMap::new());
}

fn sha1_hex(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub struct ScriptCommand;

impl CommandHandler for ScriptCommand {
    fn name(&self) -> &str {
        "SCRIPT"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'script' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("LOAD") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'script|load' command");
                }
                let script = match args[2].as_str() {
                    Some(s) => s.to_string(),
                    None => return RespValue::error("ERR script must be a string"),
                };
                let sha = sha1_hex(script.as_bytes());
                SCRIPT_STORE.lock().insert(sha.clone(), script);
                RespValue::bulk_str(&sha)
            }
            Some("FLUSH") => {
                SCRIPT_STORE.lock().clear();
                RespValue::ok()
            }
            Some("EXISTS") => {
                let store = SCRIPT_STORE.lock();
                let result: Vec<RespValue> = args[2..].iter()
                    .map(|a| {
                        let exists = a.as_str().map_or(false, |sha| store.contains_key(sha));
                        RespValue::integer(if exists { 1 } else { 0 })
                    })
                    .collect();
                RespValue::Array(Some(result))
            }
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("SCRIPT <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("EXISTS <sha1> [<sha1> ...] -- Return information about the existence and length of the script corresponding to the specified SHA1 digest."),
                    RespValue::bulk_str("FLUSH [ASYNC|SYNC] -- Flush the Lua scripts cache."),
                    RespValue::bulk_str("LOAD <script> -- Load the specified Lua script into the script cache."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand or wrong number of arguments"),
        }
    }
}

pub struct EvalCommand {
    pub registry: Arc<CommandRegistry>,
}
impl CommandHandler for EvalCommand {
    fn name(&self) -> &str {
        "EVAL"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // EVAL script numkeys [key [key ...]] [arg [arg ...]]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'eval' command");
        }
        let script = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR script must be a string"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR Number of keys can't be greater than number of args");
        }
        let keys = &args[3..3 + numkeys];
        let argv = &args[3 + numkeys..];
        crate::lua_engine::eval_lua(&script, keys, argv, *db_index, &self.registry)
    }
}

pub struct EvalshaCommand {
    pub registry: Arc<CommandRegistry>,
}
impl CommandHandler for EvalshaCommand {
    fn name(&self) -> &str {
        "EVALSHA"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'evalsha' command");
        }
        let sha = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR sha must be a string"),
        };
        let script = match SCRIPT_STORE.lock().get(&sha).cloned() {
            Some(s) => s,
            None => return RespValue::error("NOSCRIPT No matching script. Please use EVAL."),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR Number of keys can't be greater than number of args");
        }
        let keys = &args[3..3 + numkeys];
        let argv = &args[3 + numkeys..];
        crate::lua_engine::eval_lua(&script, keys, argv, *db_index, &self.registry)
    }
}

pub struct EvalRoCommand {
    pub registry: Arc<CommandRegistry>,
}
impl CommandHandler for EvalRoCommand {
    fn name(&self) -> &str {
        "EVAL_RO"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // Same as EVAL — we don't enforce read-only at this layer
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'eval_ro' command");
        }
        let script = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR script must be a string"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR Number of keys can't be greater than number of args");
        }
        let keys = &args[3..3 + numkeys];
        let argv = &args[3 + numkeys..];
        crate::lua_engine::eval_lua(&script, keys, argv, *db_index, &self.registry)
    }
}

pub struct EvalshaRoCommand {
    pub registry: Arc<CommandRegistry>,
}
impl CommandHandler for EvalshaRoCommand {
    fn name(&self) -> &str {
        "EVALSHA_RO"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'evalsha_ro' command");
        }
        let sha = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR sha must be a string"),
        };
        let script = match SCRIPT_STORE.lock().get(&sha).cloned() {
            Some(s) => s,
            None => return RespValue::error("NOSCRIPT No matching script. Please use EVAL."),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR Number of keys can't be greater than number of args");
        }
        let keys = &args[3..3 + numkeys];
        let argv = &args[3 + numkeys..];
        crate::lua_engine::eval_lua(&script, keys, argv, *db_index, &self.registry)
    }
}

// ─── FUNCTION ─────────────────────────────────────────────────────────────────

/// Extract function names from library code by scanning for redis.register_function calls.
/// Handles both forms:
///   redis.register_function('name', fn)
///   redis.register_function{function_name='name', ...}
fn extract_function_names(code: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in code.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("redis.register_function") {
            let rest = rest.trim_start();
            if rest.starts_with('(') {
                // Form: redis.register_function('name', fn)
                let inner = &rest[1..];
                let quote_char = inner.chars().next();
                if quote_char == Some('\'') || quote_char == Some('"') {
                    let q = quote_char.unwrap();
                    if let Some(end) = inner[1..].find(q) {
                        names.push(inner[1..end + 1].to_string());
                    }
                }
            } else if rest.starts_with('{') {
                // Form: redis.register_function{function_name='name', ...}
                if let Some(fn_pos) = rest.find("function_name") {
                    let after = &rest[fn_pos + "function_name".len()..];
                    let after = after.trim_start().trim_start_matches('=').trim_start();
                    let quote_char = after.chars().next();
                    if quote_char == Some('\'') || quote_char == Some('"') {
                        let q = quote_char.unwrap();
                        if let Some(end) = after[1..].find(q) {
                            names.push(after[1..end + 1].to_string());
                        }
                    }
                }
            }
        }
    }
    names
}

pub struct FunctionCommand;

impl CommandHandler for FunctionCommand {
    fn name(&self) -> &str {
        "FUNCTION"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'function' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("FLUSH") => {
                FUNCTION_STORE.lock().clear();
                RespValue::ok()
            }
            Some("LOAD") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'function|load' command");
                }
                let (replace, code_arg) = if args.len() >= 4
                    && args[2].as_str().map(|s| s.to_uppercase() == "REPLACE").unwrap_or(false)
                {
                    (true, &args[3])
                } else {
                    (false, &args[2])
                };
                let code = match code_arg.as_str() {
                    Some(s) => s.to_string(),
                    None => return RespValue::error("ERR function body must be a string"),
                };
                let lib_name = code.lines().next()
                    .and_then(|l| l.strip_prefix("#!lua name="))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let mut store = FUNCTION_STORE.lock();
                if store.contains_key(&lib_name) && !replace {
                    return RespValue::error(&format!("ERR Library '{}' already exists", lib_name));
                }
                store.insert(lib_name.clone(), code);
                RespValue::bulk_str(&lib_name)
            }
            Some("LIST") => {
                let store = FUNCTION_STORE.lock();
                let result: Vec<RespValue> = store.iter().map(|(lib_name, code)| {
                    // Parse function names from redis.register_function calls
                    let func_names = extract_function_names(code);
                    let funcs: Vec<RespValue> = func_names.into_iter().map(|fname| {
                        RespValue::Array(Some(vec![
                            RespValue::bulk_str("name"),
                            RespValue::bulk_str(&fname),
                            RespValue::bulk_str("description"),
                            RespValue::null_bulk(),
                            RespValue::bulk_str("flags"),
                            RespValue::Array(Some(vec![])),
                        ]))
                    }).collect();
                    RespValue::Array(Some(vec![
                        RespValue::bulk_str("library_name"),
                        RespValue::bulk_str(lib_name),
                        RespValue::bulk_str("engine"),
                        RespValue::bulk_str("LUA"),
                        RespValue::bulk_str("functions"),
                        RespValue::Array(Some(funcs)),
                    ]))
                }).collect();
                RespValue::Array(Some(result))
            }
            Some("DELETE") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'function|delete' command");
                }
                let lib_name = match args[2].as_str() {
                    Some(s) => s.to_string(),
                    None => return RespValue::error("ERR library name must be a string"),
                };
                if FUNCTION_STORE.lock().remove(&lib_name).is_none() {
                    return RespValue::error("ERR Library not found");
                }
                RespValue::ok()
            }
            Some("STATS") => {
                let count = FUNCTION_STORE.lock().len();
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("running_script"),
                    RespValue::null_bulk(),
                    RespValue::bulk_str("engines"),
                    RespValue::Array(Some(vec![
                        RespValue::bulk_str("LUA"),
                        RespValue::Array(Some(vec![
                            RespValue::bulk_str("libraries_count"),
                            RespValue::integer(count as i64),
                            RespValue::bulk_str("functions_count"),
                            RespValue::integer(0),
                        ])),
                    ])),
                ]))
            }
            Some("RESTORE") => RespValue::ok(),
            Some("DUMP") => RespValue::bulk_str(""),
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("FUNCTION <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("DELETE <library-name> -- Delete a library and all its functions."),
                    RespValue::bulk_str("DUMP -- Return a serialized payload representing the current state of the server libraries."),
                    RespValue::bulk_str("FLUSH [ASYNC|SYNC] -- Delete all libraries."),
                    RespValue::bulk_str("LIST [LIBRARYNAME library-name-pattern] [WITHCODE] -- Return general information on all the libraries."),
                    RespValue::bulk_str("LOAD [REPLACE] function-code -- Create a new library with the given library name and code."),
                    RespValue::bulk_str("RESTORE serialized-value [FLUSH|APPEND|REPLACE] -- Restore libraries from the serialized payload."),
                    RespValue::bulk_str("STATS -- Return information about the function running."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand or wrong number of arguments"),
        }
    }
}

pub struct FcallCommand {
    pub registry: Arc<CommandRegistry>,
}
impl CommandHandler for FcallCommand {
    fn name(&self) -> &str {
        "FCALL"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        // FCALL function numkeys [key [key ...]] [arg [arg ...]]
        if args.len() < 3 {
            return RespValue::error("ERR wrong number of arguments for 'fcall' command");
        }
        let func_name = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR function name must be a string"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        if args.len() < 3 + numkeys {
            return RespValue::error("ERR Number of keys can't be greater than number of args");
        }
        // Find library code containing this function
        let library_code = {
            let store = FUNCTION_STORE.lock();
            store
                .values()
                .find(|code| code.contains(&func_name))
                .cloned()
        };
        let library_code = match library_code {
            Some(c) => c,
            None => return RespValue::error(&format!("ERR Function not found")),
        };
        let keys = &args[3..3 + numkeys];
        let argv = &args[3 + numkeys..];
        // Strip #!lua shebang line (not valid Lua syntax)
        let clean_code: String = library_code
            .lines()
            .filter(|l| !l.trim_start().starts_with("#!"))
            .collect::<Vec<_>>()
            .join("\n");
        // Wrap library code so redis.register_function stores functions,
        // then call the requested function by name
        let script = format!(
            r#"
local __funcs = {{}}
redis.register_function = function(name_or_tbl, fn)
    if type(name_or_tbl) == 'table' then
        __funcs[name_or_tbl.function_name] = name_or_tbl.callback
    else
        __funcs[name_or_tbl] = fn
    end
end
-- Execute library setup (registers functions)
{clean_code}
-- Call the requested function
local __fn = __funcs['{func_name}']
if __fn == nil then error('ERR Function not found: {func_name}') end
return __fn(KEYS, ARGV)
"#,
            clean_code = clean_code,
            func_name = func_name,
        );
        crate::lua_engine::eval_lua(&script, keys, argv, *db_index, &self.registry)
    }
}

pub struct FcallRoCommand {
    pub registry: Arc<CommandRegistry>,
}
impl CommandHandler for FcallRoCommand {
    fn name(&self) -> &str {
        "FCALL_RO"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        FcallCommand {
            registry: self.registry.clone(),
        }
        .execute(db_index, args)
    }
}

// ─── ACL ──────────────────────────────────────────────────────────────────────

pub struct AclCommand {
    pub acl: Arc<AclManager>,
    pub current_username: String,
}

impl CommandHandler for AclCommand {
    fn name(&self) -> &str {
        "ACL"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'acl' command");
        }
        match args[1].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("WHOAMI") => {
                RespValue::bulk_str(&self.current_username)
            }
            Some("LIST") => {
                let rules = self.acl.list_users();
                RespValue::Array(Some(rules.into_iter().map(|r| RespValue::bulk_str(&r)).collect()))
            }
            Some("USERS") => {
                let names = self.acl.user_names();
                RespValue::Array(Some(names.into_iter().map(|n| RespValue::bulk_str(&n)).collect()))
            }
            Some("GETUSER") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'acl|getuser' command");
                }
                let name = match args[2].as_str() {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid username"),
                };
                match self.acl.get_user(name) {
                    None => RespValue::null_bulk(),
                    Some(user) => {
                        RespValue::Array(Some(vec![
                            RespValue::bulk_str("flags"),
                            RespValue::Array(Some({
                                let mut flags = vec![];
                                if user.enabled { flags.push(RespValue::bulk_str("on")); }
                                else { flags.push(RespValue::bulk_str("off")); }
                                if user.nopass { flags.push(RespValue::bulk_str("nopass")); }
                                if user.allkeys { flags.push(RespValue::bulk_str("allkeys")); }
                                if user.allchannels { flags.push(RespValue::bulk_str("allchannels")); }
                                flags
                            })),
                            RespValue::bulk_str("passwords"),
                            RespValue::Array(Some(user.passwords.iter().map(|p| RespValue::bulk_str(&format!("#{}", p))).collect())),
                            RespValue::bulk_str("commands"),
                            RespValue::bulk_str(if user.allowed_categories == crate::acl::CAT_ALL { "+@all" } else { "-@all" }),
                            RespValue::bulk_str("keys"),
                            RespValue::Array(Some(user.key_patterns.iter().map(|k| RespValue::bulk_str(k)).collect())),
                            RespValue::bulk_str("channels"),
                            RespValue::Array(Some(user.channel_patterns.iter().map(|c| RespValue::bulk_str(c)).collect())),
                            RespValue::bulk_str("selectors"),
                            RespValue::Array(Some(vec![])),
                        ]))
                    }
                }
            }
            Some("SETUSER") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'acl|setuser' command");
                }
                let name = match args[2].as_str() {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid username"),
                };
                let rules: Vec<&str> = args[3..].iter()
                    .filter_map(|a| a.as_str())
                    .collect();
                match self.acl.set_user(name, &rules) {
                    Ok(()) => RespValue::ok(),
                    Err(e) => RespValue::error(&e),
                }
            }
            Some("DELUSER") => {
                if args.len() < 3 {
                    return RespValue::error("ERR wrong number of arguments for 'acl|deluser' command");
                }
                let names: Vec<&str> = args[2..].iter().filter_map(|a| a.as_str()).collect();
                let deleted = self.acl.del_user(&names);
                RespValue::integer(deleted as i64)
            }
            Some("CAT") => {
                if args.len() >= 3 {
                    // ACL CAT category — list commands in category (stub)
                    RespValue::Array(Some(vec![]))
                } else {
                    let cats = AclManager::categories();
                    RespValue::Array(Some(cats.into_iter().map(|c| RespValue::bulk_str(c)).collect()))
                }
            }
            Some("LOG") => {
                if args.len() >= 3 {
                    let sub = args[2].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
                    if sub == "RESET" {
                        self.acl.reset_log();
                        return RespValue::ok();
                    }
                    let count: usize = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(10);
                    let entries = self.acl.get_log(Some(count));
                    return RespValue::Array(Some(entries.into_iter().map(|e| {
                        RespValue::Array(Some(vec![
                            RespValue::bulk_str("count"),
                            RespValue::integer(e.count as i64),
                            RespValue::bulk_str("reason"),
                            RespValue::bulk_str(&e.reason),
                            RespValue::bulk_str("context"),
                            RespValue::bulk_str(&e.context),
                            RespValue::bulk_str("object"),
                            RespValue::bulk_str(&e.object),
                            RespValue::bulk_str("username"),
                            RespValue::bulk_str(&e.username),
                            RespValue::bulk_str("age-seconds"),
                            RespValue::bulk_str(&format!("{:.3}", e.age_seconds)),
                            RespValue::bulk_str("client-info"),
                            RespValue::bulk_str(&e.client_info),
                            RespValue::bulk_str("entry-id"),
                            RespValue::integer(e.entry_id as i64),
                            RespValue::bulk_str("timestamp-created"),
                            RespValue::integer(e.timestamp_created as i64),
                            RespValue::bulk_str("timestamp-last-updated"),
                            RespValue::integer(e.timestamp_last_updated as i64),
                        ]))
                    }).collect()));
                }
                let entries = self.acl.get_log(None);
                RespValue::Array(Some(entries.into_iter().map(|e| {
                    RespValue::Array(Some(vec![
                        RespValue::bulk_str("count"),
                        RespValue::integer(e.count as i64),
                        RespValue::bulk_str("reason"),
                        RespValue::bulk_str(&e.reason),
                        RespValue::bulk_str("username"),
                        RespValue::bulk_str(&e.username),
                    ]))
                }).collect()))
            }
            Some("SAVE") => RespValue::ok(),
            Some("LOAD") => RespValue::ok(),
            Some("HELP") => {
                RespValue::Array(Some(vec![
                    RespValue::bulk_str("ACL <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                    RespValue::bulk_str("CAT [<category>] -- List all commands that belong to <category>, or all user-defined categories if no category specified."),
                    RespValue::bulk_str("DELUSER <username> [<username> ...] -- Delete user accounts."),
                    RespValue::bulk_str("GETUSER <username> -- Get the rules for <username>."),
                    RespValue::bulk_str("LIST -- List the current ACL rules in ACL config file format."),
                    RespValue::bulk_str("LOAD -- Reload the ACLs from the configured ACL file."),
                    RespValue::bulk_str("LOG [<count> | RESET] -- List latest events that were blocked due to ACLs, or reset it."),
                    RespValue::bulk_str("SAVE -- Save the current ACL rules in the configured ACL file."),
                    RespValue::bulk_str("SETUSER <username> [<rule> [<rule> ...]] -- Modify or create the rules for <username>."),
                    RespValue::bulk_str("USERS -- List all the registered usernames."),
                    RespValue::bulk_str("WHOAMI -- Return the current connection username."),
                ]))
            }
            _ => RespValue::error("ERR unknown subcommand or wrong number of arguments for 'acl' command"),
        }
    }
}

// ─── SHARDED PUBSUB ───────────────────────────────────────────────────────────

pub struct SpublishCommand {
    pub hub: Arc<PubSubHub>,
}
impl CommandHandler for SpublishCommand {
    fn name(&self) -> &str {
        "SPUBLISH"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() != 3 {
            return RespValue::error("ERR wrong number of arguments for 'spublish' command");
        }
        let channel = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return RespValue::error("ERR invalid channel name"),
        };
        let data = match args[2].as_bytes() {
            Some(b) => b.to_vec(),
            None => return RespValue::error("ERR invalid message"),
        };
        RespValue::integer(self.hub.publish(&channel, data) as i64)
    }
}

pub struct SsubscribeCommand;
impl CommandHandler for SsubscribeCommand {
    fn name(&self) -> &str {
        "SSUBSCRIBE"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        let mut result = Vec::new();
        for (i, arg) in args[1..].iter().enumerate() {
            if let Some(ch) = arg.as_bytes() {
                result.push(RespValue::Array(Some(vec![
                    RespValue::bulk_str("ssubscribe"),
                    RespValue::bulk_bytes(ch.to_vec()),
                    RespValue::integer((i + 1) as i64),
                ])));
            }
        }
        if result.len() == 1 {
            result.remove(0)
        } else {
            RespValue::Array(Some(result))
        }
    }
}

pub struct SunsubscribeCommand;
impl CommandHandler for SunsubscribeCommand {
    fn name(&self) -> &str {
        "SUNSUBSCRIBE"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() <= 1 {
            return RespValue::Array(Some(vec![
                RespValue::bulk_str("sunsubscribe"),
                RespValue::null_bulk(),
                RespValue::integer(0),
            ]));
        }
        let mut result = Vec::new();
        for (i, arg) in args[1..].iter().enumerate() {
            if let Some(ch) = arg.as_bytes() {
                result.push(RespValue::Array(Some(vec![
                    RespValue::bulk_str("sunsubscribe"),
                    RespValue::bulk_bytes(ch.to_vec()),
                    RespValue::integer((args.len() - 2 - i) as i64),
                ])));
            }
        }
        if result.len() == 1 {
            result.remove(0)
        } else {
            RespValue::Array(Some(result))
        }
    }
}

// ─── SINTERCARD alias in server_cmds ─────────────────────────────────────────

pub struct ReplicaofCommand;
impl CommandHandler for ReplicaofCommand {
    fn name(&self) -> &str {
        "REPLICAOF"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        if args.len() >= 3 {
            let host = args[1].as_str().unwrap_or("").to_uppercase();
            if host == "NO" {
                return RespValue::ok();
            }
        }
        RespValue::error("ERR Replication not supported in ForgeKV standalone mode")
    }
}

pub struct SlaveofCommand;
impl CommandHandler for SlaveofCommand {
    fn name(&self) -> &str {
        "SLAVEOF"
    }
    fn execute(&self, db_index: &mut usize, args: &[RespValue]) -> RespValue {
        ReplicaofCommand.execute(db_index, args)
    }
}

pub struct FailoverCommand;
impl CommandHandler for FailoverCommand {
    fn name(&self) -> &str {
        "FAILOVER"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::error("ERR Failover not supported")
    }
}

pub struct PsyncCommand;
impl CommandHandler for PsyncCommand {
    fn name(&self) -> &str {
        "PSYNC"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::error("ERR Replication not supported")
    }
}

pub struct ReplconfCommand;
impl CommandHandler for ReplconfCommand {
    fn name(&self) -> &str {
        "REPLCONF"
    }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

// ─── CLUSTER ──────────────────────────────────────────────────────────────────

pub struct ClusterCommand;
impl CommandHandler for ClusterCommand {
    fn name(&self) -> &str {
        "CLUSTER"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        let sub = args
            .get(1)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase());
        match sub.as_deref() {
            Some("INFO") => RespValue::bulk_str(
                "cluster_enabled:0\r\n\
                     cluster_state:ok\r\n\
                     cluster_slots_assigned:0\r\n\
                     cluster_slots_ok:0\r\n\
                     cluster_slots_pfail:0\r\n\
                     cluster_slots_fail:0\r\n\
                     cluster_known_nodes:0\r\n\
                     cluster_size:0\r\n\
                     cluster_current_epoch:0\r\n\
                     cluster_my_epoch:0\r\n\
                     cluster_stats_messages_sent:0\r\n\
                     cluster_stats_messages_received:0\r\n\
                     total_cluster_links_buffer_limit_exceeded:0\r\n",
            ),
            Some("MYID") => RespValue::bulk_str("0000000000000000000000000000000000000000"),
            Some("NODES") => RespValue::bulk_str(""),
            Some("SLOTS") => RespValue::Array(Some(vec![])),
            Some("SHARDS") => RespValue::Array(Some(vec![])),
            Some("KEYSLOT") => {
                // CRC16 of key % 16384
                let key = args.get(2).and_then(|a| a.as_bytes()).unwrap_or(b"");
                let slot = crc16_xmodem(key) % 16384;
                RespValue::integer(slot as i64)
            }
            Some("COUNTKEYSINSLOT") => RespValue::integer(0),
            Some("GETKEYSINSLOT") => RespValue::Array(Some(vec![])),
            Some("RESET") => RespValue::ok(),
            Some("FLUSHSLOTS") => RespValue::ok(),
            Some("ADDSLOTS") | Some("ADDSLOTSRANGE") | Some("DELSLOTS") | Some("DELSLOTSRANGE") => {
                RespValue::ok()
            }
            Some("MEET") | Some("FORGET") | Some("REPLICATE") | Some("FAILOVER") => RespValue::ok(),
            Some("SETSLOT") => RespValue::ok(),
            Some("LINKS") => RespValue::Array(Some(vec![])),
            _ => RespValue::error("ERR Wrong number of args calling cluster subcommand"),
        }
    }
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

// ─── DFLY (Dragonfly-specific commands) ───────────────────────────────────────

pub struct DflyCommand;
impl CommandHandler for DflyCommand {
    fn name(&self) -> &str {
        "DFLY"
    }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        let sub = args
            .get(1)
            .and_then(|a| a.as_str())
            .map(|s| s.to_uppercase());
        match sub.as_deref() {
            Some("GETRESOURCESTATS") => RespValue::Array(Some(vec![
                RespValue::bulk_str("used_memory"),
                RespValue::integer(0),
                RespValue::bulk_str("used_memory_peak"),
                RespValue::integer(0),
            ])),
            Some("GETVER") => RespValue::bulk_str("v1.0.0-ssd"),
            Some("SHARD") | Some("SHARD_COUNT") => RespValue::integer(1),
            Some("DECOMMIT") => RespValue::ok(),
            Some("PAUSE") | Some("RESUME") => RespValue::ok(),
            Some("SETCRDT") | Some("GETCRDT") => RespValue::ok(),
            _ => RespValue::error("ERR unknown DFLY subcommand"),
        }
    }
}

// ─── XADD_STREAM (alias helper for duplicate XADD name) ──────────────────────
// Nothing new here — just a placeholder to close the server_cmds module.
