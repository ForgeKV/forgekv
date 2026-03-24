use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::acl::AclManager;
use crate::blocking::BLOCKING_NOTIFIER;
use crate::commands::CommandRegistry;
use crate::config::ServerConfig;
use crate::database::RedisDatabase;
use crate::keyspace::{cmd_event, event_allowed, KS_NOTIFIER};
use crate::pubsub::{PubSubHub, PubSubMessage};
use crate::resp::{RespParser, RespValue, RespWriter};

/// Per-connection client state.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: u64,
    pub name: String,
    pub addr: String,
    pub db: usize,
    pub flags: String,
    pub authenticated: bool,
    pub username: String,
    pub resp_version: u8,
    pub created_at: u64,
    pub last_cmd: String,
    pub fd: i64,
}

impl ClientInfo {
    fn new(id: u64, addr: &str) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        ClientInfo {
            id,
            name: String::new(),
            addr: addr.to_string(),
            db: 0,
            flags: "N".to_string(),
            authenticated: false,
            username: "default".to_string(),
            resp_version: 2,
            created_at: ts,
            last_cmd: String::new(),
            fd: id as i64,
        }
    }

    pub fn to_list_entry(&self) -> String {
        let age = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.created_at);
        format!(
            "id={} addr={} laddr=127.0.0.1:6379 fd={} name={} age={} idle=0 flags={} db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=32768 argv-mem=0 multi-mem=0 tot-mem=22312 rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd={} user={} library-name= resp={}\r\n",
            self.id, self.addr, self.fd, self.name, age, self.flags, self.db, self.last_cmd, self.username, self.resp_version
        )
    }
}

/// Global registry of all connected clients.
pub struct ClientRegistry {
    clients: RwLock<HashMap<u64, ClientInfo>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        ClientRegistry {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, info: ClientInfo) {
        self.clients.write().insert(info.id, info);
    }

    pub fn unregister(&self, id: u64) {
        self.clients.write().remove(&id);
    }

    pub fn update(&self, id: u64, f: impl FnOnce(&mut ClientInfo)) {
        if let Some(info) = self.clients.write().get_mut(&id) {
            f(info);
        }
    }

    pub fn list(&self) -> Vec<ClientInfo> {
        self.clients.read().values().cloned().collect()
    }

    pub fn count(&self) -> usize {
        self.clients.read().len()
    }

    pub fn get(&self, id: u64) -> Option<ClientInfo> {
        self.clients.read().get(&id).cloned()
    }

    pub fn kill_by_id(&self, id: u64) -> bool {
        self.clients.write().remove(&id).is_some()
    }

    pub fn kill_by_addr(&self, addr: &str) -> bool {
        let mut clients = self.clients.write();
        let ids: Vec<u64> = clients
            .values()
            .filter(|c| c.addr == addr)
            .map(|c| c.id)
            .collect();
        let found = !ids.is_empty();
        for id in ids {
            clients.remove(&id);
        }
        found
    }
}

pub struct TcpServer {
    config: ServerConfig,
    registry: Arc<CommandRegistry>,
    pub hub: Arc<PubSubHub>,
    pub client_registry: Arc<ClientRegistry>,
    pub acl: Arc<AclManager>,
    pub db: Arc<RedisDatabase>,
    pub start_time: Instant,
    pub total_connections: Arc<AtomicU64>,
    pub total_commands: Arc<AtomicU64>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    pub port: u16,
    next_client_id: Arc<AtomicU64>,
}

impl TcpServer {
    pub fn new(
        config: ServerConfig,
        registry: Arc<CommandRegistry>,
        hub: Arc<PubSubHub>,
        acl: Arc<AclManager>,
        db: Arc<RedisDatabase>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let port = config.port;
        TcpServer {
            config,
            registry,
            hub,
            client_registry: Arc::new(ClientRegistry::new()),
            acl,
            db,
            start_time: Instant::now(),
            total_connections: Arc::new(AtomicU64::new(0)),
            total_commands: Arc::new(AtomicU64::new(0)),
            shutdown_tx,
            shutdown_rx,
            port,
            next_client_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn run(&self) {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr)
            .await
            .unwrap_or_else(|_| panic!("Failed to bind to {}", addr));

        eprintln!("ForgeKV listening on {}", addr);

        // ── Keyspace notification dispatcher ──────────────────────────────────
        {
            let hub = self.hub.clone();
            tokio::spawn(async move {
                let mut sub = KS_NOTIFIER.subscribe();
                loop {
                    match sub.recv().await {
                        Ok((db, key, event)) => {
                            let cfg = KS_NOTIFIER.cfg.read().clone();
                            let key_str = String::from_utf8_lossy(&key);
                            if cfg.keyspace {
                                let chan = format!("__keyspace@{}__:{}", db, key_str);
                                hub.publish(&chan, event.as_bytes().to_vec());
                            }
                            if cfg.keyevent {
                                let chan = format!("__keyevent@{}__:{}", db, event);
                                hub.publish(&chan, key);
                            }
                        }
                        Err(_) => {
                            // Lagged - resubscribe
                            sub = KS_NOTIFIER.subscribe();
                        }
                    }
                }
            });
        }

        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let client_id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
                            let registry = self.registry.clone();
                            let hub = self.hub.clone();
                            let total_conns = self.total_connections.clone();
                            let total_cmds = self.total_commands.clone();
                            let client_registry = self.client_registry.clone();
                            let acl = self.acl.clone();
                            let requirepass = self.config.requirepass.clone();
                            let db = self.db.clone();
                            let mut client_shutdown = self.shutdown_rx.clone();
                            let addr_str = peer_addr.to_string();

                            total_conns.fetch_add(1, Ordering::Relaxed);

                            // Register client
                            let mut info = ClientInfo::new(client_id, &addr_str);
                            // If no requirepass, default user is already authenticated
                            info.authenticated = requirepass.is_empty();
                            client_registry.register(info);

                            let cr2 = client_registry.clone();
                            tokio::spawn(async move {
                                handle_client(
                                    stream,
                                    client_id,
                                    registry,
                                    hub,
                                    db,
                                    total_cmds,
                                    &mut client_shutdown,
                                    client_registry,
                                    acl,
                                    requirepass,
                                ).await;
                                cr2.unregister(client_id);
                            });
                        }
                        Err(e) => {
                            eprintln!("Accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        eprintln!("Server shutting down");
                        break;
                    }
                }
            }
        }
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

// Internal message type for the subscriber aggregator channel.
enum AggMsg {
    Channel(PubSubMessage),
    Pattern { pattern: String, msg: PubSubMessage },
}

#[allow(clippy::too_many_arguments)]
async fn handle_client(
    stream: tokio::net::TcpStream,
    client_id: u64,
    registry: Arc<CommandRegistry>,
    hub: Arc<PubSubHub>,
    db: Arc<RedisDatabase>,
    total_commands: Arc<AtomicU64>,
    shutdown_rx: &mut watch::Receiver<bool>,
    client_registry: Arc<ClientRegistry>,
    acl: Arc<AclManager>,
    requirepass: String,
) {
    let (reader, writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    let mut parser = RespParser::new();
    let mut db_index: usize = 0;
    let mut buf = vec![0u8; 8192];
    let mut in_multi = false;
    let mut queued_cmds: Vec<Vec<RespValue>> = Vec::new();

    // Per-connection state
    let mut authenticated = requirepass.is_empty();
    let mut client_name = String::new();
    let mut username = "default".to_string();
    let mut resp_version: u8 = 2;
    let mut no_evict = false;
    let mut no_touch = false;
    let _ = (no_evict, no_touch);

    loop {
        tokio::select! {
            result = reader.read(&mut buf) => {
                match result {
                    Ok(0) => break,
                    Ok(n) => {
                        parser.feed(&buf[..n]);

                        loop {
                            match parser.try_parse() {
                                None => break,
                                Some(value) => {
                                    total_commands.fetch_add(1, Ordering::Relaxed);

                                    let args = match value {
                                        RespValue::Array(Some(items)) => items,
                                        other => vec![other],
                                    };

                                    if args.is_empty() {
                                        continue;
                                    }

                                    let cmd_upper = args[0].as_str()
                                        .map(|s| s.to_uppercase())
                                        .unwrap_or_default();

                                    // Update last command in registry
                                    client_registry.update(client_id, |info| {
                                        info.last_cmd = cmd_upper.to_lowercase();
                                        info.db = db_index;
                                    });

                                    // ── AUTH ──────────────────────────────────────────────
                                    if cmd_upper == "AUTH" {
                                        let response = handle_auth(
                                            &args, &acl, &requirepass,
                                            &mut authenticated, &mut username,
                                        );
                                        if write_resp(&mut writer, &response).await.is_err() { let _ = writer.flush().await; return; }
                                        continue;
                                    }

                                    // ── HELLO ────────────────────────────────────────────
                                    if cmd_upper == "HELLO" {
                                        let response = handle_hello(
                                            &args, &acl, &requirepass,
                                            &mut authenticated, &mut username,
                                            &mut resp_version, &client_name, client_id, db_index,
                                        );
                                        if write_resp(&mut writer, &response).await.is_err() { let _ = writer.flush().await; return; }
                                        if cmd_upper == "QUIT" { let _ = writer.flush().await; return; }
                                        continue;
                                    }

                                    // ── Require authentication ────────────────────────────
                                    if !authenticated {
                                        let err = RespValue::error("NOAUTH Authentication required.");
                                        if write_resp(&mut writer, &err).await.is_err() { let _ = writer.flush().await; return; }
                                        continue;
                                    }

                                    // ── SUBSCRIBE / PSUBSCRIBE ───────────────────────────
                                    if cmd_upper == "SUBSCRIBE" || cmd_upper == "PSUBSCRIBE" {
                                        // Flush pending responses before entering subscriber mode
                                        let _ = writer.flush().await;
                                        let is_pattern = cmd_upper == "PSUBSCRIBE";
                                        handle_subscriber_mode(
                                            &args, is_pattern,
                                            &mut reader, &mut writer, &mut parser,
                                            &hub, shutdown_rx,
                                        ).await;
                                        return;
                                    }

                                    // ── UNSUBSCRIBE / PUNSUBSCRIBE outside sub mode ───────
                                    if cmd_upper == "UNSUBSCRIBE" || cmd_upper == "PUNSUBSCRIBE" {
                                        let kind = if cmd_upper == "UNSUBSCRIBE" { "unsubscribe" } else { "punsubscribe" };
                                        let channel = args.get(1).cloned().unwrap_or(RespValue::null_bulk());
                                        let resp = RespValue::Array(Some(vec![
                                            RespValue::bulk_str(kind),
                                            channel,
                                            RespValue::integer(0),
                                        ]));
                                        if write_resp(&mut writer, &resp).await.is_err() { let _ = writer.flush().await; return; }
                                        continue;
                                    }

                                    // ── CLIENT (per-connection state) ────────────────────
                                    if cmd_upper == "CLIENT" {
                                        let response = handle_client_cmd(
                                            &args, client_id, &mut client_name, &mut no_evict, &mut no_touch,
                                            &client_registry, resp_version,
                                        );
                                        if write_resp(&mut writer, &response).await.is_err() { let _ = writer.flush().await; return; }
                                        if let Some(RespValue::SimpleString(ref s)) = Some(response.clone()) {
                                            let _ = s; // suppress warning
                                        }
                                        // Update registry name
                                        client_registry.update(client_id, |info| {
                                            info.name = client_name.clone();
                                            info.username = username.clone();
                                            info.resp_version = resp_version;
                                        });
                                        continue;
                                    }

                                    // ── Blocking list/zset commands ──────────────────────
                                    if cmd_upper == "BLPOP" || cmd_upper == "BRPOP"
                                        || cmd_upper == "BLMOVE" || cmd_upper == "BLMPOP"
                                        || cmd_upper == "BRPOPLPUSH"
                                        || cmd_upper == "BZPOPMIN" || cmd_upper == "BZPOPMAX"
                                        || cmd_upper == "BZMPOP"
                                    {
                                        // Flush pending responses before blocking
                                        let _ = writer.flush().await;
                                        let response = handle_blocking_cmd(
                                            &cmd_upper, &args, db_index, &db, shutdown_rx,
                                        ).await;
                                        if send_resp(&mut writer, &response).await.is_err() {
                                            return;
                                        }
                                        continue;
                                    }

                                    // ── MULTI/EXEC/DISCARD ───────────────────────────────
                                    let response = if cmd_upper == "MULTI" {
                                        if in_multi {
                                            RespValue::error("ERR MULTI calls can not be nested")
                                        } else {
                                            in_multi = true;
                                            queued_cmds.clear();
                                            RespValue::ok()
                                        }
                                    } else if cmd_upper == "DISCARD" {
                                        if !in_multi {
                                            RespValue::error("ERR DISCARD without MULTI")
                                        } else {
                                            in_multi = false;
                                            queued_cmds.clear();
                                            RespValue::ok()
                                        }
                                    } else if cmd_upper == "EXEC" {
                                        if !in_multi {
                                            RespValue::error("ERR EXEC without MULTI")
                                        } else {
                                            in_multi = false;
                                            let cmds = std::mem::take(&mut queued_cmds);
                                            let results: Vec<RespValue> = cmds
                                                .iter()
                                                .map(|c| registry.execute(&mut db_index, c))
                                                .collect();
                                            RespValue::Array(Some(results))
                                        }
                                    } else if in_multi {
                                        queued_cmds.push(args.clone());
                                        RespValue::simple("QUEUED")
                                    } else {
                                        registry.execute(&mut db_index, &args)
                                    };

                                    // ── Keyspace notifications ────────────────
                                    {
                                        let (event_name, cat) = cmd_event(&cmd_upper);
                                        if !event_name.is_empty() {
                                            let cfg = KS_NOTIFIER.cfg.read();
                                            if cfg.enabled && event_allowed(&cfg, cat) {
                                                drop(cfg);
                                                // MSET: notify each key
                                                if cmd_upper == "MSET" || cmd_upper == "MSETNX" {
                                                    let mut ki = 1usize;
                                                    while ki + 1 < args.len() {
                                                        if let Some(k) = args[ki].as_bytes() {
                                                            KS_NOTIFIER.notify(db_index, k, event_name);
                                                        }
                                                        ki += 2;
                                                    }
                                                } else if cmd_upper == "DEL" || cmd_upper == "UNLINK" {
                                                    for a in &args[1..] {
                                                        if let Some(k) = a.as_bytes() {
                                                            KS_NOTIFIER.notify(db_index, k, event_name);
                                                        }
                                                    }
                                                } else if cmd_upper == "RENAME" || cmd_upper == "RENAMENX" {
                                                    if let Some(src) = args.get(1).and_then(|a| a.as_bytes()) {
                                                        KS_NOTIFIER.notify(db_index, src, "rename_from");
                                                    }
                                                    if let Some(dst) = args.get(2).and_then(|a| a.as_bytes()) {
                                                        KS_NOTIFIER.notify(db_index, dst, "rename_to");
                                                    }
                                                } else if let Some(key) = args.get(1).and_then(|a| a.as_bytes()) {
                                                    KS_NOTIFIER.notify(db_index, key, event_name);
                                                }
                                            }
                                        }
                                    }

                                    // Write without flushing — flush once at end of batch
                                    if write_resp(&mut writer, &response).await.is_err() {
                                        let _ = writer.flush().await;
                                        return;
                                    }

                                    if cmd_upper == "QUIT" {
                                        let _ = writer.flush().await;
                                        return;
                                    }
                                }
                            }
                        }
                        // Flush all responses for this TCP read batch at once
                        if writer.flush().await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    let _ = writer.flush().await;
}

// ── Blocking list commands ─────────────────────────────────────────────────────

async fn handle_blocking_cmd(
    cmd: &str,
    args: &[RespValue],
    db_index: usize,
    db: &Arc<RedisDatabase>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> RespValue {
    use std::time::Duration;

    match cmd {
        "BLPOP" | "BRPOP" => {
            if args.len() < 3 {
                return RespValue::error(&format!("ERR wrong number of arguments for '{}' command", cmd.to_lowercase()));
            }
            let is_left = cmd == "BLPOP";
            let keys: Vec<Vec<u8>> = args[1..args.len() - 1]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            if keys.is_empty() {
                return RespValue::error("ERR syntax error");
            }
            let timeout_secs: f64 = args
                .last()
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            if timeout_secs < 0.0 {
                return RespValue::error("ERR timeout is negative");
            }

            // Helper: try popping from each key in order
            let try_pop = |keys: &[Vec<u8>]| -> Option<RespValue> {
                for key in keys {
                    let res = if is_left { db.lpop(db_index, key) } else { db.rpop(db_index, key) };
                    if let Ok(Some(val)) = res {
                        return Some(RespValue::Array(Some(vec![
                            RespValue::bulk_bytes(key.clone()),
                            RespValue::bulk_bytes(val),
                        ])));
                    }
                }
                None
            };

            // Immediate pop
            if let Some(r) = try_pop(&keys) {
                return r;
            }

            // Subscribe before double-check to avoid missing a push
            let mut sub = BLOCKING_NOTIFIER.subscribe();

            // Double-check after subscribe (race prevention)
            if let Some(r) = try_pop(&keys) {
                return r;
            }

            let use_timeout = timeout_secs > 0.0;
            let deadline = if use_timeout {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            } else {
                None
            };

            loop {
                let recv_fut = sub.recv();
                let notif_result = if let Some(dl) = deadline {
                    match tokio::time::timeout_at(dl, recv_fut).await {
                        Ok(v) => v,
                        Err(_) => return RespValue::null_array(), // timeout
                    }
                } else {
                    tokio::select! {
                        v = recv_fut => v,
                        _ = shutdown_rx.changed() => return RespValue::null_array(),
                    }
                };

                match notif_result {
                    Ok((notif_db, notif_key)) => {
                        if notif_db != db_index { continue; }
                        if !keys.contains(&notif_key) { continue; }
                        if let Some(r) = try_pop(&keys) {
                            return r;
                        }
                    }
                    Err(_) => {
                        // Broadcast lagged - resubscribe and re-check
                        sub = BLOCKING_NOTIFIER.subscribe();
                        if let Some(r) = try_pop(&keys) {
                            return r;
                        }
                    }
                }
            }
        }
        "BRPOPLPUSH" => {
            // BRPOPLPUSH src dst timeout
            if args.len() < 4 {
                return RespValue::error("ERR wrong number of arguments for 'brpoplpush' command");
            }
            let src = match args[1].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
            let dst = match args[2].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
            let timeout_secs: f64 = args[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);

            let try_move = || db.lmove(db_index, &src, &dst, false, true)
                .ok()
                .flatten()
                .map(|v| RespValue::bulk_bytes(v));

            if let Some(r) = try_move() { return r; }

            let mut sub = BLOCKING_NOTIFIER.subscribe();
            if let Some(r) = try_move() { return r; }

            let deadline = if timeout_secs > 0.0 {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            } else { None };

            loop {
                let notif = if let Some(dl) = deadline {
                    match tokio::time::timeout_at(dl, sub.recv()).await {
                        Ok(v) => v,
                        Err(_) => return RespValue::null_bulk(),
                    }
                } else {
                    tokio::select! {
                        v = sub.recv() => v,
                        _ = shutdown_rx.changed() => return RespValue::null_bulk(),
                    }
                };
                match notif {
                    Ok((notif_db, notif_key)) if notif_db == db_index && notif_key == src => {
                        if let Some(r) = try_move() { return r; }
                    }
                    Ok(_) => continue,
                    Err(_) => {
                        sub = BLOCKING_NOTIFIER.subscribe();
                        if let Some(r) = try_move() { return r; }
                    }
                }
            }
        }
        "BLMOVE" => {
            // BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout
            if args.len() < 6 {
                return RespValue::error("ERR wrong number of arguments for 'blmove' command");
            }
            let src = match args[1].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
            let dst = match args[2].as_bytes() { Some(k) => k.to_vec(), None => return RespValue::null_bulk() };
            let wherefrom = args[3].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let whereto   = args[4].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let left_from = wherefrom == "LEFT";
            let left_to   = whereto   == "LEFT";
            let timeout_secs: f64 = args[5].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);

            let try_move = || db.lmove(db_index, &src, &dst, left_from, left_to)
                .ok()
                .flatten()
                .map(|v| RespValue::bulk_bytes(v));

            if let Some(r) = try_move() { return r; }
            let mut sub = BLOCKING_NOTIFIER.subscribe();
            if let Some(r) = try_move() { return r; }

            let deadline = if timeout_secs > 0.0 {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            } else { None };

            loop {
                let notif = if let Some(dl) = deadline {
                    match tokio::time::timeout_at(dl, sub.recv()).await {
                        Ok(v) => v,
                        Err(_) => return RespValue::null_bulk(),
                    }
                } else {
                    tokio::select! {
                        v = sub.recv() => v,
                        _ = shutdown_rx.changed() => return RespValue::null_bulk(),
                    }
                };
                match notif {
                    Ok((notif_db, notif_key)) if notif_db == db_index && notif_key == src => {
                        if let Some(r) = try_move() { return r; }
                    }
                    Ok(_) => continue,
                    Err(_) => {
                        sub = BLOCKING_NOTIFIER.subscribe();
                        if let Some(r) = try_move() { return r; }
                    }
                }
            }
        }
        "BLMPOP" => {
            // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
            if args.len() < 5 {
                return RespValue::error("ERR wrong number of arguments for 'blmpop' command");
            }
            let timeout_secs: f64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR numkeys is not an integer"),
            };
            if args.len() < 3 + numkeys + 1 {
                return RespValue::error("ERR syntax error");
            }
            let keys: Vec<Vec<u8>> = args[3..3 + numkeys]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            let direction = args[3 + numkeys].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let is_left = direction == "LEFT";
            let count: usize = if args.len() > 3 + numkeys + 2 {
                if args[3 + numkeys + 1].as_str().map(|s| s.to_uppercase()) == Some("COUNT".to_string()) {
                    args[3 + numkeys + 2].as_str().and_then(|s| s.parse().ok()).unwrap_or(1)
                } else { 1 }
            } else { 1 };

            let try_pop_many = |keys: &[Vec<u8>]| -> Option<RespValue> {
                for key in keys {
                    let mut popped = Vec::new();
                    for _ in 0..count {
                        let res = if is_left { db.lpop(db_index, key) } else { db.rpop(db_index, key) };
                        match res {
                            Ok(Some(v)) => popped.push(RespValue::bulk_bytes(v)),
                            _ => break,
                        }
                    }
                    if !popped.is_empty() {
                        return Some(RespValue::Array(Some(vec![
                            RespValue::bulk_bytes(key.clone()),
                            RespValue::Array(Some(popped)),
                        ])));
                    }
                }
                None
            };

            if let Some(r) = try_pop_many(&keys) { return r; }
            let mut sub = BLOCKING_NOTIFIER.subscribe();
            if let Some(r) = try_pop_many(&keys) { return r; }

            let deadline = if timeout_secs > 0.0 {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            } else { None };

            loop {
                let notif = if let Some(dl) = deadline {
                    match tokio::time::timeout_at(dl, sub.recv()).await {
                        Ok(v) => v,
                        Err(_) => return RespValue::null_array(),
                    }
                } else {
                    tokio::select! {
                        v = sub.recv() => v,
                        _ = shutdown_rx.changed() => return RespValue::null_array(),
                    }
                };
                match notif {
                    Ok((notif_db, notif_key)) => {
                        if notif_db != db_index { continue; }
                        if !keys.contains(&notif_key) { continue; }
                        if let Some(r) = try_pop_many(&keys) { return r; }
                    }
                    Err(_) => {
                        sub = BLOCKING_NOTIFIER.subscribe();
                        if let Some(r) = try_pop_many(&keys) { return r; }
                    }
                }
            }
        }
        "BZPOPMIN" | "BZPOPMAX" => {
            // BZPOPMIN key [key ...] timeout
            if args.len() < 3 {
                return RespValue::error(&format!("ERR wrong number of arguments for '{}' command", cmd.to_lowercase()));
            }
            let is_min = cmd == "BZPOPMIN";
            let keys: Vec<Vec<u8>> = args[1..args.len() - 1]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            if keys.is_empty() {
                return RespValue::error("ERR syntax error");
            }
            let timeout_secs: f64 = args
                .last()
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            if timeout_secs < 0.0 {
                return RespValue::error("ERR timeout is negative");
            }

            let try_zpop = |keys: &[Vec<u8>]| -> Option<RespValue> {
                for key in keys {
                    let res = if is_min {
                        db.zpopmin(db_index, key, 1)
                    } else {
                        db.zpopmax(db_index, key, 1)
                    };
                    if let Ok(items) = res {
                        if !items.is_empty() {
                            let (member, score) = &items[0];
                            let score_str = if *score == f64::INFINITY {
                                "inf".to_string()
                            } else if *score == f64::NEG_INFINITY {
                                "-inf".to_string()
                            } else if *score == score.floor() && score.abs() < 1e15 {
                                format!("{}", *score as i64)
                            } else {
                                format!("{}", score)
                            };
                            return Some(RespValue::Array(Some(vec![
                                RespValue::bulk_bytes(key.clone()),
                                RespValue::bulk_bytes(member.clone()),
                                RespValue::bulk_str(&score_str),
                            ])));
                        }
                    }
                }
                None
            };

            // Immediate pop attempt
            if let Some(r) = try_zpop(&keys) {
                return r;
            }

            // Subscribe before double-check to avoid missing a ZADD
            let mut sub = BLOCKING_NOTIFIER.subscribe();

            // Double-check after subscribe (race prevention)
            if let Some(r) = try_zpop(&keys) {
                return r;
            }

            let deadline = if timeout_secs > 0.0 {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            } else {
                None
            };

            loop {
                let recv_fut = sub.recv();
                let notif_result = if let Some(dl) = deadline {
                    match tokio::time::timeout_at(dl, recv_fut).await {
                        Ok(v) => v,
                        Err(_) => return RespValue::null_array(), // timeout
                    }
                } else {
                    tokio::select! {
                        v = recv_fut => v,
                        _ = shutdown_rx.changed() => return RespValue::null_array(),
                    }
                };

                match notif_result {
                    Ok((notif_db, notif_key)) => {
                        if notif_db != db_index { continue; }
                        if !keys.contains(&notif_key) { continue; }
                        if let Some(r) = try_zpop(&keys) {
                            return r;
                        }
                    }
                    Err(_) => {
                        // Broadcast lagged - resubscribe and re-check
                        sub = BLOCKING_NOTIFIER.subscribe();
                        if let Some(r) = try_zpop(&keys) {
                            return r;
                        }
                    }
                }
            }
        }
        "BZMPOP" => {
            // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
            if args.len() < 5 {
                return RespValue::error("ERR wrong number of arguments for 'bzmpop' command");
            }
            let timeout_secs: f64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if timeout_secs < 0.0 {
                return RespValue::error("ERR timeout is negative");
            }
            let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR value is not an integer or out of range"),
            };
            if numkeys == 0 || args.len() < 3 + numkeys + 1 {
                return RespValue::error("ERR syntax error");
            }
            let keys: Vec<Vec<u8>> = args[3..3 + numkeys]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            let direction = args[3 + numkeys].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
            let pop_min = direction == "MIN";
            let count: i64 = {
                let rest = &args[4 + numkeys..];
                let mut c = 1i64;
                let mut i = 0;
                while i < rest.len() {
                    if rest[i].as_str().map(|s| s.to_uppercase() == "COUNT").unwrap_or(false) {
                        if i + 1 < rest.len() {
                            c = rest[i + 1].as_str().and_then(|s| s.parse().ok()).unwrap_or(1);
                        }
                    }
                    i += 1;
                }
                c
            };

            let build_result = |key: Vec<u8>, items: Vec<(Vec<u8>, f64)>| -> RespValue {
                let elements: Vec<RespValue> = items.into_iter().map(|(member, score)| {
                    let score_str = if score == f64::INFINITY {
                        "inf".to_string()
                    } else if score == f64::NEG_INFINITY {
                        "-inf".to_string()
                    } else if score == score.floor() && score.abs() < 1e15 {
                        format!("{}", score as i64)
                    } else {
                        format!("{}", score)
                    };
                    RespValue::Array(Some(vec![
                        RespValue::bulk_bytes(member),
                        RespValue::bulk_str(&score_str),
                    ]))
                }).collect();
                RespValue::Array(Some(vec![
                    RespValue::bulk_bytes(key),
                    RespValue::Array(Some(elements)),
                ]))
            };

            let try_zpop_multi = |keys: &[Vec<u8>]| -> Option<RespValue> {
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                match db.zpop_multi(db_index, &key_refs, pop_min, count) {
                    Ok(Some((key, items))) => Some(build_result(key, items)),
                    _ => None,
                }
            };

            if let Some(r) = try_zpop_multi(&keys) { return r; }

            let mut sub = BLOCKING_NOTIFIER.subscribe();
            if let Some(r) = try_zpop_multi(&keys) { return r; }

            let deadline = if timeout_secs > 0.0 {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            } else { None };

            loop {
                let notif = if let Some(dl) = deadline {
                    match tokio::time::timeout_at(dl, sub.recv()).await {
                        Ok(v) => v,
                        Err(_) => return RespValue::null_array(),
                    }
                } else {
                    tokio::select! {
                        v = sub.recv() => v,
                        _ = shutdown_rx.changed() => return RespValue::null_array(),
                    }
                };
                match notif {
                    Ok((notif_db, notif_key)) => {
                        if notif_db != db_index { continue; }
                        if !keys.contains(&notif_key) { continue; }
                        if let Some(r) = try_zpop_multi(&keys) { return r; }
                    }
                    Err(_) => {
                        sub = BLOCKING_NOTIFIER.subscribe();
                        if let Some(r) = try_zpop_multi(&keys) { return r; }
                    }
                }
            }
        }
        _ => RespValue::error(&format!("ERR unknown blocking command '{}'", cmd)),
    }
}

fn handle_auth(
    args: &[RespValue],
    acl: &Arc<AclManager>,
    requirepass: &str,
    authenticated: &mut bool,
    username: &mut String,
) -> RespValue {
    // AUTH password  OR  AUTH username password
    match args.len() {
        2 => {
            // AUTH password (use "default" user)
            let password = match args[1].as_str() {
                Some(s) => s.to_string(),
                None => return RespValue::error("ERR invalid password"),
            };
            if requirepass.is_empty() {
                // No password set — AUTH with any password succeeds but warns
                *authenticated = true;
                return RespValue::ok();
            }
            match acl.authenticate("default", &password) {
                Ok(u) => {
                    *authenticated = true;
                    *username = u;
                    RespValue::ok()
                }
                Err(e) => {
                    *authenticated = false;
                    RespValue::error(&e)
                }
            }
        }
        3 => {
            // AUTH username password
            let user = match args[1].as_str() {
                Some(s) => s.to_string(),
                None => return RespValue::error("ERR invalid username"),
            };
            let password = match args[2].as_str() {
                Some(s) => s.to_string(),
                None => return RespValue::error("ERR invalid password"),
            };
            match acl.authenticate(&user, &password) {
                Ok(u) => {
                    *authenticated = true;
                    *username = u;
                    RespValue::ok()
                }
                Err(e) => {
                    *authenticated = false;
                    RespValue::error(&e)
                }
            }
        }
        _ => RespValue::error("ERR wrong number of arguments for 'auth' command"),
    }
}

fn handle_hello(
    args: &[RespValue],
    acl: &Arc<AclManager>,
    requirepass: &str,
    authenticated: &mut bool,
    username: &mut String,
    resp_version: &mut u8,
    _client_name: &str,
    client_id: u64,
    _db_index: usize,
) -> RespValue {
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    let mut new_version = *resp_version;

    if args.len() >= 2 {
        let ver: u8 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(*resp_version);
        if ver != 2 && ver != 3 {
            return RespValue::error("NOPROTO unsupported protocol version");
        }
        new_version = ver;
    }

    // Process optional AUTH and SETNAME
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
        match opt.as_str() {
            "AUTH" => {
                if i + 2 >= args.len() {
                    return RespValue::error("ERR Syntax error in HELLO option 'auth'");
                }
                let user = args[i + 1].as_str().unwrap_or("default").to_string();
                let pass = args[i + 2].as_str().unwrap_or("").to_string();
                match acl.authenticate(&user, &pass) {
                    Ok(u) => {
                        *authenticated = true;
                        *username = u;
                    }
                    Err(e) => return RespValue::error(&e),
                }
                i += 3;
            }
            "SETNAME" => {
                // Name handled outside, skip
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    if !requirepass.is_empty() && !*authenticated {
        return RespValue::error("NOAUTH Authentication required.");
    }

    *resp_version = new_version;

    // Return server info as a map (flat array of key-value pairs)
    let entries: Vec<RespValue> = vec![
        RespValue::bulk_str("server"),
        RespValue::bulk_str("redis"),
        RespValue::bulk_str("version"),
        RespValue::bulk_str("7.2.0"),
        RespValue::bulk_str("proto"),
        RespValue::integer(*resp_version as i64),
        RespValue::bulk_str("id"),
        RespValue::integer(client_id as i64),
        RespValue::bulk_str("mode"),
        RespValue::bulk_str("standalone"),
        RespValue::bulk_str("role"),
        RespValue::bulk_str("master"),
        RespValue::bulk_str("modules"),
        RespValue::Array(Some(vec![])),
    ];
    RespValue::Array(Some(entries))
}

fn handle_client_cmd(
    args: &[RespValue],
    client_id: u64,
    client_name: &mut String,
    no_evict: &mut bool,
    no_touch: &mut bool,
    client_registry: &Arc<ClientRegistry>,
    _resp_version: u8,
) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'client' command");
    }
    let sub = args[1].as_str().map(|s| s.to_uppercase()).unwrap_or_default();
    match sub.as_str() {
        "SETNAME" => {
            if args.len() < 3 {
                return RespValue::error("ERR wrong number of arguments for 'client|setname' command");
            }
            let name = args[2].as_str().unwrap_or("");
            // Validate: no spaces or special chars
            if name.chars().any(|c| c == ' ' || c < ' ') {
                return RespValue::error("ERR Client names cannot contain spaces, newlines or special characters.");
            }
            *client_name = name.to_string();
            RespValue::ok()
        }
        "GETNAME" => {
            if client_name.is_empty() {
                RespValue::null_bulk()
            } else {
                RespValue::bulk_str(client_name)
            }
        }
        "ID" => RespValue::integer(client_id as i64),
        "LIST" => {
            let all = client_registry.list();
            let mut output = String::new();
            for c in &all {
                output.push_str(&c.to_list_entry());
            }
            RespValue::bulk_str(&output)
        }
        "INFO" => {
            // Return info about this specific client
            if let Some(info) = client_registry.get(client_id) {
                RespValue::bulk_str(&info.to_list_entry())
            } else {
                RespValue::bulk_str("")
            }
        }
        "KILL" => {
            if args.len() < 3 {
                return RespValue::error("ERR wrong number of arguments for 'client|kill' command");
            }
            // CLIENT KILL addr  OR  CLIENT KILL [ID id] [ADDR addr] [LADDR laddr] [USER user] [SKIPME yes/no]
            let first_arg = args[2].as_str().unwrap_or("").to_uppercase();
            if first_arg == "ID" && args.len() >= 4 {
                let kill_id: u64 = args[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
                if client_registry.kill_by_id(kill_id) {
                    RespValue::integer(1)
                } else {
                    RespValue::integer(0)
                }
            } else if first_arg == "ADDR" && args.len() >= 4 {
                let addr = args[3].as_str().unwrap_or("");
                if client_registry.kill_by_addr(addr) {
                    RespValue::integer(1)
                } else {
                    RespValue::integer(0)
                }
            } else {
                // Legacy: CLIENT KILL addr:port
                let addr = args[2].as_str().unwrap_or("");
                if client_registry.kill_by_addr(addr) {
                    RespValue::ok()
                } else {
                    RespValue::error("ERR No such client")
                }
            }
        }
        "NO-EVICT" => {
            if args.len() >= 3 {
                *no_evict = args[2].as_str().map(|s| s.to_lowercase() == "on").unwrap_or(false);
            }
            RespValue::ok()
        }
        "NO-TOUCH" => {
            if args.len() >= 3 {
                *no_touch = args[2].as_str().map(|s| s.to_lowercase() == "on").unwrap_or(false);
            }
            RespValue::ok()
        }
        "REPLY" => {
            // CLIENT REPLY on/off/skip — just ack
            RespValue::ok()
        }
        "CACHING" => {
            // CLIENT CACHING yes/no — client-side tracking stub
            RespValue::ok()
        }
        "TRACKING" => {
            // CLIENT TRACKING on/off [REDIRECT id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
            RespValue::ok()
        }
        "TRACKINGINFO" => {
            RespValue::Array(Some(vec![
                RespValue::bulk_str("flags"),
                RespValue::Array(Some(vec![RespValue::bulk_str("off")])),
                RespValue::bulk_str("redirect"),
                RespValue::integer(-1),
                RespValue::bulk_str("prefixes"),
                RespValue::Array(Some(vec![])),
            ]))
        }
        "UNPAUSE" => RespValue::ok(),
        "PAUSE" => {
            // CLIENT PAUSE timeout [WRITE|ALL]
            RespValue::ok()
        }
        "GETREDIR" => RespValue::integer(-1),
        "MYID" => RespValue::integer(client_id as i64),
        _ => RespValue::error(&format!(
            "ERR unknown subcommand or wrong number of arguments for '{}' command",
            sub.to_lowercase()
        )),
    }
}

async fn send_resp(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    value: &RespValue,
) -> std::io::Result<()> {
    let bytes = RespWriter::write(value);
    writer.write_all(&bytes).await?;
    writer.flush().await
}

/// Write without flushing (for pipelined batches — caller must flush when batch is done).
async fn write_resp(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    value: &RespValue,
) -> std::io::Result<()> {
    let bytes = RespWriter::write(value);
    writer.write_all(&bytes).await
}

/// Subscriber-mode connection loop.
async fn handle_subscriber_mode(
    initial_args: &[RespValue],
    initial_is_pattern: bool,
    reader: &mut tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    parser: &mut RespParser,
    hub: &Arc<PubSubHub>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let (agg_tx, mut agg_rx) = tokio::sync::mpsc::channel::<AggMsg>(256);

    let mut channel_tasks: HashMap<String, tokio::task::AbortHandle> = HashMap::new();
    let mut pattern_tasks: HashMap<String, tokio::task::AbortHandle> = HashMap::new();

    if !subscribe_channels(
        initial_args, initial_is_pattern, hub, &agg_tx,
        &mut channel_tasks, &mut pattern_tasks, writer,
    ).await {
        abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
        return;
    }
    if writer.flush().await.is_err() {
        abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
        return;
    }

    let mut buf = vec![0u8; 4096];

    loop {
        tokio::select! {
            result = reader.read(&mut buf) => {
                match result {
                    Ok(0) | Err(_) => {
                        abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                        return;
                    }
                    Ok(n) => {
                        parser.feed(&buf[..n]);
                        loop {
                            match parser.try_parse() {
                                None => break,
                                Some(value) => {
                                    let args = match value {
                                        RespValue::Array(Some(items)) => items,
                                        other => vec![other],
                                    };
                                    if args.is_empty() { continue; }
                                    let cmd = args[0].as_str()
                                        .map(|s| s.to_uppercase())
                                        .unwrap_or_default();

                                    match cmd.as_str() {
                                        "SUBSCRIBE" => {
                                            if !subscribe_channels(
                                                &args, false, hub, &agg_tx,
                                                &mut channel_tasks, &mut pattern_tasks, writer,
                                            ).await {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                            if writer.flush().await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                        }
                                        "PSUBSCRIBE" => {
                                            if !subscribe_channels(
                                                &args, true, hub, &agg_tx,
                                                &mut channel_tasks, &mut pattern_tasks, writer,
                                            ).await {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                            if writer.flush().await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                        }
                                        "UNSUBSCRIBE" => {
                                            let total = unsubscribe_channels(
                                                &args, false,
                                                &mut channel_tasks, &mut pattern_tasks, writer,
                                            ).await;
                                            if writer.flush().await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                            if total == 0 {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                        }
                                        "PUNSUBSCRIBE" => {
                                            let total = unsubscribe_channels(
                                                &args, true,
                                                &mut channel_tasks, &mut pattern_tasks, writer,
                                            ).await;
                                            if writer.flush().await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                            if total == 0 {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                        }
                                        "PING" => {
                                            let msg = if args.len() > 1 {
                                                args[1].clone()
                                            } else {
                                                RespValue::bulk_str("")
                                            };
                                            let pong = RespValue::Array(Some(vec![
                                                RespValue::bulk_str("pong"),
                                                msg,
                                            ]));
                                            let bytes = RespWriter::write(&pong);
                                            if writer.write_all(&bytes).await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                            if writer.flush().await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                        }
                                        "RESET" | "QUIT" => {
                                            abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                            return;
                                        }
                                        _ => {
                                            let err = RespValue::error(
                                                "ERR Command not allowed inside a subscription context"
                                            );
                                            let bytes = RespWriter::write(&err);
                                            if writer.write_all(&bytes).await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                            if writer.flush().await.is_err() {
                                                abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Some(msg) = agg_rx.recv() => {
                let resp = match msg {
                    AggMsg::Channel(m) => RespValue::Array(Some(vec![
                        RespValue::bulk_str("message"),
                        RespValue::bulk_bytes(m.channel.into_bytes()),
                        RespValue::bulk_bytes(m.data),
                    ])),
                    AggMsg::Pattern { pattern, msg: m } => RespValue::Array(Some(vec![
                        RespValue::bulk_str("pmessage"),
                        RespValue::bulk_bytes(pattern.into_bytes()),
                        RespValue::bulk_bytes(m.channel.into_bytes()),
                        RespValue::bulk_bytes(m.data),
                    ])),
                };
                let bytes = RespWriter::write(&resp);
                if writer.write_all(&bytes).await.is_err() {
                    abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                    return;
                }
                if writer.flush().await.is_err() {
                    abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                    return;
                }
            }

            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    abort_all_tasks(&mut channel_tasks, &mut pattern_tasks);
                    return;
                }
            }
        }
    }
}

fn abort_all_tasks(
    channel_tasks: &mut HashMap<String, tokio::task::AbortHandle>,
    pattern_tasks: &mut HashMap<String, tokio::task::AbortHandle>,
) {
    for (_, handle) in channel_tasks.drain() {
        handle.abort();
    }
    for (_, handle) in pattern_tasks.drain() {
        handle.abort();
    }
}

async fn subscribe_channels(
    args: &[RespValue],
    is_pattern: bool,
    hub: &Arc<PubSubHub>,
    agg_tx: &tokio::sync::mpsc::Sender<AggMsg>,
    channel_tasks: &mut HashMap<String, tokio::task::AbortHandle>,
    pattern_tasks: &mut HashMap<String, tokio::task::AbortHandle>,
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
) -> bool {
    for i in 1..args.len() {
        let name = match args[i].as_bytes().and_then(|b| String::from_utf8(b.to_vec()).ok()) {
            Some(s) => s,
            None => continue,
        };

        if is_pattern {
            if !pattern_tasks.contains_key(&name) {
                let mut rx = hub.psubscribe(&name);
                let tx = agg_tx.clone();
                let pattern_clone = name.clone();
                let handle = tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(msg) => {
                                if tx.send(AggMsg::Pattern { pattern: pattern_clone.clone(), msg }).await.is_err() {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(_) => break,
                        }
                    }
                }).abort_handle();
                pattern_tasks.insert(name.clone(), handle);
            }
        } else if !channel_tasks.contains_key(&name) {
            let mut rx = hub.subscribe(&name);
            let tx = agg_tx.clone();
            let handle = tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            if tx.send(AggMsg::Channel(msg)).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
            }).abort_handle();
            channel_tasks.insert(name.clone(), handle);
        }

        let total = (channel_tasks.len() + pattern_tasks.len()) as i64;
        let kind = if is_pattern { "psubscribe" } else { "subscribe" };
        let conf = RespValue::Array(Some(vec![
            RespValue::bulk_str(kind),
            RespValue::bulk_bytes(name.into_bytes()),
            RespValue::integer(total),
        ]));
        if writer.write_all(&RespWriter::write(&conf)).await.is_err() {
            return false;
        }
    }
    true
}

async fn unsubscribe_channels(
    args: &[RespValue],
    is_pattern: bool,
    channel_tasks: &mut HashMap<String, tokio::task::AbortHandle>,
    pattern_tasks: &mut HashMap<String, tokio::task::AbortHandle>,
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
) -> usize {
    let names: Vec<String> = if args.len() <= 1 {
        if is_pattern {
            pattern_tasks.keys().cloned().collect()
        } else {
            channel_tasks.keys().cloned().collect()
        }
    } else {
        args[1..]
            .iter()
            .filter_map(|a| a.as_bytes().and_then(|b| String::from_utf8(b.to_vec()).ok()))
            .collect()
    };

    if names.is_empty() {
        let kind = if is_pattern { "punsubscribe" } else { "unsubscribe" };
        let resp = RespValue::Array(Some(vec![
            RespValue::bulk_str(kind),
            RespValue::null_bulk(),
            RespValue::integer(0),
        ]));
        let _ = writer.write_all(&RespWriter::write(&resp)).await;
        return 0;
    }

    for name in names {
        let tasks = if is_pattern { &mut *pattern_tasks } else { &mut *channel_tasks };
        if let Some(handle) = tasks.remove(&name) {
            handle.abort();
        }
        let total = (channel_tasks.len() + pattern_tasks.len()) as i64;
        let kind = if is_pattern { "punsubscribe" } else { "unsubscribe" };
        let conf = RespValue::Array(Some(vec![
            RespValue::bulk_str(kind),
            RespValue::bulk_bytes(name.into_bytes()),
            RespValue::integer(total),
        ]));
        if writer.write_all(&RespWriter::write(&conf)).await.is_err() {
            return 0;
        }
    }

    channel_tasks.len() + pattern_tasks.len()
}
