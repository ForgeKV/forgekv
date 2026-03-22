#![allow(dead_code)]

mod acl;
mod blocking;
mod commands;
mod config;
mod ext_type_registry;
mod keyspace;
mod lua_engine;
mod database;
mod pubsub;
mod resp;
mod server;
mod storage;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use commands::server_cmds::*;
use commands::CommandRegistry;
use config::ServerConfig;
use database::RedisDatabase;
use pubsub::PubSubHub;
use server::TcpServer;
use storage::LsmStorage;
use acl::AclManager;
use parking_lot::RwLock;

pub struct ServerInfoData {
    pub num_dbs: usize,
    pub start_time: std::time::Instant,
    pub port: u16,
    pub total_connections: Arc<AtomicU64>,
    pub total_commands: Arc<AtomicU64>,
}

fn register_all(
    registry: Arc<CommandRegistry>,
    db: Arc<RedisDatabase>,
    hub: Arc<PubSubHub>,
    config: Arc<RwLock<ServerConfig>>,
    acl: Arc<AclManager>,
    tcp_server: &TcpServer,
) {
    use commands::bf_cmds::*;
    use commands::cf_cmds::*;
    use commands::geo_cmds::*;
    use commands::json_cmds::*;
    use commands::probabilistic_cmds::*;
    use commands::search_cmds::*;
    use commands::stream_cmds::*;
    use commands::tdigest_cmds::*;
    use commands::hash_cmds::*;
    use commands::hll_cmds::*;
    use commands::key_cmds::*;
    use commands::list_cmds::*;
    use commands::set_cmds::*;
    use commands::string_cmds::*;
    use commands::zset_cmds::*;

    let cfg_snap = config.read().clone();
    let requirepass_snap = cfg_snap.requirepass.clone();

    let server_info = Arc::new(ServerInfo {
        num_dbs: db.num_dbs,
        start_time: tcp_server.start_time,
        port: tcp_server.port,
        total_connections: tcp_server.total_connections.clone(),
        total_commands: tcp_server.total_commands.clone(),
    });

    let slowlog_store = Arc::new(SlowlogStore::new(
        cfg_snap.slowlog_max_len as usize,
        cfg_snap.slowlog_log_slower_than,
    ));
    let latency_store = Arc::new(LatencyStore::new());

    // ── String commands ──────────────────────────────────────────────────────
    registry.register(Arc::new(SetCommandV2 { db: db.clone() }));
    registry.register(Arc::new(GetCommand { db: db.clone() }));
    registry.register(Arc::new(IncrCommand { db: db.clone() }));
    registry.register(Arc::new(DecrCommand { db: db.clone() }));
    registry.register(Arc::new(IncrByCommand { db: db.clone() }));
    registry.register(Arc::new(DecrByCommand { db: db.clone() }));
    registry.register(Arc::new(AppendCommand { db: db.clone() }));
    registry.register(Arc::new(MGetCommand { db: db.clone() }));
    registry.register(Arc::new(MSetCommand { db: db.clone() }));
    registry.register(Arc::new(GetSetCommand { db: db.clone() }));
    registry.register(Arc::new(SetNxCommand { db: db.clone() }));
    registry.register(Arc::new(SetExCommand { db: db.clone() }));
    registry.register(Arc::new(PSetExCommand { db: db.clone() }));
    registry.register(Arc::new(IncrByFloatCommand { db: db.clone() }));
    registry.register(Arc::new(StrLenCommand { db: db.clone() }));
    registry.register(Arc::new(GetRangeCommand { db: db.clone() }));
    registry.register(Arc::new(SubStrCommand { db: db.clone() }));
    registry.register(Arc::new(SetRangeCommand { db: db.clone() }));
    registry.register(Arc::new(GetDelCommand { db: db.clone() }));
    registry.register(Arc::new(GetExCommand { db: db.clone() }));
    registry.register(Arc::new(MSetNxCommand { db: db.clone() }));
    registry.register(Arc::new(GetBitCommand { db: db.clone() }));
    registry.register(Arc::new(SetBitCommand { db: db.clone() }));
    registry.register(Arc::new(BitCountCommand { db: db.clone() }));
    registry.register(Arc::new(BitOpCommand { db: db.clone() }));
    registry.register(Arc::new(BitPosCommand { db: db.clone() }));
    registry.register(Arc::new(BitFieldCommand { db: db.clone() }));
    registry.register(Arc::new(BitFieldRoCommand { db: db.clone() }));
    registry.register(Arc::new(LcsCommand { db: db.clone() }));

    // ── Key commands ──────────────────────────────────────────────────────────
    registry.register(Arc::new(DelCommand { db: db.clone() }));
    registry.register(Arc::new(UnlinkCommand { db: db.clone() }));
    registry.register(Arc::new(ExistsCommand { db: db.clone() }));
    registry.register(Arc::new(ExpireCommand { db: db.clone() }));
    registry.register(Arc::new(PExpireCommand { db: db.clone() }));
    registry.register(Arc::new(ExpireAtCommand { db: db.clone() }));
    registry.register(Arc::new(PExpireAtCommand { db: db.clone() }));
    registry.register(Arc::new(ExpiretimeCommand { db: db.clone() }));
    registry.register(Arc::new(PExpiretimeCommand { db: db.clone() }));
    registry.register(Arc::new(TtlCommand { db: db.clone() }));
    registry.register(Arc::new(PttlCommand { db: db.clone() }));
    registry.register(Arc::new(KeysCommand { db: db.clone() }));
    registry.register(Arc::new(ScanCommandV2 { db: db.clone() }));
    registry.register(Arc::new(TypeCommand { db: db.clone() }));
    registry.register(Arc::new(PersistCommand { db: db.clone() }));
    registry.register(Arc::new(RenameCommand { db: db.clone() }));
    registry.register(Arc::new(RenameNxCommand { db: db.clone() }));
    registry.register(Arc::new(RandomKeyCommand { db: db.clone() }));
    registry.register(Arc::new(TouchCommand { db: db.clone() }));
    registry.register(Arc::new(MoveCommand { db: db.clone() }));
    registry.register(Arc::new(CopyCommand { db: db.clone() }));
    registry.register(Arc::new(SortCommand { db: db.clone() }));
    registry.register(Arc::new(SortRoCommand { db: db.clone() }));
    registry.register(Arc::new(DumpCommand { db: db.clone() }));
    registry.register(Arc::new(RestoreCommand { db: db.clone() }));
    // ── Hash commands ─────────────────────────────────────────────────────────
    registry.register(Arc::new(HSetCommand { db: db.clone() }));
    registry.register(Arc::new(HGetCommand { db: db.clone() }));
    registry.register(Arc::new(HDelCommand { db: db.clone() }));
    registry.register(Arc::new(HGetAllCommand { db: db.clone() }));
    registry.register(Arc::new(HMSetCommand { db: db.clone() }));
    registry.register(Arc::new(HMGetCommand { db: db.clone() }));
    registry.register(Arc::new(HExistsCommand { db: db.clone() }));
    registry.register(Arc::new(HLenCommand { db: db.clone() }));
    registry.register(Arc::new(HKeysCommand { db: db.clone() }));
    registry.register(Arc::new(HValsCommand { db: db.clone() }));
    registry.register(Arc::new(HIncrByCommand { db: db.clone() }));
    registry.register(Arc::new(HSetNxCommand { db: db.clone() }));
    registry.register(Arc::new(HIncrByFloatCommand { db: db.clone() }));
    registry.register(Arc::new(HStrLenCommand { db: db.clone() }));
    registry.register(Arc::new(HRandFieldCommand { db: db.clone() }));
    registry.register(Arc::new(HScanCommand { db: db.clone() }));
    // Hash field TTL (Redis 7.4+)
    registry.register(Arc::new(HExpireCommand { db: db.clone(), name: "HEXPIRE", in_ms: false, is_at: false }));
    registry.register(Arc::new(HExpireCommand { db: db.clone(), name: "HPEXPIRE", in_ms: true, is_at: false }));
    registry.register(Arc::new(HExpireCommand { db: db.clone(), name: "HEXPIREAT", in_ms: false, is_at: true }));
    registry.register(Arc::new(HExpireCommand { db: db.clone(), name: "HPEXPIREAT", in_ms: true, is_at: true }));
    registry.register(Arc::new(HTtlCommand { db: db.clone(), name: "HTTL", in_ms: false }));
    registry.register(Arc::new(HTtlCommand { db: db.clone(), name: "HPTTL", in_ms: true }));
    registry.register(Arc::new(HPersistCommand { db: db.clone() }));
    registry.register(Arc::new(HExpireTimeCommand { db: db.clone(), name: "HEXPIRETIME", in_ms: false }));
    registry.register(Arc::new(HExpireTimeCommand { db: db.clone(), name: "HPEXPIRETIME", in_ms: true }));

    // ── List commands ─────────────────────────────────────────────────────────
    registry.register(Arc::new(LPushCommand { db: db.clone() }));
    registry.register(Arc::new(RPushCommand { db: db.clone() }));
    registry.register(Arc::new(LPushXCommand { db: db.clone() }));
    registry.register(Arc::new(RPushXCommand { db: db.clone() }));
    registry.register(Arc::new(LPopCommand { db: db.clone() }));
    registry.register(Arc::new(RPopCommand { db: db.clone() }));
    registry.register(Arc::new(LRangeCommand { db: db.clone() }));
    registry.register(Arc::new(LLenCommand { db: db.clone() }));
    registry.register(Arc::new(LIndexCommand { db: db.clone() }));
    registry.register(Arc::new(LInsertCommand { db: db.clone() }));
    registry.register(Arc::new(LSetCommand { db: db.clone() }));
    registry.register(Arc::new(LRemCommand { db: db.clone() }));
    registry.register(Arc::new(LTrimCommand { db: db.clone() }));
    registry.register(Arc::new(LMoveCommand { db: db.clone() }));
    registry.register(Arc::new(RPoplPushCommand { db: db.clone() }));
    registry.register(Arc::new(LPosCommand { db: db.clone() }));
    registry.register(Arc::new(BlpopCommand { db: db.clone() }));
    registry.register(Arc::new(BrpopCommand { db: db.clone() }));
    registry.register(Arc::new(BlmoveCommand { db: db.clone() }));
    registry.register(Arc::new(BrpoplpushCommand { db: db.clone() }));
    registry.register(Arc::new(LmpopCommand { db: db.clone() }));
    registry.register(Arc::new(BlmpopCommand { db: db.clone() }));

    // ── Set commands ──────────────────────────────────────────────────────────
    registry.register(Arc::new(SAddCommand { db: db.clone() }));
    registry.register(Arc::new(SRemCommand { db: db.clone() }));
    registry.register(Arc::new(SMembersCommand { db: db.clone() }));
    registry.register(Arc::new(SIsMemberCommand { db: db.clone() }));
    registry.register(Arc::new(SCardCommand { db: db.clone() }));
    registry.register(Arc::new(SMIsMemberCommand { db: db.clone() }));
    registry.register(Arc::new(SDiffCommand { db: db.clone() }));
    registry.register(Arc::new(SDiffStoreCommand { db: db.clone() }));
    registry.register(Arc::new(SInterCommand { db: db.clone() }));
    registry.register(Arc::new(SInterStoreCommand { db: db.clone() }));
    registry.register(Arc::new(SUnionCommand { db: db.clone() }));
    registry.register(Arc::new(SUnionStoreCommand { db: db.clone() }));
    registry.register(Arc::new(SMoveCommand { db: db.clone() }));
    registry.register(Arc::new(SPopCommand { db: db.clone() }));
    registry.register(Arc::new(SRandMemberCommand { db: db.clone() }));
    registry.register(Arc::new(SScanCommand { db: db.clone() }));
    registry.register(Arc::new(SInterCardCommand { db: db.clone() }));

    // ── Sorted set commands ───────────────────────────────────────────────────
    registry.register(Arc::new(ZAddCommandV2 { db: db.clone() }));
    registry.register(Arc::new(ZRemCommand { db: db.clone() }));
    registry.register(Arc::new(ZRangeCommand { db: db.clone() }));
    registry.register(Arc::new(ZRangeWithScoresCommand { db: db.clone() }));
    registry.register(Arc::new(ZScoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZCardCommand { db: db.clone() }));
    registry.register(Arc::new(ZRankCommand { db: db.clone() }));
    registry.register(Arc::new(ZRevRangeCommand { db: db.clone() }));
    registry.register(Arc::new(ZRevRankCommand { db: db.clone() }));
    registry.register(Arc::new(ZIncrByCommand { db: db.clone() }));
    registry.register(Arc::new(ZCountCommand { db: db.clone() }));
    registry.register(Arc::new(ZLexCountCommand { db: db.clone() }));
    registry.register(Arc::new(ZRangeByScoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZRangeByLexCommand { db: db.clone() }));
    registry.register(Arc::new(ZRevRangeByScoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZRevRangeByLexCommand { db: db.clone() }));
    registry.register(Arc::new(ZPopMaxCommand { db: db.clone() }));
    registry.register(Arc::new(ZPopMinCommand { db: db.clone() }));
    registry.register(Arc::new(ZRemRangeByScoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZRemRangeByLexCommand { db: db.clone() }));
    registry.register(Arc::new(ZRemRangeByRankCommand { db: db.clone() }));
    registry.register(Arc::new(ZMScoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZDiffCommand { db: db.clone() }));
    registry.register(Arc::new(ZDiffStoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZInterCommand { db: db.clone() }));
    registry.register(Arc::new(ZInterStoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZUnionCommand { db: db.clone() }));
    registry.register(Arc::new(ZUnionStoreCommand { db: db.clone() }));
    registry.register(Arc::new(ZRandMemberCommand { db: db.clone() }));
    registry.register(Arc::new(ZScanCommand { db: db.clone() }));
    registry.register(Arc::new(ZRangeStoreCommand { db: db.clone() }));
    registry.register(Arc::new(BzpopmaxCommand { db: db.clone() }));
    registry.register(Arc::new(BzpopminCommand { db: db.clone() }));
    registry.register(Arc::new(ZInterCardCommand { db: db.clone() }));
    registry.register(Arc::new(ZmpopCommand { db: db.clone() }));
    registry.register(Arc::new(BzmpopCommand { db: db.clone() }));

    // ── Stream commands ───────────────────────────────────────────────────────
    registry.register(Arc::new(XAddCommand));
    registry.register(Arc::new(XLenCommand));
    registry.register(Arc::new(XRangeCommand));
    registry.register(Arc::new(XRevRangeCommand));
    registry.register(Arc::new(XDelCommand));
    registry.register(Arc::new(XTrimCommand));
    registry.register(Arc::new(XReadCommand));
    registry.register(Arc::new(XGroupCommand));
    registry.register(Arc::new(XReadGroupCommand));
    registry.register(Arc::new(XAckCommand));
    registry.register(Arc::new(XClaimCommand));
    registry.register(Arc::new(XPendingCommand));
    registry.register(Arc::new(XInfoCommand));
    registry.register(Arc::new(XAutoClaimCommand));
    registry.register(Arc::new(XSetIdCommand));

    // ── Geo commands ──────────────────────────────────────────────────────────
    registry.register(Arc::new(GeoAddCommand { db: db.clone() }));
    registry.register(Arc::new(GeoDistCommand { db: db.clone() }));
    registry.register(Arc::new(GeoPosCommand { db: db.clone() }));
    registry.register(Arc::new(GeoHashCommand { db: db.clone() }));
    registry.register(Arc::new(GeoRadiusCommand { db: db.clone(), read_only: false }));
    registry.register(Arc::new(GeoRadiusCommand { db: db.clone(), read_only: true }));
    registry.register(Arc::new(GeoRadiusByMemberCommand { db: db.clone(), read_only: false }));
    registry.register(Arc::new(GeoRadiusByMemberCommand { db: db.clone(), read_only: true }));
    registry.register(Arc::new(GeoSearchCommand { db: db.clone() }));
    registry.register(Arc::new(GeoSearchStoreCommand { db: db.clone() }));

    // ── HyperLogLog commands ──────────────────────────────────────────────────
    registry.register(Arc::new(PfAddCommand { db: db.clone() }));
    registry.register(Arc::new(PfCountCommand { db: db.clone() }));
    registry.register(Arc::new(PfMergeCommand { db: db.clone() }));

    // ── Cuckoo Filter commands ────────────────────────────────────────────────
    registry.register(Arc::new(CfReserveCommand));
    registry.register(Arc::new(CfAddCommand));
    registry.register(Arc::new(CfAddNxCommand));
    registry.register(Arc::new(CfInsertCommand { nx: false }));
    registry.register(Arc::new(CfInsertCommand { nx: true }));
    registry.register(Arc::new(CfExistsCommand));
    registry.register(Arc::new(CfMExistsCommand));
    registry.register(Arc::new(CfDelCommand));
    registry.register(Arc::new(CfCountCommand));
    registry.register(Arc::new(CfInfoCommand));
    registry.register(Arc::new(CfCompactCommand));
    registry.register(Arc::new(CfScanDumpCommand));
    registry.register(Arc::new(CfLoadChunkCommand));

    // ── Bloom Filter commands ─────────────────────────────────────────────────
    registry.register(Arc::new(BfReserveCommand));
    registry.register(Arc::new(BfAddCommand));
    registry.register(Arc::new(BfMAddCommand));
    registry.register(Arc::new(BfExistsCommand));
    registry.register(Arc::new(BfMExistsCommand));
    registry.register(Arc::new(BfCardCommand));
    registry.register(Arc::new(BfInfoCommand));
    registry.register(Arc::new(BfInsertCommand));
    registry.register(Arc::new(BfScanDumpCommand));
    registry.register(Arc::new(BfLoadChunkCommand));

    // ── JSON commands ─────────────────────────────────────────────────────────
    registry.register(Arc::new(JsonSetCommand));
    registry.register(Arc::new(JsonGetCommand));
    registry.register(Arc::new(JsonMGetCommand));
    registry.register(Arc::new(JsonDelCommand { name: "JSON.DEL" }));
    registry.register(Arc::new(JsonDelCommand { name: "JSON.FORGET" }));
    registry.register(Arc::new(JsonTypeCommand));
    registry.register(Arc::new(JsonNumIncrByCommand));
    registry.register(Arc::new(JsonNumMultByCommand));
    registry.register(Arc::new(JsonStrLenCommand));
    registry.register(Arc::new(JsonArrLenCommand));
    registry.register(Arc::new(JsonArrAppendCommand));
    registry.register(Arc::new(JsonObjKeysCommand));
    registry.register(Arc::new(JsonObjLenCommand));
    registry.register(Arc::new(JsonClearCommand));
    registry.register(Arc::new(JsonToggleCommand));
    registry.register(Arc::new(JsonRespCommand));
    registry.register(Arc::new(JsonDebugCommand));
    registry.register(Arc::new(JsonMSetCommand));
    registry.register(Arc::new(JsonStrAppendCommand));
    registry.register(Arc::new(JsonArrPopCommand));
    registry.register(Arc::new(JsonArrTrimCommand));
    registry.register(Arc::new(JsonArrIndexCommand));
    registry.register(Arc::new(JsonArrInsertCommand));
    registry.register(Arc::new(JsonMergeCommand));
    registry.register(Arc::new(JsonDumpCommand));

    // ── Count-Min Sketch commands ─────────────────────────────────────────────
    registry.register(Arc::new(CmsInitByDimCommand));
    registry.register(Arc::new(CmsInitByProbCommand));
    registry.register(Arc::new(CmsIncrByCommand));
    registry.register(Arc::new(CmsQueryCommand));
    registry.register(Arc::new(CmsInfoCommand));
    registry.register(Arc::new(CmsMergeCommand));

    // ── Top-K commands ────────────────────────────────────────────────────────
    registry.register(Arc::new(TopKReserveCommand));
    registry.register(Arc::new(TopKAddCommand));
    registry.register(Arc::new(TopKIncrByCommand));
    registry.register(Arc::new(TopKQueryCommand));
    registry.register(Arc::new(TopKCountCommand));
    registry.register(Arc::new(TopKListCommand));
    registry.register(Arc::new(TopKInfoCommand));

    // ── Rate limiter ──────────────────────────────────────────────────────────
    registry.register(Arc::new(ClThrottleCommand));

    // ── Server commands ───────────────────────────────────────────────────────
    registry.register(Arc::new(PingCommand));
    registry.register(Arc::new(SelectCommand { num_dbs: db.num_dbs }));
    registry.register(Arc::new(FlushAllCommand { db: db.clone() }));
    registry.register(Arc::new(FlushDbCommand { db: db.clone() }));
    registry.register(Arc::new(InfoCommand {
        db: db.clone(),
        info: server_info,
        config: Arc::new(cfg_snap.clone()),
    }));
    registry.register(Arc::new(DbSizeCommand { db: db.clone() }));
    registry.register(Arc::new(CommandCommand));
    registry.register(Arc::new(EchoCommand));
    registry.register(Arc::new(ConfigCommand { config: config.clone() }));

    // CLIENT/AUTH are handled in server.rs; register fallback stubs
    registry.register(Arc::new(ClientCommand));
    registry.register(Arc::new(AuthCommand {
        acl: acl.clone(),
        requirepass: requirepass_snap,
    }));

    registry.register(Arc::new(QuitCommand));
    registry.register(Arc::new(DebugCommand));
    registry.register(Arc::new(SlowlogCommand { store: slowlog_store }));
    registry.register(Arc::new(LatencyCommand { store: latency_store }));
    registry.register(Arc::new(MemoryCommand { db: db.clone() }));
    registry.register(Arc::new(SaveCommand));
    registry.register(Arc::new(BgSaveCommand));
    registry.register(Arc::new(BgRewriteAofCommand));
    registry.register(Arc::new(LastSaveCommand));
    registry.register(Arc::new(TimeCommand));
    registry.register(Arc::new(WatchCommand));
    registry.register(Arc::new(UnwatchCommand));
    registry.register(Arc::new(PublishCommand { hub: hub.clone() }));
    registry.register(Arc::new(PubSubCommand { hub: hub.clone() }));
    registry.register(Arc::new(SwapDbCommand { db: db.clone() }));
    registry.register(Arc::new(WaitCommand));
    registry.register(Arc::new(WaitAofCommand));
    registry.register(Arc::new(ObjectCommand { db: db.clone(), config: config.clone() }));
    registry.register(Arc::new(ResetCommand));
    registry.register(Arc::new(LolwutCommand));
    registry.register(Arc::new(ScriptCommand));
    registry.register(Arc::new(EvalCommand { registry: registry.clone() }));
    registry.register(Arc::new(EvalshaCommand { registry: registry.clone() }));
    registry.register(Arc::new(EvalRoCommand { registry: registry.clone() }));
    registry.register(Arc::new(EvalshaRoCommand { registry: registry.clone() }));
    registry.register(Arc::new(FunctionCommand));
    registry.register(Arc::new(FcallCommand { registry: registry.clone() }));
    registry.register(Arc::new(FcallRoCommand { registry: registry.clone() }));
    registry.register(Arc::new(AclCommand {
        acl: acl.clone(),
        current_username: "default".to_string(),
    }));
    registry.register(Arc::new(SpublishCommand { hub: hub.clone() }));
    registry.register(Arc::new(SsubscribeCommand));
    registry.register(Arc::new(SunsubscribeCommand));
    registry.register(Arc::new(ReplicaofCommand));
    registry.register(Arc::new(SlaveofCommand));
    registry.register(Arc::new(FailoverCommand));
    registry.register(Arc::new(PsyncCommand));
    registry.register(Arc::new(ReplconfCommand));

    // ── Cluster commands ──────────────────────────────────────────────────────
    registry.register(Arc::new(ClusterCommand));
    registry.register(Arc::new(DflyCommand));

    // ── RediSearch / FT.* commands (stubs) ───────────────────────────────────
    registry.register(Arc::new(FtCreateCommand));
    registry.register(Arc::new(FtDropIndexCommand));
    registry.register(Arc::new(FtDropCommand));
    registry.register(Arc::new(FtSearchCommand));
    registry.register(Arc::new(FtAggregateCommand));
    registry.register(Arc::new(FtInfoCommand));
    registry.register(Arc::new(FtListCommand));
    registry.register(Arc::new(FtExplainCommand { name: "FT.EXPLAIN" }));
    registry.register(Arc::new(FtExplainCommand { name: "FT.EXPLAINCL" }));
    registry.register(Arc::new(FtAlterCommand));
    registry.register(Arc::new(FtAliasAddCommand));
    registry.register(Arc::new(FtAliasDel));
    registry.register(Arc::new(FtAliasUpdate));
    registry.register(Arc::new(FtCursorCommand));
    registry.register(Arc::new(FtDictAddCommand));
    registry.register(Arc::new(FtDictDelCommand));
    registry.register(Arc::new(FtDictDumpCommand));
    registry.register(Arc::new(FtSpellCheckCommand));
    registry.register(Arc::new(FtSugAddCommand));
    registry.register(Arc::new(FtSugDelCommand));
    registry.register(Arc::new(FtSugGetCommand));
    registry.register(Arc::new(FtSugLenCommand));
    registry.register(Arc::new(FtSynUpdateCommand));
    registry.register(Arc::new(FtSynDumpCommand));
    registry.register(Arc::new(FtTagValsCommand));
    registry.register(Arc::new(FtProfileCommand));
    registry.register(Arc::new(FtConfigCommand));

    // ── T-Digest commands ─────────────────────────────────────────────────────
    registry.register(Arc::new(TDigestCreateCommand));
    registry.register(Arc::new(TDigestAddCommand));
    registry.register(Arc::new(TDigestResetCommand));
    registry.register(Arc::new(TDigestMergeCommand));
    registry.register(Arc::new(TDigestQuantileCommand));
    registry.register(Arc::new(TDigestCdfCommand));
    registry.register(Arc::new(TDigestRankCommand));
    registry.register(Arc::new(TDigestRevRankCommand));
    registry.register(Arc::new(TDigestByRankCommand));
    registry.register(Arc::new(TDigestByRevRankCommand));
    registry.register(Arc::new(TDigestMinCommand));
    registry.register(Arc::new(TDigestMaxCommand));
    registry.register(Arc::new(TDigestTrimmedMeanCommand));
    registry.register(Arc::new(TDigestInfoCommand));
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let base_config = if args.len() >= 3 && args[1] == "--config" {
        match config::ConfigParser::parse_file(&args[2]) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to parse config: {}", e);
                ServerConfig::default()
            }
        }
    } else if args.len() >= 2 && !args[1].starts_with("--") {
        match config::ConfigParser::parse_file(&args[1]) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to parse config: {}", e);
                ServerConfig::default()
            }
        }
    } else {
        ServerConfig::default()
    };

    eprintln!("Starting ForgeKV on {}:{}", base_config.bind, base_config.port);
    eprintln!("Data directory: {}", base_config.dir);

    // Shared config (writable at runtime via CONFIG SET)
    let config = Arc::new(RwLock::new(base_config.clone()));

    // Apply keyspace notification config
    if !base_config.notify_keyspace_events.is_empty() {
        keyspace::KS_NOTIFIER.configure(&base_config.notify_keyspace_events);
    }

    // Create ACL manager
    let acl = Arc::new(AclManager::new(base_config.acllog_max_len as usize));
    if !base_config.requirepass.is_empty() {
        acl.set_requirepass(&base_config.requirepass);
    }

    // Create storage engine
    let storage = match LsmStorage::new(base_config.clone()) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("Failed to initialize storage: {}", e);
            std::process::exit(1);
        }
    };

    let num_dbs = base_config.databases;
    let db = Arc::new(RedisDatabase::new(storage.clone(), num_dbs));
    let registry = Arc::new(CommandRegistry::new());
    let hub = Arc::new(PubSubHub::new());

    let tcp_server = TcpServer::new(
        base_config.clone(),
        registry.clone(),
        hub.clone(),
        acl.clone(),
        db.clone(),
    );

    register_all(registry.clone(), db.clone(), hub, config, acl, &tcp_server);

    // TTL sweeper
    let db_for_sweep = db.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            db_for_sweep.sweep_expired();
        }
    });

    // Shutdown handler: handle both SIGINT (Ctrl+C) and SIGTERM (docker stop)
    let storage_for_shutdown = storage.clone();
    let shutdown_handle = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to listen for SIGINT");
            tokio::select! {
                _ = sigterm.recv() => eprintln!("\nReceived SIGTERM — flushing storage..."),
                _ = sigint.recv() => eprintln!("\nReceived SIGINT — flushing storage..."),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
            eprintln!("\nReceived shutdown signal — flushing storage...");
        }
        storage_for_shutdown.close();
        std::process::exit(0);
    });

    tcp_server.run().await;

    storage.close();
    shutdown_handle.abort();
}
