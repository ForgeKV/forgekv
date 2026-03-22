/// RediSearch / FT.* command stubs
/// Full-text search is acknowledged but returns empty results.
/// This ensures client compatibility without crashing.
use crate::resp::RespValue;
use super::CommandHandler;

// ── FT.CREATE ─────────────────────────────────────────────────────────────────

pub struct FtCreateCommand;
impl CommandHandler for FtCreateCommand {
    fn name(&self) -> &str { "FT.CREATE" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

// ── FT.DROPINDEX ──────────────────────────────────────────────────────────────

pub struct FtDropIndexCommand;
impl CommandHandler for FtDropIndexCommand {
    fn name(&self) -> &str { "FT.DROPINDEX" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

pub struct FtDropCommand;
impl CommandHandler for FtDropCommand {
    fn name(&self) -> &str { "FT.DROP" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

// ── FT.SEARCH ─────────────────────────────────────────────────────────────────

pub struct FtSearchCommand;
impl CommandHandler for FtSearchCommand {
    fn name(&self) -> &str { "FT.SEARCH" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        // Return empty result: [0, (no results)]
        RespValue::Array(Some(vec![
            RespValue::integer(0),
        ]))
    }
}

// ── FT.AGGREGATE ──────────────────────────────────────────────────────────────

pub struct FtAggregateCommand;
impl CommandHandler for FtAggregateCommand {
    fn name(&self) -> &str { "FT.AGGREGATE" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![RespValue::integer(0)]))
    }
}

// ── FT.INFO ───────────────────────────────────────────────────────────────────

pub struct FtInfoCommand;
impl CommandHandler for FtInfoCommand {
    fn name(&self) -> &str { "FT.INFO" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        let name = args.get(1).and_then(|a| a.as_str()).unwrap_or("").to_string();
        RespValue::Array(Some(vec![
            RespValue::bulk_str("index_name"),
            RespValue::bulk_str(&name),
            RespValue::bulk_str("index_options"),
            RespValue::Array(Some(vec![])),
            RespValue::bulk_str("index_definition"),
            RespValue::Array(Some(vec![])),
            RespValue::bulk_str("attributes"),
            RespValue::Array(Some(vec![])),
            RespValue::bulk_str("num_docs"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("max_doc_id"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("num_terms"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("num_records"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("inverted_sz_mb"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("total_inverted_index_blocks"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("offset_vectors_sz_mb"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("doc_table_size_mb"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("sortable_values_size_mb"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("key_table_size_mb"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("records_per_doc_avg"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("bytes_per_record_avg"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("offsets_per_term_avg"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("offset_bits_per_record_avg"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("hash_indexing_failures"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("indexing"),
            RespValue::bulk_str("0"),
            RespValue::bulk_str("percent_indexed"),
            RespValue::bulk_str("1"),
            RespValue::bulk_str("gc_stats"),
            RespValue::Array(Some(vec![])),
            RespValue::bulk_str("cursor_stats"),
            RespValue::Array(Some(vec![])),
        ]))
    }
}

// ── FT.LIST ───────────────────────────────────────────────────────────────────

pub struct FtListCommand;
impl CommandHandler for FtListCommand {
    fn name(&self) -> &str { "FT.LIST" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

// ── FT.EXPLAIN / FT.EXPLAINCL ─────────────────────────────────────────────────

pub struct FtExplainCommand { pub name: &'static str }
impl CommandHandler for FtExplainCommand {
    fn name(&self) -> &str { self.name }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::bulk_str("")
    }
}

// ── FT.ALTER ──────────────────────────────────────────────────────────────────

pub struct FtAlterCommand;
impl CommandHandler for FtAlterCommand {
    fn name(&self) -> &str { "FT.ALTER" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::ok()
    }
}

// ── FT.ALIAS* ────────────────────────────────────────────────────────────────

pub struct FtAliasAddCommand;
impl CommandHandler for FtAliasAddCommand {
    fn name(&self) -> &str { "FT.ALIASADD" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::ok() }
}

pub struct FtAliasDel;
impl CommandHandler for FtAliasDel {
    fn name(&self) -> &str { "FT.ALIASDEL" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::ok() }
}

pub struct FtAliasUpdate;
impl CommandHandler for FtAliasUpdate {
    fn name(&self) -> &str { "FT.ALIASUPDATE" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::ok() }
}

// ── FT.CURSOR ─────────────────────────────────────────────────────────────────

pub struct FtCursorCommand;
impl CommandHandler for FtCursorCommand {
    fn name(&self) -> &str { "FT.CURSOR" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![RespValue::Array(Some(vec![])), RespValue::integer(0)]))
    }
}

// ── FT.DICT* ─────────────────────────────────────────────────────────────────

pub struct FtDictAddCommand;
impl CommandHandler for FtDictAddCommand {
    fn name(&self) -> &str { "FT.DICTADD" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::integer(0) }
}

pub struct FtDictDelCommand;
impl CommandHandler for FtDictDelCommand {
    fn name(&self) -> &str { "FT.DICTDEL" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::integer(0) }
}

pub struct FtDictDumpCommand;
impl CommandHandler for FtDictDumpCommand {
    fn name(&self) -> &str { "FT.DICTDUMP" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

// ── FT.SPELLCHECK ─────────────────────────────────────────────────────────────

pub struct FtSpellCheckCommand;
impl CommandHandler for FtSpellCheckCommand {
    fn name(&self) -> &str { "FT.SPELLCHECK" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

// ── FT.SUGADD / FT.SUGDEL / FT.SUGGET / FT.SUGLEN ───────────────────────────

pub struct FtSugAddCommand;
impl CommandHandler for FtSugAddCommand {
    fn name(&self) -> &str { "FT.SUGADD" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::integer(1) }
}

pub struct FtSugDelCommand;
impl CommandHandler for FtSugDelCommand {
    fn name(&self) -> &str { "FT.SUGDEL" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::integer(0) }
}

pub struct FtSugGetCommand;
impl CommandHandler for FtSugGetCommand {
    fn name(&self) -> &str { "FT.SUGGET" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

pub struct FtSugLenCommand;
impl CommandHandler for FtSugLenCommand {
    fn name(&self) -> &str { "FT.SUGLEN" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::integer(0) }
}

// ── FT.SYNUPDATE / FT.SYNDUMP ─────────────────────────────────────────────────

pub struct FtSynUpdateCommand;
impl CommandHandler for FtSynUpdateCommand {
    fn name(&self) -> &str { "FT.SYNUPDATE" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue { RespValue::ok() }
}

pub struct FtSynDumpCommand;
impl CommandHandler for FtSynDumpCommand {
    fn name(&self) -> &str { "FT.SYNDUMP" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

// ── FT.TAGVALS ────────────────────────────────────────────────────────────────

pub struct FtTagValsCommand;
impl CommandHandler for FtTagValsCommand {
    fn name(&self) -> &str { "FT.TAGVALS" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![]))
    }
}

// ── FT.PROFILE ────────────────────────────────────────────────────────────────

pub struct FtProfileCommand;
impl CommandHandler for FtProfileCommand {
    fn name(&self) -> &str { "FT.PROFILE" }
    fn execute(&self, _db_index: &mut usize, _args: &[RespValue]) -> RespValue {
        RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![RespValue::integer(0)])),
            RespValue::Array(Some(vec![])),
        ]))
    }
}

// ── FT.CONFIG ────────────────────────────────────────────────────────────────

pub struct FtConfigCommand;
impl CommandHandler for FtConfigCommand {
    fn name(&self) -> &str { "FT.CONFIG" }
    fn execute(&self, _db_index: &mut usize, args: &[RespValue]) -> RespValue {
        let sub = args.get(1).and_then(|a| a.as_str()).map(|s| s.to_uppercase()).unwrap_or_default();
        match sub.as_str() {
            "SET" => RespValue::ok(),
            "GET" => RespValue::Array(Some(vec![])),
            "RESETSTAT" => RespValue::ok(),
            _ => RespValue::ok(),
        }
    }
}
