//! Background in-place migration of existing string values:
//! `FORGEKV.COMPRESS` (raw -> zstd) and `FORGEKV.DECOMPRESS` (zstd -> raw).
//!
//! * One job at a time (single-flight). Runs on a dedicated OS thread.
//! * Per key, the database re-reads and rewrites the value under that key's
//!   shard write lock, so a concurrent client write to the same key always
//!   wins or is converted, never lost. Metadata/TTL are not modified.
//! * Throttled by a keys-per-second rate limit; resumable via a cursor (the
//!   last processed metadata storage key); `DRYRUN` computes the projected
//!   savings without writing.
//! * Memory: the job materialises the *metadata* entries of the string keys
//!   of the database it is working on (a few dozen bytes each, like SCAN
//!   does), never the values.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::database::RedisDatabase;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Compress,
    Decompress,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Compress => "compress",
            Direction::Decompress => "decompress",
        }
    }
}

/// Result of processing one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOutcome {
    /// Rewritten (or would be, in dry-run). Sizes are stored (on-disk value) bytes.
    Converted {
        before: u64,
        after: u64,
    },
    /// Already in the target format.
    AlreadyTarget {
        stored_len: u64,
    },
    /// Left as is (too small, incompressible, magic-escaped).
    Unchanged {
        stored_len: u64,
    },
    /// Key vanished, expired, or changed type between listing and processing.
    Gone,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Idle,
    Running,
    Completed,
    Stopped,
}

impl RunState {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunState::Idle => "idle",
            RunState::Running => "running",
            RunState::Completed => "completed",
            RunState::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobConfig {
    pub direction: Direction,
    pub dry_run: bool,
    /// Keys per second; 0 = unlimited.
    pub rate: u64,
    /// Restrict to one database.
    pub db: Option<usize>,
    /// Stop (resumably) after examining this many keys in this run (0 = no limit).
    pub limit: u64,
}

#[derive(Debug, Clone)]
pub struct Cursor {
    pub db: usize,
    /// Last processed metadata storage key in `db` (None = start of db).
    pub after: Option<Vec<u8>>,
}

#[derive(Debug)]
struct Inner {
    state: RunState,
    config: Option<JobConfig>,
    cursor: Option<Cursor>,
    started_ms: i64,
    finished_ms: i64,
}

/// Shared job state. Counters are lock-free so INFO never blocks the job.
#[derive(Debug)]
pub struct JobState {
    inner: Mutex<Inner>,
    stop: AtomicBool,
    pub examined: AtomicU64,
    pub converted: AtomicU64,
    pub already_target: AtomicU64,
    pub unchanged: AtomicU64,
    pub gone: AtomicU64,
    pub errors: AtomicU64,
    /// Inventory seen by the job: values stored compressed / not compressed.
    pub seen_compressed: AtomicU64,
    pub seen_raw: AtomicU64,
    /// Sum of stored sizes of examined values before / after (projected if dry-run).
    pub bytes_before: AtomicU64,
    pub bytes_after: AtomicU64,
}

impl Default for JobState {
    fn default() -> Self {
        JobState {
            inner: Mutex::new(Inner {
                state: RunState::Idle,
                config: None,
                cursor: None,
                started_ms: 0,
                finished_ms: 0,
            }),
            stop: AtomicBool::new(false),
            examined: AtomicU64::new(0),
            converted: AtomicU64::new(0),
            already_target: AtomicU64::new(0),
            unchanged: AtomicU64::new(0),
            gone: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            seen_compressed: AtomicU64::new(0),
            seen_raw: AtomicU64::new(0),
            bytes_before: AtomicU64::new(0),
            bytes_after: AtomicU64::new(0),
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl JobState {
    fn reset_counters(&self) {
        for c in [
            &self.examined,
            &self.converted,
            &self.already_target,
            &self.unchanged,
            &self.gone,
            &self.errors,
            &self.seen_compressed,
            &self.seen_raw,
            &self.bytes_before,
            &self.bytes_after,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }

    pub fn run_state(&self) -> RunState {
        self.inner.lock().state
    }

    pub fn config(&self) -> Option<JobConfig> {
        self.inner.lock().config.clone()
    }

    pub fn request_stop(&self) -> bool {
        let running = self.inner.lock().state == RunState::Running;
        if running {
            self.stop.store(true, Ordering::SeqCst);
        }
        running
    }

    /// Forget the cursor and counters (only when not running).
    pub fn reset(&self) -> bool {
        let mut g = self.inner.lock();
        if g.state == RunState::Running {
            return false;
        }
        g.state = RunState::Idle;
        g.config = None;
        g.cursor = None;
        g.started_ms = 0;
        g.finished_ms = 0;
        drop(g);
        self.reset_counters();
        true
    }

    /// Key/value lines for STATUS and INFO.
    pub fn status_pairs(&self) -> Vec<(String, String)> {
        let g = self.inner.lock();
        let before = self.bytes_before.load(Ordering::Relaxed);
        let after = self.bytes_after.load(Ordering::Relaxed);
        let ratio = if after > 0 {
            format!("{:.3}", before as f64 / after as f64)
        } else {
            "0".to_string()
        };
        let cfg = g.config.clone();
        let mut v = vec![
            ("state".to_string(), g.state.as_str().to_string()),
            (
                "direction".to_string(),
                cfg.as_ref()
                    .map(|c| c.direction.as_str().to_string())
                    .unwrap_or_default(),
            ),
            (
                "dry_run".to_string(),
                cfg.as_ref()
                    .map(|c| (c.dry_run as u8).to_string())
                    .unwrap_or_default(),
            ),
            (
                "rate_keys_per_sec".to_string(),
                cfg.as_ref().map(|c| c.rate.to_string()).unwrap_or_default(),
            ),
            (
                "cursor_db".to_string(),
                g.cursor
                    .as_ref()
                    .map(|c| c.db.to_string())
                    .unwrap_or_default(),
            ),
            (
                "cursor_key".to_string(),
                g.cursor
                    .as_ref()
                    .and_then(|c| c.after.as_ref())
                    .map(|k| hex::encode(k))
                    .unwrap_or_default(),
            ),
            ("started_ms".to_string(), g.started_ms.to_string()),
            ("finished_ms".to_string(), g.finished_ms.to_string()),
        ];
        let c = |a: &AtomicU64| a.load(Ordering::Relaxed).to_string();
        v.push(("keys_examined".to_string(), c(&self.examined)));
        v.push(("keys_converted".to_string(), c(&self.converted)));
        v.push((
            "keys_already_in_target_format".to_string(),
            c(&self.already_target),
        ));
        v.push(("keys_unchanged".to_string(), c(&self.unchanged)));
        v.push(("keys_gone".to_string(), c(&self.gone)));
        v.push(("keys_error".to_string(), c(&self.errors)));
        v.push(("compressed_keys".to_string(), c(&self.seen_compressed)));
        v.push(("raw_keys".to_string(), c(&self.seen_raw)));
        v.push(("bytes_before".to_string(), before.to_string()));
        v.push(("bytes_after".to_string(), after.to_string()));
        v.push(("ratio".to_string(), ratio));
        v
    }
}

/// Start the job. `resume` continues from the saved cursor and keeps counters;
/// otherwise cursor and counters start from scratch.
pub fn start(db: &Arc<RedisDatabase>, cfg: JobConfig, resume: bool) -> Result<(), String> {
    let job = db.compress_job.clone();
    {
        let mut g = job.inner.lock();
        if g.state == RunState::Running {
            return Err("ERR a compression job is already running".to_string());
        }
        if let Some(d) = cfg.db {
            if d >= db.num_dbs {
                return Err("ERR DB index is out of range".to_string());
            }
        }
        if resume {
            if g.cursor.is_none() {
                return Err("ERR nothing to resume".to_string());
            }
            if let Some(prev) = &g.config {
                if prev.direction != cfg.direction || prev.dry_run != cfg.dry_run {
                    return Err(
                        "ERR RESUME must use the same command and DRYRUN setting as the previous job (use RESET to start over)"
                            .to_string(),
                    );
                }
            }
        } else {
            g.cursor = None;
            job.reset_counters();
            g.started_ms = now_ms();
        }
        g.config = Some(cfg.clone());
        g.state = RunState::Running;
        g.finished_ms = 0;
        job.stop.store(false, Ordering::SeqCst);
    }

    let db2 = db.clone();
    let spawn = std::thread::Builder::new()
        .name("forgekv-compress".to_string())
        .spawn(move || run(db2, cfg));
    if let Err(e) = spawn {
        let mut g = job.inner.lock();
        g.state = RunState::Stopped;
        return Err(format!("ERR failed to start job thread: {e}"));
    }
    Ok(())
}

fn run(db: Arc<RedisDatabase>, cfg: JobConfig) {
    let job = db.compress_job.clone();
    let start_cursor = job.inner.lock().cursor.clone();
    let (first_db, mut resume_after) = match start_cursor {
        Some(c) => (c.db, c.after),
        None => (cfg.db.unwrap_or(0), None),
    };
    let last_db = cfg.db.unwrap_or(db.num_dbs.saturating_sub(1));
    let t0 = Instant::now();
    let mut processed_this_run: u64 = 0;
    let mut stopped = false;

    'dbs: for dbi in first_db..=last_db {
        if dbi >= db.num_dbs {
            break;
        }
        let keys = db.list_string_keys_after(dbi, resume_after.as_deref());
        resume_after = None;
        for (meta_key, user_key) in keys {
            if job.stop.load(Ordering::SeqCst) {
                stopped = true;
                break 'dbs;
            }
            if cfg.limit > 0 && processed_this_run >= cfg.limit {
                stopped = true; // limit reached: resumable
                break 'dbs;
            }
            let outcome = db.migrate_string_key(dbi, &user_key, cfg.direction, cfg.dry_run);
            record(&job, outcome, cfg.direction);
            job.inner.lock().cursor = Some(Cursor {
                db: dbi,
                after: Some(meta_key),
            });
            processed_this_run += 1;

            if cfg.rate > 0 {
                let target = Duration::from_secs_f64(processed_this_run as f64 / cfg.rate as f64);
                loop {
                    let el = t0.elapsed();
                    if el >= target || job.stop.load(Ordering::SeqCst) {
                        break;
                    }
                    std::thread::sleep((target - el).min(Duration::from_millis(50)));
                }
            }
        }
        // Finished this db: cursor moves to the start of the next one.
        job.inner.lock().cursor = Some(Cursor {
            db: dbi + 1,
            after: None,
        });
    }

    let mut g = job.inner.lock();
    g.state = if stopped {
        RunState::Stopped
    } else {
        RunState::Completed
    };
    g.finished_ms = now_ms();
}

fn record(job: &JobState, outcome: KeyOutcome, dir: Direction) {
    job.examined.fetch_add(1, Ordering::Relaxed);
    match outcome {
        KeyOutcome::Converted { before, after } => {
            job.converted.fetch_add(1, Ordering::Relaxed);
            job.bytes_before.fetch_add(before, Ordering::Relaxed);
            job.bytes_after.fetch_add(after, Ordering::Relaxed);
            // Inventory is "as found" before conversion.
            match dir {
                Direction::Compress => job.seen_raw.fetch_add(1, Ordering::Relaxed),
                Direction::Decompress => job.seen_compressed.fetch_add(1, Ordering::Relaxed),
            };
        }
        KeyOutcome::AlreadyTarget { stored_len } => {
            job.already_target.fetch_add(1, Ordering::Relaxed);
            job.bytes_before.fetch_add(stored_len, Ordering::Relaxed);
            job.bytes_after.fetch_add(stored_len, Ordering::Relaxed);
            match dir {
                Direction::Compress => job.seen_compressed.fetch_add(1, Ordering::Relaxed),
                Direction::Decompress => job.seen_raw.fetch_add(1, Ordering::Relaxed),
            };
        }
        KeyOutcome::Unchanged { stored_len } => {
            job.unchanged.fetch_add(1, Ordering::Relaxed);
            job.bytes_before.fetch_add(stored_len, Ordering::Relaxed);
            job.bytes_after.fetch_add(stored_len, Ordering::Relaxed);
            match dir {
                Direction::Compress => job.seen_raw.fetch_add(1, Ordering::Relaxed),
                Direction::Decompress => job.seen_compressed.fetch_add(1, Ordering::Relaxed),
            };
        }
        KeyOutcome::Gone => {
            job.gone.fetch_add(1, Ordering::Relaxed);
        }
        KeyOutcome::Error => {
            job.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
