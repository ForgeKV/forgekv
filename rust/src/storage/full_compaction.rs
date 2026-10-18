//! `COMPACT FULL` — throttled, bounded-I/O garbage collection of shadowed versions.
//!
//! Normal compaction is threshold driven (L0 >= 64 files, L3 > 16 GB, L4 > 64 GB), so old
//! versions of keys that were rewritten (e.g. by the value-compression migration) can sit in
//! deep levels for a very long time. `COMPACT FULL` reclaims that space *without* a 2x
//! rewrite of the data directory:
//!
//! * It walks every SST of levels >= 1 **one file at a time**, oldest level first and, inside a
//!   level, lowest read-precedence first.
//! * For each file it drops
//!     - every entry that is **shadowed**: a higher-precedence file (L0, a lower level, or an
//!       earlier file of the same level) contains the same key (value or tombstone), i.e. GET
//!       can never return it; and
//!     - every **tombstone** for which no lower-precedence file holds an older version.
//! * The surviving entries are written to a new file that takes the **same position** in the
//!   manifest (so GET/SCAN precedence is unchanged), the manifest is saved atomically, and
//!   the input file is deleted after a short grace period. Peak extra disk = one SST.
//! * Each step holds the single-flight `compacting` flag only while it runs, so normal
//!   flush/compaction keeps working in between; new writes land in the memtable/L0 which are
//!   always newer than anything this job touches.
//! * Throttled by input MB/s, aborts when free disk < MINFREE, can be stopped at any time and
//!   is idempotent/resumable (just run it again). `DRYRUN` reports the savings without writing.

use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use super::compaction::Compaction;
use super::sstable::{SSTableReader, SSTableWriter};

#[derive(Debug, Clone)]
pub struct FullCompactConfig {
    /// Input MB/s (read side). 0 = unlimited.
    pub rate_mb_s: u64,
    /// Refuse to start / abort when free disk space is below this many bytes.
    pub min_free_bytes: u64,
    /// Report only; write nothing.
    pub dry_run: bool,
    /// How long a replaced SST stays on disk/open so that in-flight reads holding an older
    /// manifest snapshot can still find it.
    pub grace: Duration,
}

impl Default for FullCompactConfig {
    fn default() -> Self {
        FullCompactConfig {
            rate_mb_s: 64,
            min_free_bytes: 30 * 1024 * 1024 * 1024,
            dry_run: false,
            grace: Duration::from_secs(5),
        }
    }
}

#[derive(Default)]
pub struct FullState {
    running: AtomicBool,
    stop: AtomicBool,
    state: Mutex<String>,
    last_error: Mutex<String>,
    current_file: Mutex<String>,
    dry_run: AtomicBool,
    started_unix: AtomicU64,
    finished_unix: AtomicU64,
    files_total: AtomicU64,
    files_done: AtomicU64,
    files_rewritten: AtomicU64,
    files_deleted: AtomicU64,
    files_unchanged: AtomicU64,
    files_failed: AtomicU64,
    entries_scanned: AtomicU64,
    entries_kept: AtomicU64,
    dropped_shadowed: AtomicU64,
    dropped_tombstones: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    bytes_reclaimed: AtomicU64,
    min_free_bytes: AtomicU64,
    rate_mb_s: AtomicU64,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Free bytes available to unprivileged writers on the filesystem holding `path`.
pub fn free_disk_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return None;
    }
    Some((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
}

struct Pending {
    level: usize,
    file: String,
    deadline: Instant,
}

enum StepOutcome {
    Unchanged { bytes_read: u64 },
    Rewritten { bytes_read: u64, bytes_written: u64, reclaimed: u64 },
    Deleted { bytes_read: u64, reclaimed: u64 },
    Skipped,
}

impl Compaction {
    pub fn full_is_running(&self) -> bool {
        self.full.running.load(Ordering::SeqCst)
    }

    pub fn full_request_stop(&self) -> bool {
        if self.full.running.load(Ordering::SeqCst) {
            self.full.stop.store(true, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    pub fn full_status_pairs(&self) -> Vec<(String, String)> {
        let f = &self.full;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let state = {
            let s = f.state.lock();
            if s.is_empty() { "idle".to_string() } else { s.clone() }
        };
        let mut v: Vec<(String, String)> = vec![
            ("state".into(), state),
            ("running".into(), (f.running.load(Ordering::Relaxed) as u8).to_string()),
            ("dry_run".into(), (f.dry_run.load(Ordering::Relaxed) as u8).to_string()),
            ("started_unix".into(), ld(&f.started_unix).to_string()),
            ("finished_unix".into(), ld(&f.finished_unix).to_string()),
            ("rate_mb_s".into(), ld(&f.rate_mb_s).to_string()),
            ("min_free_bytes".into(), ld(&f.min_free_bytes).to_string()),
            ("files_total".into(), ld(&f.files_total).to_string()),
            ("files_done".into(), ld(&f.files_done).to_string()),
            ("files_rewritten".into(), ld(&f.files_rewritten).to_string()),
            ("files_deleted".into(), ld(&f.files_deleted).to_string()),
            ("files_unchanged".into(), ld(&f.files_unchanged).to_string()),
            ("files_failed".into(), ld(&f.files_failed).to_string()),
            ("entries_scanned".into(), ld(&f.entries_scanned).to_string()),
            ("entries_kept".into(), ld(&f.entries_kept).to_string()),
            ("dropped_shadowed".into(), ld(&f.dropped_shadowed).to_string()),
            ("dropped_tombstones".into(), ld(&f.dropped_tombstones).to_string()),
            ("bytes_read".into(), ld(&f.bytes_read).to_string()),
            ("bytes_written".into(), ld(&f.bytes_written).to_string()),
            ("bytes_reclaimed".into(), ld(&f.bytes_reclaimed).to_string()),
            ("current_file".into(), f.current_file.lock().clone()),
        ];
        let e = f.last_error.lock().clone();
        v.push(("last_error".into(), if e.is_empty() { "none".into() } else { e }));
        v
    }

    /// Start the background job. Fails if one is already running or free disk is too low.
    pub fn full_start(self: &Arc<Self>, cfg: FullCompactConfig) -> Result<(), String> {
        if self
            .full
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("ERR a full compaction is already running; COMPACT FULL STATUS / STOP".into());
        }
        if !cfg.dry_run {
            if let Some(free) = free_disk_bytes(&self.data_dir) {
                if free < cfg.min_free_bytes {
                    self.full.running.store(false, Ordering::SeqCst);
                    return Err(format!(
                        "ERR free disk {} bytes is below MINFREE {} bytes; refusing to start",
                        free, cfg.min_free_bytes
                    ));
                }
            }
        }
        let f = &self.full;
        f.stop.store(false, Ordering::SeqCst);
        *f.state.lock() = "running".into();
        f.last_error.lock().clear();
        f.current_file.lock().clear();
        f.dry_run.store(cfg.dry_run, Ordering::Relaxed);
        f.started_unix.store(now_unix(), Ordering::Relaxed);
        f.finished_unix.store(0, Ordering::Relaxed);
        f.min_free_bytes.store(cfg.min_free_bytes, Ordering::Relaxed);
        f.rate_mb_s.store(cfg.rate_mb_s, Ordering::Relaxed);
        for a in [
            &f.files_total, &f.files_done, &f.files_rewritten, &f.files_deleted,
            &f.files_unchanged, &f.files_failed, &f.entries_scanned, &f.entries_kept,
            &f.dropped_shadowed, &f.dropped_tombstones, &f.bytes_read, &f.bytes_written,
            &f.bytes_reclaimed,
        ] {
            a.store(0, Ordering::Relaxed);
        }
        let me = self.clone();
        std::thread::Builder::new()
            .name("forgekv-compact-full".into())
            .spawn(move || {
                let end_state = me.full_run(&cfg);
                *me.full.state.lock() = end_state;
                me.full.finished_unix.store(now_unix(), Ordering::Relaxed);
                me.full.current_file.lock().clear();
                me.full.running.store(false, Ordering::SeqCst);
            })
            .map_err(|e| {
                self.full.running.store(false, Ordering::SeqCst);
                format!("ERR cannot spawn thread: {e}")
            })?;
        Ok(())
    }

    fn full_set_error(&self, msg: String) {
        *self.full.last_error.lock() = msg;
    }

    fn full_run(&self, cfg: &FullCompactConfig) -> String {
        let snapshot = self.manifest.snapshot();
        let total: usize = snapshot.iter().skip(1).map(|l| l.len()).sum();
        self.full.files_total.store(total as u64, Ordering::Relaxed);
        let mut pending: Vec<Pending> = Vec::new();
        let mut end_state = "completed".to_string();

        'levels: for level in (1..snapshot.len()).rev() {
            // Lowest precedence (= last in the level) first, so that older versions are
            // already gone when the tombstones that shadowed them are examined.
            let files: Vec<String> = snapshot[level].iter().rev().cloned().collect();
            for file in files {
                if self.full.stop.load(Ordering::SeqCst) {
                    end_state = "stopped".into();
                    break 'levels;
                }
                self.purge_pending(&mut pending, false);
                if !cfg.dry_run {
                    if let Some(free) = free_disk_bytes(&self.data_dir) {
                        if free < cfg.min_free_bytes {
                            self.full_set_error(format!(
                                "aborted: free disk {} bytes < MINFREE {} bytes",
                                free, cfg.min_free_bytes
                            ));
                            end_state = "aborted_low_disk".into();
                            break 'levels;
                        }
                    }
                }
                // Single-flight with normal compaction; wait (cancellably) for our turn.
                loop {
                    if self
                        .compacting
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                    if self.full.stop.load(Ordering::SeqCst) {
                        end_state = "stopped".into();
                        break 'levels;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                *self.full.current_file.lock() = format!("L{level}:{file}");
                let t0 = Instant::now();
                let res = self.full_step(level, &file, cfg);
                self.compacting.store(false, Ordering::SeqCst);

                let mut processed: u64 = 0;
                match res {
                    Ok(StepOutcome::Unchanged { bytes_read }) => {
                        processed = bytes_read;
                        self.full.files_unchanged.fetch_add(1, Ordering::Relaxed);
                        self.full.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                    }
                    Ok(StepOutcome::Rewritten { bytes_read, bytes_written, reclaimed }) => {
                        processed = bytes_read + bytes_written;
                        self.full.files_rewritten.fetch_add(1, Ordering::Relaxed);
                        self.full.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                        self.full.bytes_written.fetch_add(bytes_written, Ordering::Relaxed);
                        self.full.bytes_reclaimed.fetch_add(reclaimed, Ordering::Relaxed);
                        if !cfg.dry_run {
                            pending.push(Pending {
                                level,
                                file: file.clone(),
                                deadline: Instant::now() + cfg.grace,
                            });
                        }
                    }
                    Ok(StepOutcome::Deleted { bytes_read, reclaimed }) => {
                        processed = bytes_read;
                        self.full.files_deleted.fetch_add(1, Ordering::Relaxed);
                        self.full.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                        self.full.bytes_reclaimed.fetch_add(reclaimed, Ordering::Relaxed);
                        if !cfg.dry_run {
                            pending.push(Pending {
                                level,
                                file: file.clone(),
                                deadline: Instant::now() + cfg.grace,
                            });
                        }
                    }
                    Ok(StepOutcome::Skipped) => {}
                    Err(e) => {
                        self.full.files_failed.fetch_add(1, Ordering::Relaxed);
                        self.full_set_error(format!("L{level}:{file}: {e}"));
                    }
                }
                self.full.files_done.fetch_add(1, Ordering::Relaxed);

                if cfg.rate_mb_s > 0 && processed > 0 {
                    let want = Duration::from_secs_f64(
                        processed as f64 / (cfg.rate_mb_s as f64 * 1024.0 * 1024.0),
                    );
                    if let Some(rest) = want.checked_sub(t0.elapsed()) {
                        // Sleep in slices so STOP stays responsive.
                        let end = Instant::now() + rest;
                        while Instant::now() < end && !self.full.stop.load(Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(50).min(rest));
                        }
                    }
                }
            }
        }
        self.purge_pending(&mut pending, true);
        end_state
    }

    /// Close + delete replaced inputs. `force` waits out the grace period first.
    fn purge_pending(&self, pending: &mut Vec<Pending>, force: bool) {
        if force {
            if let Some(max) = pending.iter().map(|p| p.deadline).max() {
                let now = Instant::now();
                if max > now {
                    std::thread::sleep(max - now);
                }
            }
        }
        let now = Instant::now();
        pending.retain(|p| {
            if p.deadline <= now {
                self.close_reader(&p.file);
                let _ = fs::remove_file(self.data_dir.join(&p.file));
                let _ = p.level;
                false
            } else {
                true
            }
        });
    }

    /// Process one SST. Must be called with the `compacting` flag held.
    fn full_step(
        &self,
        level: usize,
        file: &str,
        cfg: &FullCompactConfig,
    ) -> Result<StepOutcome, String> {
        // Fresh view of the manifest: normal compaction may have changed it since we started.
        let levels = self.manifest.snapshot();
        let idx = match levels.get(level).and_then(|l| l.iter().position(|f| f == file)) {
            Some(i) => i,
            None => return Ok(StepOutcome::Skipped), // already compacted away
        };
        let input_len = fs::metadata(self.data_dir.join(file)).map(|m| m.len()).unwrap_or(0);
        let reader = self.get_or_open_reader(file).map_err(|e| format!("open: {e}"))?;
        let entries = reader.scan(None, None);
        self.full.entries_scanned.fetch_add(entries.len() as u64, Ordering::Relaxed);

        let (lo, hi) = (reader.smallest_key.clone(), reader.largest_key.clone());
        let overlaps = |r: &SSTableReader| -> bool {
            !(r.largest_key.as_slice() < lo.as_slice() || r.smallest_key.as_slice() > hi.as_slice())
        };

        // Precedence exactly as in GET: L0 (newest first), then L1.., files in manifest order.
        // "higher" = consulted before `file`; "lower" = consulted after it.
        let mut higher: Vec<Arc<SSTableReader>> = Vec::new();
        let mut lower: Vec<Arc<SSTableReader>> = Vec::new();
        for (lvl, files) in levels.iter().enumerate() {
            for (pos, f) in files.iter().enumerate() {
                if lvl == level && pos == idx {
                    continue;
                }
                let is_higher = lvl < level || (lvl == level && pos < idx);
                let r = self.get_or_open_reader(f).map_err(|e| format!("open {f}: {e}"))?;
                if lvl >= 1 && !overlaps(&r) {
                    continue;
                }
                if is_higher { higher.push(r) } else { lower.push(r) }
            }
        }
        // Same range pre-filter GET applies for levels >= 1 (L0 has none).
        let in_range = |r: &SSTableReader, k: &[u8]| -> bool {
            k >= r.smallest_key.as_slice() && k <= r.largest_key.as_slice()
        };
        let l0_len = levels.first().map(|l| l.len()).unwrap_or(0);

        let mut kept: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let (mut d_shadow, mut d_tomb) = (0u64, 0u64);
        let mut dropped_bytes = 0u64;
        for (k, v) in entries.iter() {
            let esize = (k.len() + v.as_ref().map(|b| b.len()).unwrap_or(0) + 10) as u64;
            // The first `l0_len` entries of `higher` are L0 files (no range filter in GET).
            let shadowed = higher.iter().enumerate().any(|(i, r)| {
                (i < l0_len || in_range(r, k)) && r.lookup(k).is_some()
            });
            if shadowed {
                d_shadow += 1;
                dropped_bytes += esize;
                continue;
            }
            if v.is_none() {
                let older = lower.iter().any(|r| in_range(r, k) && r.lookup(k).is_some());
                if !older {
                    d_tomb += 1;
                    dropped_bytes += esize;
                    continue;
                }
            }
            kept.insert(k.clone(), v.clone());
        }
        self.full.entries_kept.fetch_add(kept.len() as u64, Ordering::Relaxed);
        self.full.dropped_shadowed.fetch_add(d_shadow, Ordering::Relaxed);
        self.full.dropped_tombstones.fetch_add(d_tomb, Ordering::Relaxed);

        if d_shadow == 0 && d_tomb == 0 {
            return Ok(StepOutcome::Unchanged { bytes_read: input_len });
        }
        if cfg.dry_run {
            // Estimate: surviving payload ~ input minus dropped payload.
            let est_out = if kept.is_empty() { 0 } else { input_len.saturating_sub(dropped_bytes) };
            return Ok(if kept.is_empty() {
                StepOutcome::Deleted { bytes_read: input_len, reclaimed: input_len }
            } else {
                StepOutcome::Rewritten {
                    bytes_read: input_len,
                    bytes_written: 0,
                    reclaimed: input_len.saturating_sub(est_out),
                }
            });
        }

        if kept.is_empty() {
            if !self.manifest.replace_in_place(level, file, None) {
                return Ok(StepOutcome::Skipped);
            }
            self.manifest.save().map_err(|e| format!("manifest save: {e}"))?;
            return Ok(StepOutcome::Deleted { bytes_read: input_len, reclaimed: input_len });
        }

        let seq = self.manifest.next_sequence();
        let new_name = format!("L{}-{}.sst", level, seq);
        let final_path = self.data_dir.join(&new_name);
        let tmp_path = self.data_dir.join(format!("{new_name}.tmp"));
        let cleanup = |p: &std::path::Path| {
            let _ = fs::remove_file(p);
        };
        if let Err(e) = SSTableWriter::write(&tmp_path, &kept) {
            cleanup(&tmp_path);
            return Err(format!("write: {e}"));
        }
        if let Err(e) = fs::rename(&tmp_path, &final_path) {
            cleanup(&tmp_path);
            return Err(format!("rename: {e}"));
        }
        // Verify before publishing: same number of entries, readable.
        let verify = SSTableReader::open(&final_path)
            .map(|r| r.scan(None, None).len() == kept.len())
            .unwrap_or(false);
        if !verify {
            cleanup(&final_path);
            return Err("verification of the rewritten file failed; original kept".into());
        }
        let out_len = fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
        if !self.manifest.replace_in_place(level, file, Some(new_name.clone())) {
            cleanup(&final_path);
            return Ok(StepOutcome::Skipped);
        }
        if let Err(e) = self.manifest.save() {
            // Roll back the in-memory swap so memory and disk agree.
            self.manifest.replace_in_place(level, &new_name, Some(file.to_string()));
            cleanup(&final_path);
            return Err(format!("manifest save: {e}"));
        }
        Ok(StepOutcome::Rewritten {
            bytes_read: input_len,
            bytes_written: out_len,
            reclaimed: input_len.saturating_sub(out_len),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::manifest::Manifest;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    type Entries = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let p = std::env::temp_dir().join(format!("forgekv-cf-{name}-{unique}"));
        fs::create_dir_all(&p).unwrap();
        p
    }

    struct Fixture {
        dir: PathBuf,
        manifest: Arc<Manifest>,
        comp: Arc<Compaction>,
    }

    fn fixture(name: &str) -> Fixture {
        let dir = temp_dir(name);
        let manifest = Arc::new(Manifest::new(dir.join("MANIFEST")));
        let comp = Arc::new(Compaction::new(dir.clone(), manifest.clone()));
        Fixture { dir, manifest, comp }
    }

    impl Fixture {
        fn add(&self, level: usize, entries: &[(&str, Option<&str>)]) -> String {
            let mut m: Entries = BTreeMap::new();
            for (k, v) in entries {
                m.insert(k.as_bytes().to_vec(), v.map(|x| x.as_bytes().to_vec()));
            }
            self.add_map(level, &m)
        }
        fn add_map(&self, level: usize, m: &Entries) -> String {
            let name = format!("L{}-{}.sst", level, self.manifest.next_sequence());
            SSTableWriter::write(&self.dir.join(&name), m).unwrap();
            self.manifest.add_file(level, name.clone());
            self.manifest.save().unwrap();
            name
        }
        /// What GET would return from the SST layers for `key`.
        fn get(&self, key: &str) -> Option<Vec<u8>> {
            self.comp.search_sstables(key.as_bytes()).flatten()
        }
        fn run(&self, cfg: FullCompactConfig) -> Vec<(String, String)> {
            self.comp.full_start(cfg).expect("start");
            let t0 = Instant::now();
            while self.comp.full_is_running() {
                assert!(t0.elapsed() < Duration::from_secs(60), "job hangs");
                std::thread::sleep(Duration::from_millis(10));
            }
            self.comp.full_status_pairs()
        }
        fn disk_bytes(&self) -> u64 {
            fs::read_dir(&self.dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
                .map(|e| e.metadata().unwrap().len())
                .sum()
        }
        fn sst_files_on_disk(&self) -> usize {
            fs::read_dir(&self.dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
                .count()
        }
    }

    fn st(p: &[(String, String)], k: &str) -> String {
        p.iter().find(|(a, _)| a == k).map(|(_, v)| v.clone()).unwrap()
    }

    fn cfg() -> FullCompactConfig {
        FullCompactConfig {
            rate_mb_s: 0,
            min_free_bytes: 0,
            dry_run: false,
            grace: Duration::from_millis(20),
        }
    }

    fn big(tag: &str) -> String {
        format!("{tag}-{}", "x".repeat(2000))
    }

    #[test]
    fn shadowed_old_versions_are_dropped_and_reads_identical() {
        let f = fixture("shadow");
        // L4: old versions of k00..k49 ; L3: newer versions of k00..k29 ; L1: even newer k00..k09
        let old: Entries = (0..50).map(|i| (format!("k{i:02}").into_bytes(), Some(big("old").into_bytes()))).collect();
        f.add_map(4, &old);
        let l3: Entries = (0..30).map(|i| (format!("k{i:02}").into_bytes(), Some(big("mid").into_bytes()))).collect();
        f.add_map(3, &l3);
        let l1: Entries = (0..10).map(|i| (format!("k{i:02}").into_bytes(), Some(big("new").into_bytes()))).collect();
        f.add_map(1, &l1);

        let before: Vec<_> = (0..60).map(|i| f.get(&format!("k{i:02}"))).collect();
        let bytes_before = f.disk_bytes();
        let p = f.run(cfg());
        let after: Vec<_> = (0..60).map(|i| f.get(&format!("k{i:02}"))).collect();

        assert_eq!(before, after, "GET results must be identical");
        assert_eq!(st(&p, "state"), "completed");
        // L4: k00..k29 shadowed by L3/L1 (30) + L3: k00..k09 shadowed by L1 (10)
        assert_eq!(st(&p, "dropped_shadowed"), "40");
        assert!(f.disk_bytes() < bytes_before);
        assert_eq!(st(&p, "files_failed"), "0");
        assert_eq!(f.get("k05").unwrap(), big("new").into_bytes());
        assert_eq!(f.get("k15").unwrap(), big("mid").into_bytes());
        assert_eq!(f.get("k45").unwrap(), big("old").into_bytes());
        assert_eq!(f.get("k99"), None);
        // inputs removed after grace; manifest and disk agree
        let in_manifest: usize = f.manifest.snapshot().iter().map(|l| l.len()).sum();
        assert_eq!(in_manifest, f.sst_files_on_disk());
    }

    #[test]
    fn fully_shadowed_file_is_deleted() {
        let f = fixture("deleted");
        let old = f.add(4, &[("a", Some("1")), ("b", Some("2"))]);
        f.add(2, &[("a", Some("10")), ("b", Some("20")), ("c", Some("30"))]);
        let p = f.run(cfg());
        assert_eq!(st(&p, "files_deleted"), "1");
        assert!(!f.dir.join(old).exists());
        assert_eq!(f.get("a").unwrap(), b"10");
        assert_eq!(f.get("c").unwrap(), b"30");
        assert!(f.manifest.get_level(4).is_empty());
    }

    #[test]
    fn tombstones_hide_old_data_and_are_dropped_when_nothing_older_remains() {
        let f = fixture("tomb");
        f.add(4, &[("dead", Some("old")), ("live", Some("old")), ("z", Some("zz"))]);
        // tombstone for `dead`, one for `never_existed`, plus new value for live
        f.add(2, &[("dead", None), ("never_existed", None), ("live", Some("new"))]);
        let before: Vec<_> = ["dead", "live", "z", "never_existed"].iter().map(|k| f.get(k)).collect();
        let p = f.run(cfg());
        let after: Vec<_> = ["dead", "live", "z", "never_existed"].iter().map(|k| f.get(k)).collect();
        assert_eq!(before, after);
        assert_eq!(after, vec![None, Some(b"new".to_vec()), Some(b"zz".to_vec()), None]);
        // L4: dead + live shadowed; L2: both tombstones dropped (older versions are gone / never existed)
        assert_eq!(st(&p, "dropped_shadowed"), "2");
        assert_eq!(st(&p, "dropped_tombstones"), "2");
        // no file may still contain the tombstone keys
        for lvl in 0..7 {
            for file in f.manifest.get_level(lvl) {
                let r = SSTableReader::open(&f.dir.join(file)).unwrap();
                assert!(r.lookup(b"dead").is_none() && r.lookup(b"never_existed").is_none());
            }
        }
    }

    #[test]
    fn tombstone_is_kept_while_an_older_version_survives_below_it() {
        let f = fixture("tomb-keep");
        // Same level: file order = precedence. First file has the tombstone, second the older value.
        f.add(3, &[("k", None), ("other", Some("o"))]);
        f.add(3, &[("k", Some("older"))]);
        // The older version is shadowed by the tombstone -> dropped first (lowest precedence first),
        // then the tombstone has nothing below it and goes too.
        let p = f.run(cfg());
        assert_eq!(f.get("k"), None);
        assert_eq!(f.get("other").unwrap(), b"o");
        assert_eq!(st(&p, "dropped_shadowed"), "1");
        assert_eq!(st(&p, "dropped_tombstones"), "1");
    }

    #[test]
    fn l0_shadows_deeper_versions_and_l0_is_never_touched() {
        let f = fixture("l0");
        f.add(4, &[("a", Some("old")), ("b", Some("old"))]);
        let l0 = f.add(0, &[("a", Some("fresh")), ("b", None)]);
        let p = f.run(cfg());
        assert_eq!(st(&p, "files_deleted"), "1"); // L4 file fully shadowed
        assert_eq!(f.get("a").unwrap(), b"fresh");
        assert_eq!(f.get("b"), None);
        assert!(f.dir.join(l0).exists());
        assert_eq!(f.manifest.get_level(0).len(), 1);
    }

    #[test]
    fn nothing_to_reclaim_leaves_files_untouched_and_second_run_is_noop() {
        let f = fixture("noop");
        let a = f.add(4, &[("a", Some("1"))]);
        let b = f.add(3, &[("b", Some("2"))]);
        let p = f.run(cfg());
        assert_eq!(st(&p, "files_unchanged"), "2");
        assert!(f.dir.join(a).exists() && f.dir.join(b).exists());

        let g = fixture("idem");
        g.add(4, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        g.add(3, &[("a", Some("11"))]);
        g.run(cfg());
        let p2 = g.run(cfg());
        assert_eq!(st(&p2, "files_rewritten"), "0");
        assert_eq!(st(&p2, "files_deleted"), "0");
        assert_eq!(g.get("a").unwrap(), b"11");
        assert_eq!(g.get("b").unwrap(), b"2");
    }

    #[test]
    fn dry_run_reports_but_changes_nothing() {
        let f = fixture("dry");
        let old = f.add(4, &[("a", Some(&big("old"))), ("b", Some(&big("old")))]);
        f.add(2, &[("a", Some("new"))]);
        let manifest_before = fs::read_to_string(f.dir.join("MANIFEST")).unwrap();
        let bytes_before = f.disk_bytes();
        let mut c = cfg();
        c.dry_run = true;
        let p = f.run(c);
        assert_eq!(st(&p, "dry_run"), "1");
        assert_eq!(st(&p, "dropped_shadowed"), "1");
        assert!(st(&p, "bytes_reclaimed").parse::<u64>().unwrap() > 1000);
        assert_eq!(fs::read_to_string(f.dir.join("MANIFEST")).unwrap(), manifest_before);
        assert_eq!(f.disk_bytes(), bytes_before);
        assert!(f.dir.join(old).exists());
    }

    #[test]
    fn low_disk_guard_refuses_to_start() {
        let f = fixture("disk");
        f.add(4, &[("a", Some("1"))]);
        let mut c = cfg();
        c.min_free_bytes = u64::MAX;
        let err = f.comp.full_start(c).unwrap_err();
        assert!(err.contains("below MINFREE"), "{err}");
        assert!(!f.comp.full_is_running());
        // dry run is allowed regardless
        let mut c = cfg();
        c.min_free_bytes = u64::MAX;
        c.dry_run = true;
        assert!(f.comp.full_start(c).is_ok());
    }

    #[test]
    fn second_start_is_rejected_and_stop_works() {
        let f = fixture("stop");
        for i in 0..20 {
            let old: Entries = (0..20).map(|j| (format!("k{i:02}-{j:02}").into_bytes(), Some(big("o").into_bytes()))).collect();
            f.add_map(4, &old);
        }
        let new: Entries = (0..20).flat_map(|i| (0..20).map(move |j| (format!("k{i:02}-{j:02}").into_bytes(), Some(b"n".to_vec())))).collect();
        f.add_map(2, &new);
        let mut c = cfg();
        c.rate_mb_s = 1; // slow: ~40KB per file => ~40ms/file
        f.comp.full_start(c).unwrap();
        assert!(f.comp.full_start(cfg()).unwrap_err().contains("already running"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(f.comp.full_request_stop());
        let t0 = Instant::now();
        while f.comp.full_is_running() {
            assert!(t0.elapsed() < Duration::from_secs(20));
            std::thread::sleep(Duration::from_millis(10));
        }
        let p = f.comp.full_status_pairs();
        assert_eq!(st(&p, "state"), "stopped");
        // data is still fully readable and consistent after a partial run
        for i in 0..20 {
            for j in 0..20 {
                assert_eq!(f.get(&format!("k{i:02}-{j:02}")).unwrap(), b"n");
            }
        }
        // and the job can be resumed to completion
        let p = f.run(cfg());
        assert_eq!(st(&p, "state"), "completed");
        assert_eq!(f.manifest.get_level(4).len(), 0);
    }

    #[test]
    fn concurrent_writes_and_normal_compaction_stay_consistent() {
        let f = fixture("concurrent");
        let old: Entries = (0..400).map(|i| (format!("k{i:04}").into_bytes(), Some(big("old").into_bytes()))).collect();
        for chunk in old.iter().collect::<Vec<_>>().chunks(40) {
            let m: Entries = chunk.iter().map(|(k, v)| ((*k).clone(), (*v).clone())).collect();
            f.add_map(4, &m);
        }
        let mid: Entries = (0..400).step_by(2).map(|i| (format!("k{i:04}").into_bytes(), Some(b"mid".to_vec()))).collect();
        f.add_map(3, &mid);

        let stop = Arc::new(AtomicBool::new(false));
        // writer: keeps adding newer L0 files overriding some keys (like a memtable flush would)
        let writer = {
            let (manifest, dir, stop) = (f.manifest.clone(), f.dir.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut n = 0u32;
                while !stop.load(Ordering::SeqCst) && n < 40 {
                    let key = format!("k{:04}", (n * 7) % 400);
                    let mut m: Entries = BTreeMap::new();
                    m.insert(key.into_bytes(), Some(format!("w{n}").into_bytes()));
                    let name = format!("L0-{}.sst", manifest.next_sequence());
                    SSTableWriter::write(&dir.join(&name), &m).unwrap();
                    manifest.add_file(0, name);
                    manifest.save().unwrap();
                    n += 1;
                    std::thread::sleep(Duration::from_millis(2));
                }
            })
        };
        // reader: every key must resolve to *some* valid version at all times
        let reader = {
            let (comp, stop) = (f.comp.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut misses = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    for i in (0..400).step_by(13) {
                        if comp.search_sstables(format!("k{i:04}").as_bytes()).flatten().is_none() {
                            misses += 1;
                        }
                    }
                }
                misses
            })
        };
        // normal compaction interleaving with the job
        let normal = {
            let (comp, stop) = (f.comp.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    comp.maybe_compact();
                    std::thread::sleep(Duration::from_millis(3));
                }
            })
        };
        let mut c = cfg();
        c.grace = Duration::from_millis(100);
        let p = f.run(c);
        writer.join().unwrap();
        stop.store(true, Ordering::SeqCst);
        let misses = reader.join().unwrap();
        normal.join().unwrap();
        assert_eq!(misses, 0, "readers must never lose a key during the job");
        assert_eq!(st(&p, "files_failed"), "0");
        assert_eq!(st(&p, "state"), "completed");
        for i in 0..400u32 {
            let expect: Vec<u8> = {
                let w = (0..40u32).filter(|n| (n * 7) % 400 == i).last();
                match w {
                    Some(n) => format!("w{n}").into_bytes(),
                    None if i % 2 == 0 => b"mid".to_vec(),
                    None => big("old").into_bytes(),
                }
            };
            assert_eq!(f.get(&format!("k{i:04}")).unwrap(), expect, "key k{i:04}");
        }
    }

    /// Deep level with many overlapping files (like prod: ~1000 L4 files shadowed by L2/L3):
    /// GET results for every key must be identical before/after and space must be reclaimed.
    #[test]
    fn many_files_in_deep_levels_reads_identical_and_space_reclaimed() {
        let f = fixture("scale");
        let n = 6000u32;
        let val = |tag: &str, i: u32| format!("{tag}-{i}-{}", "v".repeat(400)).into_bytes();
        // L4: 60 files of old versions of every key (interleaved key ranges, like repeated promotions)
        for part in 0..60u32 {
            let m: Entries = (0..n).filter(|i| i % 60 == part).map(|i| (format!("key{i:05}").into_bytes(), Some(val("old", i)))).collect();
            f.add_map(4, &m);
        }
        // L3: 10 files with newer versions of 2/3 of the keys, overlapping ranges
        for part in 0..10u32 {
            let m: Entries = (0..n).filter(|i| i % 3 != 0 && i % 10 == part).map(|i| (format!("key{i:05}").into_bytes(), Some(val("new", i)))).collect();
            f.add_map(3, &m);
        }
        // L2: tombstones for some keys, even newer versions for others
        let mut m: Entries = BTreeMap::new();
        for i in (0..n).filter(|i| i % 7 == 0) { m.insert(format!("key{i:05}").into_bytes(), None); }
        for i in (0..n).filter(|i| i % 11 == 0 && i % 7 != 0) { m.insert(format!("key{i:05}").into_bytes(), Some(val("newest", i))); }
        f.add_map(2, &m);
        // L0: freshest overrides
        let mut m0: Entries = BTreeMap::new();
        for i in (0..n).filter(|i| i % 13 == 0) { m0.insert(format!("key{i:05}").into_bytes(), Some(val("l0", i))); }
        f.add_map(0, &m0);

        let all: Vec<String> = (0..n + 50).map(|i| format!("key{i:05}")).collect();
        let before: Vec<_> = all.iter().map(|k| f.get(k)).collect();
        let bytes_before = f.disk_bytes();
        let t0 = Instant::now();
        let p = f.run(cfg());
        let took = t0.elapsed();
        let after: Vec<_> = all.iter().map(|k| f.get(k)).collect();
        assert_eq!(before, after, "every GET must be identical");
        assert_eq!(st(&p, "state"), "completed");
        assert_eq!(st(&p, "files_failed"), "0");
        assert!(f.disk_bytes() < bytes_before * 7 / 10, "{} -> {}", bytes_before, f.disk_bytes());
        // only live versions remain: exactly one entry per surviving key across all levels
        let mut live = std::collections::HashSet::new();
        let mut dups = 0;
        for lvl in 0..7 {
            for file in f.manifest.get_level(lvl) {
                let r = SSTableReader::open(&f.dir.join(file)).unwrap();
                for (k, v) in r.scan(None, None) {
                    if !live.insert(k) { dups += 1; }
                    let _ = v;
                }
            }
        }
        assert_eq!(dups, 0, "no shadowed duplicate may remain");
        assert!(took < Duration::from_secs(30));
        let in_manifest: usize = f.manifest.snapshot().iter().map(|l| l.len()).sum();
        assert_eq!(in_manifest, f.sst_files_on_disk());
    }

    #[test]
    fn survives_restart_through_manifest_and_lsm_storage() {
        use crate::config::ServerConfig;
        use crate::storage::lsm::LsmStorage;
        let f = fixture("restart");
        let old: Entries = (0..100).map(|i| (format!("key{i:03}").into_bytes(), Some(big("old").into_bytes()))).collect();
        f.add_map(4, &old);
        let new: Entries = (0..100).step_by(3).map(|i| (format!("key{i:03}").into_bytes(), Some(b"new".to_vec()))).collect();
        f.add_map(2, &new);
        let mut gone: Entries = BTreeMap::new();
        gone.insert(b"key001".to_vec(), None);
        f.add_map(1, &gone);
        let dir = f.dir.clone();
        drop(f);

        let mut config = ServerConfig::default();
        config.dir = dir.to_string_lossy().to_string();
        config.memtable_size_mb = 16;
        let expected = |i: u32| -> Option<Vec<u8>> {
            if i == 1 { None } else if i % 3 == 0 { Some(b"new".to_vec()) } else { Some(big("old").into_bytes()) }
        };
        let storage = LsmStorage::new(config.clone()).unwrap();
        for i in 0..100u32 {
            assert_eq!(storage.get(format!("key{i:03}").as_bytes()), expected(i));
        }
        let scan_before = storage.scan(None, None);
        storage.compact_full_start(cfg()).unwrap();
        let t0 = Instant::now();
        while storage.compact_full_status().iter().any(|(k, v)| k == "running" && v == "1") {
            assert!(t0.elapsed() < Duration::from_secs(30));
            std::thread::sleep(Duration::from_millis(10));
        }
        for i in 0..100u32 {
            assert_eq!(storage.get(format!("key{i:03}").as_bytes()), expected(i));
        }
        // live writes after the job still win
        storage.put(b"key050".to_vec(), b"live".to_vec());
        assert_eq!(storage.get(b"key050").unwrap(), b"live");
        let live_scan: Vec<_> = storage.scan(None, None).into_iter().filter(|(_, v)| v.is_some()).collect();
        assert_eq!(live_scan.len(), scan_before.iter().filter(|(_, v)| v.is_some()).count());
        storage.close();
        drop(storage);
        // restart: manifest was saved atomically and matches the files on disk
        let storage = LsmStorage::new(config).unwrap();
        for i in 0..100u32 {
            let want = if i == 50 { Some(b"live".to_vec()) } else { expected(i) };
            assert_eq!(storage.get(format!("key{i:03}").as_bytes()), want, "after restart key{i:03}");
        }
        storage.close();
        let _ = fs::remove_dir_all(&dir);
    }
}
