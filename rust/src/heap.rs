//! Returning freed heap memory to the operating system.
//!
//! glibc keeps pages freed by large, short-lived allocations (range scans that materialise
//! millions of keys, compaction merges, flushed memtables) inside its arenas instead of giving
//! them back to the kernel, so the process RSS ratchets up to the peak of the last heavy
//! operation and stays there. When enabled (`FORGEKV_HEAP_RELEASE=trim`), a background thread
//! calls `malloc_trim(0)` (or purges all jemalloc arenas when built with `--features jemalloc`)
//! shortly after such an operation and, optionally, on a fixed interval.
//!
//! The release never runs on a request thread. `MEMORY PURGE` triggers one synchronously.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

static ENABLED: AtomicBool = AtomicBool::new(false);
static REQUESTED: AtomicBool = AtomicBool::new(false);
static RELEASES: AtomicU64 = AtomicU64::new(0);
static LAST_RELEASE_US: AtomicU64 = AtomicU64::new(0);
static MAX_RELEASE_US: AtomicU64 = AtomicU64::new(0);
static LAST_RELEASED_BYTES: AtomicU64 = AtomicU64::new(0);
static TOTAL_RELEASED_BYTES: AtomicU64 = AtomicU64::new(0);
static INTERVAL_SECS: AtomicU64 = AtomicU64::new(0);

/// Minimum spacing between event-driven releases, so a burst of compactions or scans
/// costs at most one trim per window.
const MIN_GAP: Duration = Duration::from_secs(5);

/// Ask the background releaser to run soon. Cheap; safe to call from any thread.
pub fn request_release() {
    if ENABLED.load(Ordering::Relaxed) {
        REQUESTED.store(true, Ordering::Relaxed);
    }
}

/// Start the background releaser. No-op when `enabled` is false.
pub fn start(enabled: bool, interval_secs: u64) {
    ENABLED.store(enabled, Ordering::Relaxed);
    INTERVAL_SECS.store(interval_secs, Ordering::Relaxed);
    if !enabled {
        return;
    }
    tune_allocator();
    std::thread::Builder::new()
        .name("forgekv-heap".into())
        .spawn(move || {
            let interval = Duration::from_secs(interval_secs);
            let mut last = Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(500));
                let since = last.elapsed();
                let requested = REQUESTED.load(Ordering::Relaxed) && since >= MIN_GAP;
                let periodic = interval_secs > 0 && since >= interval;
                if requested || periodic {
                    REQUESTED.store(false, Ordering::Relaxed);
                    release_now();
                    last = Instant::now();
                }
            }
        })
        .expect("heap release thread");
}

/// Release free heap pages to the OS now. Returns (bytes of anonymous RSS released, duration).
pub fn release_now() -> (u64, Duration) {
    let before = rss_anon_bytes().unwrap_or(0);
    let t0 = Instant::now();
    trim();
    let took = t0.elapsed();
    let after = rss_anon_bytes().unwrap_or(0);
    let released = before.saturating_sub(after);
    let us = took.as_micros() as u64;
    RELEASES.fetch_add(1, Ordering::Relaxed);
    LAST_RELEASE_US.store(us, Ordering::Relaxed);
    MAX_RELEASE_US.fetch_max(us, Ordering::Relaxed);
    LAST_RELEASED_BYTES.store(released, Ordering::Relaxed);
    TOTAL_RELEASED_BYTES.fetch_add(released, Ordering::Relaxed);
    (released, took)
}

/// glibc: grow/shrink the heap in 64 MiB steps instead of 128 KiB. After a trim hands pages
/// back, a heavy command re-grows the heap; with the default pad that is tens of thousands of
/// brk/mprotect calls, each taking the process-wide mmap write lock and stalling every thread
/// that page-faults on the mmapped SSTs. Allocations of 32 MiB+ still use mmap directly.
#[cfg(all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu"))]
fn tune_allocator() {
    unsafe {
        libc::mallopt(libc::M_TOP_PAD, 64 << 20);
        libc::mallopt(libc::M_MMAP_THRESHOLD, 32 << 20);
    }
}

#[cfg(not(all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu")))]
fn tune_allocator() {}

#[cfg(feature = "jemalloc")]
fn trim() {
    // MALLCTL_ARENAS_ALL = 4096: purge dirty/muzzy pages of every arena.
    unsafe {
        tikv_jemalloc_sys::mallctl(
            b"arena.4096.purge\0".as_ptr() as *const _,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        );
    }
}

#[cfg(all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu"))]
fn trim() {
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(all(not(feature = "jemalloc"), not(all(target_os = "linux", target_env = "gnu"))))]
fn trim() {}

pub fn allocator_name() -> &'static str {
    if cfg!(feature = "jemalloc") {
        "jemalloc"
    } else if cfg!(all(target_os = "linux", target_env = "gnu")) {
        "glibc"
    } else {
        "system"
    }
}

/// Anonymous (heap) resident memory of this process, from /proc/self/status `RssAnon`.
pub fn rss_anon_bytes() -> Option<u64> {
    proc_status_kb("RssAnon:").map(|kb| kb * 1024)
}

/// Peak resident set size, from /proc/self/status `VmHWM`.
pub fn peak_rss_bytes() -> Option<u64> {
    proc_status_kb("VmHWM:").map(|kb| kb * 1024)
}

fn proc_status_kb(field: &str) -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    s.lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
}

/// `INFO memory` lines (CRLF-terminated).
pub fn info_lines() -> String {
    format!(
        "heap_allocator:{}\r\n\
        heap_release:{}\r\n\
        heap_release_interval_s:{}\r\n\
        heap_rss_anon_bytes:{}\r\n\
        heap_peak_rss_bytes:{}\r\n\
        heap_releases:{}\r\n\
        heap_last_release_us:{}\r\n\
        heap_max_release_us:{}\r\n\
        heap_last_released_bytes:{}\r\n\
        heap_total_released_bytes:{}\r\n",
        allocator_name(),
        if ENABLED.load(Ordering::Relaxed) { "trim" } else { "off" },
        INTERVAL_SECS.load(Ordering::Relaxed),
        rss_anon_bytes().unwrap_or(0),
        peak_rss_bytes().unwrap_or(0),
        RELEASES.load(Ordering::Relaxed),
        LAST_RELEASE_US.load(Ordering::Relaxed),
        MAX_RELEASE_US.load(Ordering::Relaxed),
        LAST_RELEASED_BYTES.load(Ordering::Relaxed),
        TOTAL_RELEASED_BYTES.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_now_runs_and_counts() {
        // Allocate and free ~64 MiB in small chunks so there is something to trim.
        let v: Vec<Vec<u8>> = (0..65_536).map(|i| vec![i as u8; 1024]).collect();
        drop(v);
        let before = RELEASES.load(Ordering::Relaxed);
        let (_released, _took) = release_now();
        assert_eq!(RELEASES.load(Ordering::Relaxed), before + 1);
        assert!(info_lines().contains("heap_releases:"));
    }

    #[test]
    fn request_is_ignored_when_disabled() {
        ENABLED.store(false, Ordering::Relaxed);
        REQUESTED.store(false, Ordering::Relaxed);
        request_release();
        assert!(!REQUESTED.load(Ordering::Relaxed));
    }
}
