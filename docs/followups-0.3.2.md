# Follow-ups found while fixing the SST mmap leak (0.3.2)

Not fixed in 0.3.2; tracked here until they get their own issues.

## 1. SCAN can miss keys during Ln -> Ln+1 compaction (stale per-level snapshot + ENOENT skip)

`scan_sstables` walks the levels from the deepest up to L1 and then L0, taking a separate
`manifest.get_level(level)` snapshot for each level. A compaction that moves data from Ln to
Ln+1 (including L0 -> L1) can publish between two of those reads:

- SCAN reads Ln+1 before the outputs are added, then Ln after the inputs were removed, so the
  moved keys are in neither snapshot; or
- SCAN's Ln snapshot still lists an input that `retire_files` has already unlinked;
  `get_or_open_reader` returns ENOENT and the `if let Ok(..)` silently skips the file.

Either way a SCAN that overlaps a compaction can return a result missing live keys (no data
is lost on disk). GET is not affected the same way because it reads L0 -> L1 -> ... in the
direction data moves, re-reading each level after the previous one.

Possible fix: take one `manifest.snapshot()` for the whole SCAN and pin it, i.e. hold the
`Arc<SSTableReader>`s of every file in the snapshot before reading (a retired file stays
readable while an Arc is held; the mapping is released when the SCAN drops it), or retry the
SCAN when an ENOENT is hit / the manifest generation changed.

## 2. `merge_files` skips inputs it cannot open

Same `if let Ok(..)` pattern: an input that fails to open is left out of the merge and then
removed from the manifest and unlinked. Aborting instead would make a permanently missing
file block compaction forever, so it needs a deliberate policy (quarantine + error metric).

## 3. WAL is replayed after a clean shutdown flush

Observed while timing restarts: after SIGTERM flushes the memtables to SSTs the WAL files are
not truncated, so the next start replays them again (about 6 s per GB). Harmless for SET, but
needs checking for non-idempotent commands (INCR, APPEND, LPUSH ...).
