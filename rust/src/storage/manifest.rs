use std::fs;
use std::io;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

pub struct Manifest {
    path: PathBuf,
    levels: Mutex<Vec<Vec<String>>>,
    next_sequence: Mutex<u64>,
    /// Tests only: make the next N saves fail with an injected I/O error.
    #[cfg(test)]
    fail_saves: AtomicUsize,
}

impl Manifest {
    pub fn new(path: PathBuf) -> Self {
        Manifest {
            path,
            levels: Mutex::new(Vec::new()),
            next_sequence: Mutex::new(1),
            #[cfg(test)]
            fail_saves: AtomicUsize::new(0),
        }
    }

    /// Tests only: the next `n` calls that write the MANIFEST fail with an injected error.
    #[cfg(test)]
    pub fn fail_next_saves(&self, n: usize) {
        self.fail_saves.store(n, Ordering::SeqCst);
    }

    pub fn load(&self) -> io::Result<()> {
        if !self.path.exists() {
            return Ok(());
        }

        let content = fs::read_to_string(&self.path)?;
        let mut levels = self.levels.lock();
        let mut seq = self.next_sequence.lock();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.splitn(3, '|').collect();
            if parts.is_empty() {
                continue;
            }

            match parts[0] {
                "seq" => {
                    if parts.len() >= 2 {
                        if let Ok(s) = parts[1].parse::<u64>() {
                            *seq = s + 1;
                        }
                    }
                }
                "L" => {
                    if parts.len() >= 3 {
                        let level: usize = parts[1].parse().unwrap_or(0);
                        while levels.len() <= level {
                            levels.push(Vec::new());
                        }
                        let files: Vec<String> = parts[2]
                            .split(',')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect();
                        levels[level] = files;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn save(&self) -> io::Result<()> {
        let levels = self.levels.lock();
        self.write_file(&levels)
    }

    /// Apply `f` to the levels and persist the result atomically with respect to other
    /// manifest users (the levels lock is held across the change and the write). If the
    /// write fails the in-memory change is rolled back, so memory and disk never disagree
    /// and the caller can keep (not unlink) the files it meant to replace.
    pub fn apply_and_save<R>(&self, f: impl FnOnce(&mut Vec<Vec<String>>) -> R) -> io::Result<R> {
        let mut levels = self.levels.lock();
        let before = levels.clone();
        let r = f(&mut levels);
        if let Err(e) = self.write_file(&levels) {
            *levels = before;
            return Err(e);
        }
        Ok(r)
    }

    /// Write `levels` (caller holds the levels lock) to disk via write-then-rename.
    fn write_file(&self, levels: &[Vec<String>]) -> io::Result<()> {
        #[cfg(test)]
        {
            if self
                .fail_saves
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(io::Error::new(io::ErrorKind::Other, "injected manifest save failure"));
            }
        }
        let seq = self.next_sequence.lock();
        let mut content = String::new();

        content.push_str(&format!("seq|{}\n", *seq));

        for (i, level) in levels.iter().enumerate() {
            if !level.is_empty() {
                content.push_str(&format!("L|{}|{}\n", i, level.join(",")));
            }
        }
        drop(seq);

        // Write-then-rename so a crash can never leave a truncated MANIFEST.
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, content)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    pub fn get_level(&self, level: usize) -> Vec<String> {
        let levels = self.levels.lock();
        if level < levels.len() {
            levels[level].clone()
        } else {
            Vec::new()
        }
    }

    /// True if `path` is listed at any level.
    pub fn contains(&self, path: &str) -> bool {
        let levels = self.levels.lock();
        levels.iter().any(|l| l.iter().any(|f| f == path))
    }

    pub fn level_count(&self) -> usize {
        let levels = self.levels.lock();
        levels.len()
    }

    pub fn next_sequence(&self) -> u64 {
        let mut seq = self.next_sequence.lock();
        let s = *seq;
        *seq += 1;
        s
    }

    pub fn add_file(&self, level: usize, path: String) {
        let mut levels = self.levels.lock();
        while levels.len() <= level {
            levels.push(Vec::new());
        }
        levels[level].push(path);
    }

    pub fn remove_file(&self, level: usize, path: &str) {
        let mut levels = self.levels.lock();
        if level < levels.len() {
            levels[level].retain(|f| f != path);
        }
    }

    /// Replace `old` by `new` at the SAME position inside `level` (or just remove `old` when
    /// `new` is None). Keeping the position preserves the read-precedence order used by GET.
    /// Returns false (and changes nothing) if `old` is not in the level.
    pub fn replace_in_place(&self, level: usize, old: &str, new: Option<String>) -> bool {
        let mut levels = self.levels.lock();
        replace_in_levels(&mut levels, level, old, new)
    }

    /// `replace_in_place` + save as one step; on a failed save the swap is rolled back and
    /// the error returned (the caller must then keep `old` on disk).
    pub fn replace_in_place_and_save(
        &self,
        level: usize,
        old: &str,
        new: Option<String>,
    ) -> io::Result<bool> {
        self.apply_and_save(|levels| replace_in_levels(levels, level, old, new))
    }

    /// Snapshot of every level (index = level).
    pub fn snapshot(&self) -> Vec<Vec<String>> {
        self.levels.lock().clone()
    }

    pub fn replace_files(
        &self,
        input_level: usize,
        input_files: &[String],
        output_level: usize,
        output_files: Vec<String>,
    ) {
        let mut levels = self.levels.lock();

        // Ensure levels exist
        let max_level = output_level.max(input_level);
        while levels.len() <= max_level {
            levels.push(Vec::new());
        }

        // Remove input files
        for f in input_files {
            levels[input_level].retain(|x| x != f);
        }

        // Add output files
        for f in output_files {
            levels[output_level].push(f);
        }
    }
}

fn replace_in_levels(levels: &mut [Vec<String>], level: usize, old: &str, new: Option<String>) -> bool {
    if level >= levels.len() {
        return false;
    }
    match levels[level].iter().position(|f| f == old) {
        Some(idx) => {
            match new {
                Some(n) => levels[level][idx] = n,
                None => {
                    levels[level].remove(idx);
                }
            }
            true
        }
        None => false,
    }
}
