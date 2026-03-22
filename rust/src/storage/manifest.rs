use std::fs;
use std::io;
use std::path::PathBuf;

use parking_lot::Mutex;

pub struct Manifest {
    path: PathBuf,
    levels: Mutex<Vec<Vec<String>>>,
    next_sequence: Mutex<u64>,
}

impl Manifest {
    pub fn new(path: PathBuf) -> Self {
        Manifest {
            path,
            levels: Mutex::new(Vec::new()),
            next_sequence: Mutex::new(1),
        }
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
        let seq = self.next_sequence.lock();
        let mut content = String::new();

        content.push_str(&format!("seq|{}\n", *seq));

        for (i, level) in levels.iter().enumerate() {
            if !level.is_empty() {
                content.push_str(&format!("L|{}|{}\n", i, level.join(",")));
            }
        }

        fs::write(&self.path, content)?;
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
