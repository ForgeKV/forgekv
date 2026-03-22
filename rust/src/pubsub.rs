use std::collections::HashMap;

use parking_lot::RwLock;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub channel: String,
    pub data: Vec<u8>,
}

pub struct PubSubHub {
    channels: RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>,
    patterns: RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>,
}

impl PubSubHub {
    pub fn new() -> Self {
        PubSubHub {
            channels: RwLock::new(HashMap::new()),
            patterns: RwLock::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<PubSubMessage> {
        let mut channels = self.channels.write();
        let tx = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub fn psubscribe(&self, pattern: &str) -> broadcast::Receiver<PubSubMessage> {
        let mut patterns = self.patterns.write();
        let tx = patterns
            .entry(pattern.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    /// Publish a message. Returns the number of clients that received it.
    pub fn publish(&self, channel: &str, data: Vec<u8>) -> u64 {
        let msg = PubSubMessage {
            channel: channel.to_string(),
            data,
        };
        let mut count = 0u64;

        {
            let channels = self.channels.read();
            if let Some(tx) = channels.get(channel) {
                count += tx.send(msg.clone()).unwrap_or(0) as u64;
            }
        }

        {
            let patterns = self.patterns.read();
            for (pattern, tx) in patterns.iter() {
                if pattern_matches(pattern, channel) {
                    count += tx.send(msg.clone()).unwrap_or(0) as u64;
                }
            }
        }

        count
    }

    /// List channels that have at least one subscriber, optionally filtered by a glob pattern.
    pub fn active_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let channels = self.channels.read();
        channels
            .iter()
            .filter(|(ch, tx)| {
                tx.receiver_count() > 0
                    && pattern.map_or(true, |p| pattern_matches(p, ch))
            })
            .map(|(ch, _)| ch.clone())
            .collect()
    }

    pub fn num_subscribers(&self, channel: &str) -> u64 {
        let channels = self.channels.read();
        channels
            .get(channel)
            .map_or(0, |tx| tx.receiver_count() as u64)
    }

    /// Number of active pattern subscriptions across all clients.
    pub fn num_patterns(&self) -> u64 {
        let patterns = self.patterns.read();
        patterns
            .values()
            .filter(|tx| tx.receiver_count() > 0)
            .count() as u64
    }
}

/// Redis-compatible glob pattern matching (*, ?, [abc], [a-z], [^abc]).
pub fn pattern_matches(pattern: &str, subject: &str) -> bool {
    glob_match(pattern.as_bytes(), subject.as_bytes())
}

fn glob_match(pat: &[u8], sub: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut si = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut star_si = 0usize;

    loop {
        if si < sub.len() {
            if pi < pat.len() {
                match pat[pi] {
                    b'*' => {
                        star_pi = Some(pi);
                        star_si = si;
                        pi += 1;
                        continue;
                    }
                    b'?' => {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    b'[' => {
                        let (matched, consumed) = match_char_class(&pat[pi..], sub[si]);
                        if matched {
                            pi += consumed;
                            si += 1;
                            continue;
                        }
                    }
                    c if c == sub[si] => {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    _ => {}
                }
            }
            if let Some(spi) = star_pi {
                pi = spi + 1;
                star_si += 1;
                si = star_si;
                continue;
            }
            return false;
        } else {
            while pi < pat.len() && pat[pi] == b'*' {
                pi += 1;
            }
            return pi == pat.len();
        }
    }
}

fn match_char_class(pat: &[u8], c: u8) -> (bool, usize) {
    if pat.is_empty() || pat[0] != b'[' {
        return (false, 0);
    }
    let mut i = 1;
    let negate = i < pat.len() && pat[i] == b'^';
    if negate {
        i += 1;
    }
    let mut matched = false;
    while i < pat.len() && pat[i] != b']' {
        if i + 2 < pat.len() && pat[i + 1] == b'-' && pat[i + 2] != b']' {
            if c >= pat[i] && c <= pat[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if c == pat[i] {
                matched = true;
            }
            i += 1;
        }
    }
    let consumed = if i < pat.len() { i + 1 } else { i };
    (if negate { !matched } else { matched }, consumed)
}

#[cfg(test)]
mod tests {
    use super::pattern_matches;

    #[test]
    fn test_glob_patterns() {
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("news.*", "news.sport"));
        assert!(!pattern_matches("news.*", "sport"));
        assert!(pattern_matches("h?llo", "hello"));
        assert!(!pattern_matches("h?llo", "hllo"));
        assert!(pattern_matches("h[ae]llo", "hello"));
        assert!(pattern_matches("h[ae]llo", "hallo"));
        assert!(!pattern_matches("h[ae]llo", "hillo"));
        assert!(pattern_matches("h[a-z]llo", "hello"));
        assert!(!pattern_matches("h[^ae]llo", "hello"));
        assert!(pattern_matches("h[^ae]llo", "hillo"));
    }
}
