/// RESP protocol types
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    BulkString(Option<Vec<u8>>),   // None = null bulk
    Array(Option<Vec<RespValue>>), // None = null array
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString(b"OK".to_vec())
    }

    pub fn pong() -> Self {
        RespValue::SimpleString(b"PONG".to_vec())
    }

    pub fn null_bulk() -> Self {
        RespValue::BulkString(None)
    }

    pub fn null_array() -> Self {
        RespValue::Array(None)
    }

    pub fn error(s: &str) -> Self {
        RespValue::Error(s.as_bytes().to_vec())
    }

    pub fn simple(s: &str) -> Self {
        RespValue::SimpleString(s.as_bytes().to_vec())
    }

    pub fn integer(n: i64) -> Self {
        RespValue::Integer(n)
    }

    pub fn bulk_bytes(b: Vec<u8>) -> Self {
        RespValue::BulkString(Some(b))
    }

    pub fn bulk_str(s: &str) -> Self {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    pub fn empty_array() -> Self {
        RespValue::Array(Some(vec![]))
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
            RespValue::SimpleString(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(Some(b)) => Some(b.as_slice()),
            RespValue::SimpleString(b) => Some(b.as_slice()),
            _ => None,
        }
    }
}

pub struct RespParser {
    buf: Vec<u8>,
    filled: usize,
}

impl RespParser {
    pub fn new() -> Self {
        RespParser {
            buf: vec![0u8; 65536],
            filled: 0,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        let needed = self.filled + data.len();
        if needed > self.buf.len() {
            self.buf.resize(needed * 2, 0);
        }
        self.buf[self.filled..self.filled + data.len()].copy_from_slice(data);
        self.filled += data.len();
    }

    pub fn try_parse(&mut self) -> Option<RespValue> {
        if self.filled == 0 {
            return None;
        }

        let (val, consumed) = Self::parse_value(&self.buf[..self.filled])?;
        // Consume the bytes
        self.buf.copy_within(consumed..self.filled, 0);
        self.filled -= consumed;
        Some(val)
    }

    fn parse_value(buf: &[u8]) -> Option<(RespValue, usize)> {
        if buf.is_empty() {
            return None;
        }

        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            _ => Self::parse_inline(buf),
        }
    }

    fn find_crlf(buf: &[u8]) -> Option<usize> {
        for i in 0..buf.len().saturating_sub(1) {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }

    fn parse_simple_string(buf: &[u8]) -> Option<(RespValue, usize)> {
        let pos = Self::find_crlf(&buf[1..])?;
        let s = buf[1..1 + pos].to_vec();
        Some((RespValue::SimpleString(s), 1 + pos + 2))
    }

    fn parse_error(buf: &[u8]) -> Option<(RespValue, usize)> {
        let pos = Self::find_crlf(&buf[1..])?;
        let s = buf[1..1 + pos].to_vec();
        Some((RespValue::Error(s), 1 + pos + 2))
    }

    fn parse_integer(buf: &[u8]) -> Option<(RespValue, usize)> {
        let pos = Self::find_crlf(&buf[1..])?;
        let s = std::str::from_utf8(&buf[1..1 + pos]).ok()?;
        let n: i64 = s.trim().parse().ok()?;
        Some((RespValue::Integer(n), 1 + pos + 2))
    }

    fn parse_bulk_string(buf: &[u8]) -> Option<(RespValue, usize)> {
        let pos = Self::find_crlf(&buf[1..])?;
        let len_str = std::str::from_utf8(&buf[1..1 + pos]).ok()?;
        let len: i64 = len_str.trim().parse().ok()?;

        if len < 0 {
            return Some((RespValue::BulkString(None), 1 + pos + 2));
        }

        let len = len as usize;
        let start = 1 + pos + 2;
        if buf.len() < start + len + 2 {
            return None;
        }

        let data = buf[start..start + len].to_vec();
        Some((RespValue::BulkString(Some(data)), start + len + 2))
    }

    fn parse_array(buf: &[u8]) -> Option<(RespValue, usize)> {
        let pos = Self::find_crlf(&buf[1..])?;
        let len_str = std::str::from_utf8(&buf[1..1 + pos]).ok()?;
        let count: i64 = len_str.trim().parse().ok()?;

        if count < 0 {
            return Some((RespValue::Array(None), 1 + pos + 2));
        }

        let count = count as usize;
        let mut offset = 1 + pos + 2;
        let mut items = Vec::with_capacity(count);

        for _ in 0..count {
            let (val, consumed) = Self::parse_value(&buf[offset..])?;
            items.push(val);
            offset += consumed;
        }

        Some((RespValue::Array(Some(items)), offset))
    }

    fn parse_inline(buf: &[u8]) -> Option<(RespValue, usize)> {
        let pos = Self::find_crlf(buf)?;
        let line = std::str::from_utf8(&buf[..pos]).ok()?;
        let parts: Vec<RespValue> = line
            .split_whitespace()
            .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
            .collect();

        if parts.is_empty() {
            return Some((RespValue::Array(Some(vec![])), pos + 2));
        }

        Some((RespValue::Array(Some(parts)), pos + 2))
    }
}

pub struct RespWriter;

impl RespWriter {
    pub fn write(value: &RespValue) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::estimate_size(value));
        Self::write_value(value, &mut out);
        out
    }

    /// Estimate serialized byte size to pre-allocate buffer capacity.
    fn estimate_size(value: &RespValue) -> usize {
        match value {
            RespValue::SimpleString(s) => 3 + s.len(),
            RespValue::Error(s) => 3 + s.len(),
            RespValue::Integer(_) => 24,
            RespValue::BulkString(None) => 5,
            RespValue::BulkString(Some(d)) => 16 + d.len(),
            RespValue::Array(None) => 5,
            RespValue::Array(Some(items)) => {
                16 + items.iter().map(Self::estimate_size).sum::<usize>()
            }
        }
    }

    fn write_value(value: &RespValue, out: &mut Vec<u8>) {
        match value {
            RespValue::SimpleString(s) => {
                out.push(b'+');
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                out.push(b':');
                write_i64(*n, out);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                out.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                out.push(b'$');
                write_usize(data.len(), out);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                out.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                out.push(b'*');
                write_usize(items.len(), out);
                out.extend_from_slice(b"\r\n");
                for item in items {
                    Self::write_value(item, out);
                }
            }
        }
    }
}

/// Write an i64 as ASCII decimal bytes without heap allocation.
fn write_i64(mut n: i64, out: &mut Vec<u8>) {
    if n == 0 {
        out.push(b'0');
        return;
    }
    if n < 0 {
        out.push(b'-');
        // handle i64::MIN specially (negation would overflow)
        if n == i64::MIN {
            out.extend_from_slice(b"9223372036854775808");
            return;
        }
        n = -n;
    }
    write_usize(n as usize, out);
}

/// Write a usize as ASCII decimal bytes without heap allocation.
fn write_usize(n: usize, out: &mut Vec<u8>) {
    if n == 0 {
        out.push(b'0');
        return;
    }
    let mut buf = [0u8; 20];
    let mut i = 20usize;
    let mut val = n;
    while val > 0 {
        i -= 1;
        buf[i] = b'0' + (val % 10) as u8;
        val /= 10;
    }
    out.extend_from_slice(&buf[i..]);
}
