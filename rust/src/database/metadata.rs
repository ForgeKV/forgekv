#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisType {
    None = 0,
    String = 1,
    Hash = 2,
    List = 3,
    Set = 4,
    ZSet = 5,
}

impl RedisType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => RedisType::String,
            2 => RedisType::Hash,
            3 => RedisType::List,
            4 => RedisType::Set,
            5 => RedisType::ZSet,
            _ => RedisType::None,
        }
    }
}

pub struct RedisMetadata {
    pub r#type: RedisType,
    pub count: i64,
    pub expiry_ms: i64, // 0 = no expiry
    pub list_head: i64,
    pub list_tail: i64,
    pub version: u64,   // Dragonfly-compatible monotonic version counter
}

impl RedisMetadata {
    /// Serialize: [type:1][count:8LE][expiryMs:8LE][listHead:8LE][listTail:8LE][version:8LE] = 41 bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(41);
        buf.push(self.r#type as u8);
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&self.expiry_ms.to_le_bytes());
        buf.extend_from_slice(&self.list_head.to_le_bytes());
        buf.extend_from_slice(&self.list_tail.to_le_bytes());
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let r#type = RedisType::from_u8(data[0]);
        let count = i64::from_le_bytes(data[1..9].try_into().unwrap());
        let expiry_ms = i64::from_le_bytes(data[9..17].try_into().unwrap());
        let list_head = i64::from_le_bytes(data[17..25].try_into().unwrap());
        let list_tail = i64::from_le_bytes(data[25..33].try_into().unwrap());
        // Backward compatible: old format is 33 bytes, new is 41
        let version = if data.len() >= 41 {
            u64::from_le_bytes(data[33..41].try_into().unwrap())
        } else {
            1
        };
        RedisMetadata {
            r#type,
            count,
            expiry_ms,
            list_head,
            list_tail,
            version,
        }
    }
}
