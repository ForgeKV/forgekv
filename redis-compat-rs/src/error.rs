use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("WRONGTYPE {0}")]
    WrongType(String),
    #[error("ERR invalid argument: {0}")]
    InvalidArgument(String),
    #[error("ERR key not found: {0}")]
    KeyNotFound(String),
    #[error("ERR value out of range: {0}")]
    OutOfRange(String),
    #[error("ERR syntax error: {0}")]
    SyntaxError(String),
}

pub type RedisResult<T> = Result<T, RedisError>;
