use std::fmt;
use std::io;

#[derive(Debug, Clone)]
pub enum RedisError {
    Io(String),
    InvalidUtf8(String),
    ParseInt(String),
    ParseFloat(String),
    InvalidCommand(String),
    InvalidRespFormat(String),
    InvalidStreamId(String),
    WrongType(String),
    KeyNotFound(String),
    LockPoisoned(String),
    ChannelSend(String),
    ConnectionClosed,
    TooManyWaiters,
    Base64Decode(String),
    Other(String),
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisError::Io(e) => write!(f, "IO error: {}", e),
            RedisError::InvalidUtf8(e) => write!(f, "UTF-8 error: {}", e),
            RedisError::ParseInt(e) => write!(f, "Parse int error: {}", e),
            RedisError::ParseFloat(e) => write!(f, "Parse float error: {}", e),
            RedisError::InvalidCommand(msg) => write!(f, "Invalid command: {}", msg),
            RedisError::InvalidRespFormat(msg) => write!(f, "Invalid RESP format: {}", msg),
            RedisError::InvalidStreamId(msg) => write!(f, "Invalid stream ID: {}", msg),
            RedisError::WrongType(msg) => write!(f, "Wrong type: {}", msg),
            RedisError::KeyNotFound(msg) => write!(f, "Key not found: {}", msg),
            RedisError::LockPoisoned(msg) => write!(f, "Lock poisoned: {}", msg),
            RedisError::ChannelSend(msg) => write!(f, "Channel send error: {}", msg),
            RedisError::ConnectionClosed => write!(f, "Connection closed"),
            RedisError::TooManyWaiters => write!(f, "ERR_TOO_MANY_WAITERS"),
            RedisError::Base64Decode(e) => write!(f, "Base64 decode error: {}", e),
            RedisError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for RedisError {}

impl From<io::Error> for RedisError {
    fn from(err: io::Error) -> Self {
        RedisError::Io(err.to_string())
    }
}

impl From<std::str::Utf8Error> for RedisError {
    fn from(err: std::str::Utf8Error) -> Self {
        RedisError::InvalidUtf8(err.to_string())
    }
}

impl From<std::num::ParseIntError> for RedisError {
    fn from(err: std::num::ParseIntError) -> Self {
        RedisError::ParseInt(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for RedisError {
    fn from(err: std::num::ParseFloatError) -> Self {
        RedisError::ParseFloat(err.to_string())
    }
}

impl From<base64::DecodeError> for RedisError {
    fn from(err: base64::DecodeError) -> Self {
        RedisError::Base64Decode(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for RedisError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        RedisError::LockPoisoned(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RedisError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        RedisError::ChannelSend(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for RedisError {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        RedisError::ChannelSend(err.to_string())
    }
}

pub type RedisResult<T> = Result<T, RedisError>;
