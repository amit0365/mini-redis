use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{RedisError, RedisResult};
use crate::protocol::RedisValue;

const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;

const RDB_TYPE_STRING: u8 = 0;

pub struct RdbParser<R> {
    reader: R,
}

impl<R: Read> RdbParser<R> {
    pub fn new(reader: R) -> Self {
        RdbParser { reader }
    }

    fn read_byte(&mut self) -> RedisResult<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)
            .map_err(|e| RedisError::Other(format!("RDB read error: {}", e)))?;
        Ok(buf[0])
    }

    fn read_bytes(&mut self, n: usize) -> RedisResult<Vec<u8>> {
        let mut buf = vec![0u8; n];
        self.reader.read_exact(&mut buf)
            .map_err(|e| RedisError::Other(format!("RDB read error: {}", e)))?;
        Ok(buf)
    }

    fn read_length(&mut self) -> RedisResult<(usize, bool)> {
        let first = self.read_byte()?;
        let enc_type = (first & 0xC0) >> 6;

        match enc_type {
            0b00 => Ok(((first & 0x3F) as usize, false)),
            0b01 => {
                let second = self.read_byte()?;
                Ok(((((first & 0x3F) as usize) << 8) | (second as usize), false))
            }
            0b10 => {
                let bytes = self.read_bytes(4)?;
                Ok((u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize, false))
            }
            0b11 => Ok(((first & 0x3F) as usize, true)),
            _ => unreachable!(),
        }
    }

    fn read_string(&mut self) -> RedisResult<Arc<str>> {
        let (len_or_type, is_special) = self.read_length()?;

        if is_special {
            match len_or_type {
                0 => Ok(Arc::from(self.read_byte()?.to_string())),
                1 => {
                    let bytes = self.read_bytes(2)?;
                    Ok(Arc::from(i16::from_le_bytes([bytes[0], bytes[1]]).to_string()))
                }
                2 => {
                    let bytes = self.read_bytes(4)?;
                    Ok(Arc::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string()))
                }
                _ => Err(RedisError::Other("Unsupported string encoding".to_string())),
            }
        } else {
            let bytes = self.read_bytes(len_or_type)?;
            String::from_utf8(bytes)
                .map(|s| Arc::from(s))
                .map_err(|e| RedisError::Other(format!("Invalid UTF-8: {}", e)))
        }
    }

    pub fn parse(&mut self) -> RedisResult<HashMap<Arc<str>, RedisValue>> {
        // Skip header (REDIS + 4 digit version)
        self.read_bytes(9)?;

        let mut data: HashMap<Arc<str>, RedisValue> = HashMap::new();
        let mut current_expiry: Option<Instant> = None;

        loop {
            let opcode = self.read_byte()?;

            match opcode {
                RDB_OPCODE_AUX => {
                    self.read_string()?;
                    self.read_string()?;
                }
                RDB_OPCODE_SELECTDB => {
                    self.read_length()?;
                }
                RDB_OPCODE_RESIZEDB => {
                    self.read_length()?;
                    self.read_length()?;
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    let bytes = self.read_bytes(8)?;
                    let expire_ms = u64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3],
                        bytes[4], bytes[5], bytes[6], bytes[7],
                    ]);
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_millis() as u64;
                    current_expiry = Some(if expire_ms > now_ms {
                        Instant::now() + Duration::from_millis(expire_ms - now_ms)
                    } else {
                        Instant::now() - Duration::from_millis(1)
                    });
                }
                RDB_OPCODE_EXPIRETIME => {
                    let bytes = self.read_bytes(4)?;
                    let expire_secs = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64;
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();
                    current_expiry = Some(if expire_secs > now_secs {
                        Instant::now() + Duration::from_secs(expire_secs - now_secs)
                    } else {
                        Instant::now() - Duration::from_secs(1)
                    });
                }
                RDB_OPCODE_EOF => break,
                RDB_TYPE_STRING => {
                    let key = self.read_string()?;
                    let value = self.read_string()?;

                    if let Some(expiry) = current_expiry.take() {
                        if expiry > Instant::now() {
                            data.insert(key, RedisValue::StringWithTimeout((value, expiry)));
                        }
                    } else {
                        data.insert(key, RedisValue::String(value));
                    }
                }
                _ => {
                    return Err(RedisError::Other(format!("Unsupported RDB value type: {}", opcode)));
                }
            }
        }

        Ok(data)
    }
}

pub fn load_rdb_file(dir: &str, filename: &str) -> RedisResult<HashMap<Arc<str>, RedisValue>> {
    let path = Path::new(dir).join(filename);

    if !path.exists() {
        return Ok(HashMap::new());
    }

    let file = File::open(&path)
        .map_err(|e| RedisError::Other(format!("Failed to open RDB: {}", e)))?;

    RdbParser::new(BufReader::new(file)).parse()
}
