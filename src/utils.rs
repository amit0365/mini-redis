use serde_json::Value;
use std::str::from_utf8;
use std::sync::Arc;

use crate::protocol::RedisValue;
use crate::error::{RedisError, RedisResult};

pub const EMPTY_RDB_FILE: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct ServerConfig {
    pub port: Arc<str>,
    pub master_contact_for_slave: Option<Arc<str>>,
}

impl ServerConfig {
    pub fn parse_from_args(args: Vec<String>) -> Self {
        let mut port = "6379";
        let mut master_contact_for_slave: Option<Arc<str>> = None;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    port = args[i + 1].as_str();
                    i += 2;
                },
                "--replicaof" => {
                    master_contact_for_slave = Some(Arc::from(args[i + 1].as_str()));
                    i += 2;
                },
                _ => i += 1,
            }
        }

        ServerConfig {
            port: Arc::from(port),
            master_contact_for_slave,
        }
    }
}

pub fn collect_as_strings<I>(iter: I) -> Vec<Arc<str>>
    where 
        I: IntoIterator<Item = RedisValue>
    {
        iter.into_iter()
        .filter_map(|v| v.as_string().cloned())
        .collect::<Vec<_>>()
    }

pub fn parse_wrapback(idx: i64, len: usize) -> RedisResult<usize> {
        if idx.is_negative() {
            let idx_abs = idx.unsigned_abs() as usize;
            if idx_abs <= len {
                Ok(len - idx.unsigned_abs() as usize)
            } else {
                Ok(0)
            }
        } else {
            idx.try_into().map_err(|_| RedisError::ParseInt(format!("Failed to convert {} to usize", idx)))
        }
    }

pub fn encode_resp_array_arc(array: &[Arc<str>]) -> String{
        let mut encoded_array = format!["*{}\r\n", array.len()];
        for item in array {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }
        encoded_array
}

pub fn encode_resp_ref_array_arc(array: &[&Arc<str>]) -> String{
        let mut encoded_array = format!["*{}\r\n", array.len()];
        for item in array {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }
        encoded_array
}

pub fn encode_resp_array_str(array: &[&str]) -> String{
        let mut encoded_array = format!["*{}\r\n", array.len()];
        for item in array {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }
        encoded_array
}

pub fn encode_resp_array_str_to_arc(array: &[&str]) -> Arc<str>{
        let mut encoded_array = format!["*{}\r\n", array.len()];
        for item in array {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }
        Arc::from(encoded_array)
}

pub fn encode_resp_array_arc_with_prefix(prefix: &[Arc<str>], arc_message: &Arc<Vec<Arc<str>>>) -> String {
        let total_len = prefix.len() + arc_message.len();
        let mut encoded_array = format!["*{}\r\n", total_len];

        // Encode prefix items
        for item in prefix {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }

        // Encode Arc items without cloning - just read through the Arc
        for item in arc_message.as_ref() {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }

        encoded_array
    }
    
pub fn encode_resp_value_array(encoded_array: &mut String, array: &Vec<Value>) {
        encoded_array.push_str(&format!["*{}\r\n", array.len()]);
        for item in array {
            match item{
                Value::Array(val) => {
                    encode_resp_value_array(encoded_array, val);
                },
                Value::String(s) => encoded_array.push_str(&format!("${}\r\n{}\r\n", s.len(), s)),
                _ => (), //not supported
            }
        }
    }
    
pub fn parse_resp(buf: &[u8]) -> RedisResult<Vec<Arc<str>>> {
    let string_buf = std::str::from_utf8(buf)?;
    let tokens = string_buf.split("\r\n").collect::<Vec<&str>>();
    let commands = tokens
        .iter()
        .skip(2) //skip msg len
        .step_by(2) //skip \r\n
        .filter(|x| !x.is_empty())
        .map(|str| Arc::from(*str))
        .collect::<Vec<Arc<str>>>();

    Ok(commands)
}

pub fn parse_multiple_resp(buf: &[u8]) -> RedisResult<Vec<Vec<Arc<str>>>> {
    let mut result = Vec::new();
    let mut pos = 0;

    while pos < buf.len() {
        // Skip any whitespace or incomplete data
        if buf[pos] != b'*' {
            break;
        }

        // Parse array size
        let mut i = pos + 1;
        while i < buf.len() && buf[i] != b'\r' {
            i += 1;
        }

        if i >= buf.len() - 1 {
            break;
        }

        let array_size_str = std::str::from_utf8(&buf[pos + 1..i])
            .map_err(|e| RedisError::InvalidUtf8(e.to_string()))?;

        let array_size: usize = array_size_str.parse()
            .map_err(|e: std::num::ParseIntError| RedisError::ParseInt(e.to_string()))?;

        // Move past *N\r\n
        i += 2; // skip \r\n

        let mut commands = Vec::new();

        // Parse each bulk string in the array
        for _ in 0..array_size {
            if i >= buf.len() || buf[i] != b'$' {
                return Ok(result); // Incomplete command, return what we have
            }

            // Parse bulk string length
            let mut j = i + 1;
            while j < buf.len() && buf[j] != b'\r' {
                j += 1;
            }

            if j >= buf.len() - 1 {
                return Ok(result); // Incomplete
            }

            let length_str = std::str::from_utf8(&buf[i + 1..j])
                .map_err(|e| RedisError::InvalidUtf8(e.to_string()))?;

            let length: usize = length_str.parse()
                .map_err(|e: std::num::ParseIntError| RedisError::ParseInt(e.to_string()))?;

            // Move past $N\r\n
            j += 2;

            if j + length > buf.len() {
                return Ok(result); // Incomplete
            }

            // Extract the actual string
            let command = std::str::from_utf8(&buf[j..j + length])
                .map_err(|e| RedisError::InvalidUtf8(e.to_string()))?;
            commands.push(Arc::from(command));

            // Move past the string and \r\n
            i = j + length + 2;
        }

        result.push(commands);
        pos = i;
    }

    Ok(result)
}

pub fn parse_rdb_with_trailing_commands(
    buf: &[u8],
    rdb_start: usize,
) -> RedisResult<usize> {
    if rdb_start >= buf.len() || buf[rdb_start] != b'$' {
        return Err(RedisError::InvalidRespFormat("Invalid RDB format".to_string()));
    }

    let size_end = buf[rdb_start..].windows(2).position(|w| w == b"\r\n")
        .ok_or_else(|| RedisError::InvalidRespFormat("Missing CRLF in RDB".to_string()))?;
    let size_end = rdb_start + size_end;

    let size_str = from_utf8(&buf[rdb_start + 1..size_end])?;
    let rdb_size = size_str.parse::<usize>()?;

    let rdb_data_start = size_end + 2;
    let rdb_end = rdb_data_start + rdb_size;

    Ok(rdb_end)
}

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

pub fn encode_coordinates_to_score(latitude: f64, longitude: f64) -> u64 {
    // Normalize to the range 0-2^26
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Truncate to integers
    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}