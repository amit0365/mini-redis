use serde_json::Value;
use std::str::from_utf8;
use std::sync::Arc;

use crate::protocol::RedisValue;

pub const EMPTY_RDB_FILE: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub struct ServerConfig {
    pub port: String,
    pub master_contact_for_slave: Option<String>,
}

impl ServerConfig {
    pub fn parse_from_args(args: Vec<String>) -> Self {
        let mut port = "6379".to_string();
        let mut master_contact_for_slave: Option<String> = None;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    port = args[i + 1].to_owned();
                    i += 2;
                },
                "--replicaof" => {
                    master_contact_for_slave = Some(args[i + 1].to_owned());
                    i += 2;
                },
                _ => i += 1,
            }
        }

        ServerConfig {
            port,
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

pub fn parse_wrapback(idx: i64, len: usize) -> usize{
        if idx.is_negative() {
            let idx_abs= idx.unsigned_abs() as usize;
            if idx_abs <= len {
                len - idx.unsigned_abs() as usize
            } else {
                0
            }
        } else { idx.try_into().unwrap() }
    }

pub fn encode_resp_array_arc(array: &[Arc<str>]) -> String{
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
    
pub fn parse_resp(buf: &[u8]) -> Option<Vec<Arc<str>>>{
    let string_buf = std::str::from_utf8(buf).unwrap();
    let tokens = string_buf.split("\r\n").collect::<Vec<&str>>();
    let commands = tokens
        .iter()
        .skip(2) //skip msg len
        .step_by(2) //skip \r\n
        .filter(|x| !x.is_empty())
        .map(|str| Arc::from(*str))
        .collect::<Vec<Arc<str>>>();

    Some(commands)
}

pub fn parse_multiple_resp(buf: &[u8]) -> Vec<Vec<Arc<str>>> {
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

        let array_size_str = match std::str::from_utf8(&buf[pos + 1..i]) {
            Ok(s) => s,
            Err(_) => break,
        };

        let array_size: usize = match array_size_str.parse() {
            Ok(n) => n,
            Err(_) => break,
        };

        // Move past *N\r\n
        i += 2; // skip \r\n

        let mut commands = Vec::new();

        // Parse each bulk string in the array
        for _ in 0..array_size {
            if i >= buf.len() || buf[i] != b'$' {
                return result; // Incomplete command
            }

            // Parse bulk string length
            let mut j = i + 1;
            while j < buf.len() && buf[j] != b'\r' {
                j += 1;
            }

            if j >= buf.len() - 1 {
                return result; // Incomplete
            }

            let length_str = match std::str::from_utf8(&buf[i + 1..j]) {
                Ok(s) => s,
                Err(_) => return result,
            };

            let length: usize = match length_str.parse() {
                Ok(n) => n,
                Err(_) => return result,
            };

            // Move past $N\r\n
            j += 2;

            if j + length > buf.len() {
                return result; // Incomplete
            }

            // Extract the actual string
            let command = match std::str::from_utf8(&buf[j..j + length]) {
                Ok(s) => s,
                Err(_) => return result,
            };
            commands.push(Arc::from(command));

            // Move past the string and \r\n
            i = j + length + 2;
        }

        result.push(commands);
        pos = i;
    }

    result
}

pub fn parse_rdb_with_trailing_commands(
    buf: &[u8],
    rdb_start: usize,
) -> Option<usize> {
    if rdb_start >= buf.len() || buf[rdb_start] != b'$' {
        return None;
    }

    let size_end = buf[rdb_start..].windows(2).position(|w| w == b"\r\n")?;
    let size_end = rdb_start + size_end;

    let size_str = from_utf8(&buf[rdb_start + 1..size_end]).ok()?;
    let rdb_size = size_str.parse::<usize>().ok()?;

    let rdb_data_start = size_end + 2;
    let rdb_end = rdb_data_start + rdb_size;

    Some(rdb_end)
}