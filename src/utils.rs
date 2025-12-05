use serde_json::Value;

use crate::protocol::RedisValue;

pub fn collect_as_strings<I>(iter: I) -> Vec<String>
    where 
        I: IntoIterator<Item = RedisValue>
    {
        iter.into_iter()
        .filter_map(|v| v.as_string().cloned())
        .collect::<Vec<String>>()
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
    
pub fn encode_resp_array(array: &Vec<String>) -> String{
        let mut encoded_array = format!["*{}\r\n", array.len()];
        for item in array {
            encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
        }
        encoded_array
    }
    
pub fn encode_resp_value_array(encoded_array: &mut String, array: &Vec<Value>) -> String{
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
    
        encoded_array.clone()
    }
    
pub fn parse_resp(buf: &[u8]) -> Option<Vec<String>>{
    let string_buf = std::str::from_utf8(buf).unwrap();
    let tokens = string_buf.split("\r\n").collect::<Vec<&str>>();
    let commands = tokens
        .iter()
        .skip(2) //skip msg len
        .step_by(2) //skip \r\n
        .filter(|x| !x.is_empty())
        .map(|str| str.to_string())
        .collect::<Vec<String>>();

    Some(commands)
}

pub fn parse_multiple_resp(buf: &[u8]) -> Vec<Vec<String>> {
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
            commands.push(command.to_string());

            // Move past the string and \r\n
            i = j + length + 2;
        }

        result.push(commands);
        pos = i;
    }

    result
}