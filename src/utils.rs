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
        println!("toks {:?}", tokens);
        let commands = tokens
            .iter()
            .skip(2)
            .step_by(2)
            .filter(|x| !x.is_empty())
            .map(|str| str.to_string()) 
            .collect::<Vec<String>>();
    
        Some(commands)
    }