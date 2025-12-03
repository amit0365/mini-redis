use std::{collections::{BTreeMap, HashMap}, time::{Instant, SystemTime, UNIX_EPOCH}};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub enum RedisValue{
    String(String),
    StringWithTimeout((String, Instant)),
    Stream(StreamValue<String, String>),
}

#[derive(Clone, Serialize)]
pub struct StreamValue<K, V>{
    last_id: K,
    time_map: HashMap<u128, u64>, //time -> last seqquence number
    map: BTreeMap<K, (u128, u64, Vec<(K, V)>)>,
    waiters_value: (K, Vec<(K, V)>)
}

impl StreamValue<String, String>{
    pub fn new() -> Self {
        StreamValue { last_id: String::new(), time_map: HashMap::new(), map: BTreeMap::new() , waiters_value: (String::new(), Vec::new())}
    }

    pub fn new_blocked(id: &String, pairs_flattened: &Vec<String>) -> Self {
        let pairs_grouped = pairs_flattened
        .chunks_exact(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect::<Vec<(String, String)>>();
        StreamValue { last_id: String::new(), time_map: HashMap::new(), map: BTreeMap::new() , waiters_value: (id.clone(), pairs_grouped)}
    }

    pub fn insert(&mut self, id: &String, id_time: u128, id_seq: u64, pairs_flattened: &Vec<String>) -> Option<(u128, u64, Vec<(String, String)>)> {
        let pairs_grouped = pairs_flattened
        .chunks_exact(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect::<Vec<(String, String)>>();
        
        self.map.insert(id.clone(), (id_time, id_seq, pairs_grouped))
    }
}

impl RedisValue{
    pub fn as_string(&self) -> Option<&String> {
        match self{
            RedisValue::String(s) => Some(s),
            RedisValue::StringWithTimeout((s, _)) => Some(s),
            RedisValue::Stream(_) => None,
        }
    }

    pub fn get_blocked_result(&self) -> Option<Vec<Value>> {
        match self{
            RedisValue::String(_) => None,
            RedisValue::StringWithTimeout((_, _)) => None,
            RedisValue::Stream(val) => {
                let (id, pairs) = &val.waiters_value;
                let pairs_flattened = pairs.iter().flat_map(|(id, pair)| [id, pair]).collect::<Vec<_>>();
                Some(vec![json!([id, pairs_flattened])])
            },
        }
    }

    pub fn get_stream_range(&self, start_id: &String, stop_id: Option<&String>) -> Vec<Value>{
        match self{
            RedisValue::String(_) => Vec::new(), // fix error handling,
            RedisValue::StringWithTimeout((_, _)) => Vec::new(), // fix error handling,
            RedisValue::Stream(stream) => {
                let mut entries = Vec::new();
                if let Some((start_id_pre, start_id_post)) = start_id.split_once("-"){
                    let stop_time: Option<u128>;
                    let stop_seq: Option<u64>;

                    match stop_id{
                        Some(stop_id ) => {
                            match stop_id.as_str(){
                                "+" => {
                                    stop_time = None;
                                    stop_seq = None;
                                },
                                _ => {
                                    match stop_id.split_once("-"){
                                        Some((stop_id_pre, stop_id_post)) => {
                                            stop_time = Some(stop_id_pre.parse::<u128>().unwrap());
                                            stop_seq = Some(stop_id_post.parse::<u64>().unwrap());
                                        },

                                        None => return Vec::new() // fix error handling,
                                    } 
                                },
                            }
                        },

                        None => {
                            stop_time = None;
                            stop_seq = None;
                        },
                    }

                    let start_time: u128;
                    let start_seq: u64;
                    if start_id.as_str() == "-"{
                        start_time = 0;
                        start_seq = 0;
                    } else {
                        start_time = start_id_pre.parse::<u128>().unwrap();
                        start_seq = start_id_post.parse::<u64>().unwrap();
                    }

                    stream.map.iter().for_each(|e| {
                        let (time, seq, pairs) = e.1;
                        let flattened = pairs.iter().flat_map(|(k, v)| [k.clone(), v.clone()]).collect::<Vec<String>>();

                        let result = if stop_time.is_some() && stop_seq.is_some() && stop_id.is_some(){
                            *time >= start_time && *time <= stop_time.unwrap() && *seq >= start_seq && *seq <= stop_seq.unwrap()
                        } else if stop_time.is_none() && stop_seq.is_none() && stop_id.is_some(){
                            *time >= start_time && *seq >= start_seq 
                        } else { // xread
                            *time > start_time || *seq > start_seq
                        };

                        if result { entries.push(json!([e.0, flattened]));}
                    });
                }

                entries
            }
        }
    }

    pub fn update_stream(&mut self, id: &String, pairs_flattened: &Vec<String>) -> String{
        match self{
            RedisValue::String(_) => format!("not supported"),
            RedisValue::StringWithTimeout((_, _)) => format!("not supported"),
            RedisValue::Stream(stream) => {
                let mut new_id = id.clone();
                let mut new_id_time = 0;
                let mut new_id_seq = 0;
                match id.as_str(){
                    "*" => {
                        let millis = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();

                        let id_sequence_num;
                        if let Some(last_sequence_num) = stream.time_map.get(&millis){
                            id_sequence_num = last_sequence_num + 1;
                        } else { 
                            id_sequence_num = 0
                        }

                        new_id_time = millis;
                        new_id_seq = id_sequence_num;
                        new_id = new_id_time.to_string() + "-" + &new_id_seq.to_string();
                    },
                    _ => {
                        if let Some((id_pre, id_post)) = id.split_once("-"){
                            if id_pre.is_empty() || id_post.is_empty(){
                                return format!("-ERR The ID");
                            }

                            let id_millisecs = id_pre.parse::<u128>().unwrap(); 
                            let mut id_sequence_num: u64 = 0;
                            match id_post{
                                "*" => {
                                    if let Some(last_sequence_num) = stream.time_map.get(&id_millisecs){
                                        id_sequence_num = last_sequence_num + 1;
                                    } else { 
                                        if id_millisecs == 0 { id_sequence_num = 1 }
                                    }

                                    new_id_time = id_millisecs;
                                    new_id_seq = id_sequence_num;
                                    new_id = new_id_time.to_string() + "-" + &new_id_seq.to_string();
                                },
                                _ => id_sequence_num = id_post.parse::<u64>().unwrap(),
                            }

                            if id_millisecs == 0 && id_sequence_num == 0 { //empty stream
                                return format!("-ERR The ID specified in XADD must be greater than 0-0\r\n")
                            }
            
                            let last_id = &stream.last_id;
                            if last_id.is_empty(){ //new entry
                                stream.insert(&new_id, id_millisecs, id_sequence_num, pairs_flattened);
                                stream.last_id = new_id.clone();
                                return format!("${}\r\n{}\r\n", new_id.len(), new_id)
                            }
                        
                            if let Some((last_id_pre, last_id_post)) = last_id.split_once("-"){
                                if last_id_pre.is_empty() || last_id_post.is_empty(){
                                    return format!("-ERR The ID");
                                }

                                let last_id_millisecs = last_id_pre.parse::<u128>().unwrap(); 
                                let last_id_sequence_num = last_id_post.parse::<u64>().unwrap();
                            
                                if last_id_millisecs > id_millisecs{
                                    return format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                } else if last_id_millisecs == id_millisecs {
                                    if last_id_sequence_num >= id_sequence_num {
                                        return format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                    }
                                }
                            }

                            if let Some(last_sequence_num) = stream.time_map.get(&id_millisecs) { // update top sequence num for a give time
                                if id_sequence_num > *last_sequence_num { stream.time_map.insert(id_millisecs, id_sequence_num);}
                            } else {
                                stream.time_map.insert(id_millisecs, id_sequence_num);
                            }

                            stream.last_id = new_id.clone(); //update id
                            new_id_time = id_millisecs;
                            new_id_seq = id_sequence_num;
                        }
                    },
                }

                stream.insert(&new_id, new_id_time, new_id_seq, pairs_flattened);
                format!("${}\r\n{}\r\n", new_id.len(), new_id)
            }
        }
    }
}