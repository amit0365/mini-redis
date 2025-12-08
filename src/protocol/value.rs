use std::{collections::{BTreeMap, HashMap}, fmt, sync::Arc, time::{Instant, SystemTime, UNIX_EPOCH}};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub enum RedisValue{
    String(String),
    Number(u64),
    StringWithTimeout((String, Instant)),
    Stream(StreamValue<String, String>),
    Commands(Vec<String>),
}

#[derive(Clone)]
pub struct StreamValue<K, V>{
    last_id: Arc<str>,
    time_map: HashMap<u128, u64>, //time -> last seqquence number
    map: BTreeMap<Arc<str>, (u128, u64, Vec<(K, V)>)>,
    waiters_value: (Arc<str>, Vec<(K, V)>)
}

impl<K: Serialize, V: Serialize> Serialize for StreamValue<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("StreamValue", 4)?;
        state.serialize_field("last_id", self.last_id.as_ref())?;
        state.serialize_field("time_map", &self.time_map)?;
        let map_serializable: BTreeMap<&str, _> = self.map.iter()
            .map(|(k, v)| (k.as_ref(), v))
            .collect();
        state.serialize_field("map", &map_serializable)?;
        state.serialize_field("waiters_value", &(self.waiters_value.0.as_ref(), &self.waiters_value.1))?;
        state.end()
    }
}

impl StreamValue<String, String>{
    pub fn new() -> Self {
        StreamValue { last_id: Arc::from(""), time_map: HashMap::new(), map: BTreeMap::new() , waiters_value: (Arc::from(""), Vec::new())}
    }

    pub fn new_blocked(id: Arc<str>, pairs_grouped: &Vec<(String, String)>) -> Self {
        StreamValue { last_id: Arc::from(""), time_map: HashMap::new(), map: BTreeMap::new() , waiters_value: (id, pairs_grouped.to_vec())}
    }

    pub fn insert(&mut self, id: Arc<str>, id_time: u128, id_seq: u64, pairs_grouped: Vec<(String, String)>) -> Option<(u128, u64, Vec<(String, String)>)> {
        self.map.insert(id, (id_time, id_seq, pairs_grouped))
    }
}

impl fmt::Display for RedisValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisValue::String(s) => write!(f, "{}", s),
            RedisValue::Number(n) => write!(f, "{}", n),
            RedisValue::StringWithTimeout((s, _)) => write!(f, "{}", s),
            RedisValue::Stream(_) => write!(f, "stream"),
            RedisValue::Commands(_) => write!(f, "commands"),
        }
    }
}

pub fn redis_value_as_string(val: RedisValue) -> Option<String> {
    match val{
        RedisValue::String(s) => Some(s),
        RedisValue::StringWithTimeout((s, _)) => Some(s),
        RedisValue::Stream(_) => None,
        RedisValue::Number(_) => None,
        RedisValue::Commands(_) => None,
    }
}

impl RedisValue{
    pub fn as_string(&self) -> Option<&String> {
        match self{
            RedisValue::String(s) => Some(s),
            RedisValue::StringWithTimeout((s, _)) => Some(s),
            RedisValue::Stream(_) => None,
            RedisValue::Number(_) => None,
            RedisValue::Commands(_) => None,
        }
    }

    pub fn get_blocked_result(&self) -> Option<Vec<Value>> {
        match self{
            RedisValue::String(_) => None,
            RedisValue::Number(_) => None,
            RedisValue::StringWithTimeout((_, _)) => None,
            RedisValue::Stream(val) => {
                let (id, pairs) = &val.waiters_value;
                let pairs_flattened = pairs.iter().flat_map(|(id, pair)| [id, pair]).collect::<Vec<_>>();
                Some(vec![json!([id.as_ref(), pairs_flattened])])
            },
            RedisValue::Commands(_) => None,
        }
    }

    pub fn get_stream_range(&self, start_id: &String, stop_id: Option<&String>) -> Vec<Value>{
        match self{
            RedisValue::String(_) => Vec::new(), // fix error handling,
            RedisValue::Number(_) => Vec::new(), // fix error handling,
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

                        let result = if stop_time.is_some() && stop_seq.is_some() && stop_id.is_some(){
                            *time >= start_time && *time <= stop_time.unwrap() && *seq >= start_seq && *seq <= stop_seq.unwrap()
                        } else if stop_time.is_none() && stop_seq.is_none() && stop_id.is_some(){
                            *time >= start_time && *seq >= start_seq
                        } else { // xread
                            *time > start_time || *seq > start_seq
                        };

                        if result {
                            let flattened = pairs.iter().flat_map(|(k, v)| [k.as_str(), v.as_str()]).collect::<Vec<&str>>();
                            entries.push(json!([e.0.as_ref(), flattened]));
                        }
                    });
                }

                entries
            },
            RedisValue::Commands(_) => Vec::new(),
        }
    }

    pub fn update_stream(&mut self, id: &String, pairs: Vec<(String, String)>) -> String{
        match self{
            RedisValue::String(_) => format!("not supported"),
            RedisValue::Number(_) => format!("not supported"),
            RedisValue::StringWithTimeout((_, _)) => format!("not supported"),
            RedisValue::Stream(stream) => {
                let mut new_id_string = id.clone();
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
                        new_id_string = new_id_time.to_string() + "-" + &new_id_seq.to_string();
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
                                    new_id_string = new_id_time.to_string() + "-" + &new_id_seq.to_string();
                                },
                                _ => id_sequence_num = id_post.parse::<u64>().unwrap(),
                            }

                            if id_millisecs == 0 && id_sequence_num == 0 { //empty stream
                                return format!("-ERR The ID specified in XADD must be greater than 0-0\r\n")
                            }

                            let last_id = &stream.last_id;
                            if last_id.is_empty(){ //new entry
                                let new_id_arc = Arc::from(new_id_string.as_str());
                                stream.insert(Arc::clone(&new_id_arc), id_millisecs, id_sequence_num, pairs);
                                stream.last_id = new_id_arc;
                                return format!("${}\r\n{}\r\n", new_id_string.len(), new_id_string)
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

                            let new_id_arc = Arc::from(new_id_string.as_str());
                            stream.last_id = Arc::clone(&new_id_arc);
                            new_id_time = id_millisecs;
                            new_id_seq = id_sequence_num;
                        }
                    },
                }

                let new_id_arc = Arc::from(new_id_string.as_str());
                stream.insert(new_id_arc, new_id_time, new_id_seq, pairs);
                format!("${}\r\n{}\r\n", new_id_string.len(), new_id_string)
            },
            RedisValue::Commands(_) => format!("not supported"),
        }
    }
}