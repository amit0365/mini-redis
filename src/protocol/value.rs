use std::{collections::{BTreeMap, HashMap}, fmt, sync::Arc, time::{Instant, SystemTime, UNIX_EPOCH}};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub enum RedisValue{
    String(Arc<str>),
    Number(u64),
    StringWithTimeout((Arc<str>, Instant)),
    Stream(StreamValue<Arc<str>, Arc<str>>),
    Commands(Vec<Arc<str>>),
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

impl StreamValue<Arc<str>, Arc<str>>{
    pub fn new() -> Self {
        StreamValue { last_id: Arc::from(""), time_map: HashMap::new(), map: BTreeMap::new() , waiters_value: (Arc::from(""), Vec::new())}
    }

    pub fn new_blocked(id: Arc<str>, pairs_grouped: &Vec<(Arc<str>, Arc<str>)>) -> Self {
        StreamValue { last_id: Arc::from(""), time_map: HashMap::new(), map: BTreeMap::new() , waiters_value: (id, pairs_grouped.to_vec())}
    }

    pub fn insert(&mut self, id: Arc<str>, id_time: u128, id_seq: u64, pairs_grouped: Vec<(Arc<str>, Arc<str>)>) -> Option<(u128, u64, Vec<(Arc<str>, Arc<str>)>)> {
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

pub fn redis_value_as_string(val: RedisValue) -> Option<Arc<str>> {
    match val{
        RedisValue::String(s) => Some(s),
        RedisValue::StringWithTimeout((s, _)) => Some(s),
        RedisValue::Stream(_) => None,
        RedisValue::Number(_) => None,
        RedisValue::Commands(_) => None,
    }
}

impl RedisValue{
    pub fn as_string(&self) -> Option<&Arc<str>> {
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
                let pairs_flattened = pairs.iter().flat_map(|(k, v)| [k.as_ref(), v.as_ref()]).collect::<Vec<&str>>();
                Some(vec![json!([id.as_ref(), pairs_flattened])])
            },
            RedisValue::Commands(_) => None,
        }
    }

    pub fn get_stream_range(&self, start_id: &str, stop_id: Option<&str>) -> Vec<Value>{
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
                            match stop_id{
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
                    if start_id == "-"{
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
                            let flattened = pairs.iter().flat_map(|(k, v)| [k.as_ref(), v.as_ref()]).collect::<Vec<&str>>();
                            entries.push(json!([e.0.as_ref(), flattened]));
                        }
                    });
                }

                entries
            },
            RedisValue::Commands(_) => Vec::new(),
        }
    }

    pub fn update_stream(&mut self, id: &Arc<str>, pairs: Vec<(Arc<str>, Arc<str>)>) -> String{
        match self{
            RedisValue::String(_) | RedisValue::Number(_) |
            RedisValue::StringWithTimeout(_) | RedisValue::Commands(_) => {
                format!("not supported")
            },
            RedisValue::Stream(stream) => {
                let (new_id_time, new_id_seq) = match id.as_ref(){
                    "*" => {
                        let millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();

                        let id_sequence_num = stream.time_map
                            .get(&millis)
                            .map(|&seq| seq + 1)
                            .unwrap_or(0);

                        (millis, id_sequence_num)
                    },
                    _ => {
                        let (id_pre, id_post) = match id.split_once("-") {
                            Some(parts) => parts,
                            None => return format!("-ERR The ID"),
                        };

                        if id_pre.is_empty() || id_post.is_empty(){
                            return format!("-ERR The ID");
                        }

                        let id_millisecs = id_pre.parse::<u128>().unwrap();
                        let id_sequence_num = match id_post{
                            "*" => {
                                stream.time_map
                                    .get(&id_millisecs)
                                    .map(|&seq| seq + 1)
                                    .unwrap_or(if id_millisecs == 0 { 1 } else { 0 })
                            },
                            _ => id_post.parse::<u64>().unwrap(),
                        };

                        if id_millisecs == 0 && id_sequence_num == 0 {
                            return format!("-ERR The ID specified in XADD must be greater than 0-0\r\n")
                        }

                        if !stream.last_id.is_empty() {
                            if let Some((last_id_pre, last_id_post)) = stream.last_id.split_once("-"){
                                if !last_id_pre.is_empty() && !last_id_post.is_empty(){
                                    let last_id_millisecs = last_id_pre.parse::<u128>().unwrap();
                                    let last_id_sequence_num = last_id_post.parse::<u64>().unwrap();

                                    if last_id_millisecs > id_millisecs ||
                                       (last_id_millisecs == id_millisecs && last_id_sequence_num >= id_sequence_num) {
                                        return format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                    }
                                }
                            }
                        }

                        stream.time_map
                            .entry(id_millisecs)
                            .and_modify(|seq| *seq = (*seq).max(id_sequence_num))
                            .or_insert(id_sequence_num);

                        (id_millisecs, id_sequence_num)
                    },
                };

                let new_id_string = format!("{}-{}", new_id_time, new_id_seq);
                let new_id_arc: Arc<str> = Arc::from(new_id_string.as_str());
                stream.last_id = new_id_arc.clone();
                stream.insert(new_id_arc, new_id_time, new_id_seq, pairs);

                format!("${}\r\n{}\r\n", new_id_string.len(), new_id_string)
            },
        }
    }
}