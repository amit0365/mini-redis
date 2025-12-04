use std::{collections::{HashMap, HashSet, VecDeque}, sync::{Arc, Mutex, RwLock}, time::{Duration, Instant}};
use base64::{Engine, engine::general_purpose};
use tokio::{sync::mpsc::{self, Receiver, Sender, error::TrySendError}, time::sleep};
use serde_json::json;

use crate::{EMPTY_RDB_FILE, protocol::{RedisValue, StreamValue}, utils::{collect_as_strings, encode_resp_array, encode_resp_value_array, parse_wrapback}};

#[derive(Clone)]
pub struct RedisState<K, RedisValue> {
    pub channels_state: ChannelState<K>,
    pub map_state: MapState<K, RedisValue>,
    pub list_state: ListState<K, RedisValue>,
    pub server_state: ServerState<K, RedisValue>
}

#[derive(Clone)]
pub struct ChannelState<K>{
    channels_map: Arc<RwLock<HashMap<K, (usize, HashSet<String>)>>>, 
    subscribers: Arc<Mutex<HashMap<K, Vec<Sender<(K, Vec<String>)>>>>>,
}

impl<K> ChannelState<K>{
    fn new() -> Self{
        let channels_map = Arc::new(RwLock::new(HashMap::new()));
        let subscribers = Arc::new(Mutex::new(HashMap::new()));
        ChannelState { channels_map, subscribers }
    }
}

#[derive(Clone)]
pub struct ServerState<K, V>{
    pub replication_mode: bool,
    pub write_commands: Vec<V>,
    pub map: HashMap<K, V>
}

impl ServerState<String, RedisValue>{
    fn new() ->Self{
        ServerState { replication_mode: false, write_commands: Vec::new(), map: HashMap::new() }
    }

    pub fn update(&mut self, pairs: &Vec<(String, RedisValue)>){
        let _ = pairs.iter().map(|(k, v)| self.map.insert(k.to_owned(), v.clone())).collect::<Vec<_>>();
    }
}

pub struct ClientState<K, V>{
    pub subscribe_mode: bool,
    pub multi_queue_mode: bool,
    pub replication_mode: bool,
    subscriptions: (usize, HashSet<V>),
    pub receiver: Option<Receiver<(K, Vec<String>)>>,
    pub sender: Option<Sender<(K, Vec<String>)>>,
    pub queued_commands: VecDeque<Vec<String>> 
}

impl ClientState<String, String>{
    pub fn new() -> Self{
        let subscribe_mode = false;
        let multi_queue_mode = false;
        let replication_mode = false;
        let subscriptions = (0, HashSet::new());
        ClientState { subscribe_mode, multi_queue_mode, replication_mode, subscriptions, receiver: None, sender: None , queued_commands: VecDeque::new() }
    }
}

#[derive(Clone)]
pub struct ListState<K, RedisValue>{
    list: Arc<Mutex<HashMap<K, VecDeque<RedisValue>>>>, // use rwlock instead
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>>,    
}

impl<K> ListState<K, RedisValue>{
    fn new() -> Self{
        let list = Arc::new(Mutex::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        ListState { list, waiters }
    }
}

#[derive(Clone)]
pub struct MapState<K, RedisValue>{
    pub map: Arc<RwLock<HashMap<K, RedisValue>>>, // add methods instread of pub fileds
    pub waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>>,    
}

impl<K> MapState<K, RedisValue>{
    fn new() -> Self{
        let map = Arc::new(RwLock::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        MapState { map, waiters }
    }
}

impl RedisState<String, RedisValue>{
    pub fn new() -> Self{
        let channels_state = ChannelState::new();
        let map_state = MapState::new();
        let list_state = ListState::new();
        let server_state = ServerState::new();
        RedisState { channels_state, map_state, list_state, server_state }
    }

    pub fn psync(&self) -> String {
        let repl_id = self.server_state.map.get("master_replid").unwrap();
        let offset = self.server_state.map.get("master_repl_offset").unwrap();
        let fullresync_response = format!("+FULLRESYNC {} {}\r\n", repl_id, offset);
        fullresync_response
    }

    pub fn set(&mut self, commands: &Vec<String>) -> String {
        match commands.iter().skip(3).next() {
            Some(str) => {
                match str.to_uppercase().as_str() {
                    "PX" => {
                        let timeout = Instant::now() + Duration::from_millis(commands[4].parse::<u64>().unwrap());
                        self.map_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)));
                    }
                    "EX" => {
                        let timeout = Instant::now() + Duration::from_secs(commands[4].parse::<u64>().unwrap());
                        self.map_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)));
                    }
                    _ => (),
                }
            }
            None => {
                let redis_val = match commands[2].parse::<u64>(){
                    Ok(num) => RedisValue::Number(num),
                    Err(_) => RedisValue::String(commands[2].to_owned()),
                };

                self.map_state.map.write().unwrap().insert(commands[1].to_owned(), redis_val);
            }
        }

        format!("+OK\r\n")
    }

    pub fn get(&mut self, commands: &Vec<String>) -> String {
        let value = self.map_state.map.read().unwrap().get(&commands[1]).cloned();
        if let Some(value) = value {
            match value {
                RedisValue::StringWithTimeout((value, timeout)) => {
                    if Instant::now() < timeout {
                        format!("${}\r\n{}\r\n", value.len(), value)
                    } else {
                        format!("$-1\r\n")
                    }
                }
                RedisValue::String(val) => {
                    format!("${}\r\n{}\r\n", val.len(), val)
                }
                RedisValue::Number(val) => {
                    format!("${}\r\n{}\r\n", val.to_string().len(), val)
                }
                _ => format!("$-1\r\n") // fix error handling
            }
        } else { format!("$-1\r\n")}
    } 

    pub fn rpush(&mut self, commands: &Vec<String>) -> String {
        let mut count = String::new();
        let key = &commands[1];

        {
            let mut list_guard = self.list_state.list.lock().unwrap();
            let items = commands.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
            list_guard.entry(key.clone())
            .or_insert(VecDeque::new())
            .extend(items);
            count = list_guard.get(key).unwrap().len().to_string();
        }
        
        let mut waiters_guard = self.list_state.waiters.lock().unwrap();
        if let Some(waiting_queue) = waiters_guard.get_mut(key){
            while let Some(sender) = waiting_queue.pop_front(){
                let mut list_guard = self.list_state.list.lock().unwrap();
                if let Some(deque) = list_guard.get_mut(key){
                    if let Some(value) = deque.pop_front(){
                        drop(list_guard); // Release lock before sending
                        match sender.try_send((key.clone(), value)){
                            Ok(_) => break,
                            Err(TrySendError::Full(_)) => return format!("ERR_TOO_MANY_WAITERS"),
                            Err(TrySendError::Closed(_)) => continue,
                        }
                    }
                }
            }
        }

        format!(":{}\r\n", count)
    } 

    pub fn lpush(&mut self, commands: &Vec<String>) -> String {
        let mut list_guard = self.list_state.list.lock().unwrap();
        let items = commands.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
        for item in items.iter() {
            list_guard.entry(commands[1].clone())
            .or_insert(VecDeque::new())
            .push_front(item.clone());
        }
        
        let count = list_guard.get(&commands[1]).unwrap().len();
        format!(":{}\r\n", count.to_string())
    } 

    pub fn llen(&self, commands: &Vec<String>) -> String {
        let list_guard = self.list_state.list.lock().unwrap();
        let len = match list_guard.get(&commands[1]){
            Some(list) => list.len().to_string(),
            None => 0.to_string()
        };

        format!(":{}\r\n", len)
    } 

    pub fn lpop(&mut self, commands: &Vec<String>) -> String {
        let mut list_guard = self.list_state.list.lock().unwrap();
        match list_guard.get_mut(&commands[1]){
            Some(list) => {
                match commands.iter().skip(2).next(){
                    Some(n) => {
                        let mut popped_list = Vec::new();
                        let len = n.parse::<usize>().unwrap(); //remove unwrap
                        for _ in 0..len{
                            match list.pop_front(){
                                Some(popped) => {
                                    if let Some(val) = popped.as_string(){
                                        popped_list.push(val.clone());
                                    }
                                }
                                None => (),
                            }
                        }

                        encode_resp_array(&popped_list)
                    },

                    None => {
                        match list.pop_front(){
                            Some(popped) => {
                                if let Some(val) = popped.as_string(){
                                    format!("${}\r\n{}\r\n", val.len(), val)
                                } else {format!("$-1\r\n")} // fix this
                            }
                            None => format!("$-1\r\n"),
                        }
                    },
                }
            },

            None => format!("$-1\r\n")
        }
    } 

    pub async fn blpop(&mut self, commands: &Vec<String>) -> String {
        let key = &commands[1];

        {
            let mut list_guard = self.list_state.list.lock().unwrap();
            if let Some(list) = list_guard.get_mut(key){
                if let Some(data) = list.pop_front(){
                    if let Some(val) = data.as_string(){
                        return encode_resp_array(&vec![key.clone(), val.clone()])
                    }
                }
            }
        }

        let mut receiver = {
            let mut waiters_guard = self.list_state.waiters.lock().unwrap();
            let queue = waiters_guard.entry(key.clone()).or_insert(VecDeque::new());
            if queue.len() > 10000 {
                return format!("ERR_TOO_MANY_BLPOP_WAITERS_FOR_THE_KEY")
            }

            let (sender, receiver) = mpsc::channel(1);
            queue.push_back(sender);
            receiver
        };

        let timeout: f64 = commands.last().unwrap().parse().unwrap(); // yo check this
        if timeout == 0.0 {
            match receiver.recv().await{
                Some((key, value)) => {
                    if let Some(val) = value.as_string(){
                        encode_resp_array(&vec![key, val.clone()])
                    } else {format!("$\r\n")} //fix this
                }
                None => format!("$\r\n"), //todo fix this
            }
        } else {
            tokio::select! {
                result = receiver.recv() => {
                    if let Some((key, value)) = result{
                        if let Some(val) = value.as_string(){
                            encode_resp_array(&vec![key, val.clone()])
                        } else {format!("$\r\n")} //fix this
                    } else {format!("*-1\r\n")} // none so disconnected or dropped
                }

                _ = sleep(Duration::from_secs_f64(timeout)) => {
                    format!("*-1\r\n") 
                }
            }
        }
    }
    
    pub fn lrange(&self, key: &String, start: &String, stop: &String) -> String {
        let list_guard = self.list_state.list.lock().unwrap();
        let array = match list_guard.get(key){
            Some(vec) => {
                let start = parse_wrapback(start.parse::<i64>().unwrap(), vec.len());
                let stop = parse_wrapback(stop.parse::<i64>().unwrap(), vec.len());

                if start >= vec.len(){
                    VecDeque::new()
                } else if stop >= vec.len(){
                    vec.range(start..).cloned().collect()
                } else if start > stop{
                    VecDeque::new() // removed stop=start should be covered by last else
                } else {
                    vec.range(start..=stop).cloned().collect()
                }
            },
            None => VecDeque::new()
        };

        encode_resp_array(&collect_as_strings(array))
    } 

    pub fn type_command(&self, commands: &Vec<String>) -> String {
        let map_guard = self.map_state.map.read().unwrap();
        let response = match map_guard.get(&commands[1]){
            Some(val) => {
                match val{
                    RedisValue::String(_) => "string".to_string(),
                    RedisValue::StringWithTimeout(_) => "string".to_string(),
                    RedisValue::Stream(_) => "stream".to_string(),
                    RedisValue::Number(_) => "number".to_string(),
                }
            },
            None => "none".to_string()
        };

        format!("+{}\r\n", response)
    } 

    pub fn xadd(&self, commands: &Vec<String>) -> String {
        let key = &commands[1];
        let mut result = String::new();
        let pairs_flattened = commands.iter().skip(3).cloned().collect::<Vec<String>>();

        {
            let mut map_guard = self.map_state.map.write().unwrap();
            result = map_guard.entry(key.clone())
            .or_insert(RedisValue::Stream(StreamValue::new()))
            .update_stream(&commands[2], &pairs_flattened);
        }

        let mut map_waiters_guard = self.map_state.waiters.lock().unwrap();
        if let Some(waiters_queue) = map_waiters_guard.get_mut(key){
            while let Some(waiter) = waiters_queue.pop_front(){
                match waiter.try_send((key.clone(), RedisValue::Stream(StreamValue::new_blocked(&commands[2], &pairs_flattened)))){
                    Ok(_) => break,
                    Err(TrySendError::Full(_)) => return format!("ERR_TOO_MANY_WAITERS"),
                    Err(TrySendError::Closed(_)) => continue,
                }
            }
        }

        result
    } 

    pub fn xrange(&self, commands: &Vec<String>) -> String {
        let mut encoded_array = String::new();
        let values = self.map_state.map.read().unwrap()
        .get(&commands[1])
        .unwrap() // no case for nil
        .get_stream_range(&commands[2], Some(&commands[3]));
        if !values.is_empty() {
            encode_resp_value_array(&mut encoded_array, &values)
        } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
    } 

    pub async fn xread(&self, commands: &Vec<String>) -> String {
        match commands[1].as_str(){
            "streams" => {
                let key_tokens = commands.iter().skip(2)
                .filter(|token| self.map_state.map.read().unwrap().get(token.as_str()).is_some())
                .collect::<Vec<_>>();
        
                let id_tokens = commands.iter().skip(2 + key_tokens.len()).collect::<Vec<_>>();
                let mut encoded_array = String::new();
                let mut key_entries = Vec::new();
        
                for (key, id) in key_tokens.iter().zip(id_tokens){
                    let values = self.map_state.map.read().unwrap()
                    .get(*key)
                    .unwrap() // no case for nil
                    .get_stream_range(id, None);
                    if !values.is_empty() { key_entries.push(json!([key, values]));}
                }
        
                if !key_entries.is_empty() {
                    encode_resp_value_array(&mut encoded_array, &key_entries)
                } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
            },

            "block" => {
                let timeout = &commands[2].parse::<u64>().unwrap();
                let key = &commands[4];

                let mut receiver = {
                    let mut waiters_guard = self.map_state.waiters.lock().unwrap();
                    let queue = waiters_guard.entry(key.to_owned()).or_insert(VecDeque::new());
                    if queue.len() > 10000{
                        return format!("ERR_TOO_MANY_XREAD_WAITERS_FOR_THE_KEY")
                    }

                    let (sender, receiver) = mpsc::channel(1);
                    queue.push_back(sender);
                    receiver
                };

                if *timeout == 0 {
                    match receiver.recv().await{
                        Some((key, redis_val)) => {
                            if let Some(value_array) = redis_val.get_blocked_result(){
                                let mut encoded_array = String::new();
                                return encode_resp_value_array(&mut encoded_array, &vec![json!([key, value_array])])
                            }
                            format!("*-1\r\n")
                        },
                        None => format!("*-1\r\n")
                    }
                } else {
                    tokio::select! {
                        result = receiver.recv() => {
                            if let Some((key, redis_val)) = result {
                                if let Some(value_array) = redis_val.get_blocked_result(){
                                    let mut encoded_array = String::new();
                                    return encode_resp_value_array(&mut encoded_array, &vec![json!([key, value_array])])
                                }
                                format!("*-1\r\n")  
                            } else {format!("*-1\r\n")}
                        },
    
                        _ = sleep(Duration::from_millis(*timeout)) => {
                            format!("*-1\r\n")
                        },
                    }
                }
            },

            _ => format!("ERR_NOT_SUPPORTED")
        }
    }

    pub fn incr(&self, commands: &Vec<String>) -> String {
        let mut map_guard = self.map_state.map.write().unwrap();
        let val = map_guard.entry(commands[1].to_owned()).or_insert(RedisValue::Number(0));
        match val{
            RedisValue::Number(n) => {
                *n += 1;
                format!(":{}\r\n", n)
            },
            _ => format!("-ERR value is not an integer or out of range\r\n"),
        }
    } 

    pub fn multi(&self, client_state: &mut ClientState<String, String>) -> String {
        client_state.multi_queue_mode = true;
        format!("+OK\r\n")
    } 

    pub fn info(&self, commands: &Vec<String>) -> String {
        match commands[1].to_uppercase().as_str(){
            "REPLICATION" => {
                let mut lines = Vec::new();
                for (key, value) in self.server_state.map.iter() {
                    lines.push(format!("{}:{}", key, value));
                }
                let content = lines.join("\r\n");
                format!("${}\r\n{}\r\n", content.len(), content)
            }
            _ => format!("NOT_SUPPORTED"),
        }
    } 

    pub fn subscribe(&mut self, client_state: &mut ClientState<String, String>, client: &String, commands: &Vec<String>) -> String{
        client_state.subscribe_mode = true;
        let subs_count = if client_state.subscriptions.1.contains(&commands[1]){
            client_state.subscriptions.0
        } else {
            let mut channel_guard = self.channels_state.channels_map.write().unwrap();
            let (count, client_set) = channel_guard.entry(commands[1].to_owned()).or_insert((0, HashSet::new()));
            *count += 1;
            client_set.insert(client.to_owned());
            
            client_state.subscriptions.1.insert(commands[1].to_owned());
            client_state.subscriptions.0 += 1;
            client_state.subscriptions.0
        };

        format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", commands[0].len(), commands[0].to_lowercase(), commands[1].len(), commands[1], subs_count)
    }

    pub async fn handle_subscriber(&self, client_state: &mut ClientState<String, String>, commands: &Vec<String>){
        if client_state.receiver.is_none() {
            let (sender, receiver) = mpsc::channel(1000);
            client_state.receiver = Some(receiver);
            client_state.sender = Some(sender);
        }

        if let Some(sender) = &client_state.sender {
            let mut subs_guard = self.channels_state.subscribers.lock().unwrap();
            subs_guard.entry(commands[1].to_owned()).or_insert(Vec::new()).push(sender.clone());
        }
    }

    pub fn publish(&self, commands: &Vec<String>) -> String{
        let subs = self.channels_state.channels_map.read().unwrap().get(&commands[1]).unwrap().0;
        let channel_name = &commands[1];
        let messages = commands.iter().skip(2).cloned().collect::<Vec<_>>();
        let mut subs_guard = self.channels_state.subscribers.lock().unwrap();
        if let Some(subs) = subs_guard.get_mut(&commands[1]){
            subs.retain(|sender| 
                match sender.try_send((channel_name.to_owned(), messages.clone())){
                    Ok(_) => true,
                    Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Closed(_)) => false,
            })
        }

        format!(":{}\r\n", subs)
    }

    pub fn unsubscribe(&self, client_state: &mut ClientState<String, String>, client: &String, commands: &Vec<String>) -> String{
        let mut channel_guard = self.channels_state.channels_map.write().unwrap();
        if let Some((count, client_set)) = channel_guard.get_mut(&commands[1]){
            *count -= 1;
            client_set.remove(client);
        }

        client_state.subscriptions.1.remove(&commands[1]);
        client_state.subscriptions.0 -= 1;
        let subs_count = client_state.subscriptions.0;
        
        format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", commands[0].len(), commands[0].to_lowercase(), commands[1].len(), commands[1], subs_count)
    }

}