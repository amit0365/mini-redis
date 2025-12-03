use std::{collections::{HashMap, HashSet, VecDeque}, sync::{Arc, Mutex, RwLock}, time::Duration};
use tokio::{sync::mpsc::{self, Receiver, Sender, error::TrySendError}, time::sleep};
use serde_json::json;

use crate::{protocol::{RedisValue, StreamValue}, utils::{collect_as_strings, encode_resp_array, encode_resp_value_array, parse_wrapback}};

#[derive(Clone)]
pub struct RedisState<K, RedisValue> {
    pub channels_state: ChannelState<K>,
    pub map_state: MapState<K, RedisValue>,
    pub list_state: ListState<K, RedisValue>,
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

pub struct ClientState<K, V>{
    pub subscribe_mode: bool,
    pub multi_queue_mode: bool,
    subscriptions: (usize, HashSet<V>),
    pub receiver: Option<Receiver<(K, Vec<String>)>>,
    pub sender: Option<Sender<(K, Vec<String>)>>,
    pub queued_commands: VecDeque<Vec<String>> 
}

impl ClientState<String, String>{
    pub fn new() -> Self{
        let subscribe_mode = false;
        let multi_queue_mode = false;
        let subscriptions = (0, HashSet::new());
        ClientState { subscribe_mode, multi_queue_mode, subscriptions, receiver: None, sender: None , queued_commands: VecDeque::new() }
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
        RedisState { channels_state, map_state, list_state }
    }

    pub fn rpush(&mut self, command: &Vec<String>) -> String {
        let mut count = String::new();
        let key = &command[1];

        {
            let mut list_guard = self.list_state.list.lock().unwrap();
            let items = command.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
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

        count
    } 

    pub fn lpush(&mut self, command: &Vec<String>) -> String {
        let mut list_guard = self.list_state.list.lock().unwrap();
        let items = command.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
        for item in items.iter() {
            list_guard.entry(command[1].clone())
            .or_insert(VecDeque::new())
            .push_front(item.clone());
        }

        list_guard.get(&command[1]).unwrap().len().to_string() //remove unwrap
    } 

    pub fn llen(&self, command: &Vec<String>) -> String {
        let list_guard = self.list_state.list.lock().unwrap();
        match list_guard.get(&command[1]){
            Some(list) => list.len().to_string(),
            None => 0.to_string()
        }
    } 

    pub fn lpop(&mut self, command: &Vec<String>) -> String {
        let mut list_guard = self.list_state.list.lock().unwrap();
        match list_guard.get_mut(&command[1]){
            Some(list) => {
                match command.iter().skip(2).next(){
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

    pub async fn blpop(&mut self, command: &Vec<String>) -> String {
        let key = &command[1];

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

        let timeout: f64 = command.last().unwrap().parse().unwrap(); // yo check this
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

    pub fn type_command(&self, command: &Vec<String>) -> String {
        let map_guard = self.map_state.map.read().unwrap();
        match map_guard.get(&command[1]){
            Some(val) => {
                match val{
                    RedisValue::String(_) => "string".to_string(),
                    RedisValue::StringWithTimeout(_) => "string".to_string(),
                    RedisValue::Stream(_) => "stream".to_string(),
                    RedisValue::Number(_) => "number".to_string(),
                }
            },
            None => "none".to_string()
        }
    } 

    pub fn xadd(&self, command: &Vec<String>) -> String {
        let key = &command[1];
        let mut result = String::new();
        let pairs_flattened = command.iter().skip(3).cloned().collect::<Vec<String>>();

        {
            let mut map_guard = self.map_state.map.write().unwrap();
            result = map_guard.entry(key.clone())
            .or_insert(RedisValue::Stream(StreamValue::new()))
            .update_stream(&command[2], &pairs_flattened);
        }

        let mut map_waiters_guard = self.map_state.waiters.lock().unwrap();
        if let Some(waiters_queue) = map_waiters_guard.get_mut(key){
            while let Some(waiter) = waiters_queue.pop_front(){
                match waiter.try_send((key.clone(), RedisValue::Stream(StreamValue::new_blocked(&command[2], &pairs_flattened)))){
                    Ok(_) => break,
                    Err(TrySendError::Full(_)) => return format!("ERR_TOO_MANY_WAITERS"),
                    Err(TrySendError::Closed(_)) => continue,
                }
            }
        }

        result
    } 

    pub fn xrange(&self, command: &Vec<String>) -> String {
        let mut encoded_array = String::new();
        let values = self.map_state.map.read().unwrap()
        .get(&command[1])
        .unwrap() // no case for nil
        .get_stream_range(&command[2], Some(&command[3]));
        if !values.is_empty() {
            encode_resp_value_array(&mut encoded_array, &values)
        } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
    } 

    pub async fn xread(&self, command: &Vec<String>) -> String {
        match command[1].as_str(){
            "streams" => {
                let key_tokens = command.iter().skip(2)
                .filter(|token| self.map_state.map.read().unwrap().get(token.as_str()).is_some())
                .collect::<Vec<_>>();
        
                let id_tokens = command.iter().skip(2 + key_tokens.len()).collect::<Vec<_>>();
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
                let timeout = &command[2].parse::<u64>().unwrap();
                let key = &command[4];

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

    pub fn incr(&self, command: &Vec<String>) -> String {
        let mut map_guard = self.map_state.map.write().unwrap();
        let val = map_guard.entry(command[1].to_owned()).or_insert(RedisValue::Number(0));
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

    pub fn subscribe(&mut self, client_state: &mut ClientState<String, String>, client: &String, command: &Vec<String>) -> String{
        client_state.subscribe_mode = true;
        let subs_count = if client_state.subscriptions.1.contains(&command[1]){
            client_state.subscriptions.0
        } else {
            let mut channel_guard = self.channels_state.channels_map.write().unwrap();
            let (count, client_set) = channel_guard.entry(command[1].to_owned()).or_insert((0, HashSet::new()));
            *count += 1;
            client_set.insert(client.to_owned());
            
            client_state.subscriptions.1.insert(command[1].to_owned());
            client_state.subscriptions.0 += 1;
            client_state.subscriptions.0
        };

        format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", command[0].len(), command[0].to_lowercase(), command[1].len(), command[1], subs_count)
    }

    pub async fn handle_subscriber(&self, client_state: &mut ClientState<String, String>, command: &Vec<String>){
        if client_state.receiver.is_none() {
            let (sender, receiver) = mpsc::channel(1000);
            client_state.receiver = Some(receiver);
            client_state.sender = Some(sender);
        }

        if let Some(sender) = &client_state.sender {
            let mut subs_guard = self.channels_state.subscribers.lock().unwrap();
            subs_guard.entry(command[1].to_owned()).or_insert(Vec::new()).push(sender.clone());
        }
    }

    pub fn publish(&self, command: &Vec<String>) -> String{
        let subs = self.channels_state.channels_map.read().unwrap().get(&command[1]).unwrap().0;
        let channel_name = &command[1];
        let messages = command.iter().skip(2).cloned().collect::<Vec<_>>();
        let mut subs_guard = self.channels_state.subscribers.lock().unwrap();
        if let Some(subs) = subs_guard.get_mut(&command[1]){
            subs.retain(|sender| 
                match sender.try_send((channel_name.to_owned(), messages.clone())){
                    Ok(_) => true,
                    Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Closed(_)) => false,
            })
        }

        format!(":{}\r\n", subs)
    }

    pub fn unsubscribe(&self, client_state: &mut ClientState<String, String>, client: &String, command: &Vec<String>) -> String{
        let mut channel_guard = self.channels_state.channels_map.write().unwrap();
        if let Some((count, client_set)) = channel_guard.get_mut(&command[1]){
            *count -= 1;
            client_set.remove(client);
        }

        client_state.subscriptions.1.remove(&command[1]);
        client_state.subscriptions.0 -= 1;
        let subs_count = client_state.subscriptions.0;
        
        format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", command[0].len(), command[0].to_lowercase(), command[1].len(), command[1], subs_count)
    }

}