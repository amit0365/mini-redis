use std::{collections::{HashMap, VecDeque}, sync::{Arc, Mutex, RwLock, mpsc::{self, Sender}}, time::Duration};

use serde_json::json;

use crate::{protocol::{RedisValue, StreamValue}, utils::{collect_as_strings, encode_resp_array, encode_resp_value_array, parse_wrapback}};

#[derive(Clone)]
pub struct RedisState<K, RedisValue> {
    pub map: Arc<RwLock<HashMap<K, RedisValue>>>,
    pub list_state: ListState<K, RedisValue>,
}

#[derive(Clone)]
pub struct ListState<K, RedisValue>{
    list: Arc<Mutex<HashMap<K, VecDeque<RedisValue>>>>,    
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>>,    

}

impl<K> ListState<K, RedisValue>{
    fn new() -> Self{
        let list = Arc::new(Mutex::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        ListState { list, waiters }
    }
}

impl RedisState<String, RedisValue>{
    pub fn new() -> Self{
        let map = Arc::new(RwLock::new(HashMap::new()));
        let list_state = ListState::<String, RedisValue>::new();
        RedisState { map, list_state }
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
            if let Some(sender) = waiting_queue.pop_front(){
                let mut list_guard = self.list_state.list.lock().unwrap();
                if let Some(deque) = list_guard.get_mut(key){
                    if let Some(value) = deque.pop_front(){
                        let _ = sender.send((key.clone(), value));
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

    pub fn blpop(&mut self, command: &Vec<String>) -> String {
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

        let receiver = {
            let mut waiters_guard = self.list_state.waiters.lock().unwrap();
            let (sender, receiver) = mpsc::channel();
            waiters_guard.entry(key.clone())
                .or_insert(VecDeque::new())
                .push_back(sender);

            receiver
        };

        let timeout: f64 = command.last().unwrap().parse().unwrap(); // yo check this
        if timeout == 0.0 {
            match receiver.recv(){
                Ok((key, value)) => {
                    if let Some(val) = value.as_string(){
                        encode_resp_array(&vec![key, val.clone()])
                    } else {format!("$\r\n")} //fix this
                }
                Err(_) => format!("$\r\n"), //fix this
            }
        } else {
            match receiver.recv_timeout(Duration::from_secs_f64(timeout)){
                Ok((key, value)) => {
                    if let Some(val) = value.as_string(){
                        encode_resp_array(&vec![key, val.clone()])
                    } else {format!("$\r\n")} //fix this
                },
                Err(mpsc::RecvTimeoutError::Timeout) => format!("*-1\r\n"),
                Err(mpsc::RecvTimeoutError::Disconnected) => format!("*-1\r\n"),
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
        let map_guard = self.map.read().unwrap();
        match map_guard.get(&command[1]){
            Some(val) => {
                match val{
                    RedisValue::String(_) => "string".to_string(),
                    RedisValue::StringWithTimeout(_) => "string".to_string(),
                    RedisValue::Stream(_) => "stream".to_string(),
                }
            },
            None => "none".to_string()
        }
    } 

    pub fn xadd(&self, command: &Vec<String>) -> String {
        let mut map_guard = self.map.write().unwrap();
        let pairs_flattened = command.iter().skip(3).cloned().collect::<Vec<String>>();
        map_guard.entry(command[1].clone())
        .or_insert(RedisValue::Stream(StreamValue::new()))
        .update_stream(&command[2], pairs_flattened)
    } 

    pub fn xrange(&self, command: &Vec<String>) -> String {
        let mut encoded_array = String::new();
        let values = self.map.read().unwrap()
        .get(&command[1])
        .unwrap() // no case for nil
        .get_stream_range(&command[2], Some(&command[3]));
        if !values.is_empty() {
            encode_resp_value_array(&mut encoded_array, &values)
        } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
    } 

    pub fn xread(&self, command: &Vec<String>) -> String {
        let key_tokens = command.iter().skip(2)
        .filter(|token| self.map.read().unwrap().get(token.as_str()).is_some())
        .collect::<Vec<_>>();

        let id_tokens = command.iter().skip(2 + key_tokens.len()).collect::<Vec<_>>();
        let mut encoded_array = String::new();
        let mut key_entries = Vec::new();

        for (key, id) in key_tokens.iter().zip(id_tokens){
            let values = self.map.read().unwrap()
            .get(*key)
            .unwrap() // no case for nil
            .get_stream_range(id, None);
            if !values.is_empty() { key_entries.push(json!([key, values]));}
        }

        if !key_entries.is_empty() {
            encode_resp_value_array(&mut encoded_array, &key_entries)
        } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
    } 
}