#![allow(unused_imports)]
use std::{collections::{HashMap, HashSet, VecDeque}, fmt::Error, hash::Hash, io::{Read, Write}, net::TcpListener, process::Command, sync::{Arc, Mutex, RwLock, mpsc::{self, Sender}}, time::{Duration, Instant}};

use tokio::sync::Notify;

#[derive(Clone)]
enum RedisValue{
    String(String),
    StringWithTimeout((String, Instant)), 
    Stream(StreamValue<String, String>)
}

#[derive(Clone)]
struct StreamValue<K, V>{
    last_id: K,
    time_map: HashMap<u64, u64>, //time -> last seqquence number
    map: HashMap<K, Vec<(K, V)>>
}

impl StreamValue<String, String>{
    fn new() -> Self {
        StreamValue { last_id: String::new(), time_map: HashMap::new(), map: HashMap::new() }
    }

    fn insert(&mut self, id: &String, pairs_flattened: Vec<String>) -> Option<Vec<(String, String)>> {
        let pairs_grouped = pairs_flattened
        .chunks_exact(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect::<Vec<(String, String)>>();
        
        self.map.insert(id.clone(), pairs_grouped)
    }
}

impl RedisValue{
    fn as_string(&self) -> Option<&String> {
        match self{
            RedisValue::String(s) => Some(s),
            RedisValue::StringWithTimeout((s, _)) => Some(s),
            _ => None
        }
    }

    fn update_stream(&mut self, id: &String, pairs_flattened: Vec<String>) -> String{
        match self{
            RedisValue::String(_) => format!("not supported"),
            RedisValue::StringWithTimeout((_, _)) => format!("not supported"),
            RedisValue::Stream(stream) => {
                let mut new_id = id.clone();
                let (id_pre, id_post) = id.split_once("-").unwrap();
                let id_millisecs = id_pre.parse::<u64>().unwrap(); 
                let id_sequence_num: u64;
                if id_post == "*"{
                    if let Some(last_sequence_num) = stream.time_map.get(&id_millisecs){
                        id_sequence_num = last_sequence_num + 1;
                    } else { if id_millisecs == 0 {id_sequence_num = 1 } else {
                        id_sequence_num = 0;
                    }
                    new_id = id_pre.to_string() + "-" + &id_sequence_num.to_string()
                } else {
                    id_sequence_num = id_post.parse::<u64>().unwrap(); 
                }
            
                if id_millisecs == 0 && id_sequence_num == 0{ //empty stream
                    return format!("-ERR The ID specified in XADD must be greater than 0-0\r\n")
                }

                let last_id = &stream.last_id;
                if last_id.is_empty(){ //new entry
                    stream.insert(&new_id, pairs_flattened);
                    stream.last_id = new_id.clone();
                    return format!("${}\r\n{}\r\n", new_id.len(), new_id)
                }
            
                let (last_id_pre, last_id_post) = last_id.split_once("-").unwrap();
                let last_id_millisecs = last_id_pre.parse::<u64>().unwrap(); 
                let last_id_sequence_num = last_id_post.parse::<u64>().unwrap();
            
                if last_id_millisecs > id_millisecs{
                    return format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                } else if last_id_millisecs == id_millisecs {
                    if last_id_sequence_num >= id_sequence_num {
                        return format!("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                    }
                }
                
                if let Some(last_sequence_num) = stream.time_map.get(&id_millisecs) { // update top sequence num for a give time
                    if id_sequence_num > *last_sequence_num { stream.time_map.insert(id_millisecs, id_sequence_num);}
                } else {
                    stream.time_map.insert(id_millisecs, id_sequence_num);
                }

                stream.insert(&new_id, pairs_flattened);
                stream.last_id = new_id.clone();
                format!("${}\r\n{}\r\n", new_id.len(), new_id)
            }
        }
    }
}

//fn generate_id

fn collect_as_strings<I>(iter: I) -> Vec<String>
    where 
        I: IntoIterator<Item = RedisValue>
    {
        iter.into_iter()
        .filter_map(|v| v.as_string().cloned())
        .collect::<Vec<String>>()
    }

#[derive(Clone)]
struct RedisState<K, RedisValue> {
    map: Arc<RwLock<HashMap<K, RedisValue>>>,
    list_state: ListState<K, RedisValue>,
}

#[derive(Clone)]
struct ListState<K, RedisValue>{
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
    fn new() -> Self{
        let map = Arc::new(RwLock::new(HashMap::new()));
        let list_state = ListState::<String, RedisValue>::new();
        RedisState { map, list_state }
    }

    fn rpush(&mut self, command: &Vec<String>) -> String {
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

    fn lpush(&mut self, command: &Vec<String>) -> String {
        let mut list_guard = self.list_state.list.lock().unwrap();
        let items = command.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
        for item in items.iter() {
            list_guard.entry(command[1].clone())
            .or_insert(VecDeque::new())
            .push_front(item.clone());
        }

        list_guard.get(&command[1]).unwrap().len().to_string() //remove unwrap
    } 

    fn llen(&self, command: &Vec<String>) -> String {
        let list_guard = self.list_state.list.lock().unwrap();
        match list_guard.get(&command[1]){
            Some(list) => list.len().to_string(),
            None => 0.to_string()
        }
    } 

    fn lpop(&mut self, command: &Vec<String>) -> String {
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

    fn blpop(&mut self, command: &Vec<String>) -> String {
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
                Err(e) => format!("$\r\n"), //fix this
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
    
    fn lrange(&self, key: &String, start: &String, stop: &String) -> String {
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

    fn type_command(&self, command: &Vec<String>) -> String {
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

    fn xadd(&self, command: &Vec<String>) -> String {
        let mut map_guard = self.map.write().unwrap();
        let id = command[2].clone();
        let pairs_flattened = command.iter().skip(3).cloned().collect::<Vec<String>>();
        map_guard.entry(command[1].clone())
        .or_insert(RedisValue::Stream(StreamValue::new()))
        .update_stream(&id, pairs_flattened)
    } 
}

fn parse_wrapback(idx: i64, len: usize) -> usize{
    if idx.is_negative() {
        let idx_abs= idx.unsigned_abs() as usize;
        if idx_abs <= len {
            len - idx.unsigned_abs() as usize
        } else {
            0
        }
    } else { idx.try_into().unwrap() }
}

fn encode_resp_array(array: &Vec<String>) -> String{
    let mut encoded_array = format!["*{}\r\n", array.len()];
    for item in array {
        encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item))
    }
    encoded_array
}

fn parse_resp(buf: &[u8]) -> Option<Vec<String>>{
    let string_buf = std::str::from_utf8(buf).unwrap();
    let tokens = string_buf.split("\r\n").collect::<Vec<&str>>();
    let commands = tokens
        .iter()
        .skip(2)
        .step_by(2)
        .filter(|x| !x.is_empty())
        .map(|str| str.to_string()) 
        .collect::<Vec<String>>();

    Some(commands)
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
        
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
        let state = RedisState::new();
    
    for stream in listener.incoming() {
        let mut local_state = state.clone();
        std::thread::spawn(move ||
            match stream {
                Ok(mut stream) => {
                    println!("accepted new connection");
                    let mut buf = [0; 512];
                
                loop{
                        match stream.read(&mut buf){
                            Ok(0) => break,
                            Ok(n) => {
                                let parsed_commands = parse_resp(&buf[..n]);
                                if let Some(commands) = parsed_commands{
                                    match commands[0].to_uppercase().as_str(){
                                        "PING" => stream.write_all(b"+PONG\r\n").unwrap(),
                                        "ECHO" => {
                                            let message = &commands[1]; //multiple arg will fail like hello world. check to use .join("")
                                            let response = format!("${}\r\n{}\r\n", message.len(), message);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "SET" => {
                                            match commands.iter().skip(3).next(){
                                                Some(str) => {
                                                    match str.to_uppercase().as_str(){
                                                        "PX" => {
                                                            let timeout = Instant::now() + Duration::from_millis(commands[4].parse::<u64>().unwrap());
                                                            local_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)));
                                                        },
                                                        "EX" => {
                                                            let timeout = Instant::now() + Duration::from_secs(commands[4].parse::<u64>().unwrap());
                                                            local_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)));
                                                        },
                                                        _ => (),
                                                    }
                                                }
                                                None => {
                                                    local_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::String(commands[2].clone()));
                                                }
                                            }
                                            
                                            stream.write_all(b"+OK\r\n").unwrap()   
                                        },
                                        "GET" => {
                                            if let Some(value) = local_state.map.read().unwrap().get(&commands[1]){
                                                match value{
                                                    RedisValue::StringWithTimeout((value, timeout)) => {
                                                        if Instant::now() < *timeout {
                                                            let response = format!("${}\r\n{}\r\n", value.len(), value);
                                                            stream.write_all(response.as_bytes()).unwrap()
                                                        } else {
                                                            stream.write_all(b"$-1\r\n").unwrap()
                                                        } 
                                                    },
                                                    RedisValue::String(val) => {
                                                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                                                        stream.write_all(response.as_bytes()).unwrap()
                                                    },
                                                    _ => (), // wrong type
                                                }
                                            }
                                        },
                                        "RPUSH" => {
                                            let response = format!(":{}\r\n", local_state.rpush(&commands));
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "LPUSH" => {
                                            let response = format!(":{}\r\n", local_state.lpush(&commands));
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "LLEN" => {
                                            let response = format!(":{}\r\n", local_state.llen(&commands));
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "LPOP" => {
                                            let response = local_state.lpop(&commands);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "BLPOP" => {
                                            let response = local_state.blpop(&commands);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "LRANGE" => {
                                            let response = local_state.lrange(&commands[1], &commands[2], &commands[3]);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                       "TYPE" => {
                                            let response = format!("+{}\r\n", local_state.type_command(&commands));
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "XADD" => {
                                            let response = local_state.xadd(&commands);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        _ => (),
                                    }
                                }
                            },
                            Err(_) => break
                        }
                    }                                   
                }
                
                Err(ref e) => {
                    println!("error: {}", e);
                }
            }
        );
    }
}
