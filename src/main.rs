#![allow(unused_imports)]
use std::{collections::{HashMap, VecDeque}, fmt::Error, hash::Hash, io::{Read, Write}, net::TcpListener, process::Command, sync::{Arc, Mutex, RwLock, mpsc::{self, Sender}}, time::{Duration, Instant}};

use tokio::sync::Notify;

#[derive(Clone)]
struct RedisState<K, E, V> {
    map: Arc<RwLock<HashMap<K, V>>>,
    list_state: ListState<K, E>,
}

#[derive(Clone)]
struct ListState<K, E>{
    list: Arc<Mutex<HashMap<K, VecDeque<E>>>>,    
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(String, String)>>>>>,    

}

impl ListState<String, String>{
    fn new() -> Self{
        let list = Arc::new(Mutex::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        ListState { list, waiters }
    }
}

impl RedisState<String, String, (String, Option<Instant>)>{
    fn new() -> Self{
        let map = Arc::new(RwLock::new(HashMap::new()));
        let list_state = ListState::new();
        RedisState { map, list_state }
    }

    fn rpush(&mut self, command: &Vec<String>) -> String {
        let mut count = String::new();
        let key = &command[1];

        {
            let mut list_guard = self.list_state.list.lock().unwrap();
            let items = command.iter().skip(2).cloned().collect::<Vec<String>>();
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

        // let list_guard = self.list_state.list.lock().unwrap();
        // list_guard.get(key).unwrap().len().to_string()
        count
    } 

    fn lpush(&mut self, command: &Vec<String>) -> String {
        let mut list_guard = self.list_state.list.lock().unwrap();
        let items = command.iter().skip(2).cloned().collect::<Vec<String>>();
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
                                Some(popped) => popped_list.push(popped),
                                None => (),
                            }
                        }

                        encode_resp_array(&popped_list)
                    },

                    None => {
                        match list.pop_front(){
                            Some(popped) => format!("${}\r\n{}\r\n", popped.len(), popped),
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
                    return encode_resp_array(&vec![key.clone(), data])
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

        match receiver.recv(){
            Ok((key, value)) => encode_resp_array(&vec![key, value]),
            Err(e) => format!("$\r\n"), // fix this
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
                } else if start >= stop{
                    VecDeque::new()
                } else {
                    vec.range(start..=stop).cloned().collect()
                }
            },
            None => VecDeque::new()
        };

        encode_resp_array(&array.into())
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
        encoded_array.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
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
                                                            local_state.map.write().unwrap().insert(commands[1].clone(), (commands[2].clone(), Some(timeout)));
                                                        },
                                                        "EX" => {
                                                            let timeout = Instant::now() + Duration::from_secs(commands[4].parse::<u64>().unwrap());
                                                            local_state.map.write().unwrap().insert(commands[1].clone(), (commands[2].clone(), Some(timeout)));
                                                        },
                                                        _ => (),
                                                    }
                                                }
                                                None => {
                                                    local_state.map.write().unwrap().insert(commands[1].clone(), (commands[2].clone(), None));
                                                }
                                            }
                                            
                                            stream.write_all(b"+OK\r\n").unwrap()   
                                        },
                                        "GET" => {
                                            if let Some(value) = local_state.map.read().unwrap().get(&commands[1]){
                                                if let Some(timeout) = value.1{
                                                    if Instant::now() < timeout {
                                                        let response = format!("${}\r\n{}\r\n", value.0.len(), value.0);
                                                        stream.write_all(response.as_bytes()).unwrap()
                                                    } else {
                                                        stream.write_all(b"$-1\r\n").unwrap()
                                                    } 
                                                } else {
                                                    let response = format!("${}\r\n{}\r\n", value.0.len(), value.0);
                                                    stream.write_all(response.as_bytes()).unwrap()
                                                }
                                            } else {
                                                stream.write_all(b"$-1\r\n").unwrap()
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
