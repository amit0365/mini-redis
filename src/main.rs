use std::{net::TcpListener, time::{Duration, Instant}, io::{Read, Write}};
use crate::{protocol::{RedisState, RedisValue}, utils::parse_resp};

mod protocol;
mod utils;

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
                                                    _ => (), // fix error handling
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
                                        "XRANGE" => {
                                            let response = local_state.xrange(&commands);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "XREAD" => {
                                            let response = local_state.xread(&commands);
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
