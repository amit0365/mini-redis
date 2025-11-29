#![allow(unused_imports)]
use std::{collections::HashMap, fmt::Error, hash::Hash, io::{Read, Write}, net::TcpListener, process::Command, sync::{Arc, Mutex, RwLock}};

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
    let database = Arc::new(RwLock::new(HashMap::new()));
    
    for stream in listener.incoming() {
        let local_database = database.clone();
        std::thread::spawn(move ||
            match stream {
                Ok(mut stream) => {
                    println!("accepted new connection");
                    let mut buf = [0; 512];
                
                loop{
                        match stream.read(&mut buf){
                            Ok(0) => break,
                            Ok(_) => {
                                let parsed_commands = parse_resp(&buf);
                                if let Some(commands) = parsed_commands{
                                    match commands[0].to_uppercase().as_str(){
                                        "PING" => stream.write_all(b"+PONG\r\n").unwrap(),
                                        "ECHO" => {
                                            let message = &commands[1]; //multiple arg will fail like hello world. check to use .join("")
                                            let response = format!("${}\r\n{}\r\n", message.len(), message);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "SET" => {
                                            let result = local_database.write().unwrap().insert(commands[1].clone(), commands[2].clone());
                                            stream.write_all(b"+OK\r\n").unwrap()
                                        },
                                        "GET" => {
                                            if let Some(value) = local_database.read().unwrap().get(&commands[1]){
                                                let response = format!("${}\r\n{}\r\n", value.len(), value);
                                                stream.write_all(response.as_bytes()).unwrap()
                                            } else {
                                                stream.write_all(b"$-1\r\n").unwrap()
                                            }
                                        }
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
