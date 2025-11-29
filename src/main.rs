#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener, process::Command};

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
    for stream in listener.incoming() {
        std::thread::spawn(||
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
                                        "ECHO" => {
                                            let message = &commands[1];
                                            let response = format!("${}\r\n{}\r\n", message.len(), message);
                                            stream.write_all(response.as_bytes()).unwrap()
                                        },
                                        "PING" => stream.write_all(b"+PONG\r\n").unwrap(),
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
