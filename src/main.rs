use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};
use crate::{protocol::{ClientState, RedisState, RedisValue}, utils::{encode_resp_array, parse_resp}};
mod protocol;
mod utils;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
        
    let listener = Arc::new(TcpListener::bind("127.0.0.1:6379").await.unwrap());
    let connection_count = Arc::new(AtomicUsize::new(0));
    let state = RedisState::new();
    
    loop {
        let (mut stream, addr) = listener.accept().await.unwrap();
            println!("accepted new connection");

        let mut buf = [0; 512];
        let mut local_state = state.clone();

        let count = connection_count.fetch_add(1, Ordering::SeqCst);
        if count >= 10000 {
            connection_count.fetch_sub(1, Ordering::SeqCst);
            continue;
        }

        let cc = connection_count.clone();
        tokio::spawn(async move {
            let mut client_state = ClientState::new();
            
            loop {
                if client_state.subscribe_mode{
                    if let Some(receiver) = client_state.receiver.as_mut(){
                        tokio::select! {
                            msg = receiver.recv() => {
                                if let Some((channel_name, message)) = msg {
                                    let mut array = vec!["message".to_string(), channel_name];
                                    array.extend(message);
                                    
                                    let response = encode_resp_array(&array);
                                    stream.write_all(response.as_bytes()).await.unwrap()
                                }
                            },

                            bytes_read = stream.read(&mut buf) => {
                                match bytes_read {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let parsed_commands = parse_resp(&buf[..n]);
                                        if let Some(commands) = parsed_commands {
                                            match commands[0].to_uppercase().as_str() {
                                                "SUBSCRIBE" => {
                                                    let response = local_state.subscribe(&mut client_state, &addr.to_string(), &commands);
                                                    local_state.handle_subscriber(&mut client_state, &commands).await;
                                                    stream.write_all(response.as_bytes()).await.unwrap()
                                                }
                                                "PING" => {
                                                    let response = encode_resp_array(&vec!["pong".to_string(), "".to_string()]);
                                                    stream.write_all(response.as_bytes()).await.unwrap()
                                                },
                                                "UNSUBSCRIBE" => {
                                                    let response = local_state.unsubscribe(&mut client_state, &addr.to_string(), &commands);
                                                    stream.write_all(response.as_bytes()).await.unwrap()
                                                }
                        
                                                _ => {
                                                    let response = format!("-ERR Can't execute '{}' in subscribed mode\r\n", commands[0].to_lowercase());
                                                    stream.write_all(response.as_bytes()).await.unwrap()
                                                }
                        
                                            }
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                } else {

                    match stream.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let parsed_commands = parse_resp(&buf[..n]);
                            if let Some(commands) = parsed_commands {
                                    match commands[0].to_uppercase().as_str() {
                                        "PING" => stream.write_all(b"+PONG\r\n").await.unwrap(),
                                        "ECHO" => {
                                            let message = &commands[1]; //multiple arg will fail like hello world. check to use .join("")
                                            let response = format!("${}\r\n{}\r\n", message.len(), message);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "SET" => {
                                            match commands.iter().skip(3).next() {
                                                Some(str) => {
                                                    match str.to_uppercase().as_str() {
                                                        "PX" => {
                                                            let timeout = Instant::now() + Duration::from_millis(commands[4].parse::<u64>().unwrap());
                                                            local_state.map_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)));
                                                        }
                                                        "EX" => {
                                                            let timeout = Instant::now() + Duration::from_secs(commands[4].parse::<u64>().unwrap());
                                                            local_state.map_state.map.write().unwrap().insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)));
                                                        }
                                                        _ => (),
                                                    }
                                                }
                                                None => {
                                                    let redis_val = match commands[2].parse::<u64>(){
                                                        Ok(num) => RedisValue::Number(num),
                                                        Err(_) => RedisValue::String(commands[2].to_owned()),
                                                    };

                                                    local_state.map_state.map.write().unwrap().insert(commands[1].to_owned(), redis_val);
                                                }
                                            }
                                            stream.write_all(b"+OK\r\n").await.unwrap()
                                        }
                                        "GET" => {
                                            let value = local_state.map_state.map.read().unwrap().get(&commands[1]).cloned();
                                            if let Some(value) = value {
                                                match value {
                                                    RedisValue::StringWithTimeout((value, timeout)) => {
                                                        if Instant::now() < timeout {
                                                            let response = format!("${}\r\n{}\r\n", value.len(), value);
                                                            stream.write_all(response.as_bytes()).await.unwrap()
                                                        } else {
                                                            stream.write_all(b"$-1\r\n").await.unwrap()
                                                        }
                                                    }
                                                    RedisValue::String(val) => {
                                                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                                                        stream.write_all(response.as_bytes()).await.unwrap()
                                                    }
                                                    RedisValue::Number(val) => {
                                                        let response = format!("${}\r\n{}\r\n", val.to_string().len(), val);
                                                        stream.write_all(response.as_bytes()).await.unwrap()
                                                    }
                                                    _ => (), // fix error handling
                                                }
                                            }
                                        }
                                        "RPUSH" => {
                                            let response = format!(":{}\r\n", local_state.rpush(&commands));
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "LPUSH" => {
                                            let response = format!(":{}\r\n", local_state.lpush(&commands));
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "LLEN" => {
                                            let response = format!(":{}\r\n", local_state.llen(&commands));
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "LPOP" => {
                                            let response = local_state.lpop(&commands);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "BLPOP" => {
                                            let response = local_state.blpop(&commands).await;
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "LRANGE" => {
                                            let response = local_state.lrange(&commands[1], &commands[2], &commands[3]);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "TYPE" => {
                                            let response = format!("+{}\r\n", local_state.type_command(&commands));
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "XADD" => {
                                            let response = local_state.xadd(&commands);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "XRANGE" => {
                                            let response = local_state.xrange(&commands);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "XREAD" => {
                                            let response = local_state.xread(&commands).await;
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "SUBSCRIBE" => {
                                            let count_response = local_state.subscribe(&mut client_state, &addr.to_string(),  &commands);
                                            local_state.handle_subscriber(&mut client_state, &commands).await;
                                            stream.write_all(count_response.as_bytes()).await.unwrap()
                                        }
                                        "PUBLISH" => {
                                            let response = local_state.publish(&commands);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "INCR" => {
                                            let response = local_state.incr(&commands);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        "MULTI" => {
                                            let response = local_state.multi(&commands);
                                            stream.write_all(response.as_bytes()).await.unwrap()
                                        }
                                        _ => (),
                                    }
                                }
                            }
                        Err(_) => break,
                    }
                }
            }

            cc.fetch_sub(1, Ordering::SeqCst);
        });
    }
}
