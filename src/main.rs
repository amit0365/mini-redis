use std::{env, str::from_utf8, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crate::{protocol::{ClientState, RedisState, RedisValue}, utils::{encode_resp_array, parse_resp}};
mod protocol;
mod utils;

async fn execute_commands_normal(stream: &mut TcpStream, write_to_stream: bool, local_state: &mut RedisState<String, RedisValue>, client_state: &mut ClientState<String, String>, client_add: String, commands: &Vec<String>) -> String{
    let response = match commands[0].to_uppercase().as_str() {
        "PING" => format!("+PONG\r\n"),
        "ECHO" => {
            let message = &commands[1]; //multiple arg will fail like hello world. check to use .join("")
            format!("${}\r\n{}\r\n", message.len(), message)
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

            format!("+OK\r\n")
        }
        "GET" => {
            let value = local_state.map_state.map.read().unwrap().get(&commands[1]).cloned();
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
        "RPUSH" => {
            format!(":{}\r\n", local_state.rpush(&commands))
        }
        "LPUSH" => {
            format!(":{}\r\n", local_state.lpush(&commands))
        }
        "LLEN" => {
            format!(":{}\r\n", local_state.llen(&commands))
        }
        "LPOP" => {
            local_state.lpop(&commands)
        }
        "BLPOP" => {
            local_state.blpop(&commands).await
        }
        "LRANGE" => {
            local_state.lrange(&commands[1], &commands[2], &commands[3])
        }
        "TYPE" => {
            format!("+{}\r\n", local_state.type_command(&commands))
        }
        "XADD" => {
            local_state.xadd(&commands)
        }
        "XRANGE" => {
            local_state.xrange(&commands)
        }
        "XREAD" => {
            local_state.xread(&commands).await
        }
        "SUBSCRIBE" => {
            let count_response = local_state.subscribe(client_state, &client_add,  &commands);
            local_state.handle_subscriber(client_state, &commands).await;
            count_response
        }
        "PUBLISH" => {
            local_state.publish(&commands)
        }
        "INCR" => {
            local_state.incr(&commands)
        }
        "MULTI" => {
            local_state.multi(client_state)
        }
        "INFO" => {
            local_state.info(&commands)
        }
        _ => format!("$-1\r\n"), //todo fix
    };

    if write_to_stream{
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    response
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let args = env::args().collect::<Vec<_>>();
    let mut port = "6379".to_string();
    let mut master_contact: Option<String> = None;

    let mut i = 1;
    while i < args.len(){
        match args[i].as_str(){
            "--port" => {
                port = args[i + 1].to_owned();
                i += 2;
            },

            "--replicaof" => {
                master_contact = Some(args[i + 1].to_owned());
                i += 2;
            },

            _ => i += 1
            
        }
    }

    let listener = Arc::new(TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap());
    let connection_count = Arc::new(AtomicUsize::new(0));
    let mut state = RedisState::new();

    if master_contact.is_some(){
        state.server_state.map.insert("role".to_string(), RedisValue::String("slave".to_string()));
        match master_contact.unwrap().split_once(" "){
            Some((master_ip, master_port)) => {
                let mut master_stream = TcpStream::connect(format!("{}:{}", master_ip, master_port)).await.unwrap();
                tokio::spawn(async move {
                    let ping_msg = encode_resp_array(&vec!["PING".to_string()]);
                    master_stream.write(ping_msg.as_bytes()).await.unwrap();

                    loop {
                        let mut buf = [0; 512];
                        match master_stream.read(&mut buf).await {
                            Ok(0) => (),
                            Ok(n) => {
                                let buffer_str = from_utf8(&buf[..n]).unwrap().split("\r\n").collect::<Vec<&str>>();;
                                match buffer_str[0]{
                                    "+PONG" => {
                                        let replconf_msg1 = encode_resp_array(&vec!["REPLCONF".to_string(), "listening-port".to_string(), port]);
                                        master_stream.write(replconf_msg1.as_bytes()).await.unwrap();
                                        let replconf_msg2 = encode_resp_array(&vec!["REPLCONF".to_string(), "capa".to_string(), "psync2".to_string()]);
                                        master_stream.write(replconf_msg2.as_bytes()).await.unwrap();
                                    },

                                    _ => {}
                                }
                            },

                            Err(_) => (),
                        }
                    }
                });
            },
            None => (),
        }
    } else {
        let master_config = vec![
            ("role".to_string(), RedisValue::String("master".to_string())),
            ("master_replid".to_string(), RedisValue::String("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string())),
            ("master_repl_offset".to_string(), RedisValue::Number(0)),
        ];

        state.server_state.update(&master_config);
    }
    
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
                                if client_state.multi_queue_mode{
                                    match commands[0].as_str(){
                                        "EXEC" => {
                                            let mut responses = Vec::new();
                                            match client_state.queued_commands.len(){
                                                0 => stream.write_all(b"*0\r\n").await.unwrap(),
                                                _ => {
                                                    while let Some(queued_command) = client_state.queued_commands.pop_front(){
                                                        let response = execute_commands_normal(&mut stream, false, &mut local_state, &mut client_state, addr.to_string(), &queued_command).await;
                                                        responses.push(response);
                                                    }

                                                    let responses_array = format!("*{}\r\n{}", responses.len(), responses.join(""));
                                                    stream.write_all(responses_array.as_bytes()).await.unwrap();
                                                },
                                            }

                                            client_state.multi_queue_mode = false;
                                        },
                                        "DISCARD" => {
                                            client_state.queued_commands.clear();
                                            client_state.multi_queue_mode = false;
                                            stream.write_all(b"+OK\r\n").await.unwrap()
                                        }
                                        _ => {
                                            client_state.queued_commands.push_back(commands);
                                            stream.write_all(b"+QUEUED\r\n").await.unwrap()
                                        },
                                    }
                                } else {
                                    match commands[0].as_str(){
                                        "EXEC" => stream.write_all(b"-ERR EXEC without MULTI\r\n").await.unwrap(),
                                        "DISCARD" => stream.write_all(b"-ERR DISCARD without MULTI\r\n").await.unwrap(),

                                        _ => {
                                            let _ = execute_commands_normal(&mut stream, true, &mut local_state, &mut client_state, addr.to_string(), &commands).await;
                                        }
                                    }
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
