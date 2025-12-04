use std::{env, str::from_utf8, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc};
use crate::{protocol::{ClientState, RedisState, RedisValue, ReplicasState}, utils::{encode_resp_array, parse_resp}};
use base64::{Engine as _, engine::general_purpose};
mod protocol;
mod utils;

const EMPTY_RDB_FILE: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

async fn execute_commands_normal(
    stream: &mut TcpStream,
    write_to_stream: bool,
    local_state: &mut RedisState<String, RedisValue>,
    client_state: &mut ClientState<String, String>,
    replicas_state: &mut ReplicasState,
    client_add: String,
    commands: &Vec<String>
) -> String{
    let response = match commands[0].to_uppercase().as_str() {
        "PING" => format!("+PONG\r\n"),
        "ECHO" => format!("${}\r\n{}\r\n", &commands[1].len(), &commands[1]), // fix multiple arg will fail like hello world. check to use .join("")
        "REPLCONF" => format!("+OK\r\n"),
        "PSYNC" => {
            let full_sync_response = local_state.psync();
            if write_to_stream {
                stream.write_all(full_sync_response.as_bytes()).await.unwrap();
                let rdb_bytes = general_purpose::STANDARD.decode(EMPTY_RDB_FILE).unwrap();
                let rdb_message = [format!("${}\r\n", rdb_bytes.len()).into_bytes(), rdb_bytes].concat();
                stream.write_all(&rdb_message).await.unwrap();
            }

            let mut replicas_senders_guard = replicas_state.replica_senders().lock().unwrap();
            let (sender, receiver) = mpsc::channel(1);
            replicas_senders_guard.push(sender);
            
            client_state.set_replica(true);
            client_state.set_replica_receiver(receiver);
            local_state.server_state_mut().set_replication_mode(true);
            format!("") // send empty string since we dont write to stream
        }
        "SET" => {
            local_state.set(&commands)
        }
        "GET" => {
            local_state.get(&commands)
        }
        "RPUSH" => {
            local_state.rpush(&commands)
        }
        "LPUSH" => {
            local_state.lpush(&commands)
        }
        "LLEN" => {
            local_state.llen(&commands)
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
            local_state.type_command(&commands)
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

    if write_to_stream && commands[0].to_uppercase().as_str() != "PSYNC" {
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    let is_write_command = matches!(
        commands[0].to_uppercase().as_str(),
        "SET" | "DEL" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR"
    );

    if local_state.server_state().replication_mode() && is_write_command && write_to_stream{
        let senders = {
            let replica_senders_guard = replicas_state.replica_senders().lock().unwrap();
            replica_senders_guard.clone()
        };

        for sender in senders{
            sender.send(commands.to_vec()).await.unwrap();
        };
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
    let replicas_state = ReplicasState::new();

    if master_contact.is_some(){ // REPLICA
        state.server_state_mut().map_mut().insert("role".to_string(), RedisValue::String("slave".to_string()));
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
                                if let Ok(buffer_str) = from_utf8(&buf[..n]) {
                                    let parts: Vec<&str> = buffer_str.split("\r\n").collect();
                                    match parts[0]{
                                        "+PONG" => {
                                            let replconf_msg1 = encode_resp_array(&vec!["REPLCONF".to_string(), "listening-port".to_string(), port.to_owned()]);
                                            master_stream.write(replconf_msg1.as_bytes()).await.unwrap();
                                            let replconf_msg2 = encode_resp_array(&vec!["REPLCONF".to_string(), "capa".to_string(), "psync2".to_string()]);
                                            master_stream.write(replconf_msg2.as_bytes()).await.unwrap();
                                        },

                                        "+OK" => {
                                            let psync_msg = encode_resp_array(&vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()]);
                                            master_stream.write(psync_msg.as_bytes()).await.unwrap();
                                        }

                                        _ => ()
                                    }
                                } else {
                                    // Buffer contains binary data (likely RDB file mixed with FULLRESYNC)
                                    // Check if it starts with +FULLRESYNC by looking at raw bytes
                                    if buf[0] == b'+' {
                                        // This is likely +FULLRESYNC followed by RDB data
                                        // Just consume it and reset the flag
                                    }
                                }
                            },

                            Err(_) => (),
                        }
                    }
                });
            },
            None => (),
        }
    } else { // MASTER
        let master_config = vec![
            ("role".to_string(), RedisValue::String("master".to_string())),
            ("master_replid".to_string(), RedisValue::String("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string())),
            ("master_repl_offset".to_string(), RedisValue::Number(0)),
        ];

        state.server_state_mut().update(&master_config);
    }
    
    loop {
        let (mut stream, addr) = listener.accept().await.unwrap();
            println!("accepted new connection");

        let mut buf = [0; 512];
        let mut local_state = state.clone();
        let mut local_replicas_state = replicas_state.clone();

        let count = connection_count.fetch_add(1, Ordering::SeqCst);
        if count >= 10000 {
            connection_count.fetch_sub(1, Ordering::SeqCst);
            continue;
        }

        let cc = connection_count.clone();
        tokio::spawn(async move {
            let mut client_state = ClientState::new(addr.to_string());
            
            loop {
                if client_state.is_subscribe_mode(){
                    if let Some(receiver) = client_state.get_sub_receiver_mut(){
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
                } else if client_state.is_replica(){
                    if let Some(receiver) = client_state.get_replica_receiver_mut(){
                        tokio::select! {
                            msg = receiver.recv() => {
                                if let Some(commands) = msg {
                                    let resp_command = encode_resp_array(&commands);
                                    stream.write_all(resp_command.as_bytes()).await.unwrap();
                                }
                            },

                            bytes_read = stream.read(&mut buf) => {
                                match bytes_read {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let parsed_commands = parse_resp(&buf[..n]);
                                        if let Some(commands) = parsed_commands {
                                            let _ = execute_commands_normal(
                                                &mut stream, 
                                                true, 
                                                &mut local_state, 
                                                &mut client_state, 
                                                &mut local_replicas_state, 
                                                addr.to_string(), 
                                                &commands
                                            ).await;
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
                                if client_state.is_multi_queue_mode(){
                                    match commands[0].as_str(){
                                        "EXEC" => {
                                            let mut responses = Vec::new();
                                            match client_state.commands_len(){
                                                0 => stream.write_all(b"*0\r\n").await.unwrap(),
                                                _ => {
                                                    while let Some(queued_command) = client_state.pop_command(){
                                                        let response = execute_commands_normal(&mut stream, false, &mut local_state, &mut client_state, &mut local_replicas_state, addr.to_string(), &queued_command).await;
                                                        responses.push(response);
                                                    }

                                                    let responses_array = format!("*{}\r\n{}", responses.len(), responses.join(""));
                                                    stream.write_all(responses_array.as_bytes()).await.unwrap();
                                                },
                                            }

                                            client_state.set_multi_queue_mode(false);
                                        },
                                        "DISCARD" => {
                                            client_state.clear_commands();
                                            client_state.set_multi_queue_mode(false);
                                            stream.write_all(b"+OK\r\n").await.unwrap()
                                        }
                                        _ => {
                                            client_state.push_command(commands);
                                            stream.write_all(b"+QUEUED\r\n").await.unwrap()
                                        },
                                    }
                                } else {
                                    match commands[0].as_str(){
                                        "EXEC" => stream.write_all(b"-ERR EXEC without MULTI\r\n").await.unwrap(),
                                        "DISCARD" => stream.write_all(b"-ERR DISCARD without MULTI\r\n").await.unwrap(),

                                        _ => {
                                            let _ = execute_commands_normal(
                                                &mut stream, 
                                                true, 
                                                &mut local_state, 
                                                &mut client_state, 
                                                &mut local_replicas_state, 
                                                addr.to_string(), 
                                                &commands
                                            ).await;
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
