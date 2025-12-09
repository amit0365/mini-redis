use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use std::{str::from_utf8, sync::Arc};
use crate::{error::RedisResult, protocol::{ClientState, RedisState, RedisValue, ReplicasState}, utils::encode_resp_array_arc};
use crate::utils::{encode_resp_array_str, parse_multiple_resp, parse_rdb_with_trailing_commands, ServerConfig};
use crate::commands::execute_commands;

// Helper function to process commands after RDB file during handshake or after handshake complete
pub async fn process_commands_from_master(
    commands_data: &[u8],
    master_stream: &mut TcpStream,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_replicas_state: &mut ReplicasState,
) -> RedisResult<()>{
    let all_commands = parse_multiple_resp(commands_data)?;
    let master_literal = Arc::from("master");
    for commands in all_commands {
        let is_getack = commands.len() == 3
            && commands[0].to_uppercase() == "REPLCONF"
            && commands[1].to_uppercase() == "GETACK"
            && commands[2].to_string() == "*";

        let response = execute_commands(
            master_stream,
            false,
            local_state,
            client_state,
            local_replicas_state,
            &master_literal, // todo fix this properly
            &commands
        ).await?;

        if is_getack {
            master_stream.write_all(response.as_bytes()).await?;
        }

        client_state.add_num_bytes_synced(encode_resp_array_arc(&commands).len()); //todo fix extra work reencoding again just for length
    }

    Ok(())
}

pub async fn handle_handshake(
    buf: &[u8],
    handshake_complete: &mut bool,
    expecting_rdb: &mut bool,
    replconf_ack_count: &mut u8,
    master_stream: &mut TcpStream,
    port: &String,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_replicas_state: &mut ReplicasState,
) -> RedisResult<()> {
    // JUST FULLRESYNC IN BUFFER
    if let Ok(buffer_str) = from_utf8(buf) {
        let parts: Vec<&str> = buffer_str.split("\r\n").collect();
        match parts[0] {
            "+PONG" => {
                let replconf_msg1 = encode_resp_array_str(&["REPLCONF", "listening-port", &port]);
                master_stream.write(replconf_msg1.as_bytes()).await?;
                let replconf_msg2 = encode_resp_array_str(&["REPLCONF", "capa", "psync2"]);
                master_stream.write(replconf_msg2.as_bytes()).await?;
            },
            "+OK" => {
                *replconf_ack_count += 1;
                if *replconf_ack_count == 2 {
                    let psync_msg = encode_resp_array_str(&["PSYNC", "?", "-1"]);
                    master_stream.write(psync_msg.as_bytes()).await?;
                }
            }
            _ => {
                if parts[0].starts_with("+FULLRESYNC") {
                    *expecting_rdb = true;
                }
            }
        }
        return Ok(());
    }

    // PARSE RDB NEXT
    if *expecting_rdb {
        if let Ok(rdb_end) = parse_rdb_with_trailing_commands(buf, 0) {
            if rdb_end <= buf.len() {
                *handshake_complete = true;
                *expecting_rdb = false;

                if rdb_end < buf.len() {
                    process_commands_from_master(
                        &buf[rdb_end..],
                        master_stream,
                        local_state,
                        client_state,
                        local_replicas_state,
                    ).await?;
                }
            }
        }
        return Ok(());
    }

    // ELSE FULLRESYNC + RDB combined
    *handshake_complete = true;

    // Skip +FULLRESYNC line if present
    let mut rdb_start = 0;
    if buf[0] == b'+' {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            rdb_start = pos + 2;
        }
    }

    if let Ok(rdb_end) = parse_rdb_with_trailing_commands(buf, rdb_start) {
        if rdb_end < buf.len() {
            process_commands_from_master(
                &buf[rdb_end..],
                master_stream,
                local_state,
                client_state,
                local_replicas_state,
            ).await?;
        }
    }
    
    Ok(())
}

pub async fn handle_replication_commands(
    buf: &[u8],
    master_stream: &mut TcpStream,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_replicas_state: &mut ReplicasState,
) -> RedisResult<()> {
    process_commands_from_master(
        buf,
        master_stream,
        local_state,
        client_state,
        local_replicas_state,
    ).await
}

pub fn initialize_master_state(state: &mut RedisState<Arc<str>, RedisValue>) {
    let master_config = vec![
        (Arc::from("role"), RedisValue::String(Arc::from("master"))),
        (Arc::from("master_replid"), RedisValue::String(Arc::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"))),
        (Arc::from("master_repl_offset"), RedisValue::Number(0)),
    ];

    state.server_state_mut().update(master_config);
}

pub async fn initialize_replica_connection(
    config: &ServerConfig,
    state: RedisState<Arc<str>, RedisValue>,
    replicas_state: ReplicasState,
) -> tokio::task::JoinHandle<()> {
    let master_contact = config.master_contact_for_slave.clone().unwrap();
    let port = config.port.clone();

    let (master_ip, master_port) = master_contact.split_once(" ").unwrap();
    let mut master_stream = TcpStream::connect(format!("{}:{}", master_ip, master_port)).await.unwrap();

    tokio::spawn(async move {
        let mut handshake_complete = false;
        let mut expecting_rdb = false;
        let mut local_state = state;
        let mut local_replicas_state = replicas_state;
        let mut client_state = ClientState::new();
        client_state.set_replica(true);
        client_state.set_address_replica(port.clone()); // is port good as identifier
        let mut replconf_ack_count = 0;

        let ping_msg = encode_resp_array_str(&["PING"]);
        if let Err(e) = master_stream.write(ping_msg.as_bytes()).await {
            eprintln!("Failed to send PING to master: {}", e);
            return;
        }

        loop {
            let mut buf = [0; 512];
            match master_stream.read(&mut buf).await {
                Ok(0) => {
                    eprintln!("Master connection closed");
                    break;
                },
                Ok(n) => {
                    let result = if !handshake_complete {
                        handle_handshake(
                            &buf[..n],
                            &mut handshake_complete,
                            &mut expecting_rdb,
                            &mut replconf_ack_count,
                            &mut master_stream,
                            &port,
                            &mut local_state,
                            &mut client_state,
                            &mut local_replicas_state,
                        ).await
                    } else {
                        handle_replication_commands(
                            &buf[..n],
                            &mut master_stream,
                            &mut local_state,
                            &mut client_state,
                            &mut local_replicas_state,
                        ).await
                    };
                    
                    if let Err(e) = result {
                        eprintln!("Replication error: {}", e);
                        break;
                    }
                },
                Err(e) => {
                    eprintln!("Failed to read from master: {}", e);
                    break;
                },
            }
        }
    })
}

pub async fn configure_server_role(
    config: &ServerConfig,
    state: &mut RedisState<Arc<str>, RedisValue>,
    replicas_state: ReplicasState,
) -> Option<tokio::task::JoinHandle<()>> {
    if let Some(_) = config.master_contact_for_slave {
        state.server_state_mut().map_mut().insert(Arc::from("role"), RedisValue::String(Arc::from("slave")));
        Some(initialize_replica_connection(config, state.clone(), replicas_state).await)
    } else {
        initialize_master_state(state);
        None
    }
}
