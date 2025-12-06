use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use crate::protocol::{ClientState, RedisState, RedisValue, ReplicasState};
use crate::utils::parse_resp;
use crate::commands::execute_commands_normal;

pub async fn handle_normal_mode(
    stream: &mut TcpStream,
    buf: &mut [u8; 512],
    client_state: &mut ClientState<String, String>,
    local_state: &mut RedisState<String, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: String,
) -> bool {
    match stream.read(buf).await {
        Ok(0) => false,
        Ok(n) => {
            let parsed_commands = parse_resp(&buf[..n]);
            if let Some(commands) = parsed_commands {
                if client_state.is_multi_queue_mode() {
                    handle_multi_mode(stream, client_state, local_state, local_replicas_state, addr, commands).await;
                } else {
                    handle_non_multi_mode(stream, client_state, local_state, local_replicas_state, addr, commands).await;
                }
            }
            true
        }

        Err(_) => false,
    }
}

async fn handle_multi_mode(
    stream: &mut TcpStream,
    client_state: &mut ClientState<String, String>,
    local_state: &mut RedisState<String, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: String,
    commands: Vec<String>,
) {
    match commands[0].as_str() {
        "EXEC" => {
            let mut responses = Vec::new();
            match client_state.commands_len() {
                0 => stream.write_all(b"*0\r\n").await.unwrap(),
                _ => {
                    while let Some(queued_command) = client_state.pop_command() {
                        let response = execute_commands_normal(
                            stream,
                            false,
                            local_state,
                            client_state,
                            local_replicas_state,
                            addr.clone(),
                            &queued_command
                        ).await;
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
}

async fn handle_non_multi_mode(
    stream: &mut TcpStream,
    client_state: &mut ClientState<String, String>,
    local_state: &mut RedisState<String, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: String,
    commands: Vec<String>,
) {
    match commands[0].as_str() {
        "EXEC" => stream.write_all(b"-ERR EXEC without MULTI\r\n").await.unwrap(),
        "DISCARD" => stream.write_all(b"-ERR DISCARD without MULTI\r\n").await.unwrap(),

        _ => {
            let _ = execute_commands_normal(
                stream,
                true,
                local_state,
                client_state,
                local_replicas_state,
                addr,
                &commands
            ).await;
        }
    }
}
