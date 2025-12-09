use std::sync::Arc;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use crate::{error::{RedisError, RedisResult}, protocol::{ClientState, RedisState, RedisValue, ReplicasState}};
use crate::utils::parse_resp;
use crate::commands::execute_commands;

pub async fn handle_normal_mode(
    stream: &mut TcpStream,
    buf: &mut [u8; 512],
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: &Arc<str>,
) -> RedisResult<()> {
    match stream.read(buf).await {
        Ok(0) => Err(RedisError::ConnectionClosed),
        Ok(n) => {
            let commands = parse_resp(&buf[..n])?;
            if client_state.is_multi_queue_mode() {
                handle_multi_mode(stream, client_state, local_state, local_replicas_state, addr, commands).await?;
            } else {
                handle_non_multi_mode(stream, client_state, local_state, local_replicas_state, addr, commands).await?;
            }
            
            Ok(())
        }

        Err(e) => Err(RedisError::from(e)),
    }
}

async fn handle_multi_mode(
    stream: &mut TcpStream,
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: &Arc<str>,
    commands: Vec<Arc<str>>,
) -> RedisResult<()> {
    match commands[0].as_ref() {
        "EXEC" => {
            let mut responses = Vec::new();
            match client_state.commands_len() {
                0 => stream.write_all(b"*0\r\n").await?,
                _ => {
                    while let Some(queued_command) = client_state.pop_command() {
                        let response = execute_commands(
                            stream,
                            false,
                            local_state,
                            client_state,
                            local_replicas_state,
                            addr,
                            &queued_command
                        ).await?;
                        responses.push(response);
                    }

                    let responses_array = format!("*{}\r\n{}", responses.len(), responses.join(""));
                    stream.write_all(responses_array.as_bytes()).await?;
                },
            }

            client_state.set_multi_queue_mode(false);
        },
        "DISCARD" => {
            client_state.clear_commands();
            client_state.set_multi_queue_mode(false);
            stream.write_all(b"+OK\r\n").await?
        }
        _ => {
            client_state.push_command(commands);
            stream.write_all(b"+QUEUED\r\n").await?
        },
    }

    Ok(())
}

async fn handle_non_multi_mode(
    stream: &mut TcpStream,
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: &Arc<str>,
    commands: Vec<Arc<str>>,
) -> RedisResult<()> {
    match commands[0].as_ref() {
        "EXEC" => stream.write_all(b"-ERR EXEC without MULTI\r\n").await?,
        "DISCARD" => stream.write_all(b"-ERR DISCARD without MULTI\r\n").await?,

        _ => {
            execute_commands(
                stream,
                true,
                local_state,
                client_state,
                local_replicas_state,
                addr,
                &commands
            ).await?;
        }
    }

    Ok(())
}
