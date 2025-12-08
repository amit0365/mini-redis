use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use crate::protocol::{ClientState, RedisState, RedisValue, ReplicasState};
use crate::utils::parse_resp;
use crate::commands::execute_commands;

pub async fn handle_replica_mode(
    stream: &mut TcpStream,
    buf: &mut [u8; 512],
    client_state: &mut ClientState<String, String>,
    local_state: &mut RedisState<String, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: String,
) -> bool {
    if let Some(receiver) = client_state.get_replica_receiver_mut() {
        tokio::select! {
            msg = receiver.recv() => {
                if let Some(encoded_resp) = msg {
                    stream.write_all(encoded_resp.as_bytes()).await.unwrap();
                }
                true
            },

            //master reading from replica?
            bytes_read = stream.read(buf) => {
                match bytes_read {
                    Ok(0) => false,
                    Ok(n) => {
                        let parsed_commands = parse_resp(&buf[..n]);
                        if let Some(commands) = parsed_commands {
                            let _ = execute_commands(
                                stream,
                                true,
                                local_state,
                                client_state,
                                local_replicas_state,
                                addr,
                                &commands
                            ).await;
                        }
                        true
                    }

                    Err(_) => false,
                }
            }
        }
    } else {
        true
    }
}
