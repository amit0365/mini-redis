use std::sync::Arc;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use crate::{error::{RedisError, RedisResult}, protocol::{ClientState, RedisState, RedisValue, ReplicasState}};
use crate::utils::parse_resp;
use crate::commands::execute_commands;

pub async fn handle_replica_mode(
    stream: &mut TcpStream,
    buf: &mut [u8; 512],
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    local_replicas_state: &mut ReplicasState,
    addr: &Arc<str>,
) -> RedisResult<()> {
    if let Some(receiver) = client_state.get_replica_receiver_mut() {
        tokio::select! {
            msg = receiver.recv() => {
                if let Some(encoded_resp) = msg {
                    stream.write_all(encoded_resp.as_bytes()).await?;
                }
                Ok(())
            },

            bytes_read = stream.read(buf) => {
                match bytes_read {
                    Ok(0) => Err(RedisError::ConnectionClosed),
                    Ok(n) => {
                        let commands = parse_resp(&buf[..n])?;
                        execute_commands(
                            stream,
                            true,
                            local_state,
                            client_state,
                            local_replicas_state,
                            addr,
                            &commands
                        ).await?;

                        Ok(())
                    }

                    Err(e) => Err(RedisError::from(e)),
                }
            }
        }
    } else {
        Ok(())
    }
}
