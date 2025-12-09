use std::sync::Arc;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use crate::{error::{RedisError, RedisResult}, protocol::{ClientState, RedisState, RedisValue}, utils::encode_resp_array_str};
use crate::utils::{encode_resp_array_arc_with_prefix, parse_resp};

pub async fn handle_subscribe_mode(
    stream: &mut TcpStream,
    buf: &mut [u8; 512],
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    addr: &Arc<str>,
) -> RedisResult<()> {
    let message_literal_arc = Arc::from("message");
    if let Some(receiver) = client_state.get_sub_receiver_mut() {
        tokio::select! {
            msg = receiver.recv() => {
                if let Some((channel_name_arc, message)) = msg {
                    let response = encode_resp_array_arc_with_prefix(&[message_literal_arc, channel_name_arc], &message);
                    stream.write_all(response.as_bytes()).await?;
                }
                Ok(())
            },

            bytes_read = stream.read(buf) => {
                match bytes_read {
                    Ok(0) => Err(RedisError::ConnectionClosed),
                    Ok(n) => {
                        let commands = parse_resp(&buf[..n])?;
                        match commands[0].to_uppercase().as_str() {
                            "SUBSCRIBE" => {
                                let response = local_state.subscribe(client_state, addr, &commands)?;
                                local_state.handle_subscriber(client_state, &commands).await?;
                                stream.write_all(response.as_bytes()).await?;
                            }
                            "PING" => {
                                let response = encode_resp_array_str(&["pong", ""]);
                                stream.write_all(response.as_bytes()).await?;
                            },
                            "UNSUBSCRIBE" => {
                                let response = local_state.unsubscribe(client_state, addr, &commands)?;
                                stream.write_all(response.as_bytes()).await?;
                            }

                            _ => {
                                let response = format!("-ERR Can't execute '{}' in subscribed mode\r\n", commands[0].to_lowercase());
                                stream.write_all(response.as_bytes()).await?;
                            }
                        }
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
