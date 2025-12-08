use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use crate::{protocol::{ClientState, RedisState, RedisValue}, utils::encode_resp_array_str};
use crate::utils::{encode_resp_array, encode_resp_array_with_arc, parse_resp};

pub async fn handle_subscribe_mode(
    stream: &mut TcpStream,
    buf: &mut [u8; 512],
    client_state: &mut ClientState<String, String>,
    local_state: &mut RedisState<String, RedisValue>,
    addr: &String,
) -> bool {
    if let Some(receiver) = client_state.get_sub_receiver_mut() {
        tokio::select! {
            msg = receiver.recv() => {
                if let Some((channel_name_arc, message)) = msg {
                    let prefix = vec!["message".to_string(), channel_name_arc.to_string()];
                    let response = encode_resp_array_with_arc(&prefix, &message);
                    stream.write_all(response.as_bytes()).await.unwrap();
                }
                true
            },

            bytes_read = stream.read(buf) => {
                match bytes_read {
                    Ok(0) => false,
                    Ok(n) => {
                        let parsed_commands = parse_resp(&buf[..n]);
                        if let Some(commands) = parsed_commands {
                            match commands[0].to_uppercase().as_str() {
                                "SUBSCRIBE" => {
                                    let response = local_state.subscribe(client_state, addr, &commands);
                                    local_state.handle_subscriber(client_state, &commands).await;
                                    stream.write_all(response.as_bytes()).await.unwrap()
                                }
                                "PING" => {
                                    let response = encode_resp_array_str(&vec!["pong", ""]);
                                    stream.write_all(response.as_bytes()).await.unwrap()
                                },
                                "UNSUBSCRIBE" => {
                                    let response = local_state.unsubscribe(client_state, addr, &commands);
                                    stream.write_all(response.as_bytes()).await.unwrap()
                                }

                                _ => {
                                    let response = format!("-ERR Can't execute '{}' in subscribed mode\r\n", commands[0].to_lowercase());
                                    stream.write_all(response.as_bytes()).await.unwrap()
                                }
                            }
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
