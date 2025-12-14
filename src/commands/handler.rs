use std::sync::Arc;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};
use base64::{Engine as _, engine::general_purpose};
use crate::error::{RedisResult, RedisError};
use crate::protocol::{ClientState, RedisState, RedisValue, ReplicasState};
use crate::utils::{EMPTY_RDB_FILE, encode_resp_array_arc, encode_resp_array_str, encode_resp_array_str_to_arc};

pub async fn execute_commands(
    stream: &mut TcpStream,
    write_to_stream: bool,
    local_state: &mut RedisState<Arc<str>, RedisValue>,
    client_state: &mut ClientState<Arc<str>, Arc<str>>,
    replicas_state: &mut ReplicasState,
    client_addr: &Arc<str>,
    commands: &Vec<Arc<str>>
) -> RedisResult<String>{
    let response = match commands[0].to_uppercase().as_str() {
        "PING" => format!("+PONG\r\n"),
        "ECHO" => format!("${}\r\n{}\r\n", &commands[1].len(), &commands[1]), // fix multiple arg will fail like hello world. check to use .join("")
        "REPLCONF" => {
            if commands[1].to_uppercase() == "ACK"{
                //master received update from replica
                let replica_id = client_state.get_replica_id();
                let replica_offset = commands[2].parse::<usize>()?;
                replicas_state.ack_tx().try_send((replica_id, replica_offset))?;
                format!("") //no response required
            } else if commands[1..3].join(" ") == "GETACK *" && client_state.is_replica(){ 
                //send update to master
                let num_bytes_synced = client_state.num_bytes_synced();
                encode_resp_array_str(&["REPLCONF", "ACK", &num_bytes_synced.to_string()]) // can use itoa for string alloc
            } else {
                format!("+OK\r\n")
            }
        },
        "PSYNC" => {
            let full_sync_response = local_state.psync()?;
            let num_connected_replicas = replicas_state.num_connected_replicas();
            if write_to_stream {
                stream.write_all(full_sync_response.as_bytes()).await?;
                let rdb_bytes = general_purpose::STANDARD.decode(EMPTY_RDB_FILE)
                    .map_err(|e| RedisError::Other(format!("Failed to decode RDB: {}", e)))?;
                let rdb_message = [format!("${}\r\n", rdb_bytes.len()).into_bytes(), rdb_bytes].concat();
                stream.write_all(&rdb_message).await?;
            }

            {
                let mut replicas_senders_guard = replicas_state.replica_senders().lock()
                    .map_err(|_| RedisError::Other("Failed to acquire replica senders lock".to_string()))?;
                let (sender, receiver) = mpsc::channel(1);
                let key = num_connected_replicas + 1;
                replicas_senders_guard.entry(key).insert_entry(sender);

                client_state.set_replica_id(key);
                client_state.set_replica(true);
                client_state.set_replica_receiver(receiver);
                replicas_state.init_replica_offset(key);
            }

            replicas_state.increment_num_connected_replicas();
            local_state.server_state_mut().set_replication_mode(true);
            format!("") // send empty string since we don't write to stream
        }
        "WAIT" => {
            let num_required_synced_replicas = commands[1].parse::<usize>()
                .map_err(|_| RedisError::Other("Invalid number of replicas".to_string()))?;
            let timeout_ms = commands[2].parse::<u64>()
                .map_err(|_| RedisError::Other("Invalid timeout value".to_string()))?;
            
            let master_write_offset = replicas_state.get_master_write_offset();
            let get_ack_request = encode_resp_array_str_to_arc(&["REPLCONF", "GETACK", "*"]);

            let senders = {
                let replica_senders_guard = replicas_state.replica_senders().lock()
                    .map_err(|_| RedisError::Other("Failed to acquire replica senders lock".to_string()))?;
                    replica_senders_guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()
            };

            let mut num_synced_replicas = 0;
            //todo dont send getack to all only if they are pending check
            for (id, sender) in &senders {
                let replica_offset = replicas_state.get_replica_offset(*id)
                    .ok_or_else(|| RedisError::KeyNotFound(format!("Replica offset not found for {}", id)))?;
                if replica_offset < master_write_offset {
                    sender.send(get_ack_request.clone()).await.unwrap();
                } else { num_synced_replicas += 1 }
            }

            let ack_rx = replicas_state.ack_rx().clone();
            let count = Arc::new(std::sync::Mutex::new(0usize));
            let offsets = Arc::new(std::sync::Mutex::new(Vec::new()));
            let count_clone = count.clone();
            let offsets_clone = offsets.clone();

            let _ = tokio::time::timeout(
                Duration::from_millis(timeout_ms), async move {
                    let mut receiver_guard = ack_rx.lock().await;
                    loop {
                        if *count_clone.lock().unwrap() >= num_required_synced_replicas {
                            break;
                        }
                        match receiver_guard.recv().await {
                            Some((replica_id, replica_offset)) => {
                                if replica_offset >= master_write_offset {
                                    *count_clone.lock().unwrap() += 1;
                                }
                                offsets_clone.lock().unwrap().push((replica_id, replica_offset));
                            },
                            None => continue,
                        }
                    }
                }
            ).await;

            num_synced_replicas += *count.lock().unwrap();
            let received_offsets = std::mem::take(&mut *offsets.lock().unwrap());

            for (replica_id, replica_offset) in received_offsets {
                replicas_state.update_replica_offsets(replica_id, replica_offset);
            }

            format!(":{}\r\n", num_synced_replicas)
        },
        "SET" => local_state.set(&commands)?,
        "GET" => local_state.get(&commands)?,
        "RPUSH" => local_state.rpush(&commands)?,
        "LPUSH" => local_state.lpush(&commands)?,
        "LLEN" => local_state.llen(&commands)?,
        "LPOP" => local_state.lpop(&commands)?,
        "BLPOP" => local_state.blpop(&commands).await?,
        "LRANGE" => local_state.lrange(&commands[1], &commands[2], &commands[3])?,
        "TYPE" => local_state.type_command(&commands)?,
        "XADD" => local_state.xadd(&commands)?,
        "XRANGE" => local_state.xrange(&commands)?,
        "XREAD" => local_state.xread(&commands).await?,
        "SUBSCRIBE" => {
            let count_response = local_state.subscribe(client_state, &client_addr,  &commands)?;
            local_state.handle_subscriber(client_state, &commands).await?;
            count_response
        }
        "PUBLISH" => local_state.publish(&commands)?,
        "INCR" => local_state.incr(&commands)?,
        "MULTI" => local_state.multi(client_state)?,
        "INFO" => local_state.info(&commands)?,
        "ZADD" => local_state.zadd(&commands)?,
        "ZRANK" => local_state.zrank(&commands)?,
        "ZRANGE" => local_state.zrange(&commands)?,
        "ZCARD" => local_state.zcard(&commands)?,
        "ZSCORE" => local_state.zscore(&commands)?,
        "ZREM" => local_state.zrem(&commands)?,
        "GEOADD" => local_state.geoadd(&commands)?,
        "GEOPOS" => local_state.geopos(&commands)?,
        "GEODIST" => local_state.geodist(&commands)?,
        "GEOSEARCH" => local_state.geosearch(&commands)?,
        "ACL" => local_state.acl(&commands)?,
        "AUTH" => local_state.auth(&commands)?,
        _ => format!("$-1\r\n"), //todo fix
    };

    if write_to_stream && commands[0].to_uppercase().as_str() != "PSYNC" {
        stream.write_all(response.as_bytes()).await?;
    }

    let is_write_command = matches!(
        commands[0].to_uppercase().as_str(),
        "SET" | "DEL" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR"
    );

    if local_state.server_state().replication_mode() && is_write_command && write_to_stream {
        let encoded: Arc<str> = Arc::from(encode_resp_array_arc(commands));
        replicas_state.increment_master_write_offset(encoded.len());
        let senders = {
            let replica_senders_guard = replicas_state.replica_senders().lock()
                .map_err(|_| RedisError::Other("Failed to acquire replica senders lock".to_string()))?;
            replica_senders_guard.values().cloned().collect::<Vec<_>>()
        };

        for sender in senders{
            sender.send(Arc::clone(&encoded)).await
                .map_err(|e| RedisError::Other(format!("Failed to send to replica: {}", e)))?;
        };
    }

    Ok(response)
}