use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};
use base64::{Engine as _, engine::general_purpose};
use crate::error::RedisResult;
use crate::protocol::{ClientState, RedisState, RedisValue, ReplicasState};
use crate::utils::{EMPTY_RDB_FILE, encode_resp_array_arc, encode_resp_array_str};

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
            if commands[1..3].join(" ") == "GETACK *" && client_state.is_replica(){ //send update to master
                let offset_str = client_state.num_bytes_synced().to_string();
                encode_resp_array_str(&["REPLCONF", "ACK", &offset_str]) // can use itoa for string alloc
            } else {
                format!("+OK\r\n")
            }
        },
        "PSYNC" => {
            let full_sync_response = local_state.psync();
            if write_to_stream {
                stream.write_all(full_sync_response.as_bytes()).await.unwrap();
                let rdb_bytes = general_purpose::STANDARD.decode(EMPTY_RDB_FILE).unwrap();
                let rdb_message = [format!("${}\r\n", rdb_bytes.len()).into_bytes(), rdb_bytes].concat();
                stream.write_all(&rdb_message).await.unwrap();
            }

            {
                let mut replicas_senders_guard = replicas_state.replica_senders().lock().unwrap();
                let (sender, receiver) = mpsc::channel(1);
                replicas_senders_guard.entry(client_addr.clone()).insert_entry(sender);
                client_state.set_replica(true);
                client_state.set_replica_receiver(receiver);
            }

            replicas_state.increment_num_connected_replicas();
            local_state.server_state_mut().set_replication_mode(true);
            format!("") // send empty string since we don't write to stream
        }
        "WAIT" => {   
            let replica_senders_guard = replicas_state.replica_senders().lock().unwrap();
            let master_write_offset = replicas_state.get_master_write_offset();
            let replicas_write_offset = replicas_state.get_replica_offsets();
            for (id, _sender) in replica_senders_guard.iter(){
                //let replica_offset = replicas_write_offset.get(id).unwrap();
                //if *replica_offset < master_write_offset{
                    //send getack
                    //receive repsonse
                    //update replica offset
                    //if anyone is fully synced decrement count for the required number of replicas
                //} else {
                    //increment count as replica already synced
                //}
            }   

            format!(":{}\r\n", replicas_state.num_connected_replicas())
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
        _ => format!("$-1\r\n"), //todo fix
    };

    if write_to_stream && commands[0].to_uppercase().as_str() != "PSYNC" {
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    let is_write_command = matches!(
        commands[0].to_uppercase().as_str(),
        "SET" | "DEL" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR"
    );

    if local_state.server_state().replication_mode() && is_write_command && write_to_stream {
        let encoded: Arc<str> = Arc::from(encode_resp_array_arc(commands));
        replicas_state.increment_master_write_offset(encoded.len());
        let senders = {
            let replica_senders_guard = replicas_state.replica_senders().lock().unwrap();
            replica_senders_guard.values().cloned().collect::<Vec<_>>()
        };

        for sender in senders{
            sender.send(Arc::clone(&encoded)).await.unwrap();
        };
    }

    Ok(response)
}