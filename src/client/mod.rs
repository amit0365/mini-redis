mod subscribe_mode;
mod replica_mode;
mod normal_mode;

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::net::TcpStream;
use crate::{error::RedisResult, protocol::{ClientState, RedisState, RedisValue, ReplicasState}};

pub use subscribe_mode::handle_subscribe_mode;
pub use replica_mode::handle_replica_mode;
pub use normal_mode::handle_normal_mode;

pub fn can_accept_connection(connection_count: &Arc<AtomicUsize>, max: usize) -> bool {
    let count = connection_count.fetch_add(1, Ordering::SeqCst);
    if count >= max {
        connection_count.fetch_sub(1, Ordering::SeqCst);
        false
    } else {
        true
    }
}

pub fn spawn_client_handler(
    stream: TcpStream,
    client_addr: Arc<str>,
    state: RedisState<Arc<str>, RedisValue>,
    replicas_state: ReplicasState,
    connection_count: Arc<AtomicUsize>,
) {
    tokio::spawn(async move {
        if let Err(e) = handle_client_connection(stream, client_addr, state, replicas_state).await {
            eprintln!("Connection handler error: {}", e);
        }
        connection_count.fetch_sub(1, Ordering::SeqCst);
    });
}

pub async fn handle_client_connection(
    mut stream: TcpStream,
    client_addr: Arc<str>,
    state: RedisState<Arc<str>, RedisValue>,
    replicas_state: ReplicasState,
) -> RedisResult<()> {
    let mut buf = [0; 512];
    let mut local_state = state;
    let mut local_replicas_state = replicas_state;
    let mut client_state = ClientState::new();

    loop {
        let result = if client_state.is_subscribe_mode() {
            handle_subscribe_mode(
                &mut stream,
                &mut buf,
                &mut client_state,
                &mut local_state,
                &client_addr,
            ).await 
        } else if client_state.is_replica() {
            handle_replica_mode(
                &mut stream,
                &mut buf,
                &mut client_state,
                &mut local_state,
                &mut local_replicas_state,
                &client_addr,
            ).await
        } else {
            handle_normal_mode(
                &mut stream,
                &mut buf,
                &mut client_state,
                &mut local_state,
                &mut local_replicas_state,
                &client_addr,
            ).await
        };

        match result {
            Ok(()) => {
                continue;
            }
            Err(e) => {
                eprintln!("Client error from {}: {}", client_addr, e);
                return Err(e);
            }
        }
    }
}
