use std::{env, sync::{Arc, atomic::AtomicUsize}};
use tokio::net::TcpListener;
use crate::{protocol::{RedisState, ReplicasState, replication}, utils::ServerConfig};
mod protocol;
mod utils;
mod commands;
mod client;
mod error;


#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let args = env::args().collect::<Vec<_>>();
    let config = ServerConfig::parse_from_args(args);

    let listener = Arc::new(TcpListener::bind(format!("127.0.0.1:{}", config.port)).await.unwrap());
    let connection_count = Arc::new(AtomicUsize::new(0));
    let mut state = RedisState::new();
    let replicas_state = ReplicasState::new();

    replication::configure_server_role(&config, &mut state, replicas_state.clone()).await;

    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        println!("accepted new connection");

        if !client::can_accept_connection(&connection_count, 10000) {
            continue;
        }

        let client_addr = Arc::from(addr.to_string());
        client::spawn_client_handler(
            stream,
            client_addr,
            state.clone(),
            replicas_state.clone(),
            Arc::clone(&connection_count),
        );
    }
}