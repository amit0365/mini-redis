# mini-redi

A Redis-like server implementation written in Rust using async/await with Tokio. This project implements a subset of Redis's functionality, focusing on core commands, data structures, replication, and pub/sub.

## Features

This implementation supports the following Redis commands:

- **String:** `SET`, `GET`, `INCR`
- **List:** `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`
- **Stream:** `XADD`, `XRANGE`, `XREAD`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- **Pub/Sub:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- **Connection:** `PING`, `ECHO`
- **Server:** `INFO`, `TYPE`
- **Replication:** `REPLCONF`, `PSYNC` (master-replica replication)

## Architecture

- Built with **Tokio** for async I/O and runtime
- Supports concurrent client connections (up to 10,000)
- Implements master-replica replication
- Pub/Sub messaging with channel-based communication
- Transaction support with command queueing

## Getting Started

### Prerequisites

- Rust 1.70 or higher
- Cargo (comes with Rust)

### Building

To build the server, clone the repository and run:

```sh
cargo build --release
```

### Running the Server

Run the server using:

```sh
cargo run --release -- [--port <port>] [--replicaof <master_host> <master_port>]
```

Or run the pre-built binary:

```sh
./target/release/codecrafters-redis [--port <port>] [--replicaof <master_host> <master_port>]
```

**Options:**
- `--port`: The port to listen on (default: `6379`)
- `--replicaof`: Configure as a replica of the specified master (format: `<host> <port>`)

**Examples:**

```sh
# Run as master on default port 6379
cargo run --release

# Run as master on custom port
cargo run --release -- --port 6380

# Run as replica
cargo run --release -- --port 6380 --replicaof 127.0.0.1 6379
```

## Usage

Connect to the server using any Redis client such as `redis-cli`:

```sh
redis-cli -p 6379
```

Once connected, you can use any of the supported commands:

```redis
SET mykey "Hello"
GET mykey
INCR counter
LPUSH mylist "item1" "item2"
XADD stream:1 * field1 value1
MULTI
SET key1 val1
SET key2 val2
EXEC
SUBSCRIBE channel1
PUBLISH channel1 "message"
```

## Development

The project structure:

- `src/main.rs` - Main server implementation and command execution
- `src/protocol/` - Redis protocol parsing and state management
- `src/utils.rs` - RESP protocol encoding/decoding utilities

### Running in Development

```sh
cargo run -- --port 6379
```

### Testing

The project uses CodeCrafters for testing. To run tests:

```sh
./your_program.sh
```