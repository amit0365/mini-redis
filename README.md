# mini-redis

A Redis server implementation written from scratch in Rust using Tokio. This project implements a subset of Redis's functionality, focusing on core commands, data structures, replication, and pub/sub. This project was built as part of the [Build your own Redis](https://app.codecrafters.io/courses/redis/overview).

## Architecture

- Built with **Tokio** for async I/O and runtime
- Supports concurrent client connections (up to 10,000)
- Implements master-replica replication with PSYNC and RDB snapshots
- Pub/Sub messaging with channel-based communication
- Transaction support with command queueing
- Rate Limit to max 10,000 concurrent connections

## Features

### Supported Commands

- **String:** `SET`, `GET`, `INCR`
- **List:** `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`
- **Stream:** `XADD`, `XRANGE`, `XREAD`
- **Sorted Set:** `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`
- **Geospatial:** `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- **Pub/Sub:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- **Connection:** `PING`, `ECHO`
- **Server:** `INFO`, `TYPE`, `WAIT`
- **Replication:** `REPLCONF`, `PSYNC` (master-replica replication)

### Core Stages
- Bind to a port
- Respond to PING
- Respond to multiple PINGs
- Handle concurrent clients
- Implement the ECHO command
- Implement the SET & GET commands
- Expiry

### Lists
- Create a list
- Append an element (RPUSH)
- Append multiple elements
- List elements with positive indexes (LRANGE)
- List elements with negative indexes
- Prepend elements (LPUSH)
- Query list length (LLEN)
- Remove an element (LPOP)
- Remove multiple elements
- Blocking retrieval (BLPOP)
- Blocking retrieval with timeout

### Streams
- The TYPE command
- Create a stream (XADD)
- Validating entry IDs
- Partially auto-generated IDs
- Fully auto-generated IDs
- Query entries from stream (XRANGE)
- Query with `-`
- Query with `+`
- Query single stream using XREAD
- Query multiple streams using XREAD
- Blocking reads
- Blocking reads without timeout
- Blocking reads using `$`

### Transactions
- The INCR command
- The MULTI command
- The EXEC command
- Empty transaction
- Queueing commands
- Executing a transaction
- The DISCARD command
- Failures within transactions
- Multiple transactions

### Replication
- Configure listening port
- The INFO command
- The INFO command on a replica
- Initial replication ID and offset
- Send handshake (REPLCONF, PSYNC)
- Receive handshake
- Empty RDB transfer
- Single-replica propagation
- Multi-replica propagation
- Command processing
- ACKs with no commands
- ACKs with commands
- WAIT with no replicas
- WAIT with no commands
- WAIT with multiple commands

### Pub/Sub
- Subscribe to a channel
- Subscribe to multiple channels
- Enter subscribed mode
- PING in subscribed mode
- Publish a message
- Deliver messages
- Unsubscribe

### Sorted Sets
- Create a sorted set
- Add members
- Retrieve member rank
- List sorted set members
- ZRANGE with negative indexes
- Count sorted set members
- Retrieve member score
- Remove a member

### Geospatial Commands
- Respond to GEOADD
- Validate coordinates
- Store a location
- Calculate location score
- Respond to GEOPOS
- Decode coordinates
- Calculate distance
- Search within radius

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

### Project Structure

The codebase has been refactored into a modular architecture for better maintainability and separation of concerns:

```
src/
├── main.rs                      # Server initialization and entry point
├── protocol/
│   ├── mod.rs                   # Protocol module exports
│   ├── state.rs                 # Redis state and server state management
│   ├── value.rs                 # RedisValue type definitions
│   └── replication.rs           # Replication handshake and sync logic
├── commands/
│   ├── mod.rs                   # Command module exports
│   └── handler.rs               # Command execution and routing
├── client/
│   ├── mod.rs                   # Client module exports
│   ├── normal_mode.rs           # Standard client connection handling
│   ├── subscribe_mode.rs        # Pub/Sub client connection handling
│   └── replica_mode.rs          # Replica-to-master connection handling
└── utils.rs                     # RESP protocol encoding/decoding utilities
```

**Key Modules:**
- **`protocol/`** - Core Redis protocol types, state management, and replication logic
- **`commands/`** - Command parsing, validation, and execution
- **`client/`** - Different client connection modes (normal, subscribe, replica)
- **`utils.rs`** - Low-level RESP protocol utilities and configuration

### Running in Development

```sh
cargo run -- --port 6379
```

### Testing

The project uses CodeCrafters for testing. To run tests:

```sh
./your_program.sh
```