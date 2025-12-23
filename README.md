# a2db — Active-Active Database

A high-performance, Redis-compatible, active-active replicated database built on CRDTs.

```
┌─────────────────┐         ┌─────────────────┐
│   a2db (NYC)    │◄───────►│   a2db (SFO)    │
│                 │  gRPC   │                 │
│  Redis:16379    │ Stream  │  Redis:16379    │
└────────┬────────┘         └────────┬────────┘
         │                           │
    ┌────┴────┐                 ┌────┴────┐
    │ Clients │                 │ Clients │
    └─────────┘                 └─────────┘
```

## Features

- **Active-Active Replication** — Write to any replica, changes merge automatically
- **Redis Protocol** — Use existing Redis clients and tools (`redis-cli`, client libraries)
- **CRDT-Based** — Conflict-free convergence using G-Counter and PN-Counter
- **TTL Support** — Keys can expire with MAX-wins merge semantics
- **String Storage** — LWW-Register for string values via `SET`/`GET`
- **High Performance** — 500K+ ops/sec on commodity hardware
- **Persistence** — Periodic snapshots in bincode or JSON format

## Installation

### Download Binary

Download the latest release for your platform:

```bash
# Linux x86_64
curl -L https://github.com/iluxav/active-active-db/releases/latest/download/a2db-linux-x86_64.tar.gz | tar xz
sudo mv a2db /usr/local/bin/

# Linux ARM64
curl -L https://github.com/iluxav/active-active-db/releases/latest/download/a2db-linux-aarch64.tar.gz | tar xz
sudo mv a2db /usr/local/bin/

# macOS Intel
curl -L https://github.com/iluxav/active-active-db/releases/latest/download/a2db-darwin-x86_64.tar.gz | tar xz
sudo mv a2db /usr/local/bin/

# macOS Apple Silicon
curl -L https://github.com/iluxav/active-active-db/releases/latest/download/a2db-darwin-aarch64.tar.gz | tar xz
sudo mv a2db /usr/local/bin/
```

### Build from Source

```bash
git clone https://github.com/iluxav/active-active-db.git
cd active-active-db
cargo build --release
sudo cp target/release/a2db /usr/local/bin/
```

## Quick Start

### Using Docker

```bash
# Start the server
docker compose up -d

# Connect with redis-cli
redis-cli -p 16379 INCR mykey
redis-cli -p 16379 GET mykey
```

### Using Binary

```bash
# Build
cargo build --release

# Run with CLI args
./target/release/a2db \
  --replica-id "node1" \
  --client-addr "0.0.0.0:9000" \
  --replication-addr "0.0.0.0:9001" \
  --redis-addr "0.0.0.0:6379"

# Or with config file
./target/release/a2db --config config/single-node.toml
```

### Using Make

```bash
make run            # Run in debug mode
make run-release    # Run in release mode
make test           # Run tests
make docker-up      # Start in Docker


```

## Supported Commands

### Counter Operations

| Command              | Description         |
| -------------------- | ------------------- |
| `INCR key`           | Increment by 1      |
| `INCRBY key amount`  | Increment by amount |
| `DECR key`           | Decrement by 1      |
| `DECRBY key amount`  | Decrement by amount |
| `GET key`            | Get current value   |
| `MGET key1 key2 ...` | Get multiple values |

### String Operations

| Command         | Description                     |
| --------------- | ------------------------------- |
| `SET key value` | Set string value (LWW-Register) |
| `GET key`       | Get string value                |
| `STRLEN key`    | Get string length               |

### TTL Operations

| Command              | Description                 |
| -------------------- | --------------------------- |
| `EXPIRE key seconds` | Set TTL in seconds          |
| `PEXPIRE key ms`     | Set TTL in milliseconds     |
| `TTL key`            | Get remaining TTL (seconds) |
| `PTTL key`           | Get remaining TTL (ms)      |
| `PERSIST key`        | Remove TTL                  |

### Utility Commands

| Command       | Description         |
| ------------- | ------------------- |
| `PING`        | Health check        |
| `INFO`        | Server information  |
| `DBSIZE`      | Number of keys      |
| `KEYS *`      | List all keys       |
| `EXISTS key`  | Check if key exists |
| `TYPE key`    | Get key type        |
| `SCAN cursor` | Iterate keys        |

## Configuration

### CLI Arguments

```bash
./target/release/a2db \
  --replica-id "us-west-1"           # Unique replica identifier
  --client-addr "0.0.0.0:9000"       # gRPC client address
  --replication-addr "0.0.0.0:9001"  # Replication address
  --redis-addr "0.0.0.0:6379"        # Redis protocol address
  --peer "http://peer1:9001"         # Peer replica (repeatable)
  --persistence                       # Enable snapshots
  --data-dir "./data"                # Snapshot directory
  --log-level "info"                 # trace/debug/info/warn/error
```

### Environment Variables

All CLI args can be set via environment variables with `A2DB_` prefix:

```bash
export A2DB_REPLICA_ID="node1"
export A2DB_CLIENT_ADDR="0.0.0.0:9000"
export A2DB_REPLICATION_ADDR="0.0.0.0:9001"
export A2DB_REDIS_ADDR="0.0.0.0:6379"
export A2DB_PEERS="http://peer1:9001,http://peer2:9001"
export A2DB_PERSISTENCE=true
```

### Config File (TOML)

```toml
[server]
client_listen_addr = "0.0.0.0:9000"
replication_listen_addr = "0.0.0.0:9001"
redis_listen_addr = "0.0.0.0:6379"

[identity]
replica_id = "us-west-1"

[replication]
peers = [
    "http://us-east-1:9001",
    "http://eu-west-1:9001",
]

[persistence]
enabled = true
data_dir = "./data"
snapshot_interval_s = 5
format = "bincode"  # or "json"

[logging]
level = "info"
format = "pretty"  # or "json"
```

## Architecture

### CRDT Types

- **G-Counter** — Grow-only counter for `INCR`
- **PN-Counter** — Positive-negative counter for `INCR`/`DECR`
- **LWW-Register** — Last-writer-wins for `SET` (timestamp-based)

### Replication

```
┌──────────┐    Delta Stream    ┌──────────┐
│ Replica A│◄──────────────────►│ Replica B│
│          │                    │          │
│  write() │                    │  write() │
│    ↓     │                    │    ↓     │
│  delta   │───► broadcast ────►│  merge   │
└──────────┘                    └──────────┘
```

- Bidirectional gRPC streaming between replicas
- Delta compaction to reduce bandwidth
- Idempotent, out-of-order safe message handling
- Automatic reconnection on network failures

### TTL Semantics

- **MAX-wins merge** — Conflicting TTLs resolve to the longest expiration
- **INCR preserves TTL** — Incrementing doesn't change existing expiration
- **Background cleanup** — Expired keys removed asynchronously with grace period

## Benchmarks

Run benchmarks against a2db:

```bash
# Local benchmark
make bench

# Docker benchmark
make docker-bench

# Compare with Redis
make compare
```

Typical results on Apple M1:

```
a2db:  ~500,000 ops/sec (INCR, 100 clients)
Redis: ~550,000 ops/sec (INCR, 100 clients)
```

## Multi-Region Deployment

See [`deploy/README.md`](deploy/README.md) for DigitalOcean multi-region deployment guide.

Example topology:

```bash
# NYC node
./a2db --replica-id nyc1 \
  --redis-addr 0.0.0.0:6379 \
  --replication-addr 0.0.0.0:9001 \
  --peer http://sfo1:9001

# SFO node
./a2db --replica-id sfo1 \
  --redis-addr 0.0.0.0:6379 \
  --replication-addr 0.0.0.0:9001 \
  --peer http://nyc1:9001
```

## Use Cases

### Rate Limiting

```bash
# Per-minute rate limit (key includes time bucket)
INCR ratelimit:user:123:202512231430
EXPIRE ratelimit:user:123:202512231430 120
```

### Distributed Counters

```bash
# Page views (safe to increment from any region)
INCR pageviews:article:456
```

### Feature Flags with Counts

```bash
# Track feature usage across regions
INCR feature:dark-mode:enabled
```

## Project Structure

```
active-active-db/
├── crates/
│   ├── a2db-core/      # CRDT logic, store, snapshots
│   ├── a2db-proto/     # Protobuf definitions
│   └── a2db-server/    # Server, Redis protocol, replication
├── config/             # Example configurations
├── deploy/             # Deployment scripts
├── proto/              # .proto files
└── Makefile
```

## Development

```bash
# Build
cargo build

# Test
cargo test

# Format & lint
make check

# Run single node
make run
```

## License

MIT
