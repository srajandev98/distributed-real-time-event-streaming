# real-time-event-streaming

A lightweight distributed event streaming platform written in Go.

`real-time-event-streaming` is a distributed log-based messaging and event streaming system inspired by modern streaming platforms such as:
- Apache Kafka
- Redpanda
- Apache Pulsar
- NATS

The project focuses on building a production-oriented streaming architecture incrementally while keeping the internals understandable, modular, and maintainable.

---

# Vision

The goal of this project is to deeply explore the architecture and internals of distributed event streaming systems by building one from scratch.

The system is evolving toward:
- durable append-only storage
- partition-based scalability
- consumer coordination
- replication
- fault tolerance
- distributed metadata management
- production-grade architecture

The implementation prioritizes:
- clean subsystem boundaries
- maintainable architecture
- incremental evolution
- correctness
- distributed systems fundamentals

---

# Current Status

⚠️ The project is currently under active development and is not yet production-ready.

The current implementation already includes the foundational building blocks of a distributed streaming platform.

---

# Implemented Features

## Broker Server
- TCP-based broker
- concurrent client handling using goroutines
- connection-oriented request processing

---

## Durable Storage
- append-only logs
- file-based persistence
- automatic recovery after restart

---

## Topic Partitioning
- multiple partitions per topic
- deterministic key-based routing
- partition-local ordering guarantees

---

## Producer API
- message publishing
- automatic partition assignment
- partition-specific offsets

---

## Consumer API
- offset-based consumption
- replay support
- partition-specific reads

---

## Consumer Groups
- group membership
- partition assignment
- round-robin balancing
- partition ownership model

---

## Offset Management
- persistent offset commits
- consumer progress tracking
- recovery after restart

---

## Replication (Simplified)
- leader log replication
- replica log persistence
- synchronous local replication model

---

# High-Level Architecture

```text
                    ┌──────────────────┐
                    │     Producer      │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      Broker       │
                    └────────┬─────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼

   Partition 0         Partition 1         Partition 2
   Leader Log          Leader Log          Leader Log
   Replica Logs        Replica Logs        Replica Logs

         │                   │                   │
         └───────────────────┼───────────────────┘
                             ▼

                    ┌──────────────────┐
                    │ Consumer Groups   │
                    └──────────────────┘
```

---

# Architecture Overview

The codebase is organized around subsystem boundaries rather than feature grouping.

```text
real-time-event-streaming/
│
├── cmd/
│   └── broker/
│       └── main.go
│
├── internal/
│   │
│   ├── broker/
│   │   └── broker.go
│   │
│   ├── storage/
│   │   └── storage.go
│   │
│   ├── coordinator/
│   │   ├── coordinator.go
│   │   ├── group.go
│   │   └── offset.go
│   │
│   ├── network/
│   │   └── handler.go
│   │
│   ├── replication/
│   │
│   ├── protocol/
│   │
│   ├── config/
│   │
│   └── types/
│       └── message.go
│
├── data/
│
├── README.md
│
└── go.mod
```

---

# Subsystem Responsibilities

## broker/
Orchestrates all major subsystems.

Acts as the composition root of the application.

---

## storage/
Responsible for:
- append-only logs
- partitions
- disk persistence
- recovery

---

## coordinator/
Responsible for:
- consumer groups
- partition assignment
- offsets
- rebalancing

---

## network/
Responsible for:
- TCP connections
- request handling
- protocol routing

---

## replication/
Reserved for:
- ISR tracking
- acknowledgements
- replica synchronization
- leader election

---

## protocol/
Reserved for:
- serialization
- request framing
- binary wire protocol

---

# Storage Model

Each partition is backed by a dedicated append-only log file.

Example:

```text
data/orders-0.log
data/orders-1.log
data/orders-2.log
```

Replica logs are stored independently:

```text
data/orders-0-replica-1.log
data/orders-0-replica-2.log
```

Offsets are persisted separately:

```text
data/offsets.json
```

---

# Partitioning Model

Messages are routed using deterministic hashing.

Messages sharing the same key always go to the same partition.

Example:

```text
user1 -> partition 2
user1 -> partition 2
user1 -> partition 2
```

This preserves ordering guarantees per key.

---

# Consumer Group Model

Partitions are distributed across consumers within the same group.

Example:

```text
consumer-a -> partitions [0,2]
consumer-b -> partitions [1]
```

This enables:
- parallel consumption
- horizontal scaling
- work distribution without duplication

---

# Replication Model

Each partition maintains:
- one leader log
- multiple replica logs

Current implementation:
- synchronous local replication
- file-based replicas

Planned implementation:
- network replication
- ISR tracking
- acknowledgement quorum
- leader election

---

# Networking Protocol

The broker currently exposes a simple text-based TCP protocol.

---

## Produce Message

### Request

```text
PRODUCE orders user1:created
```

### Meaning

```text
topic = orders
key = user1
message = created
```

---

## Consume Messages

### Request

```text
CONSUME orders 1 0
```

### Meaning

```text
topic = orders
partition = 1
offset = 0
```

---

## Join Consumer Group

### Request

```text
JOIN analytics orders consumer-a
```

### Response

```text
ASSIGNED [0 2]
```

---

## Commit Offset

### Request

```text
COMMIT analytics orders 1 42
```

### Response

```text
COMMIT OK
```

---

## Fetch Offset

### Request

```text
OFFSET analytics orders 1
```

### Response

```text
42
```

---

# Running Locally

## Requirements

- Go 1.24+

---

## Start Broker

```bash
go run ./cmd/broker
```

Expected output:

```text
real-time-event-streaming broker listening on port 9092
```

---

# Deploy With Docker

## Prerequisites

- Docker
- Docker Compose (v2)

---

## Start Broker

```bash
docker compose up --build -d
```

This starts the broker on `localhost:9092` and persists broker data in a named volume (`rtes_data`).

---

## View Logs

```bash
docker compose logs -f broker
```

---

## Stop Broker

```bash
docker compose down
```

---

## Stop Broker And Remove Data

```bash
docker compose down -v
```

---

## Runtime Configuration (Environment Variables)

- `RTES_LISTEN_ADDR` default: `:9092`
- `RTES_DATA_DIR` default: `/app/data`
- `RTES_NUM_PARTITIONS` default: `3`

Update these values in `docker-compose.yml` under `services.broker.environment`.

---

# Example Usage

## Open Producer Connection

```bash
nc localhost 9092
```

Publish messages:

```text
PRODUCE orders user1:created
PRODUCE orders user1:paid
PRODUCE orders user2:shipped
```

---

## Open Consumer Connection

```bash
nc localhost 9092
```

Consume messages:

```text
CONSUME orders 2 0
```

---

# Design Principles

## Append-Only Storage
Messages are immutable and only appended to logs.

---

## Partition-Based Scalability
Partitions are the fundamental scalability unit.

---

## Ordering Guarantees
Ordering is guaranteed per partition.

---

## Durable Persistence
Logs and offsets survive broker restarts.

---

## Explicit Coordination
Consumer ownership and partition assignment are coordinated explicitly.

---

## Subsystem Isolation
Each subsystem owns a clearly defined responsibility boundary.

---

# Current Limitations

The current implementation intentionally keeps many distributed systems concerns simplified.

Not yet implemented:
- multi-node clustering
- network replication
- ISR tracking
- replication acknowledgements
- leader election
- batching
- compression
- retention policies
- segment files
- indexed reads
- binary wire protocol
- authentication
- authorization
- metrics
- observability
- backpressure
- exactly-once semantics
- metadata quorum management

---

# Roadmap

## Replication
- acknowledgement quorum
- ISR tracking
- replica synchronization
- leader election

---

## Storage Engine
- segment files
- sparse indexes
- retention policies
- log compaction

---

## Networking
- binary wire protocol
- request framing
- protocol versioning

---

## Distributed Coordination
- metadata quorum
- broker discovery
- distributed consensus

---

## Performance
- batching
- compression
- zero-copy reads
- page cache optimization

---

## Observability
- metrics
- tracing
- health checks
- structured logging

---

# Why This Project Exists

Modern distributed streaming systems are often difficult to understand internally because of their scale and operational complexity.

This project exists to:
- make distributed log internals understandable
- incrementally build production-grade streaming primitives
- provide a readable Go implementation of distributed systems concepts

---

# Contributing

Contributions, discussions, and architectural feedback are welcome.

Areas of interest:
- distributed systems
- storage engines
- replication protocols
- consensus algorithms
- stream processing
- Go systems programming

---

# License

MIT
