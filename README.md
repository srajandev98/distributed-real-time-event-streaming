# real-time-event-streaming

A lightweight distributed event streaming platform written in Go.

This project is an educational-but-production-oriented implementation of a distributed log system inspired by modern streaming platforms such as:
- Apache Kafka
- Redpanda
- Apache Pulsar
- NATS

The goal of the project is to incrementally evolve from a minimal log-based broker into a production-grade distributed streaming system while keeping the implementation understandable and approachable.

---

# Vision

This project aims to explore and implement the core architectural primitives behind modern event streaming systems:

- append-only logs
- partition-based scalability
- durable storage
- consumer coordination
- offset management
- replication
- fault tolerance
- distributed consensus
- stream processing foundations

The project prioritizes:
- architectural clarity
- incremental evolution
- production-grade design principles
- systems programming fundamentals

---

# Current Status

⚠️ This project is currently in active development and is not production-ready.

Implemented features represent the foundational building blocks of a distributed log broker.

---

# Implemented Features

## Networking
- TCP-based broker server
- concurrent client handling using goroutines
- simple text-based protocol

---

## Persistent Storage
- append-only log files
- durable message persistence
- automatic log recovery on restart

---

## Topic Partitioning
- multi-partition topics
- key-based partition routing
- deterministic hashing
- partition-local ordering guarantees

---

## Producers
- message publishing
- automatic partition assignment
- per-partition offsets

---

## Consumers
- offset-based message consumption
- partition-specific reads
- replay capability

---

## Consumer Groups
- group membership
- round-robin partition assignment
- partition ownership model
- basic rebalancing

---

## Offset Management
- durable offset commits
- persistent consumer progress tracking
- offset recovery after restart

---

## Replication (Simplified)
- leader log replication
- replica log persistence
- synchronous local replication model

---

# Architecture Overview

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

# Storage Model

This project uses an append-only log architecture.

Each partition is backed by a dedicated log file:

```text
data/orders-0.log
data/orders-1.log
data/orders-2.log
```

Replica logs are stored separately:

```text
data/orders-0-replica-1.log
data/orders-0-replica-2.log
```

Offsets are persisted independently:

```text
data/offsets.json
```

---

# Partitioning Model

Messages are routed to partitions using deterministic hashing.

Messages sharing the same key are always routed to the same partition.

Example:

```text
user1 -> partition 2
user1 -> partition 2
user1 -> partition 2
```

This guarantees ordering per key.

---

# Consumer Group Model

Partitions are distributed across consumers within the same group.

Example:

```text
consumer-a -> partitions [0,2]
consumer-b -> partitions [1]
```

This enables:
- horizontal scaling
- parallel processing
- work distribution without duplication

---

# Replication Model

Each partition maintains:
- one leader log
- multiple replica logs

Current implementation uses:
- synchronous local replication
- file-based replicas

Planned future implementation:
- network-based replication
- replica synchronization
- leader election
- ISR tracking

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

# Project Structure

```text
real-time-event-streaming/
│
├── go.mod
├── main.go
│
├── broker/
│   └── broker.go
│
├── group/
│   └── group.go
│
├── offset/
│   └── offset.go
│
├── network/
│   └── handler.go
│
└── data/
```

---

# Running Locally

## Requirements

- Go 1.22+

---

## Start Broker

```bash
go run .
```

Expected output:

```text
Mini Kafka Broker listening on port 9092
```

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

# Current Limitations

The current implementation is intentionally simplified.

Not yet implemented:

- multi-node clustering
- distributed replication
- leader election
- replication acknowledgements
- ISR (in-sync replicas)
- batching
- compression
- retention policies
- segment files
- indexed reads
- binary protocol
- authentication
- authorization
- metrics
- observability
- backpressure
- exactly-once semantics
- Raft-based metadata coordination

---

# Roadmap

## Storage Engine
- segment files
- sparse indexes
- retention policies
- compaction

---

## Replication
- network replication
- ISR tracking
- acknowledgements
- leader election

---

## Broker Coordination
- metadata quorum
- distributed consensus
- controller nodes

---

## Performance
- batching
- compression
- zero-copy reads
- page cache optimization

---

## Protocol
- binary wire protocol
- schema support
- producer acknowledgements

---

## Operations
- metrics
- observability
- health checks
- tracing

---

# Why This Project Exists

Modern distributed streaming systems are often difficult to understand internally because of their scale and complexity.

This project exists to:
- make distributed log internals understandable
- explore production-grade architecture incrementally
- provide a readable Go implementation of streaming system fundamentals

---

# Contributing

Contributions, discussions, and architectural feedback are welcome.

Areas of interest include:
- distributed systems
- storage engines
- replication protocols
- consensus algorithms
- streaming architectures
- Go systems programming

---

# Inspiration

Inspired by:
- Apache Kafka
- Redpanda
- Apache Pulsar
- NATS
- distributed log architectures

---

# License

MIT