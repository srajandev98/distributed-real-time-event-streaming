# Distributed Real-time Event Streaming

Distributed Real-time Event Streaming is a TCP-based event streaming broker written in Go.

This README explains how to run and use the broker.

## What the Broker Supports

- produce messages to topics
- deterministic key-to-partition routing
- consume messages from a partition offset
- consumer group join and partition assignment
- offset commit and offset lookup
- local persistence for messages and offsets

## Run Broker (Local)

```bash
cd core
go run ./cmd/broker
```

Default broker address: `localhost:9092`

## Run Broker (Docker)

```bash
cd core
docker compose up --build -d
```

Stop:

```bash
docker compose down
```

## Connect a Client

```bash
nc localhost 9092
```

## Protocol Format

Every request is one line:

```text
V1|<correlation_id>|<command>|<args>
```

## Common Commands

Produce:

```text
V1|1|PRODUCE|orders user1:created
```

Consume:

```text
V1|2|CONSUME|orders 2 0
```

Join group:

```text
V1|3|JOIN|analytics orders consumer-a
```

Commit offset:

```text
V1|4|COMMIT|analytics orders 2 1
```

Read committed offset:

```text
V1|5|OFFSET|analytics orders 2
```

## Expected Responses

Success:

```text
V1|<correlation_id>|OK|<payload>
```

Error:

```text
V1|<correlation_id>|ERR|<code>|<message>
```

## Data Files

Broker data is stored under `core/data/` by default.

Examples:

```text
orders-2-segment-000000.log
orders-2-offset.idx
orders-2-time.idx
offsets.json
```

Local replica mirror files may also appear:

```text
orders-2-replica-1-segment-000000.log
orders-2-replica-2-segment-000000.log
```

## Configuration (Environment Variables)

- `RTES_LISTEN_ADDR` (default `:9092`)
- `RTES_DATA_DIR` (default `data`)
- `RTES_NUM_PARTITIONS` (default `3`)
- `RTES_SEGMENT_MAX_BYTES`
- `RTES_RETENTION_MAX_BYTES`
- `RTES_RETENTION_MAX_AGE_SECONDS`
- `RTES_FLUSH_INTERVAL_MS`
- `RTES_FLUSH_BYTES`
- `RTES_FSYNC_MODE` (`always`, `interval`, `never`)

Example:

```bash
RTES_LISTEN_ADDR=:9092 \
RTES_DATA_DIR=./data \
RTES_NUM_PARTITIONS=3 \
go run ./cmd/broker
```

## Run Tests

```bash
cd core
go test ./...
```

## SDKs

- TypeScript SDK: `packages/sdk/typescript`
- Python SDK: `packages/sdk/python`
