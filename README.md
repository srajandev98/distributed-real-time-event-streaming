# Flux - Distributed Real-time Event Streaming

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

- `FLUX_LISTEN_ADDR` (default `:9092`)
- `FLUX_DATA_DIR` (default `data`)
- `FLUX_NUM_PARTITIONS` (default `3`)
- `FLUX_REPLICATION_FACTOR` (default `3`)
- `FLUX_MIN_ISR` (default `2`)
- `FLUX_REPLICA_MAX_LAG` (default `0`)
- `FLUX_REPLICA_LAG_TIMEOUT_MS` (default `10000`)
- `FLUX_ACK_ALL_TIMEOUT_MS` (default `2000`)
- `FLUX_SEGMENT_MAX_BYTES`
- `FLUX_RETENTION_MAX_BYTES`
- `FLUX_RETENTION_MAX_AGE_SECONDS`
- `FLUX_FLUSH_INTERVAL_MS`
- `FLUX_FLUSH_BYTES`
- `FLUX_FSYNC_MODE` (`always`, `interval`, `never`)

Example:

```bash
FLUX_LISTEN_ADDR=:9092 \
FLUX_DATA_DIR=./data \
FLUX_NUM_PARTITIONS=3 \
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

## Basic Usage (TypeScript)

Install from local repo path:

```bash
cd packages/sdk/typescript
pnpm install
pnpm run build
```

Example:

```ts
import { FLUXClient, FLUXRuntime } from '@flux/typescript-sdk';

async function main() {
  const client = new FLUXClient({ host: '127.0.0.1', port: 9092 });
  await client.connect();

  const produced = await client.produce('orders', 'user1', 'created', '1');
  console.log('produced', produced);

  const messages = await client.consume('orders', produced.partition, 0);
  console.log('messages', messages);

  await client.close();

  // High-level runtime API
  const runtime = new FLUXRuntime({ host: '127.0.0.1', port: 9092 });
  const producer = runtime.producer();
  await producer.send({
    topic: 'orders',
    messages: [{ key: 'user2', value: 'paid' }],
    acks: '1',
  });
}

main().catch(console.error);
```

## Basic Usage (Python)

Install from local repo path:

```bash
pip install ./packages/sdk/python
```

Example:

```python
from flux_sdk import FLUXClient, FLUXRuntime, ProducerMessage

client = FLUXClient(host="127.0.0.1", port=9092)
client.connect()

produced = client.produce("orders", "user1", "created", acks="1")
print("produced", produced)

messages = client.consume("orders", produced.partition, 0)
print("messages", messages)

client.close()

# High-level runtime API
runtime = FLUXRuntime(host="127.0.0.1", port=9092)
producer = runtime.producer()
producer.send(
    topic="orders",
    messages=[ProducerMessage(key="user2", value="paid")],
    acks="1",
)
```

## Admin Usage (Operator/Platform)

Control-plane admin APIs are intended for operator/platform workflows, not regular app producer/consumer code.

TypeScript admin flow is available at:

```text
packages/sdk/typescript/examples/admin-usage.ts
```
