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
  - Broker TCP listen address.
  - Use `:9092` to listen on all interfaces on port 9092, or `127.0.0.1:9092` for local-only binding.

- `FLUX_DATA_DIR` (default `data`)
  - Root directory where broker files are stored.
  - Contains segment logs, indexes, replica mirror files, `offsets.json`, and `groups.json`.

- `FLUX_NUM_PARTITIONS` (default `3`)
  - Default partition count used by runtime/components for topic behavior.
  - Affects key hashing distribution and parallelism.

- `FLUX_REPLICATION_FACTOR` (default `3`)
  - Number of replicas per partition in current replication model.
  - Higher value increases durability intent but also replication coordination work.

- `FLUX_MIN_ISR` (default `2`)
  - Minimum in-sync replicas required for a partition to be considered safely replicated for strict write paths.
  - Mainly impacts `acks=all` success behavior.

- `FLUX_REPLICA_MAX_LAG` (default `0`)
  - Maximum follower offset lag (in messages) allowed to remain in ISR.
  - Lower values are stricter; replicas drop from ISR sooner.

- `FLUX_REPLICA_LAG_TIMEOUT_MS` (default `10000`)
  - Follower freshness timeout in milliseconds.
  - If a replica does not report progress within this window, it can be considered out of ISR.

- `FLUX_ACK_ALL_TIMEOUT_MS` (default `2000`)
  - Timeout (ms) for produce requests using `acks=all`.
  - If ISR conditions are not met before timeout, request fails with replication timeout.

- `FLUX_SEGMENT_MAX_BYTES` (default `1048576`)
  - Maximum size of an active segment file before rollover to a new segment.
  - Smaller values roll segments more often; larger values reduce segment churn.

- `FLUX_RETENTION_MAX_BYTES` (default `52428800`)
  - Total per-topic-partition data budget in bytes for retention cleanup.
  - Old segments are removed when usage crosses this threshold.

- `FLUX_RETENTION_MAX_AGE_SECONDS` (default `86400`)
  - Maximum age of retained data (seconds).
  - Segments older than this age are eligible for deletion.

- `FLUX_FLUSH_INTERVAL_MS` (default `1000`)
  - Flush interval used by interval-based fsync policy.
  - Smaller interval favors durability, larger interval favors throughput.

- `FLUX_FLUSH_BYTES` (default `65536`)
  - Byte threshold used by interval-based flush/fsync logic.
  - Triggers fsync behavior after enough buffered writes.

- `FLUX_FSYNC_MODE` (default `always`; allowed: `always`, `interval`, `never`)
  - `always`: fsync on every write (highest durability, lower throughput).
  - `interval`: fsync based on `FLUX_FLUSH_INTERVAL_MS` and `FLUX_FLUSH_BYTES`.
  - `never`: no explicit fsync (highest throughput, weakest crash durability).

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
