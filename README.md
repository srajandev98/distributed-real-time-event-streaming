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

Produce with explicit ack mode:

```text
V1|1|PRODUCE|orders user1:created acks=all
```

Consume:

```text
V1|2|CONSUME|orders 2 0
```

Join group:

```text
V1|3|JOIN|analytics orders consumer-a
```

Sync group assignment:

```text
V1|4|SYNC|analytics orders consumer-a 2
```

Heartbeat:

```text
V1|5|HEARTBEAT|analytics orders consumer-a 2
```

Commit offset:

```text
V1|6|COMMIT|analytics orders consumer-a 2 1 42
```

Read committed offset:

```text
V1|7|OFFSET|analytics orders 1
```

Follower replication progress (for replica simulation/testing):

```text
V1|8|REPLICA_FETCH|orders 1 1 42
```

Set local partition role:

```text
V1|9|SET_PARTITION_ROLE|orders 1 leader
```

Admin create topic:

```text
V1|10|ADMIN_CREATE_TOPIC|payments 3 2
```

Admin metadata snapshot:

```text
V1|11|ADMIN_GET_METADATA|
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

  // Admin setup (optional)
  await client.adminCreateTopic('orders', 3, 2);
  await client.adminRegisterBroker(1, '127.0.0.1', 9093);

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

# Admin setup (optional)
client.admin_create_topic("orders", 3, 2)
client.admin_register_broker(1, "127.0.0.1", 9093)

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
