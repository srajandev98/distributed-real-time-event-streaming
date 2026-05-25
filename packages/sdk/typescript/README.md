# FLUX TypeScript SDK

TypeScript client SDK for Real-time Event Streaming (FLUX).

## Install

From this repo (local path):

```bash
pnpm add ../../packages/sdk/typescript
```

Or publish this package and install by package name.

## Build

```bash
pnpm install
pnpm run build
```

## Usage

```ts
import { FLUXClient } from '@flux/typescript-sdk';

async function run() {
  const client = new FLUXClient({ host: '127.0.0.1', port: 9092 });

  await client.connect();

  const produced = await client.produce('orders', 'user1', 'created', 'all');
  console.log('produced', produced);

  const messages = await client.consume('orders', produced.partition, 0);
  console.log('messages', messages);

  const join = await client.join('analytics', 'orders', 'consumer-a', 'round_robin');
  console.log('generation', join.generation);
  console.log('assigned', join.assigned);

  await client.heartbeat('analytics', 'orders', 'consumer-a', join.generation);
  const sync = await client.sync('analytics', 'orders', 'consumer-a', join.generation);
  console.log('synced', sync);

  await client.commit('analytics', 'orders', 'consumer-a', join.generation, produced.partition, produced.offset + 1);
  const committedOffset = await client.offset('analytics', 'orders', produced.partition);
  console.log('offset', committedOffset);

  const replica = await client.replicaFetch('orders', produced.partition, 1, produced.offset);
  console.log('replica', replica);

  const role = await client.setPartitionRole('orders', produced.partition, 'leader');
  console.log('role', role);

  await client.leave('analytics', 'orders', 'consumer-a', join.generation);
  await client.close();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

## Runtime failover (multi-broker)

```ts
import { FLUXRuntime } from '@flux/typescript-sdk';

const runtime = new FLUXRuntime({
  brokers: [
    { host: '127.0.0.1', port: 9092 },
    { host: '127.0.0.1', port: 9093 },
    { host: '127.0.0.1', port: 9094 },
  ],
});
```

When enabled, producer/consumer runtime operations automatically rotate to the next broker on
`NOT_LEADER` and common transient connection failures.


## Example

A runnable example is included at:

```text
examples/basic-usage.ts
```

Run it:

```bash
pnpm install
pnpm run build
npx tsc --module commonjs --target es2020 --outDir examples/dist examples/basic-usage.ts
node examples/dist/basic-usage.js
```

## Admin APIs (Operator Flow)

Admin APIs are intended for platform/operator workflows, not typical app producer/consumer code paths.

```ts
import { FLUXClient } from '@flux/typescript-sdk';

const client = new FLUXClient({ host: '127.0.0.1', port: 9092 });
await client.connect();

await client.adminCreateTopic('payments', 3, 2);
await client.adminRegisterBroker(1, '127.0.0.1', 9093);
await client.adminBrokerHeartbeat(1);
await client.adminSetPartitionLeader('payments', 0, 1, [1, 0]);
const metadata = await client.adminGetMetadata();
console.log(metadata);

await client.close();
```

## API

- `connect(): Promise<void>`
- `close(): Promise<void>`
- `sendCommand(command: string, args?: string): Promise<FLUXResponse>`
- `produce(topic: string, key: string, value: string, acks?: '0' | '1' | 'all'): Promise<ProduceResult>`
- `consume(topic: string, partition: number, offset: number): Promise<ConsumedMessage[]>`
- `join(group: string, topic: string, consumerId: string, assignor?: 'round_robin' | 'range'): Promise<JoinResult>`
- `sync(group: string, topic: string, consumerId: string, generation: number): Promise<SyncResult>`
- `heartbeat(group: string, topic: string, consumerId: string, generation: number): Promise<boolean>`
- `leave(group: string, topic: string, consumerId: string, generation: number): Promise<boolean>`
- `commit(group: string, topic: string, consumerId: string, generation: number, partition: number, offset: number): Promise<boolean>`
- `offset(group: string, topic: string, partition: number): Promise<number>`
- `replicaFetch(topic: string, partition: number, replicaId: number, offset: number): Promise<ReplicaFetchResult>`
- `setPartitionRole(topic: string, partition: number, role: 'leader' | 'follower'): Promise<PartitionRoleResult>`
- `adminCreateTopic(topic: string, partitions: number, replicationFactor: number): Promise<AdminCreateTopicResult>`
- `adminRegisterBroker(brokerId: number, host: string, port: number, epoch?: number): Promise<AdminRegisterBrokerResult>`
- `adminBrokerHeartbeat(brokerId: number): Promise<AdminBrokerHeartbeatResult>`
- `adminSetPartitionLeader(topic: string, partition: number, leaderId: number, isr: number[]): Promise<AdminSetPartitionLeaderResult>`
- `adminGetMetadata(): Promise<AdminMetadataResult>`

## Errors

Protocol errors are thrown as `FLUXProtocolError` with:

- `code` (for example `BAD_REQUEST`, `UNKNOWN_COMMAND`)
- `message`
- `response` (full parsed response)
