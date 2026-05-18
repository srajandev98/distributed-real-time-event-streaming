# RTES TypeScript SDK

TypeScript client SDK for Real-time Event Streaming (RTES).

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
import { RTESClient } from '@rtes/typescript-sdk';

async function run() {
  const client = new RTESClient({ host: '127.0.0.1', port: 9092 });

  await client.connect();

  const produced = await client.produce('orders', 'user1', 'created', 'all');
  console.log('produced', produced);

  const messages = await client.consume('orders', produced.partition, 0);
  console.log('messages', messages);

  const join = await client.join('analytics', 'orders', 'consumer-a');
  console.log('assigned', join.assigned);

  await client.commit('analytics', 'orders', produced.partition, produced.offset + 1);
  const committedOffset = await client.offset('analytics', 'orders', produced.partition);
  console.log('offset', committedOffset);

  const replica = await client.replicaFetch('orders', produced.partition, 1, produced.offset);
  console.log('replica', replica);

  await client.close();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
```


## Example

A runnable example is included at:

```text
examples/basic-usage.js
```

Run it:

```bash
pnpm install
pnpm run build
node examples/basic-usage.js
```

## API

- `connect(): Promise<void>`
- `close(): Promise<void>`
- `sendCommand(command: string, args?: string): Promise<RTESResponse>`
- `produce(topic: string, key: string, value: string, acks?: '0' | '1' | 'all'): Promise<ProduceResult>`
- `consume(topic: string, partition: number, offset: number): Promise<ConsumedMessage[]>`
- `join(group: string, topic: string, consumerId: string): Promise<JoinResult>`
- `commit(group: string, topic: string, partition: number, offset: number): Promise<boolean>`
- `offset(group: string, topic: string, partition: number): Promise<number>`
- `replicaFetch(topic: string, partition: number, replicaId: number, offset: number): Promise<ReplicaFetchResult>`

## Errors

Protocol errors are thrown as `RTESProtocolError` with:

- `code` (for example `BAD_REQUEST`, `UNKNOWN_COMMAND`)
- `message`
- `response` (full parsed response)
