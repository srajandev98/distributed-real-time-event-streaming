const { RTESClient } = require('../dist');

async function main() {
  const client = new RTESClient({ host: '127.0.0.1', port: 9092, timeoutMs: 5000 });

  await client.connect();
  console.log('Connected to RTES broker');

  const produced = await client.produce('orders', 'user1', 'created');
  console.log('Produced:', produced);

  const messages = await client.consume('orders', produced.partition, 0);
  console.log('Consumed:', messages);

  const join = await client.join('analytics', 'orders', 'consumer-a', 'round_robin');
  console.log('Join generation:', join.generation);
  console.log('Join assignment:', join.assigned);

  const heartbeatOk = await client.heartbeat('analytics', 'orders', 'consumer-a', join.generation);
  console.log('Heartbeat:', heartbeatOk);

  const synced = await client.sync('analytics', 'orders', 'consumer-a', join.generation);
  console.log('Sync assignment:', synced.assigned);

  const committed = await client.commit(
    'analytics',
    'orders',
    'consumer-a',
    join.generation,
    produced.partition,
    produced.offset + 1,
  );
  console.log('Commit status:', committed);

  const committedOffset = await client.offset('analytics', 'orders', produced.partition);
  console.log('Committed offset:', committedOffset);

  await client.close();
  console.log('Connection closed');
}

main().catch((err) => {
  console.error('Example failed:', err);
  process.exit(1);
});
