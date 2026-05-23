import { RTESClient } from '../dist';

const config = {
  host: process.env.RTES_HOST || '127.0.0.1',
  port: Number(process.env.RTES_PORT || '9092'),
  timeoutMs: Number(process.env.RTES_TIMEOUT_MS || '5000'),
  topic: process.env.RTES_TOPIC || 'orders',
  group: process.env.RTES_GROUP || 'analytics',
  consumerId: process.env.RTES_CONSUMER_ID || `consumer-${process.pid}`,
  heartbeatIntervalMs: Number(process.env.RTES_HEARTBEAT_INTERVAL_MS || '2000'),
};

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function withRetry<T>(operationName: string, fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let attempt = 0;
  while (attempt <= maxRetries) {
    try {
      return await fn();
    } catch (err) {
      attempt += 1;
      if (attempt > maxRetries) {
        throw err;
      }
      const backoffMs = 250 * attempt;
      const message = err instanceof Error ? err.message : String(err);
      console.warn(`${operationName} failed (attempt ${attempt}/${maxRetries + 1}): ${message}`);
      await sleep(backoffMs);
    }
  }
  throw new Error(`unreachable retry state for operation: ${operationName}`);
}

async function main() {
  const client = new RTESClient({
    host: config.host,
    port: config.port,
    timeoutMs: config.timeoutMs,
  });
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let stopping = false;
  let generation = 0;

  const stopHeartbeat = () => {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  };

  const safeShutdown = async (signalOrReason: string): Promise<void> => {
    if (stopping) {
      return;
    }
    stopping = true;
    stopHeartbeat();
    console.log(`\nShutting down (${signalOrReason})...`);
    try {
      if (generation > 0) {
        try {
          await client.leave(config.group, config.topic, config.consumerId, generation);
          console.log('Left consumer group');
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          console.warn(`Leave failed during shutdown: ${message}`);
        }
      }
      await client.close();
      console.log('Connection closed');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('Close error:', message);
    }
  };

  process.once('SIGINT', () => {
    void safeShutdown('SIGINT').finally(() => process.exit(0));
  });
  process.once('SIGTERM', () => {
    void safeShutdown('SIGTERM').finally(() => process.exit(0));
  });

  await withRetry('connect', () => client.connect(), 5);
  console.log(`Connected to RTES broker at ${config.host}:${config.port}`);

  const produced = await withRetry('produce', () =>
    client.produce(config.topic, 'user1', `created-at-${Date.now()}`, '1'),
  );
  console.log('Produced:', produced);

  const join = await withRetry('join', () =>
    client.join(config.group, config.topic, config.consumerId, 'round_robin'),
  );
  generation = join.generation;
  console.log('Join generation:', generation);
  console.log('Assigned partitions:', join.assigned);

  const synced = await withRetry('sync', () =>
    client.sync(config.group, config.topic, config.consumerId, generation),
  );
  generation = synced.generation;
  console.log('Sync generation:', generation);
  console.log('Sync assignment:', synced.assigned);

  heartbeatTimer = setInterval(() => {
    void client
      .heartbeat(config.group, config.topic, config.consumerId, generation)
      .catch((err) => {
        // In production this should trigger rejoin logic.
        const message = err instanceof Error ? err.message : String(err);
        console.error('Heartbeat failed:', message);
      });
  }, config.heartbeatIntervalMs);

  for (const partition of synced.assigned) {
    const startOffset = await withRetry('offset', () =>
      client.offset(config.group, config.topic, partition),
    );
    const messages = await withRetry('consume', () =>
      client.consume(config.topic, partition, startOffset),
    );

    console.log(`Partition ${partition} startOffset=${startOffset} messages=`, messages);

    if (messages.length === 0) {
      continue;
    }

    const nextOffset = messages[messages.length - 1].offset + 1;
    const committed = await withRetry('commit', () =>
      client.commit(
        config.group,
        config.topic,
        config.consumerId,
        generation,
        partition,
        nextOffset,
      ),
    );
    console.log(`Partition ${partition} commit status:`, committed);

    const committedOffset = await withRetry('offset', () =>
      client.offset(config.group, config.topic, partition),
    );
    console.log(`Partition ${partition} committed offset:`, committedOffset);
  }

  stopHeartbeat();
  await safeShutdown('completed');
}

main().catch((err) => {
  if (typeof err === 'object' && err !== null && 'code' in err && err.code === 'GENERATION_MISMATCH') {
    console.error('Generation mismatch: consumer must rejoin group and retry workflow.');
  }
  console.error('Example failed:', err);
  process.exit(1);
});
