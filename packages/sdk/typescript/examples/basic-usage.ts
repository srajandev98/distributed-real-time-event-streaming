import { RTESRuntime } from '../dist';

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

async function main() {
  const runtime = new RTESRuntime({
    host: config.host,
    port: config.port,
    timeoutMs: config.timeoutMs,
  });
  const producer = runtime.producer({
    maxRetries: 5,
    retryBackoffMs: 200,
    batchSize: 200,
    lingerMs: 5,
  });
  const consumer = runtime.consumer({
    groupId: config.group,
    consumerId: config.consumerId,
    assignor: 'round_robin',
    heartbeatIntervalMs: config.heartbeatIntervalMs,
    pollIntervalMs: 500,
    autoCommit: true,
    autoRejoin: true,
    maxRetries: 5,
    retryBackoffMs: 200,
    onAssign: (partitions) => {
      console.log('Assigned partitions:', partitions);
    },
    onRevoke: (partitions) => {
      console.log('Revoked partitions:', partitions);
    },
    onCrash: (error) => {
      console.error('Consumer crashed:', error);
    },
  });

  let stopping = false;

  const safeShutdown = async (signalOrReason: string): Promise<void> => {
    if (stopping) {
      return;
    }
    stopping = true;
    console.log(`\nShutting down (${signalOrReason})...`);
    try {
      await consumer.disconnect();
      console.log('Consumer disconnected');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('Shutdown error:', message);
    }
  };

  process.once('SIGINT', () => {
    void safeShutdown('SIGINT').finally(() => process.exit(0));
  });
  process.once('SIGTERM', () => {
    void safeShutdown('SIGTERM').finally(() => process.exit(0));
  });

  await producer.send({
    topic: config.topic,
    messages: [{ key: 'user1', value: `created-at-${Date.now()}` }],
    acks: '1',
  });
  console.log('Produced one test message');

  await consumer.subscribe({ topic: config.topic });
  const runPromise = consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`[${topic}] partition=${partition} offset=${message.offset} value=${message.value}`);
    },
  });

  // Keep the example finite: run consumer briefly then shutdown.
  await sleep(3000);
  await safeShutdown('completed');
  await runPromise;
}

main().catch((err) => {
  if (typeof err === 'object' && err !== null && 'code' in err && err.code === 'GENERATION_MISMATCH') {
    console.error('Generation mismatch: consumer must rejoin group and retry workflow.');
  }
  console.error('Example failed:', err);
  process.exit(1);
});
