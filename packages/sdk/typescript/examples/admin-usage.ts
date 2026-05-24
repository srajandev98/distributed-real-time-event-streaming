import { FLUXClient } from '../dist';

const config = {
  host: process.env.FLUX_HOST || '127.0.0.1',
  port: Number(process.env.FLUX_PORT || '9092'),
  timeoutMs: Number(process.env.FLUX_TIMEOUT_MS || '5000'),
  topic: process.env.FLUX_TOPIC || 'orders',
  adminBrokerId: Number(process.env.FLUX_ADMIN_BROKER_ID || '1'),
};

async function main() {
  const client = new FLUXClient({
    host: config.host,
    port: config.port,
    timeoutMs: config.timeoutMs,
  });
  await client.connect();
  try {
    console.log('[admin] bootstrapping control-plane metadata');
    const created = await client.adminCreateTopic(config.topic, 3, 2);
    console.log('[admin] created topic:', created);

    const registered = await client.adminRegisterBroker(config.adminBrokerId, config.host, config.port + 1);
    console.log('[admin] registered broker:', registered);

    const heartbeat = await client.adminBrokerHeartbeat(config.adminBrokerId);
    console.log('[admin] broker heartbeat:', heartbeat);

    const leader = await client.adminSetPartitionLeader(config.topic, 0, 0, [0, config.adminBrokerId]);
    console.log('[admin] set partition leader:', leader);

    const metadata = await client.adminGetMetadata();
    console.log('[admin] metadata snapshot:', metadata);
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error('Admin example failed:', err);
  process.exit(1);
});
