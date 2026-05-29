import assert from 'assert';
import net from 'net';

import { FLUXRuntime } from '../dist/index.js';

function startServer(handler) {
  const server = net.createServer((socket) => {
    socket.setEncoding('utf8');
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk;
      for (;;) {
        const idx = buffer.indexOf('\n');
        if (idx === -1) break;
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line) continue;
        const parts = line.split('|');
        const correlationId = parts[1] ?? '0';
        const command = parts[2] ?? '';
        handler({ socket, correlationId, command });
      }
    });
  });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        reject(new Error('unable to resolve test server address'));
        return;
      }
      resolve({ server, port: address.port });
    });
  });
}

async function run() {
  let primary;
  let secondary;
  try {
    primary = await startServer(({ socket, correlationId, command }) => {
      if (command === 'PRODUCE') {
        socket.write(`V1|${correlationId}|ERR|NOT_LEADER|leader moved\n`);
        return;
      }
      socket.write(`V1|${correlationId}|OK|\n`);
    });
    let producedOnSecondary = false;
    secondary = await startServer(({ socket, correlationId, command }) => {
      if (command === 'PRODUCE') {
        producedOnSecondary = true;
        socket.write(`V1|${correlationId}|OK|partition=0 offset=0 hw=0\n`);
        return;
      }
      socket.write(`V1|${correlationId}|OK|\n`);
    });

    const runtime = new FLUXRuntime({
      brokers: [
        { host: '127.0.0.1', port: primary.port },
        { host: '127.0.0.1', port: secondary.port },
      ],
    });
    const producer = runtime.producer({ maxRetries: 2, retryBackoffMs: 10 });
    await producer.send({
      topic: 'orders',
      messages: [{ key: 'user-1', value: 'created' }],
      acks: '1',
    });
    assert.equal(producedOnSecondary, true);
    console.log('PASS runtime failover test');

    // Connection-refused path: first broker is unreachable, second is healthy.
    let refusedTarget;
    const healthy = await startServer(({ socket, correlationId, command }) => {
      if (command === 'PRODUCE') {
        socket.write(`V1|${correlationId}|OK|partition=0 offset=1 hw=1\n`);
        return;
      }
      socket.write(`V1|${correlationId}|OK|\n`);
    });
    try {
      refusedTarget = net.createServer();
      await new Promise((resolve) => refusedTarget.listen(0, '127.0.0.1', resolve));
      const refusedPort = refusedTarget.address().port;
      await new Promise((resolve) => refusedTarget.close(resolve));

      const runtime2 = new FLUXRuntime({
        brokers: [
          { host: '127.0.0.1', port: refusedPort },
          { host: '127.0.0.1', port: healthy.port },
        ],
      });
      const producer2 = runtime2.producer({ maxRetries: 2, retryBackoffMs: 10 });
      await producer2.send({
        topic: 'orders',
        messages: [{ key: 'user-2', value: 'paid' }],
        acks: '1',
      });
      console.log('PASS runtime connection-refused failover test');
    } finally {
      await new Promise((resolve) => healthy.server.close(resolve));
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes('operation not permitted') || msg.includes('EACCES')) {
      console.log(`SKIP runtime failover test in restricted sandbox: ${msg}`);
      return;
    }
    throw err;
  } finally {
    if (primary) {
      await new Promise((resolve) => primary.server.close(resolve));
    }
    if (secondary) {
      await new Promise((resolve) => secondary.server.close(resolve));
    }
  }
}

run().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
