import { FLUXClient } from './client';
import { FLUXProtocolError } from './errors';
import type { AckMode, ConsumedMessage, FLUXClientOptions } from './types';

export interface FLUXRuntimeOptions extends FLUXClientOptions {
  brokers?: Array<{ host: string; port: number }>;
}

export interface ProducerSendParams {
  topic: string;
  messages: Array<{ key: string; value: string }>;
  acks?: AckMode;
}

export interface FLUXProducerOptions {
  maxRetries?: number;
  retryBackoffMs?: number;
  batchSize?: number;
  lingerMs?: number;
}

export interface FLUXProducer {
  send(params: ProducerSendParams): Promise<void>;
}

export interface ConsumerRunContext {
  topic: string;
  partition: number;
  message: ConsumedMessage;
}

export interface ConsumerRunConfig {
  eachMessage: (ctx: ConsumerRunContext) => Promise<void>;
}

export interface FLUXConsumerOptions {
  groupId: string;
  consumerId: string;
  assignor?: 'round_robin' | 'range';
  heartbeatIntervalMs?: number;
  pollIntervalMs?: number;
  autoCommit?: boolean;
  autoRejoin?: boolean;
  maxRetries?: number;
  retryBackoffMs?: number;
  onAssign?: (partitions: number[]) => Promise<void> | void;
  onRevoke?: (partitions: number[]) => Promise<void> | void;
  onCrash?: (error: unknown) => Promise<void> | void;
}

export interface FLUXConsumer {
  subscribe(config: { topic: string }): Promise<void>;
  run(config: ConsumerRunConfig): Promise<void>;
  disconnect(): Promise<void>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isGenerationMismatch(err: unknown): boolean {
  return typeof err === 'object' && err !== null && 'code' in err && err.code === 'GENERATION_MISMATCH';
}

async function withRetry<T>(
  operationName: string,
  fn: () => Promise<T>,
  maxRetries: number,
  retryBackoffMs: number,
  onRetry?: (err: unknown, attempt: number) => Promise<void> | void,
): Promise<T> {
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      if (attempt > maxRetries) {
        throw err;
      }
      const backoff = retryBackoffMs * attempt;
      const message = err instanceof Error ? err.message : String(err);
      // eslint-disable-next-line no-console
      console.warn(`${operationName} failed (attempt ${attempt}/${maxRetries + 1}): ${message}`);
      if (onRetry) {
        await onRetry(err, attempt);
      }
      await sleep(backoff);
    }
  }
}

function isRetryableBrokerError(err: unknown): boolean {
  if (err instanceof FLUXProtocolError && err.code === 'NOT_LEADER') {
    return true;
  }
  const message = err instanceof Error ? err.message : String(err);
  return (
    message.includes('connection closed') ||
    message.includes('ECONNREFUSED') ||
    message.includes('timed out') ||
    message.includes('socket hang up')
  );
}

export class FLUXRuntime {
  private readonly options: FLUXRuntimeOptions;

  constructor(options: FLUXRuntimeOptions = {}) {
    this.options = options;
  }

  producer(options: FLUXProducerOptions = {}): FLUXProducer {
    const endpoints =
      this.options.brokers && this.options.brokers.length > 0
        ? this.options.brokers
        : [{ host: this.options.host ?? '127.0.0.1', port: this.options.port ?? 9092 }];
    let endpointIndex = 0;
    let client = new FLUXClient({
      host: endpoints[endpointIndex].host,
      port: endpoints[endpointIndex].port,
      timeoutMs: this.options.timeoutMs,
    });
    const rotateClient = async (): Promise<void> => {
      endpointIndex = (endpointIndex + 1) % endpoints.length;
      try {
        await client.close();
      } catch {
        // best effort
      }
      client = new FLUXClient({
        host: endpoints[endpointIndex].host,
        port: endpoints[endpointIndex].port,
        timeoutMs: this.options.timeoutMs,
      });
    };
    const maxRetries = options.maxRetries ?? 3;
    const retryBackoffMs = options.retryBackoffMs ?? 250;
    const batchSize = options.batchSize ?? 100;
    const lingerMs = options.lingerMs ?? 0;

    return {
      send: async ({ topic, messages, acks = '1' }: ProducerSendParams): Promise<void> => {
        if (messages.length === 0) {
          return;
        }

        await withRetry('producer.connect', () => client.connect(), maxRetries, retryBackoffMs, async (err) => {
          if (isRetryableBrokerError(err)) {
            await rotateClient();
          }
        });
        try {
          for (let i = 0; i < messages.length; i += batchSize) {
            const batch = messages.slice(i, i + batchSize);
            for (const msg of batch) {
              await withRetry(
                'producer.produce',
                () => client.produce(topic, msg.key, msg.value, acks),
                maxRetries,
                retryBackoffMs,
                async (err) => {
                  if (isRetryableBrokerError(err)) {
                    await rotateClient();
                    await client.connect();
                  }
                },
              );
            }
            if (lingerMs > 0 && i + batchSize < messages.length) {
              await sleep(lingerMs);
            }
          }
        } finally {
          await client.close();
        }
      },
    };
  }

  consumer(options: FLUXConsumerOptions): FLUXConsumer {
    const endpoints =
      this.options.brokers && this.options.brokers.length > 0
        ? this.options.brokers
        : [{ host: this.options.host ?? '127.0.0.1', port: this.options.port ?? 9092 }];
    let endpointIndex = 0;
    let client = new FLUXClient({
      host: endpoints[endpointIndex].host,
      port: endpoints[endpointIndex].port,
      timeoutMs: this.options.timeoutMs,
    });
    const rotateClient = async (): Promise<void> => {
      endpointIndex = (endpointIndex + 1) % endpoints.length;
      try {
        await client.close();
      } catch {
        // best effort
      }
      client = new FLUXClient({
        host: endpoints[endpointIndex].host,
        port: endpoints[endpointIndex].port,
        timeoutMs: this.options.timeoutMs,
      });
    };
    const assignor = options.assignor ?? 'round_robin';
    const heartbeatIntervalMs = options.heartbeatIntervalMs ?? 2000;
    const pollIntervalMs = options.pollIntervalMs ?? 500;
    const autoCommit = options.autoCommit ?? true;
    const autoRejoin = options.autoRejoin ?? true;
    const maxRetries = options.maxRetries ?? 3;
    const retryBackoffMs = options.retryBackoffMs ?? 250;

    let topic = '';
    let generation = 0;
    let assigned: number[] = [];
    let running = false;
    let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    let rejoinNeeded = false;

    const runAssignCallback = async (nextAssigned: number[]): Promise<void> => {
      const prev = [...assigned].sort((a, b) => a - b);
      const next = [...nextAssigned].sort((a, b) => a - b);
      const changed = prev.length !== next.length || prev.some((v, idx) => v !== next[idx]);
      if (!changed) {
        assigned = nextAssigned;
        return;
      }

      if (assigned.length > 0 && options.onRevoke) {
        await options.onRevoke([...assigned]);
      }
      assigned = nextAssigned;
      if (options.onAssign) {
        await options.onAssign([...assigned]);
      }
    };

    const joinAndSync = async (): Promise<void> => {
      const joined = await withRetry(
        'consumer.join',
        () => client.join(options.groupId, topic, options.consumerId, assignor),
        maxRetries,
        retryBackoffMs,
        async (err) => {
          if (isRetryableBrokerError(err)) {
            await rotateClient();
            await client.connect();
          }
        },
      );
      generation = joined.generation;

      const synced = await withRetry(
        'consumer.sync',
        () => client.sync(options.groupId, topic, options.consumerId, generation),
        maxRetries,
        retryBackoffMs,
        async (err) => {
          if (isRetryableBrokerError(err)) {
            await rotateClient();
            await client.connect();
          }
        },
      );
      generation = synced.generation;
      await runAssignCallback(synced.assigned);
      rejoinNeeded = false;
    };

    const startHeartbeat = (): void => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
      heartbeatTimer = setInterval(() => {
        void client
          .heartbeat(options.groupId, topic, options.consumerId, generation)
          .catch((err) => {
            if (isGenerationMismatch(err)) {
              rejoinNeeded = true;
              return;
            }
            if (isRetryableBrokerError(err)) {
              void rotateClient();
            }
            if (!running) {
              return;
            }
            // transient failures will be handled by rejoin/retry in run loop
            rejoinNeeded = true;
          });
      }, heartbeatIntervalMs);
    };

    return {
      subscribe: async ({ topic: nextTopic }): Promise<void> => {
        topic = nextTopic;
      },

      run: async ({ eachMessage }: ConsumerRunConfig): Promise<void> => {
        if (!topic) {
          throw new Error('consumer.subscribe({ topic }) must be called before run()');
        }
        if (running) {
          throw new Error('consumer is already running');
        }

        running = true;
        await withRetry('consumer.connect', () => client.connect(), maxRetries, retryBackoffMs, async (err) => {
          if (isRetryableBrokerError(err)) {
            await rotateClient();
          }
        });
        await joinAndSync();
        startHeartbeat();

        try {
          while (running) {
            if (rejoinNeeded) {
              if (!autoRejoin) {
                throw new Error('consumer requires rejoin but autoRejoin=false');
              }
              await joinAndSync();
              startHeartbeat();
            }

            for (const partition of assigned) {
              if (!running) {
                break;
              }

              try {
                const startOffset = await withRetry(
                  'consumer.offset',
                  () => client.offset(options.groupId, topic, partition),
                  maxRetries,
                  retryBackoffMs,
                  async (err) => {
                    if (isRetryableBrokerError(err)) {
                      await rotateClient();
                      await client.connect();
                    }
                  },
                );
                const messages = await withRetry(
                  'consumer.consume',
                  () => client.consume(topic, partition, startOffset),
                  maxRetries,
                  retryBackoffMs,
                  async (err) => {
                    if (isRetryableBrokerError(err)) {
                      await rotateClient();
                      await client.connect();
                    }
                  },
                );

                for (const message of messages) {
                  await eachMessage({ topic, partition, message });
                }

                if (autoCommit && messages.length > 0) {
                  const nextOffset = messages[messages.length - 1].offset + 1;
                  await withRetry(
                    'consumer.commit',
                    () =>
                      client.commit(
                        options.groupId,
                        topic,
                        options.consumerId,
                        generation,
                        partition,
                        nextOffset,
                    ),
                    maxRetries,
                    retryBackoffMs,
                    async (err) => {
                      if (isRetryableBrokerError(err)) {
                        await rotateClient();
                        await client.connect();
                      }
                    },
                  );
                }
              } catch (err) {
                if (isGenerationMismatch(err) && autoRejoin) {
                  rejoinNeeded = true;
                  break;
                }
                throw err;
              }
            }

            await sleep(pollIntervalMs);
          }
        } catch (err) {
          if (options.onCrash) {
            await options.onCrash(err);
          }
          throw err;
        } finally {
          if (heartbeatTimer) {
            clearInterval(heartbeatTimer);
            heartbeatTimer = null;
          }
          if (assigned.length > 0 && options.onRevoke) {
            await options.onRevoke([...assigned]);
          }
        }
      },

      disconnect: async (): Promise<void> => {
        running = false;
        if (heartbeatTimer) {
          clearInterval(heartbeatTimer);
          heartbeatTimer = null;
        }
        if (generation > 0 && topic) {
          try {
            await client.leave(options.groupId, topic, options.consumerId, generation);
          } catch {
            // best effort
          }
        }
        await client.close();
      },
    };
  }
}
