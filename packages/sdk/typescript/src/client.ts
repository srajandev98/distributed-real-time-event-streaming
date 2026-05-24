import net from 'node:net';

import { FLUXProtocolError } from './errors';
import { buildCommandLine, parseResponse } from './protocol';
import {
  extractStringField,
  extractIntField,
  mustGetPayload,
  parseBoolField,
  parseConsumedMessages,
  parseIntListField,
  parseJoinAssignments,
  parseJoinGeneration,
} from './parsers';
import type {
  AckMode,
  ConsumedMessage,
  JoinResult,
  AdminBrokerHeartbeatResult,
  AdminCreateTopicResult,
  AdminMetadataResult,
  AdminRegisterBrokerResult,
  AdminSetPartitionLeaderResult,
  PartitionRoleResult,
  PendingRequest,
  ProduceResult,
  ReplicaFetchResult,
  FLUXClientOptions,
  FLUXResponse,
  SyncResult,
} from './types';

/**
 * FLUXClient is a simple Node.js client for the FLUX TCP text protocol.
 */
export class FLUXClient {
  private readonly host: string;
  private readonly port: number;
  private readonly timeoutMs: number;

  private socket: net.Socket | null = null;
  private connected = false;
  private recvBuffer = '';
  private correlationSeq = 1;
  private pending = new Map<string, PendingRequest>();

  constructor(options: FLUXClientOptions = {}) {
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 9092;
    this.timeoutMs = options.timeoutMs ?? 5000;
  }

  async connect(): Promise<void> {
    if (this.connected && this.socket) {
      return;
    }

    const socket = net.createConnection({ host: this.host, port: this.port });

    await new Promise<void>((resolve, reject) => {
      const onConnect = (): void => {
        cleanup();
        resolve();
      };

      const onError = (err: Error): void => {
        cleanup();
        reject(err);
      };

      const cleanup = (): void => {
        socket.off('connect', onConnect);
        socket.off('error', onError);
      };

      socket.once('connect', onConnect);
      socket.once('error', onError);
    });

    this.socket = socket;
    this.connected = true;
    socket.setEncoding('utf8');

    socket.on('data', (chunk: string) => this.handleData(chunk));
    socket.on('error', (err: Error) => this.rejectAllPending(err));
    socket.on('close', () => {
      this.connected = false;
      this.socket = null;
      this.rejectAllPending(new Error('FLUX connection closed'));
    });
  }

  async close(): Promise<void> {
    if (!this.socket) {
      return;
    }

    const socket = this.socket;
    this.socket = null;
    this.connected = false;

    await new Promise<void>((resolve) => {
      socket.once('close', () => resolve());
      socket.end();
    });

    this.rejectAllPending(new Error('FLUX client closed'));
  }

  async produce(topic: string, key: string, value: string, acks: AckMode = '1'): Promise<ProduceResult> {
    const resp = await this.sendCommand('PRODUCE', `${topic} ${key}:${value} acks=${acks}`);
    const payload = mustGetPayload(resp, 'produce response payload missing');

    let highWatermark: number | undefined;
    try {
      highWatermark = extractIntField(payload, 'hw');
    } catch {
      highWatermark = undefined;
    }

    return {
      partition: extractIntField(payload, 'partition'),
      offset: extractIntField(payload, 'offset'),
      highWatermark,
      raw: resp,
    };
  }

  async consume(topic: string, partition: number, offset: number): Promise<ConsumedMessage[]> {
    const resp = await this.sendCommand('CONSUME', `${topic} ${partition} ${offset}`);
    const payload = mustGetPayload(resp, 'consume response payload missing');
    return parseConsumedMessages(payload);
  }

  async join(
    group: string,
    topic: string,
    consumerId: string,
    assignor?: 'round_robin' | 'range',
  ): Promise<JoinResult> {
    const assignorArg = assignor ? ` assignor=${assignor}` : '';
    const resp = await this.sendCommand('JOIN', `${group} ${topic} ${consumerId}${assignorArg}`);
    const payload = mustGetPayload(resp, 'join response payload missing');
    return {
      generation: parseJoinGeneration(payload),
      assigned: parseJoinAssignments(payload),
      raw: resp,
    };
  }

  async sync(group: string, topic: string, consumerId: string, generation: number): Promise<SyncResult> {
    const resp = await this.sendCommand('SYNC', `${group} ${topic} ${consumerId} ${generation}`);
    const payload = mustGetPayload(resp, 'sync response payload missing');
    return {
      generation: parseJoinGeneration(payload),
      assigned: parseJoinAssignments(payload),
      raw: resp,
    };
  }

  async heartbeat(group: string, topic: string, consumerId: string, generation: number): Promise<boolean> {
    const resp = await this.sendCommand('HEARTBEAT', `${group} ${topic} ${consumerId} ${generation}`);
    return mustGetPayload(resp, 'heartbeat response payload missing').includes('heartbeat=ok');
  }

  async leave(group: string, topic: string, consumerId: string, generation: number): Promise<boolean> {
    const resp = await this.sendCommand('LEAVE', `${group} ${topic} ${consumerId} ${generation}`);
    return mustGetPayload(resp, 'leave response payload missing').includes('left=true');
  }

  async commit(
    group: string,
    topic: string,
    consumerId: string,
    generation: number,
    partition: number,
    offset: number,
  ): Promise<boolean> {
    const resp = await this.sendCommand(
      'COMMIT',
      `${group} ${topic} ${consumerId} ${generation} ${partition} ${offset}`,
    );
    return mustGetPayload(resp, 'commit response payload missing').includes('committed=true');
  }

  async offset(group: string, topic: string, partition: number): Promise<number> {
    const resp = await this.sendCommand('OFFSET', `${group} ${topic} ${partition}`);
    return extractIntField(mustGetPayload(resp, 'offset response payload missing'), 'offset');
  }

  async replicaFetch(
    topic: string,
    partition: number,
    replicaId: number,
    offset: number,
  ): Promise<ReplicaFetchResult> {
    const resp = await this.sendCommand('REPLICA_FETCH', `${topic} ${partition} ${replicaId} ${offset}`);
    const payload = mustGetPayload(resp, 'replica fetch response payload missing');
    return {
      replicaId: extractIntField(payload, 'replica'),
      ackedOffset: extractIntField(payload, 'acked_offset'),
      highWatermark: extractIntField(payload, 'hw'),
      isr: parseIntListField(payload, 'isr'),
      underReplicated: parseBoolField(payload, 'under_replicated'),
      raw: resp,
    };
  }

  async setPartitionRole(
    topic: string,
    partition: number,
    role: 'leader' | 'follower',
  ): Promise<PartitionRoleResult> {
    const resp = await this.sendCommand('SET_PARTITION_ROLE', `${topic} ${partition} ${role}`);
    const payload = mustGetPayload(resp, 'set partition role payload missing');
    const parsedRole = extractStringField(payload, 'role');
    if (parsedRole !== 'leader' && parsedRole !== 'follower') {
      throw new Error(`Invalid role in response payload: ${payload}`);
    }
    return {
      topic: extractStringField(payload, 'topic'),
      partition: extractIntField(payload, 'partition'),
      role: parsedRole,
      highWatermark: extractIntField(payload, 'hw'),
      raw: resp,
    };
  }

  async adminCreateTopic(
    topic: string,
    partitions: number,
    replicationFactor: number,
  ): Promise<AdminCreateTopicResult> {
    const resp = await this.sendCommand('ADMIN_CREATE_TOPIC', `${topic} ${partitions} ${replicationFactor}`);
    const payload = mustGetPayload(resp, 'admin create topic payload missing');
    return {
      topic: extractStringField(payload, 'topic'),
      partitions: extractIntField(payload, 'partitions'),
      replicationFactor: extractIntField(payload, 'replication_factor'),
      term: extractIntField(payload, 'term'),
      index: extractIntField(payload, 'index'),
      raw: resp,
    };
  }

  async adminRegisterBroker(
    brokerId: number,
    host: string,
    port: number,
    epoch?: number,
  ): Promise<AdminRegisterBrokerResult> {
    const epochArg = typeof epoch === 'number' ? ` ${epoch}` : '';
    const resp = await this.sendCommand('ADMIN_REGISTER_BROKER', `${brokerId} ${host} ${port}${epochArg}`);
    const payload = mustGetPayload(resp, 'admin register broker payload missing');
    return {
      brokerId: extractIntField(payload, 'broker_id'),
      host: extractStringField(payload, 'host'),
      port: extractIntField(payload, 'port'),
      term: extractIntField(payload, 'term'),
      index: extractIntField(payload, 'index'),
      raw: resp,
    };
  }

  async adminBrokerHeartbeat(brokerId: number): Promise<AdminBrokerHeartbeatResult> {
    const resp = await this.sendCommand('ADMIN_BROKER_HEARTBEAT', `${brokerId}`);
    const payload = mustGetPayload(resp, 'admin broker heartbeat payload missing');
    return {
      brokerId: extractIntField(payload, 'broker_id'),
      heartbeatOk: payload.includes('heartbeat=ok'),
      term: extractIntField(payload, 'term'),
      index: extractIntField(payload, 'index'),
      raw: resp,
    };
  }

  async adminSetPartitionLeader(
    topic: string,
    partition: number,
    leaderId: number,
    isr: number[],
  ): Promise<AdminSetPartitionLeaderResult> {
    const isrArg = isr.join(',');
    const resp = await this.sendCommand('ADMIN_SET_PARTITION_LEADER', `${topic} ${partition} ${leaderId} ${isrArg}`);
    const payload = mustGetPayload(resp, 'admin set partition leader payload missing');
    return {
      topic: extractStringField(payload, 'topic'),
      partition: extractIntField(payload, 'partition'),
      leader: extractIntField(payload, 'leader'),
      isr: parseIntListField(payload, 'isr'),
      term: extractIntField(payload, 'term'),
      index: extractIntField(payload, 'index'),
      raw: resp,
    };
  }

  async adminGetMetadata(): Promise<AdminMetadataResult> {
    const resp = await this.sendCommand('ADMIN_GET_METADATA');
    const payload = mustGetPayload(resp, 'admin get metadata payload missing');
    const topics = extractBracketedList(payload, 'topics').filter((v) => v.length > 0);
    const brokers = extractBracketedList(payload, 'brokers')
      .filter((v) => v.length > 0)
      .map((v) => Number(v));
    return { topics, brokers, raw: resp };
  }

  async sendCommand(command: string, args = ''): Promise<FLUXResponse> {
    if (!this.socket || !this.connected) {
      throw new Error('FLUX client is not connected. Call connect() first.');
    }

    const correlationId = String(this.correlationSeq++);
    const line = buildCommandLine(correlationId, command, args);

    return await new Promise<FLUXResponse>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(correlationId);
        reject(new Error(`Request timed out for correlation_id=${correlationId}`));
      }, this.timeoutMs);

      this.pending.set(correlationId, { resolve, reject, timeout });

      this.socket!.write(`${line}\n`, (err?: Error | null) => {
        if (err) {
          const pending = this.pending.get(correlationId);
          if (!pending) {
            return;
          }
          clearTimeout(pending.timeout);
          this.pending.delete(correlationId);
          reject(err);
        }
      });
    });
  }

  private handleData(chunk: string): void {
    this.recvBuffer += chunk;

    let newlineIdx = this.recvBuffer.indexOf('\n');
    while (newlineIdx !== -1) {
      const line = this.recvBuffer.slice(0, newlineIdx).trim();
      this.recvBuffer = this.recvBuffer.slice(newlineIdx + 1);

      if (line.length > 0) {
        this.handleLine(line);
      }

      newlineIdx = this.recvBuffer.indexOf('\n');
    }
  }

  private handleLine(line: string): void {
    const response = parseResponse(line);

    const pending = this.pending.get(response.correlationId);
    if (!pending) {
      return;
    }

    clearTimeout(pending.timeout);
    this.pending.delete(response.correlationId);

    if (response.status === 'ERR') {
      pending.reject(
        new FLUXProtocolError(
          response.message ?? 'FLUX protocol error',
          response.code ?? 'UNKNOWN_ERROR',
          response,
        ),
      );
      return;
    }

    pending.resolve(response);
  }

  private rejectAllPending(error: Error): void {
    for (const [correlationId, pending] of this.pending.entries()) {
      clearTimeout(pending.timeout);
      pending.reject(new Error(`${error.message}; correlation_id=${correlationId}`));
    }
    this.pending.clear();
  }
}

function extractBracketedList(payload: string, key: string): string[] {
  const token = `${key}=[`;
  const start = payload.indexOf(token);
  if (start === -1) {
    return [];
  }
  const rest = payload.slice(start + token.length);
  const end = rest.indexOf(']');
  if (end === -1) {
    return [];
  }
  return rest
    .slice(0, end)
    .split(' ')
    .map((v) => v.trim())
    .filter((v) => v.length > 0);
}
