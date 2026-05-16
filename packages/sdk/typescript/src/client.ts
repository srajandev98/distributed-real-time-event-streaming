import net from 'node:net';

import { RTESProtocolError } from './errors';
import { buildCommandLine, parseResponse } from './protocol';
import { extractIntField, mustGetPayload, parseConsumedMessages, parseJoinAssignments } from './parsers';
import type {
  ConsumedMessage,
  JoinResult,
  PendingRequest,
  ProduceResult,
  RTESClientOptions,
  RTESResponse,
} from './types';

/**
 * RTESClient is a simple Node.js client for the RTES TCP text protocol.
 */
export class RTESClient {
  private readonly host: string;
  private readonly port: number;
  private readonly timeoutMs: number;

  private socket: net.Socket | null = null;
  private connected = false;
  private recvBuffer = '';
  private correlationSeq = 1;
  private pending = new Map<string, PendingRequest>();

  constructor(options: RTESClientOptions = {}) {
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
      this.rejectAllPending(new Error('RTES connection closed'));
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

    this.rejectAllPending(new Error('RTES client closed'));
  }

  async produce(topic: string, key: string, value: string): Promise<ProduceResult> {
    const resp = await this.sendCommand('PRODUCE', `${topic} ${key}:${value}`);
    const payload = mustGetPayload(resp, 'produce response payload missing');

    return {
      partition: extractIntField(payload, 'partition'),
      offset: extractIntField(payload, 'offset'),
      raw: resp,
    };
  }

  async consume(topic: string, partition: number, offset: number): Promise<ConsumedMessage[]> {
    const resp = await this.sendCommand('CONSUME', `${topic} ${partition} ${offset}`);
    const payload = mustGetPayload(resp, 'consume response payload missing');
    return parseConsumedMessages(payload);
  }

  async join(group: string, topic: string, consumerId: string): Promise<JoinResult> {
    const resp = await this.sendCommand('JOIN', `${group} ${topic} ${consumerId}`);
    const payload = mustGetPayload(resp, 'join response payload missing');
    return { assigned: parseJoinAssignments(payload), raw: resp };
  }

  async commit(group: string, topic: string, partition: number, offset: number): Promise<boolean> {
    const resp = await this.sendCommand('COMMIT', `${group} ${topic} ${partition} ${offset}`);
    return mustGetPayload(resp, 'commit response payload missing').includes('committed=true');
  }

  async offset(group: string, topic: string, partition: number): Promise<number> {
    const resp = await this.sendCommand('OFFSET', `${group} ${topic} ${partition}`);
    return extractIntField(mustGetPayload(resp, 'offset response payload missing'), 'offset');
  }

  async sendCommand(command: string, args = ''): Promise<RTESResponse> {
    if (!this.socket || !this.connected) {
      throw new Error('RTES client is not connected. Call connect() first.');
    }

    const correlationId = String(this.correlationSeq++);
    const line = buildCommandLine(correlationId, command, args);

    return await new Promise<RTESResponse>((resolve, reject) => {
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
        new RTESProtocolError(
          response.message ?? 'RTES protocol error',
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
