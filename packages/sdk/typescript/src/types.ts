export interface RTESClientOptions {
  host?: string;
  port?: number;
  timeoutMs?: number;
}

export interface RTESResponse {
  version: string;
  correlationId: string;
  status: 'OK' | 'ERR';
  payload?: string;
  code?: string;
  message?: string;
  raw: string;
}

export interface ProduceResult {
  partition: number;
  offset: number;
  highWatermark?: number;
  raw: RTESResponse;
}

export type AckMode = '0' | '1' | 'all';

export interface ConsumedMessage {
  offset: number;
  value: string;
}

export interface JoinResult {
  assigned: number[];
  raw: RTESResponse;
}

export interface ReplicaFetchResult {
  replicaId: number;
  ackedOffset: number;
  highWatermark: number;
  isr: number[];
  underReplicated: boolean;
  raw: RTESResponse;
}

export interface PendingRequest {
  resolve: (value: RTESResponse) => void;
  reject: (reason?: unknown) => void;
  timeout: NodeJS.Timeout;
}
