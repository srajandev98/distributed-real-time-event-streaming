export interface FLUXClientOptions {
  host?: string;
  port?: number;
  timeoutMs?: number;
}

export interface FLUXResponse {
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
  raw: FLUXResponse;
}

export type AckMode = '0' | '1' | 'all';

export interface ConsumedMessage {
  offset: number;
  value: string;
}

export interface JoinResult {
  generation: number;
  assigned: number[];
  raw: FLUXResponse;
}

export interface SyncResult {
  generation: number;
  assigned: number[];
  raw: FLUXResponse;
}

export interface PartitionRoleResult {
  topic: string;
  partition: number;
  role: 'leader' | 'follower';
  highWatermark: number;
  raw: FLUXResponse;
}

export interface ReplicaFetchResult {
  replicaId: number;
  ackedOffset: number;
  highWatermark: number;
  isr: number[];
  underReplicated: boolean;
  raw: FLUXResponse;
}

export interface AdminCreateTopicResult {
  topic: string;
  partitions: number;
  replicationFactor: number;
  term: number;
  index: number;
  raw: FLUXResponse;
}

export interface AdminRegisterBrokerResult {
  brokerId: number;
  host: string;
  port: number;
  term: number;
  index: number;
  raw: FLUXResponse;
}

export interface AdminBrokerHeartbeatResult {
  brokerId: number;
  heartbeatOk: boolean;
  term: number;
  index: number;
  raw: FLUXResponse;
}

export interface AdminSetPartitionLeaderResult {
  topic: string;
  partition: number;
  leader: number;
  isr: number[];
  term: number;
  index: number;
  raw: FLUXResponse;
}

export interface AdminMetadataResult {
  topics: string[];
  brokers: number[];
  raw: FLUXResponse;
}

export interface PendingRequest {
  resolve: (value: FLUXResponse) => void;
  reject: (reason?: unknown) => void;
  timeout: NodeJS.Timeout;
}
