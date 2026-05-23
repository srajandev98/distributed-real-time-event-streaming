import type { ConsumedMessage, JoinResult, RTESResponse } from './types';

export function mustGetPayload(resp: RTESResponse, fallback: string): string {
  if (typeof resp.payload !== 'string') {
    throw new Error(fallback);
  }
  return resp.payload;
}

export function extractIntField(payload: string, key: string): number {
  const match = payload.match(new RegExp(`${key}=(-?\\d+)`));
  if (!match) {
    throw new Error(`Field ${key} missing in payload: ${payload}`);
  }

  const parsed = Number.parseInt(match[1], 10);
  if (Number.isNaN(parsed)) {
    throw new Error(`Invalid integer for ${key}: ${payload}`);
  }

  return parsed;
}

export function parseConsumedMessages(payload: string): ConsumedMessage[] {
  const [, rawData = ''] = payload.split('messages=');
  const data = rawData.split(' hw=')[0].trim();

  if (data.trim() === '') {
    return [];
  }

  return data
    .split(',')
    .filter((item) => item.length > 0)
    .map((item) => {
      const [rawOffset, ...valueParts] = item.split(':');
      const parsedOffset = Number.parseInt(rawOffset, 10);
      if (Number.isNaN(parsedOffset)) {
        throw new Error(`Invalid message offset in consume response: ${item}`);
      }

      return {
        offset: parsedOffset,
        value: valueParts.join(':'),
      } satisfies ConsumedMessage;
    });
}

export function parseIntListField(payload: string, key: string): number[] {
  const match = payload.match(new RegExp(`${key}=\\[([^\\]]*)\\]`));
  if (!match) {
    return [];
  }
  const body = match[1].trim();
  if (body === '') {
    return [];
  }
  return body
    .split(/\s+/)
    .map((v) => Number.parseInt(v, 10))
    .filter((v) => !Number.isNaN(v));
}

export function parseBoolField(payload: string, key: string): boolean {
  const match = payload.match(new RegExp(`${key}=(true|false)`));
  if (!match) {
    throw new Error(`Field ${key} missing in payload: ${payload}`);
  }
  return match[1] === 'true';
}

export function parseJoinAssignments(payload: string): JoinResult['assigned'] {
  const start = payload.indexOf('[');
  const end = payload.indexOf(']');
  if (start === -1 || end === -1 || end <= start) {
    return [];
  }

  const body = payload.slice(start + 1, end).trim();
  if (body === '') {
    return [];
  }

  return body
    .split(/\s+/)
    .map((v) => Number.parseInt(v, 10))
    .filter((v) => !Number.isNaN(v));
}

export function parseJoinGeneration(payload: string): number {
  return extractIntField(payload, 'generation');
}

export function extractStringField(payload: string, key: string): string {
  const match = payload.match(new RegExp(`${key}=([^\\s]+)`));
  if (!match) {
    throw new Error(`Field ${key} missing in payload: ${payload}`);
  }
  return match[1];
}
