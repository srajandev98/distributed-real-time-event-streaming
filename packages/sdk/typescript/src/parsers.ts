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
  const [, data = ''] = payload.split('messages=');

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
