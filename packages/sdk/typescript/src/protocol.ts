import type { FLUXResponse } from './types';

export const PROTOCOL_VERSION = 'V1';

export function parseResponse(line: string): FLUXResponse {
  const parts = line.split('|');
  if (parts.length < 3) {
    throw new Error(`Invalid FLUX response: ${line}`);
  }

  const [version, correlationId, status] = parts;
  if (version !== PROTOCOL_VERSION) {
    throw new Error(`Unsupported FLUX response version: ${version}`);
  }

  if (status === 'OK') {
    return {
      version,
      correlationId,
      status: 'OK',
      payload: parts.slice(3).join('|'),
      raw: line,
    };
  }

  if (status === 'ERR') {
    return {
      version,
      correlationId,
      status: 'ERR',
      code: parts[3] ?? 'UNKNOWN_ERROR',
      message: parts.slice(4).join('|') || 'unknown error',
      raw: line,
    };
  }

  throw new Error(`Unknown FLUX response status: ${status}`);
}

export function buildCommandLine(correlationId: string, command: string, args = ''): string {
  return `${PROTOCOL_VERSION}|${correlationId}|${command}|${args}`;
}
