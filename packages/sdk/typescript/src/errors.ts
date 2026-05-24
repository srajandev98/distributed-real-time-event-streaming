import type { FLUXResponse } from './types';

export class FLUXProtocolError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly response: FLUXResponse,
  ) {
    super(message);
    this.name = 'FLUXProtocolError';
  }
}
