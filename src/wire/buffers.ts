/** Narrow `Buffer | string` to `Buffer` without relying on the global `Buffer` in type positions. */
export function isBufferLike(value: unknown): value is Buffer {
  return Buffer.isBuffer(value);
}

/** Encodes a header or payload value for the wire. Objects and arrays are JSON-encoded. */
export function toWireValue(value: unknown): Buffer | string {
  if (value === undefined || value === null) {
    return '';
  }
  if (isBufferLike(value) || typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  return JSON.stringify(value);
}
