import type { MessageToProduce } from '@platformatic/kafka';

import { toWireValue } from './buffers.js';
import { CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON } from './headers.js';
import type { ParsedKafkaRecord } from './parser.js';

/**
 * Shape a caller can pass to `send()` / `emit()` to control the record explicitly, the same
 * convention the built-in transport uses: an object with a `value` property is treated as a
 * record, anything else becomes the record value.
 *
 * @example
 * client.emit('order.created', { key: order.id, value: order, headers: { source: 'api' } });
 */
export interface KafkaOutgoingRecord {
  key?: unknown;
  value: unknown;
  headers?: Record<string, string | Buffer> | undefined;
  partition?: number | undefined;
}

/** Record accepted by the Platformatic producer as configured by this transport. */
export interface WireRecord extends MessageToProduce<Buffer, Buffer, string> {
  value: Buffer;
  headers: Map<string, Buffer>;
}

export function isOutgoingRecord(data: unknown): data is KafkaOutgoingRecord {
  return typeof data === 'object' && data !== null && !Buffer.isBuffer(data) && 'value' in data;
}

/** Encodes a value to a `Buffer` for the wire (`toWireValue` rules, then UTF-8). */
export function encode(value: unknown): Buffer {
  const wire = toWireValue(value);
  return Buffer.isBuffer(wire) ? wire : Buffer.from(wire, 'utf8');
}

/**
 * Builds the record to produce for a pattern and a payload, merging transport headers
 * (correlation id, reply topic, ...) with headers set by the caller.
 */
export function toWireRecord(
  topic: string,
  data: unknown,
  transportHeaders: Record<string, string> = {},
): WireRecord {
  const record: KafkaOutgoingRecord = isOutgoingRecord(data) ? data : { value: data };
  const headers = new Map<string, Buffer>();
  for (const [name, value] of Object.entries(record.headers ?? {})) {
    headers.set(name, Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8'));
  }
  if (isJsonEncoded(record.value)) {
    headers.set(CONTENT_TYPE_HEADER, Buffer.from(CONTENT_TYPE_JSON, 'utf8'));
  }
  for (const [name, value] of Object.entries(transportHeaders)) {
    headers.set(name, Buffer.from(value, 'utf8'));
  }
  const wire: WireRecord = { topic, value: encode(record.value), headers };
  if (record.key !== undefined && record.key !== null) {
    wire.key = encode(record.key);
  }
  if (record.partition !== undefined) {
    wire.partition = record.partition;
  }
  return wire;
}

/** Whether `value` is sent JSON-encoded and therefore stamped with the content-type header. */
export function isJsonEncoded(value: unknown): boolean {
  return value !== undefined && typeof value !== 'string' && !Buffer.isBuffer(value);
}

/**
 * Returns the payload of a parsed record: the value as parsed by `KafkaParser`, or, when the
 * record carries the JSON content-type header set by this transport, the JSON-decoded value so
 * numbers, booleans and `null` round-trip exactly.
 */
export function decodePayload(record: ParsedKafkaRecord): unknown {
  const contentType = record.headers[CONTENT_TYPE_HEADER];
  const text = Buffer.isBuffer(contentType) ? contentType.toString('utf8') : contentType;
  if (text === CONTENT_TYPE_JSON && typeof record.value === 'string') {
    try {
      return JSON.parse(record.value) as unknown;
    } catch {
      return record.value;
    }
  }
  return record.value;
}
