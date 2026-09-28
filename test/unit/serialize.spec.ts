import { describe, expect, it } from 'vitest';

import { CONTENT_TYPE_HEADER, KafkaHeaders } from '../../src/wire/headers.js';
import { encode, isOutgoingRecord, toWireRecord } from '../../src/wire/serialize.js';

describe('toWireRecord', () => {
  it('wraps a plain payload as the record value, JSON-encoded', () => {
    const record = toWireRecord('orders', { id: 1 });
    expect(record.topic).toBe('orders');
    expect(record.value.toString()).toBe('{"id":1}');
    expect(record.key).toBeUndefined();
    expect(record.partition).toBeUndefined();
    expect([...record.headers.keys()]).toEqual([CONTENT_TYPE_HEADER]);
    expect(record.headers.get(CONTENT_TYPE_HEADER)?.toString()).toBe('application/json');
  });

  it('honours an explicit record (key, value, headers, partition) like the built-in transport', () => {
    const record = toWireRecord('orders', {
      key: 'k1',
      value: 'v',
      headers: { source: 'api', bin: Buffer.from([1]) },
      partition: 2,
    });
    expect(record.key?.toString()).toBe('k1');
    expect(record.value.toString()).toBe('v');
    expect(record.partition).toBe(2);
    expect(record.headers.get('source')?.toString()).toBe('api');
    expect(record.headers.get('bin')).toEqual(Buffer.from([1]));
  });

  it('adds transport headers after user headers so they cannot be overridden', () => {
    const record = toWireRecord(
      'orders',
      { value: 1, headers: { [KafkaHeaders.CORRELATION_ID]: 'user' } },
      { [KafkaHeaders.CORRELATION_ID]: 'transport' },
    );
    expect(record.headers.get(KafkaHeaders.CORRELATION_ID)?.toString()).toBe('transport');
  });

  it('encodes null / undefined values as empty buffers and keeps buffers untouched', () => {
    expect(toWireRecord('t', null).value).toEqual(Buffer.alloc(0));
    expect(toWireRecord('t', undefined).value).toEqual(Buffer.alloc(0));
    const buf = Buffer.from([0, 1, 2]);
    expect(toWireRecord('t', buf).value).toBe(buf);
    expect(encode('x')).toEqual(Buffer.from('x'));
  });

  it('treats buffers and non-objects as plain values, objects with `value` as records', () => {
    expect(isOutgoingRecord(Buffer.from('x'))).toBe(false);
    expect(isOutgoingRecord('x')).toBe(false);
    expect(isOutgoingRecord({ value: 1 })).toBe(true);
    expect(isOutgoingRecord({ id: 1 })).toBe(false);
    expect(isOutgoingRecord(null)).toBe(false);
  });
});
