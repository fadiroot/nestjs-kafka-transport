import { describe, expect, it } from 'vitest';

import { KafkaParser } from '../../src/wire/parser.js';

const base = { topic: 'orders', partition: 2, offset: 42n, timestamp: 1700000000000n };

describe('KafkaParser', () => {
  const parser = new KafkaParser();

  it('JSON-parses object and array payloads', () => {
    expect(parser.parse({ ...base, value: Buffer.from('{"id":1}') }).value).toEqual({ id: 1 });
    expect(parser.parse({ ...base, value: Buffer.from('[1,2]') }).value).toEqual([1, 2]);
  });

  it('keeps plain strings and numbers as strings, like the built-in transport', () => {
    expect(parser.parse({ ...base, value: Buffer.from('hello') }).value).toBe('hello');
    expect(parser.parse({ ...base, value: Buffer.from('42') }).value).toBe('42');
  });

  it('hands back the raw string when a payload only looks like JSON', () => {
    expect(parser.parse({ ...base, value: Buffer.from('{not json') }).value).toBe('{not json');
  });

  it('passes Schema Registry payloads (leading zero byte) through untouched', () => {
    const payload = Buffer.from([0, 0, 0, 0, 7, 0x7b]);
    const parsed = parser.parse({ ...base, value: payload });
    expect(parsed.value).toBe(payload);
  });

  it('maps null / missing values and keys to null', () => {
    const parsed = parser.parse({ ...base, value: null });
    expect(parsed.value).toBeNull();
    expect(parsed.key).toBeNull();
  });

  it('decodes keys and headers, keeping binary headers as buffers', () => {
    const binary = Buffer.from([0xff, 0xfe, 0xfd]);
    const parsed = parser.parse({
      ...base,
      key: Buffer.from('{"k":"v"}'),
      value: Buffer.from('x'),
      headers: new Map<string, Buffer | string>([
        ['kafka_correlationId', Buffer.from('abc')],
        ['plain', 'text'],
        ['bin', binary],
      ]),
    });
    expect(parsed.key).toEqual({ k: 'v' });
    expect(parsed.headers.kafka_correlationId).toBe('abc');
    expect(parsed.headers.plain).toBe('text');
    expect(parsed.headers.bin).toBe(binary);
  });

  it('accepts headers given as a plain object and skips undefined entries', () => {
    const parsed = parser.parse({ ...base, value: 'v', headers: { a: 'x', b: undefined } });
    expect(parsed.headers).toEqual({ a: 'x' });
  });

  it('stringifies offsets and timestamps regardless of their input type', () => {
    const parsed = parser.parse({ ...base, value: 'v' });
    expect(parsed.offset).toBe('42');
    expect(parsed.timestamp).toBe('1700000000000');
    expect(
      parser.parse({ topic: 't', partition: 0, offset: 3, value: 'v' }).timestamp,
    ).toBeUndefined();
  });

  it('keeps the value binary when keepBinary is set but still decodes keys and headers', () => {
    const binaryParser = new KafkaParser({ keepBinary: true });
    const value = Buffer.from('{"id":1}');
    const parsed = binaryParser.parse({
      ...base,
      key: Buffer.from('k'),
      value,
      headers: { h: Buffer.from('1') },
    });
    expect(parsed.value).toBe(value);
    expect(parsed.key).toBe('k');
    expect(parsed.headers.h).toBe('1');
  });

  it('does not mutate the input record', () => {
    const headers = new Map([['a', Buffer.from('1')]]);
    const record = { ...base, value: Buffer.from('{"x":1}'), headers };
    parser.parse(record);
    expect(headers.get('a')).toEqual(Buffer.from('1'));
    expect(record.value).toEqual(Buffer.from('{"x":1}'));
  });
});
