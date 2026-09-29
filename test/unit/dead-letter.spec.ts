import { describe, expect, it } from 'vitest';

import type { KafkaTransportMessage } from '../../src/context/kafka.context.js';
import { buildDeadLetterRecord } from '../../src/wire/dead-letter.js';
import { deadLetterTopicOf, KafkaHeaders } from '../../src/wire/headers.js';

const message: KafkaTransportMessage = {
  key: 'order-1',
  value: { id: 1, total: 9.5 },
  headers: { 'x-trace': 'abc', [KafkaHeaders.OPERATION_ID]: Buffer.from('op-1') },
  topic: 'orders.created',
  partition: 2,
  offset: '41',
  timestamp: '1700000000000',
};

const text = (record: ReturnType<typeof buildDeadLetterRecord>, name: string): string | undefined =>
  record.headers.get(name)?.toString('utf8');

describe('deadLetterTopicOf', () => {
  it('appends .dlq by default, a custom suffix, or calls a naming function', () => {
    expect(deadLetterTopicOf('orders.created')).toBe('orders.created.dlq');
    expect(deadLetterTopicOf('orders.created', '.DLT')).toBe('orders.created.DLT');
    expect(deadLetterTopicOf('orders.created', (t) => `dead.${t}`)).toBe('dead.orders.created');
  });
});

describe('buildDeadLetterRecord', () => {
  it('copies key, value and headers and describes the failure with kafka_dlt-* headers', () => {
    const error = new TypeError('cannot read total');
    const record = buildDeadLetterRecord(message, { error, attempts: 4 });

    expect(record.topic).toBe('orders.created.dlq');
    expect(record.key?.toString('utf8')).toBe('order-1');
    expect(JSON.parse(record.value.toString('utf8'))).toEqual({ id: 1, total: 9.5 });
    expect(text(record, 'x-trace')).toBe('abc');
    expect(text(record, KafkaHeaders.OPERATION_ID)).toBe('op-1');
    expect(text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC)).toBe('orders.created');
    expect(text(record, KafkaHeaders.DLT_ORIGINAL_PARTITION)).toBe('2');
    expect(text(record, KafkaHeaders.DLT_ORIGINAL_OFFSET)).toBe('41');
    expect(text(record, KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).toBe('1700000000000');
    expect(text(record, KafkaHeaders.DLT_EXCEPTION_FQCN)).toBe('TypeError');
    expect(text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).toBe('cannot read total');
    expect(text(record, KafkaHeaders.DELIVERY_ATTEMPT)).toBe('4');
    expect(record.headers.has(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).toBe(false);
  });

  it('adds the stack trace only when asked, and honours the topic option', () => {
    const error = new Error('boom');
    const record = buildDeadLetterRecord(
      message,
      { error, attempts: 1 },
      { includeStackTrace: true, topic: '.DLT' },
    );
    expect(record.topic).toBe('orders.created.DLT');
    expect(text(record, KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).toContain('Error: boom');
  });

  it('names Error subclasses by their class and filter output objects as RpcError', () => {
    class PaymentDeclined extends Error {}
    const byClass = buildDeadLetterRecord(message, {
      error: new PaymentDeclined('card'),
      attempts: 1,
    });
    expect(text(byClass, KafkaHeaders.DLT_EXCEPTION_FQCN)).toBe('PaymentDeclined');
    const filtered = buildDeadLetterRecord(message, {
      error: { status: 'error', message: 'Internal server error' },
      attempts: 1,
    });
    expect(text(filtered, KafkaHeaders.DLT_EXCEPTION_FQCN)).toBe('RpcError');
    expect(text(filtered, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).toBe('Internal server error');
  });

  it('describes non-Error throwables and records without a key or timestamp', () => {
    const record = buildDeadLetterRecord(
      { ...message, key: null, timestamp: undefined },
      { error: 'plain string', attempts: 1 },
    );
    expect(record.key).toBeUndefined();
    expect(text(record, KafkaHeaders.DLT_EXCEPTION_FQCN)).toBe('string');
    expect(text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).toBe('plain string');
    expect(text(record, KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)).toBe('');
  });
});
