import { describe, expect, it } from 'vitest';

import { KafkaHeaders, REPLY_TOPIC_SUFFIX, replyTopicOf } from '../../src/wire/headers.js';
import { toWireValue } from '../../src/wire/buffers.js';

describe('KafkaHeaders', () => {
  it('uses the exact header names of the built-in @nestjs/microservices transport', () => {
    expect(KafkaHeaders.CORRELATION_ID).toBe('kafka_correlationId');
    expect(KafkaHeaders.REPLY_TOPIC).toBe('kafka_replyTopic');
    expect(KafkaHeaders.REPLY_PARTITION).toBe('kafka_replyPartition');
    expect(KafkaHeaders.NEST_ERR).toBe('kafka_nest-err');
    expect(KafkaHeaders.NEST_IS_DISPOSED).toBe('kafka_nest-is-disposed');
  });

  it('derives reply topics with the ".reply" suffix', () => {
    expect(REPLY_TOPIC_SUFFIX).toBe('.reply');
    expect(replyTopicOf('user.create')).toBe('user.create.reply');
  });
});

describe('toWireValue', () => {
  it('passes strings and buffers through and JSON-encodes structured values', () => {
    const buf = Buffer.from('b');
    expect(toWireValue('s')).toBe('s');
    expect(toWireValue(buf)).toBe(buf);
    expect(toWireValue({ a: 1 })).toBe('{"a":1}');
    expect(toWireValue([1])).toBe('[1]');
    expect(toWireValue(3)).toBe('3');
    expect(toWireValue(true)).toBe('true');
    expect(toWireValue(10n)).toBe('10');
    expect(toWireValue(null)).toBe('');
    expect(toWireValue(undefined)).toBe('');
  });
});
