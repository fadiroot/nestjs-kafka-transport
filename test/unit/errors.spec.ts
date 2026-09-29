import { describe, expect, it } from 'vitest';

import {
  KafkaRemoteHandlerError,
  KafkaReplyLostError,
  KafkaReplyTopicNotSubscribedError,
  KafkaTransportError,
  KafkaTransportNotConnectedError,
} from '../../src/errors.js';

describe('errors', () => {
  it('all derive from KafkaTransportError and Error, with stable names', () => {
    const errors = [
      new KafkaTransportError('base'),
      new KafkaTransportNotConnectedError('nc'),
      new KafkaReplyTopicNotSubscribedError('p'),
      new KafkaRemoteHandlerError('p', { message: 'boom' }),
      new KafkaReplyLostError('p', 'timeout', 'c-1'),
    ];
    for (const error of errors) {
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(KafkaTransportError);
    }
    expect(errors.map((e) => e.name)).toEqual([
      'KafkaTransportError',
      'KafkaTransportNotConnectedError',
      'KafkaReplyTopicNotSubscribedError',
      'KafkaRemoteHandlerError',
      'KafkaReplyLostError',
    ]);
  });

  it('explains how to fix a missing reply subscription', () => {
    const error = new KafkaReplyTopicNotSubscribedError('order.total');
    expect(error.pattern).toBe('order.total');
    expect(error.message).toContain('subscribeToResponseOf("order.total")');
  });

  it('keeps the remote error payload', () => {
    const error = new KafkaRemoteHandlerError('order.total', { status: 'error' });
    expect(error.remoteError).toEqual({ status: 'error' });
    expect(error.message).toContain('order.total');
  });
});

describe('KafkaReplyLostError', () => {
  it('names the pattern, the reason and the ids, and says the outcome is unknown', () => {
    const error = new KafkaReplyLostError('order.pay', 'rebalance', 'corr-1', 'op-1');
    expect(error).toMatchObject({
      pattern: 'order.pay',
      reason: 'rebalance',
      correlationId: 'corr-1',
      operationId: 'op-1',
    });
    expect(error.message).toContain('order.pay');
    expect(error.message).toContain('corr-1');
    expect(error.message).toContain('reassigned');
    expect(error.message).toContain('outcome is unknown');
  });

  it('describes every reason', () => {
    for (const reason of ['rebalance', 'timeout', 'closed', 'disconnected'] as const) {
      expect(new KafkaReplyLostError('p', reason, 'c').message).not.toMatch(/undefined/);
    }
  });
});
