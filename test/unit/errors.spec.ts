import { describe, expect, it } from 'vitest';

import {
  KafkaRemoteHandlerError,
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
