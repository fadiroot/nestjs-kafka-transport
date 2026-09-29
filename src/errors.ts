/** Base class for every error raised by nestjs-kafka-transport. */
export class KafkaTransportError extends Error {
  public override readonly name: string = 'KafkaTransportError';
}

/** The transport was used before `listen()` / `connect()` completed, or after `close()`. */
export class KafkaTransportNotConnectedError extends KafkaTransportError {
  public override readonly name = 'KafkaTransportNotConnectedError';
}

/** A request pattern was sent without a prior `subscribeToResponseOf(pattern)`. */
export class KafkaReplyTopicNotSubscribedError extends KafkaTransportError {
  public override readonly name = 'KafkaReplyTopicNotSubscribedError';

  constructor(public readonly pattern: string) {
    super(
      `No reply topic subscription for pattern "${pattern}". ` +
        `Call client.subscribeToResponseOf("${pattern}") before sending requests.`,
    );
  }
}

/** Why a reply stopped being awaited; see {@link KafkaReplyLostError}. */
export type KafkaReplyLostReason = 'rebalance' | 'timeout' | 'closed' | 'disconnected';

/**
 * The request was produced but its reply can no longer reach this client, so the outcome is
 * **unknown**: the handler may or may not have run. Raised when the reply partition was taken
 * away by a rebalance, the request deadline passed, or the client closed / lost its reply stream
 * while waiting.
 *
 * Every other error coming out of `send()` means the request never left, so retrying it is safe.
 * Before retrying after this one, make the handler idempotent with an operation id:
 *
 * @example
 * try {
 *   return await firstValueFrom(client.send('order.pay', order, { operationId, timeout: 5000 }));
 * } catch (error) {
 *   if (error instanceof KafkaReplyLostError) {
 *     // same operationId on the retry lets the handler detect the duplicate
 *     return await firstValueFrom(client.send('order.pay', order, { operationId }));
 *   }
 *   throw error;
 * }
 */
export class KafkaReplyLostError extends KafkaTransportError {
  public override readonly name = 'KafkaReplyLostError';

  constructor(
    public readonly pattern: string,
    public readonly reason: KafkaReplyLostReason,
    public readonly correlationId: string,
    public readonly operationId?: string,
  ) {
    super(
      `Reply for pattern "${pattern}" (correlation ${correlationId}) was lost: ${describeReason(reason)}. ` +
        'The outcome is unknown; the handler may have run.',
    );
  }
}

function describeReason(reason: KafkaReplyLostReason): string {
  switch (reason) {
    case 'rebalance':
      return 'the reply partition was reassigned to another client instance';
    case 'timeout':
      return 'no reply arrived before the deadline';
    case 'closed':
      return 'the client was closed while waiting';
    case 'disconnected':
      return 'the reply stream failed while waiting';
  }
}

/** Rejects a reply that carries an error header; mirrors the built-in transport's behaviour. */
export class KafkaRemoteHandlerError extends KafkaTransportError {
  public override readonly name = 'KafkaRemoteHandlerError';

  constructor(
    public readonly pattern: string,
    public readonly remoteError: unknown,
  ) {
    super(`Handler for pattern "${pattern}" failed on the remote service`);
  }
}
