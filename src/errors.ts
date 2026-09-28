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
