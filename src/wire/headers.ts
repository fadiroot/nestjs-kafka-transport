/**
 * Kafka record headers used by the transport.
 *
 * The values are byte-for-byte identical to the ones used by the built-in
 * `@nestjs/microservices` Kafka transport (which in turn mirrors Spring Kafka), so a service
 * running this transport can exchange requests and replies with a service still running the
 * kafkajs-based transport.
 *
 * @see https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/KafkaHeaders.html
 */
export const KafkaHeaders = {
  /** Correlation id that ties a reply record to the request that produced it. */
  CORRELATION_ID: 'kafka_correlationId',
  /** Topic the requester listens on for the reply. */
  REPLY_TOPIC: 'kafka_replyTopic',
  /** Partition of the reply topic assigned to the requester. */
  REPLY_PARTITION: 'kafka_replyPartition',
  /** Serialized error attached to a reply when the handler failed. */
  NEST_ERR: 'kafka_nest-err',
  /** Marks the last reply of an observable response stream. */
  NEST_IS_DISPOSED: 'kafka_nest-is-disposed',
  /**
   * Application-level key of the logical operation behind a request. Unlike the correlation id
   * it stays the same when the caller retries, so handlers can deduplicate. Specific to this
   * transport: the built-in kafkajs transport ignores it.
   */
  OPERATION_ID: 'kafka_nest-operation-id',
  /** Number of the delivery attempt, set by the retry pipeline. */
  DELIVERY_ATTEMPT: 'kafka_deliveryAttempt',
  /** Dead-letter metadata (same names as Spring Kafka's `kafka_dlt-*` headers). */
  DLT_EXCEPTION_FQCN: 'kafka_dlt-exception-fqcn',
  DLT_EXCEPTION_MESSAGE: 'kafka_dlt-exception-message',
  DLT_EXCEPTION_STACKTRACE: 'kafka_dlt-exception-stacktrace',
  DLT_ORIGINAL_OFFSET: 'kafka_dlt-original-offset',
  DLT_ORIGINAL_PARTITION: 'kafka_dlt-original-partition',
  DLT_ORIGINAL_TIMESTAMP: 'kafka_dlt-original-timestamp',
  DLT_ORIGINAL_TOPIC: 'kafka_dlt-original-topic',
} as const;

export type KafkaHeaderName = (typeof KafkaHeaders)[keyof typeof KafkaHeaders];

/** Header values as seen by handlers: decoded strings, or `Buffer` when binary is kept. */
export type KafkaHeaderValue = string | Buffer | null | undefined;

/** Value that reaches the wire: `@platformatic/kafka` accepts strings and buffers. */
export type KafkaWireHeaders = Map<string, string | Buffer>;

/** Suffix appended to a request pattern to name its reply topic, as the built-in transport does. */
export const REPLY_TOPIC_SUFFIX = '.reply';

/**
 * Name of the reply topic for a request pattern.
 *
 * @example
 * replyTopicOf('user.create') // 'user.create.reply'
 */
export function replyTopicOf(pattern: string): string {
  return `${pattern}${REPLY_TOPIC_SUFFIX}`;
}

/**
 * Header stamped by this transport on payloads it JSON-encoded. Receivers use it to decode
 * primitives (numbers, booleans, `null`) faithfully; services on the built-in kafkajs transport
 * ignore it and keep their usual behaviour, so the two can still interoperate.
 */
export const CONTENT_TYPE_HEADER = 'kafka_nest-content-type';
/** Value of {@link CONTENT_TYPE_HEADER} for JSON-encoded payloads. */
export const CONTENT_TYPE_JSON = 'application/json';
