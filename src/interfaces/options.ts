import type { Deserializer, Serializer } from '@nestjs/microservices';
import type {
  BaseOptions,
  ConsumeBaseOptions,
  GroupOptions,
  ProduceOptions,
  StreamOptions,
} from '@platformatic/kafka';

import type { KafkaParserConfig } from '../wire/parser.js';

/**
 * Connection options shared by the consumer and the producer of one transport instance.
 * Everything is forwarded to `@platformatic/kafka` (`BaseOptions`), including `tls`, `sasl`,
 * `timeout`, `retries`, `retryDelay`, `metadataMaxAge`, `autocreateTopics` and `metrics`.
 */
export type KafkaTransportConnectionOptions = Omit<BaseOptions, 'context' | 'strict'>;

/**
 * Consumer options. `groupId` is required; the rest maps to `@platformatic/kafka`'s consumer
 * and stream options (session/heartbeat timeouts, fetch sizes, start mode, ...).
 */
export type KafkaTransportConsumerOptions = {
  /** Consumer group id. The transport appends {@link KafkaTransportOptions.postfixId} to it. */
  groupId: string;
  /**
   * Number of records processed concurrently by this instance. Records of the same partition
   * are always processed in order; concurrency only applies across partitions.
   * @defaultValue 1
   */
  concurrency?: number;
} & Omit<GroupOptions, 'protocols' | 'protocolsMetadata'> &
  Omit<ConsumeBaseOptions<Buffer, Buffer, string, Buffer>, 'deserializers' | 'registry'> &
  Pick<StreamOptions, 'mode' | 'fallbackMode' | 'maxFetches' | 'onDeserializationError'>;

/** Producer options: acks, compression, idempotence, partitioner, `transactionalId`. */
export type KafkaTransportProducerOptions = Omit<
  ProduceOptions<Buffer, Buffer, string, Buffer>,
  'producerId' | 'producerEpoch'
> & {
  transactionalId?: string;
};

/** Options accepted by `KafkaTransportServer` and `KafkaTransportClient`. */
export interface KafkaTransportOptions {
  /** Connection options shared by the consumer and the producer. */
  client: KafkaTransportConnectionOptions;
  /** Consumer options. Required for servers and for clients that receive replies. */
  consumer?: KafkaTransportConsumerOptions;
  /** Producer options. */
  producer?: KafkaTransportProducerOptions;
  /**
   * Suffix appended to `clientId` and `groupId` so the server and client instances of one process
   * do not collide, exactly like the built-in transport's `postfixId`.
   * @defaultValue '-server' on the server, '-client' on the client
   */
  postfixId?: string;
  /** Custom serializer for outgoing packets (same contract as the built-in transport). */
  serializer?: Serializer;
  /** Custom deserializer for incoming packets (same contract as the built-in transport). */
  deserializer?: Deserializer;
  /** Parsing rules for incoming records; see `KafkaParser`. */
  parser?: KafkaParserConfig;
  /**
   * Client only: do not create a consumer. `send()` throws; `emit()` works.
   * @defaultValue false
   */
  producerOnlyMode?: boolean;
  /**
   * Server only: how many times a handler that throws `KafkaRetriableException` is re-run
   * in-process before the record is skipped (and logged). `0` disables in-process retries.
   * @defaultValue 3
   */
  retriableAttempts?: number;
  /**
   * Server only: delay in milliseconds between in-process retries; doubles on each attempt.
   * @defaultValue 200
   */
  retriableDelay?: number;
  /**
   * Client only: deadline in milliseconds for the first reply of a `send()`. When it passes, the
   * request fails with `KafkaReplyLostError` (`reason: 'timeout'`). `send()` options can
   * override it per request. Unset: wait forever (apply RxJS `timeout()` yourself).
   */
  requestTimeout?: number;
  /**
   * Client only: generates the operation id stamped on requests that do not pass one in their
   * `send()` options (`kafka_nest-operation-id` header). Unset: no header unless provided.
   * @example
   * generateOperationId: () => randomUUID()
   */
  generateOperationId?: () => string;
}

/** Per-request options of `KafkaTransportClient.send()`. */
export interface KafkaSendOptions {
  /**
   * Key of the logical operation, sent as the `kafka_nest-operation-id` header. Reuse the same
   * value when retrying after a `KafkaReplyLostError` so the handler can deduplicate
   * (`ctx.getOperationId()`); each attempt still gets its own correlation id.
   */
  operationId?: string;
  /**
   * Deadline in milliseconds for the first reply; overrides `requestTimeout`. `0` disables it.
   */
  timeout?: number;
}

/** Statuses emitted on `server.status` / `client.status`. */
export type KafkaTransportStatus = 'connected' | 'disconnected' | 'rebalancing';
