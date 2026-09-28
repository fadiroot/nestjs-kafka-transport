import { KafkaContext } from '@nestjs/microservices';
import type { Consumer, Producer } from '@platformatic/kafka';

import type { ParsedKafkaRecord } from '../wire/parser.js';

/** Consumer type as instantiated by this transport (buffers in, string header names). */
export type TransportConsumer = Consumer<Buffer, Buffer, string>;
/** Producer type as instantiated by this transport. */
export type TransportProducer = Producer<Buffer, Buffer, string>;

/**
 * The record handed to handlers through `KafkaContext.getMessage()`. It carries the same fields
 * as the built-in transport's `KafkaMessage` (`key`, `value`, `headers`, `timestamp`, `offset`)
 * plus `topic` and `partition`, with values already parsed by `KafkaParser`.
 */
export type KafkaTransportMessage<TValue = unknown, TKey = unknown> = ParsedKafkaRecord<
  TValue,
  TKey
>;

type KafkaContextArgs = ConstructorParameters<typeof KafkaContext>[0];

/**
 * Builds the `KafkaContext` handlers receive through `@Ctx()`. The class is the one exported by
 * `@nestjs/microservices`, so existing handlers typed against it keep compiling; the consumer and
 * producer returned by `getConsumer()` / `getProducer()` are the `@platformatic/kafka` instances.
 */
export function createKafkaContext(
  message: KafkaTransportMessage,
  consumer: TransportConsumer,
  producer: TransportProducer,
): KafkaContext {
  // `@platformatic/kafka` heartbeats on its own; the callback exists for API compatibility.
  const heartbeat = (): Promise<void> => Promise.resolve();
  const args = [message, message.partition, message.topic, consumer, heartbeat, producer];
  return new KafkaContext(args as unknown as KafkaContextArgs);
}
