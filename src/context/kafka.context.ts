import { KafkaContext } from '@nestjs/microservices';
import type { Consumer, Producer } from '@platformatic/kafka';

import { KafkaHeaders } from '../wire/headers.js';
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
 * The context handlers receive through `@Ctx()`. It is the `KafkaContext` of
 * `@nestjs/microservices` (handlers typed against it keep compiling) plus this transport's
 * extras.
 *
 * @example
 * @MessagePattern('order.pay')
 * async pay(@Payload() order: Order, @Ctx() ctx: KafkaTransportContext) {
 *   const operationId = ctx.getOperationId(); // same value on every retry of one payment
 *   if (operationId && (await this.payments.alreadyDone(operationId))) {
 *     return this.payments.resultOf(operationId);
 *   }
 *   // ...
 * }
 */
export class KafkaTransportContext extends KafkaContext {
  /**
   * Operation id stamped by the caller (`client.send(pattern, data, { operationId })` or the
   * client's `generateOperationId`), read from the `kafka_nest-operation-id` header. `undefined`
   * when the request carries none (events, callers on the built-in transport).
   */
  public getOperationId(): string | undefined {
    const message = this.getMessage() as unknown as KafkaTransportMessage;
    const value = message.headers[KafkaHeaders.OPERATION_ID];
    if (value === undefined) {
      return undefined;
    }
    return Buffer.isBuffer(value) ? value.toString('utf8') : value;
  }
}

/**
 * Builds the context handlers receive through `@Ctx()`. The consumer and producer returned by
 * `getConsumer()` / `getProducer()` are the `@platformatic/kafka` instances.
 */
export function createKafkaContext(
  message: KafkaTransportMessage,
  consumer: TransportConsumer,
  producer: TransportProducer,
): KafkaTransportContext {
  // `@platformatic/kafka` heartbeats on its own; the callback exists for API compatibility.
  const heartbeat = (): Promise<void> => Promise.resolve();
  const args = [message, message.partition, message.topic, consumer, heartbeat, producer];
  return new KafkaTransportContext(args as unknown as KafkaContextArgs);
}
