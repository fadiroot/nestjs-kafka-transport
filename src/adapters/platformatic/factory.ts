import { Consumer, Producer, stringDeserializer, stringSerializer } from '@platformatic/kafka';
import type { ConsumerOptions, ProducerOptions } from '@platformatic/kafka';

import type { TransportConsumer, TransportProducer } from '../../context/kafka.context.js';
import type {
  KafkaTransportConnectionOptions,
  KafkaTransportConsumerOptions,
  KafkaTransportProducerOptions,
} from '../../interfaces/options.js';

/** Transport-internal consumer options: connection + group + fetch settings, minus our extras. */
export type ConsumerFactoryOptions = Omit<KafkaTransportConsumerOptions, 'concurrency'>;

const identity = (data?: Buffer): Buffer | undefined => data;

/**
 * Creates the `@platformatic/kafka` consumer used by the transport: keys and values stay raw
 * `Buffer`s (parsing is done by `KafkaParser`), header names are decoded to strings.
 */
export function createConsumer(
  connection: KafkaTransportConnectionOptions,
  consumer: ConsumerFactoryOptions,
  postfixId: string,
): TransportConsumer {
  const {
    mode: _mode,
    fallbackMode: _fallbackMode,
    maxFetches: _maxFetches,
    onDeserializationError: _onDeserializationError,
    ...constructorOptions
  } = consumer;
  const options = {
    ...connection,
    ...constructorOptions,
    clientId: `${connection.clientId}${postfixId}`,
    groupId: `${consumer.groupId}${postfixId}`,
    deserializers: {
      key: identity,
      value: identity,
      headerKey: stringDeserializer,
      headerValue: identity,
    },
  } satisfies ConsumerOptions<Buffer, Buffer, string, Buffer>;
  return new Consumer<Buffer, Buffer, string, Buffer>(options);
}

/** Creates the `@platformatic/kafka` producer used by the transport (buffers in, string header names). */
export function createProducer(
  connection: KafkaTransportConnectionOptions,
  producer: KafkaTransportProducerOptions | undefined,
  postfixId: string,
): TransportProducer {
  const options = {
    ...connection,
    ...(producer ?? {}),
    clientId: `${connection.clientId}${postfixId}`,
    serializers: {
      key: identity,
      value: identity,
      headerKey: stringSerializer,
      headerValue: identity,
    },
  } satisfies ProducerOptions<Buffer, Buffer, string, Buffer>;
  return new Producer<Buffer, Buffer, string, Buffer>(options);
}
