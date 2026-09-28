import { randomUUID } from 'node:crypto';

import { Logger } from '@nestjs/common';
import { ClientProxy, type ReadPacket, type WritePacket } from '@nestjs/microservices';
import type { Message, MessagesStream } from '@platformatic/kafka';

import { createConsumer, createProducer } from '../adapters/platformatic/factory.js';
import type { TransportConsumer, TransportProducer } from '../context/kafka.context.js';
import {
  KafkaReplyTopicNotSubscribedError,
  KafkaTransportError,
  KafkaTransportNotConnectedError,
} from '../errors.js';
import type { KafkaTransportOptions, KafkaTransportStatus } from '../interfaces/options.js';
import { KafkaHeaders, replyTopicOf } from '../wire/headers.js';
import { KafkaParser } from '../wire/parser.js';
import { decodePayload, toWireRecord } from '../wire/serialize.js';
import { replyPartitionAssigner } from './reply-partition-assigner.js';

const DEFAULT_POSTFIX = '-client';

/** What `client.unwrap()` returns. `consumer` is `null` in `producerOnlyMode`. */
export interface KafkaTransportClientInstances {
  consumer: TransportConsumer | null;
  producer: TransportProducer;
}

/**
 * `ClientProxy` for Kafka built on `@platformatic/kafka`. Wire-compatible with the built-in
 * `ClientKafka`: requests carry `kafka_correlationId`, `kafka_replyTopic` and
 * `kafka_replyPartition`; replies are read from `<pattern>.reply`.
 *
 * @example
 * const client = new KafkaTransportClient({
 *   client: { clientId: 'gateway', bootstrapBrokers: ['localhost:9092'] },
 *   consumer: { groupId: 'gateway' },
 * });
 * client.subscribeToResponseOf('order.total');
 * await client.connect();
 * const total = await firstValueFrom(client.send<number>('order.total', order));
 */
export class KafkaTransportClient extends ClientProxy<
  Record<string, (...args: unknown[]) => void>,
  KafkaTransportStatus
> {
  protected readonly logger = new Logger(KafkaTransportClient.name);
  protected consumer: TransportConsumer | null = null;
  protected producer: TransportProducer | null = null;
  protected stream: MessagesStream<Buffer, Buffer, string, Buffer> | null = null;
  protected readonly parser: KafkaParser;
  protected readonly postfixId: string;
  protected readonly responseTopics = new Set<string>();
  protected assignments = new Map<string, number>();
  private connecting: Promise<TransportProducer> | null = null;
  private consuming: Promise<void> | null = null;
  private closing = false;

  constructor(protected readonly options: KafkaTransportOptions) {
    super();
    this.parser = new KafkaParser(options.parser);
    this.postfixId = options.postfixId ?? DEFAULT_POSTFIX;
    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  /** Registers the reply topic of `pattern`; call it for every pattern used with `send()`. */
  public subscribeToResponseOf(pattern: unknown): void {
    this.responseTopics.add(replyTopicOf(this.normalizePattern(pattern as never)));
  }

  public connect(): Promise<TransportProducer> {
    if (this.producer) {
      return Promise.resolve(this.producer);
    }
    this.connecting ??= this.initialize().catch((error: unknown) => {
      this.connecting = null;
      throw error;
    });
    return this.connecting;
  }

  public async close(): Promise<void> {
    this.closing = true;
    try {
      await this.stream?.close();
      await this.consuming;
    } finally {
      this.stream = null;
      await this.consumer?.close(true);
      await this.producer?.close(true);
      this.consumer = null;
      this.producer = null;
      this.connecting = null;
      this.closing = false;
      this._status$.next('disconnected');
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- signature fixed by ClientProxy
  public override unwrap<T>(): T {
    if (!this.producer) {
      throw new KafkaTransportNotConnectedError('Call connect() before unwrap()');
    }
    const instances: KafkaTransportClientInstances = {
      consumer: this.consumer,
      producer: this.producer,
    };
    return instances as T;
  }

  /** Partition of each reply topic currently assigned to this client. */
  public getConsumerAssignments(): Record<string, number> {
    return Object.fromEntries(this.assignments);
  }

  protected async initialize(): Promise<TransportProducer> {
    const { client, consumer, producerOnlyMode } = this.options;
    this.producer = createProducer(client, this.options.producer, this.postfixId);

    if (!producerOnlyMode) {
      if (!consumer) {
        throw new KafkaTransportError(
          'KafkaTransportClient requires `consumer.groupId` unless `producerOnlyMode` is set',
        );
      }
      this.consumer = createConsumer(
        client,
        { ...consumer, partitionAssigner: replyPartitionAssigner },
        this.postfixId,
      );
      this.consumer.on('consumer:group:join', (payload) => {
        this.assignments = new Map(
          (payload.assignments ?? [])
            .filter((a) => a.partitions.length > 0)
            .map((a) => [a.topic, a.partitions[0] ?? 0]),
        );
        this._status$.next('connected');
      });
      this.consumer.on('consumer:group:rebalance', () => {
        this._status$.next('rebalancing');
      });
      if (this.responseTopics.size > 0) {
        if (client.autocreateTopics !== false) {
          await this.consumer.metadata({
            topics: [...this.responseTopics],
            autocreateTopics: true,
            forceUpdate: true,
          });
        }
        this.stream = await this.consumer.consume({
          topics: [...this.responseTopics],
          mode: 'latest',
          autocommit: true,
          ...(consumer.maxFetches !== undefined ? { maxFetches: consumer.maxFetches } : {}),
        });
        this.consuming = this.consumeReplies(this.stream);
        await this.waitForAssignments();
      }
    }
    this._status$.next('connected');
    return this.producer;
  }

  protected async waitForAssignments(): Promise<void> {
    const consumer = this.consumer;
    if (!consumer || this.assignments.size > 0) {
      return;
    }
    await new Promise<void>((resolve) => {
      consumer.once('consumer:group:join', () => {
        resolve();
      });
    });
  }

  protected async consumeReplies(
    stream: MessagesStream<Buffer, Buffer, string, Buffer>,
  ): Promise<void> {
    try {
      for await (const raw of stream) {
        this.handleReply(raw);
      }
    } catch (error) {
      if (!this.closing) {
        this.logger.error(
          'Reply stream failed',
          error instanceof Error ? error.stack : String(error),
        );
        this._status$.next('disconnected');
      }
    }
  }

  protected handleReply(raw: Message<Buffer, Buffer, string>): void {
    const message = this.parser.parse(raw);
    const id = this.header(message.headers, KafkaHeaders.CORRELATION_ID);
    if (!id) {
      return;
    }
    const callback = this.routingMap.get(id) as ((packet: WritePacket) => void) | undefined;
    if (!callback) {
      return;
    }
    const err = this.header(message.headers, KafkaHeaders.NEST_ERR);
    const isDisposed = this.header(message.headers, KafkaHeaders.NEST_IS_DISPOSED) !== undefined;
    if (err !== undefined) {
      callback({ err: this.parseError(err), isDisposed: true });
      return;
    }
    callback({ response: decodePayload(message), isDisposed });
  }

  protected publish(
    partialPacket: ReadPacket,
    callback: (packet: WritePacket) => void,
  ): () => void {
    const packet = this.assignPacketId(partialPacket);
    const pattern = this.normalizePattern(packet.pattern as never);
    const replyTopic = replyTopicOf(pattern);
    if (!this.responseTopics.has(replyTopic)) {
      callback({ err: new KafkaReplyTopicNotSubscribedError(pattern), isDisposed: true });
      return () => undefined;
    }
    const id = randomUUID();
    this.routingMap.set(id, callback);

    const headers: Record<string, string> = {
      [KafkaHeaders.CORRELATION_ID]: id,
      [KafkaHeaders.REPLY_TOPIC]: replyTopic,
    };
    const partition = this.assignments.get(replyTopic);
    if (partition !== undefined) {
      headers[KafkaHeaders.REPLY_PARTITION] = String(partition);
    }

    this.connect()
      .then((producer) =>
        producer.send({ messages: [toWireRecord(pattern, packet.data, headers)] }),
      )
      .catch((error: unknown) => {
        this.routingMap.delete(id);
        callback({ err: error, isDisposed: true });
      });

    return () => {
      this.routingMap.delete(id);
    };
  }

  protected async dispatchEvent<T = unknown>(packet: ReadPacket): Promise<T> {
    const pattern = this.normalizePattern(packet.pattern as never);
    const producer = await this.connect();
    await producer.send({ messages: [toWireRecord(pattern, packet.data)] });
    return undefined as T;
  }

  protected header(headers: Record<string, string | Buffer>, name: string): string | undefined {
    const value = headers[name];
    if (value === undefined) {
      return undefined;
    }
    return Buffer.isBuffer(value) ? value.toString('utf8') : value;
  }

  protected parseError(raw: string): unknown {
    try {
      return JSON.parse(raw) as unknown;
    } catch {
      return raw;
    }
  }
}
