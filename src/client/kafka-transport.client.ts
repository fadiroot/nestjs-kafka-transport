import { randomUUID } from 'node:crypto';

import { Logger } from '@nestjs/common';
import { ClientProxy, type ReadPacket, type WritePacket } from '@nestjs/microservices';
import type { Message, MessagesStream } from '@platformatic/kafka';
import { defer, mergeMap, Observable, type Observer } from 'rxjs';

import { createConsumer, createProducer } from '../adapters/platformatic/factory.js';
import type { TransportConsumer, TransportProducer } from '../context/kafka.context.js';
import {
  KafkaReplyLostError,
  KafkaReplyTopicNotSubscribedError,
  KafkaTransportError,
  KafkaTransportNotConnectedError,
  type KafkaReplyLostReason,
} from '../errors.js';
import type {
  KafkaSendOptions,
  KafkaTransportOptions,
  KafkaTransportStatus,
} from '../interfaces/options.js';
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

/** `ReadPacket` plus the per-request options of `send()`, handed from `send()` to `publish()`. */
type KafkaRequestPacket = ReadPacket & KafkaSendOptions;

/** A request that was handed to the producer and whose first reply has not arrived yet. */
interface InFlightRequest {
  pattern: string;
  replyTopic: string;
  /** Reply partition stamped on the request; `undefined` when none was known at send time. */
  partition: number | undefined;
  operationId: string | undefined;
  timer: NodeJS.Timeout | undefined;
}

/**
 * `ClientProxy` for Kafka built on `@platformatic/kafka`. Wire-compatible with the built-in
 * `ClientKafka`: requests carry `kafka_correlationId`, `kafka_replyTopic` and
 * `kafka_replyPartition`; replies are read from `<pattern>.reply`.
 *
 * On top of that, `send()` accepts an operation id and a deadline, and reports a reply it can no
 * longer receive (rebalance, deadline, close) as a `KafkaReplyLostError` so callers can tell an
 * unknown outcome from a request that never left.
 *
 * @example
 * const client = new KafkaTransportClient({
 *   client: { clientId: 'gateway', bootstrapBrokers: ['localhost:9092'] },
 *   consumer: { groupId: 'gateway' },
 *   requestTimeout: 10_000,
 * });
 * client.subscribeToResponseOf('order.total');
 * await client.connect();
 * const total = await firstValueFrom(
 *   client.send<number>('order.total', order, { operationId: order.id }),
 * );
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
  /** Partition stamped on outgoing requests, per reply topic (the first one assigned). */
  protected assignments = new Map<string, number>();
  /** Every partition of each reply topic owned by this client, per the last group join. */
  protected ownedPartitions = new Map<string, Set<number>>();
  /** Requests awaiting their first reply, by correlation id. */
  protected readonly inFlight = new Map<string, InFlightRequest>();
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

  /**
   * Sends a request and streams its replies, like `ClientProxy.send()`, with two extras:
   * `options.operationId` (stable key across retries, see `KafkaTransportContext.getOperationId`)
   * and `options.timeout` (deadline for the first reply).
   *
   * Errors: `KafkaReplyLostError` means the request was produced and the outcome is unknown; any
   * other error means the request never left and can be retried as is.
   */
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- signature fixed by ClientProxy
  public override send<TResult = unknown, TInput = unknown>(
    pattern: unknown,
    data: TInput,
    options: KafkaSendOptions = {},
  ): Observable<TResult> {
    if (pattern === undefined || pattern === null || data === undefined || data === null) {
      // Same `InvalidMessageException` as the base class.
      return super.send<TResult, TInput>(pattern, data);
    }
    return defer(async () => this.connect()).pipe(
      mergeMap(
        () =>
          new Observable((observer: Observer<TResult>) => {
            const callback = this.createObserver(observer);
            const packet: KafkaRequestPacket = { pattern, data, ...options };
            return this.publish(packet, callback);
          }),
      ),
    );
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
    // Nobody will read the reply topics after this: settle every pending request now instead of
    // leaving its observable hanging forever.
    this.failInFlight('closed');
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
        const assigned = (payload.assignments ?? []).filter((a) => a.partitions.length > 0);
        this.assignments = new Map(assigned.map((a) => [a.topic, a.partitions[0] ?? 0]));
        this.ownedPartitions = new Map(assigned.map((a) => [a.topic, new Set(a.partitions)]));
        this._status$.next('connected');
        this.failOrphanedRequests();
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
        this.failInFlight('disconnected');
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
    // Any reply settles the outcome: the handler ran. Later replies of an observable result no
    // longer count against the deadline or the reply partition.
    this.markAnswered(id);
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
    const packet = this.assignPacketId(partialPacket) as KafkaRequestPacket;
    const pattern = this.normalizePattern(packet.pattern as never);
    const replyTopic = replyTopicOf(pattern);
    if (!this.responseTopics.has(replyTopic)) {
      callback({ err: new KafkaReplyTopicNotSubscribedError(pattern), isDisposed: true });
      return () => undefined;
    }
    const id = randomUUID();
    const operationId = packet.operationId ?? this.options.generateOperationId?.();
    const headers: Record<string, string> = {
      [KafkaHeaders.CORRELATION_ID]: id,
      [KafkaHeaders.REPLY_TOPIC]: replyTopic,
    };
    if (operationId !== undefined) {
      headers[KafkaHeaders.OPERATION_ID] = operationId;
    }
    const partition = this.assignments.get(replyTopic);
    if (partition !== undefined) {
      headers[KafkaHeaders.REPLY_PARTITION] = String(partition);
    }

    const request: InFlightRequest = {
      pattern,
      replyTopic,
      partition,
      operationId,
      timer: undefined,
    };
    const timeout = packet.timeout ?? this.options.requestTimeout;
    if (timeout !== undefined && timeout > 0) {
      request.timer = setTimeout(() => {
        this.failRequest(id, 'timeout');
      }, timeout);
      request.timer.unref();
    }
    this.routingMap.set(id, callback);
    this.inFlight.set(id, request);

    this.connect()
      .then((producer) =>
        producer.send({ messages: [toWireRecord(pattern, packet.data, headers)] }),
      )
      .catch((error: unknown) => {
        // The request never reached the broker: a known, retriable failure.
        this.settle(id);
        callback({ err: error, isDisposed: true });
      });

    return () => {
      this.settle(id);
    };
  }

  protected async dispatchEvent<T = unknown>(packet: ReadPacket): Promise<T> {
    const pattern = this.normalizePattern(packet.pattern as never);
    const producer = await this.connect();
    await producer.send({ messages: [toWireRecord(pattern, packet.data)] });
    return undefined as T;
  }

  /** Forgets a request entirely (callback included). Idempotent. */
  protected settle(id: string): void {
    this.markAnswered(id);
    this.routingMap.delete(id);
  }

  /** Stops watching a request for loss; its callback stays registered for further replies. */
  protected markAnswered(id: string): void {
    const request = this.inFlight.get(id);
    if (!request) {
      return;
    }
    if (request.timer) {
      clearTimeout(request.timer);
    }
    this.inFlight.delete(id);
  }

  /** Fails one pending request with `KafkaReplyLostError`, if it is still pending. */
  protected failRequest(id: string, reason: KafkaReplyLostReason): void {
    const request = this.inFlight.get(id);
    const callback = this.routingMap.get(id) as ((packet: WritePacket) => void) | undefined;
    if (!request || !callback) {
      return;
    }
    this.settle(id);
    callback({
      err: new KafkaReplyLostError(request.pattern, reason, id, request.operationId),
      isDisposed: true,
    });
  }

  protected failInFlight(reason: KafkaReplyLostReason): void {
    for (const id of [...this.inFlight.keys()]) {
      this.failRequest(id, reason);
    }
  }

  /**
   * After a group join, fails the requests whose reply partition this client no longer owns: the
   * server produces the reply straight to that partition, and the member that owns it now has
   * no callback for the correlation id, so the reply can never be delivered here.
   */
  protected failOrphanedRequests(): void {
    for (const [id, request] of [...this.inFlight]) {
      if (request.partition === undefined) {
        continue;
      }
      if (!this.ownedPartitions.get(request.replyTopic)?.has(request.partition)) {
        this.failRequest(id, 'rebalance');
      }
    }
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
