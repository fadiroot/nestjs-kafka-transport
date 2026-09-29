import { Logger } from '@nestjs/common';
import {
  KafkaRetriableException,
  Server,
  Transport,
  type BaseRpcContext,
  type CustomTransportStrategy,
  type MessageHandler,
  type ReadPacket,
  type WritePacket,
} from '@nestjs/microservices';
import { Admin, type Message, type MessagesStream } from '@platformatic/kafka';
import { isObservable, lastValueFrom, type Observable } from 'rxjs';

import { createConsumer, createProducer } from '../adapters/platformatic/factory.js';
import {
  createKafkaContext,
  type KafkaTransportMessage,
  type TransportConsumer,
  type TransportProducer,
} from '../context/kafka.context.js';
import { KafkaTransportError, KafkaTransportNotConnectedError } from '../errors.js';
import type { KafkaTransportOptions, KafkaTransportStatus } from '../interfaces/options.js';
import { buildDeadLetterRecord } from '../wire/dead-letter.js';
import { KafkaHeaders, REPLY_TOPIC_SUFFIX } from '../wire/headers.js';
import { KafkaParser } from '../wire/parser.js';
import { decodePayload, toWireRecord } from '../wire/serialize.js';

/** Events forwarded from the underlying consumer and producer through `server.on()`. */
export type KafkaTransportServerEvents = Record<string, (...args: unknown[]) => void>;

/** What `server.unwrap()` returns. */
export interface KafkaTransportInstances {
  consumer: TransportConsumer;
  producer: TransportProducer;
}

const DEFAULT_POSTFIX = '-server';
const DEFAULT_RETRIABLE_ATTEMPTS = 3;
const DEFAULT_RETRIABLE_DELAY = 200;

/** Result of running a handler through the retry loop. */
type HandlerOutcome = { ok: true } | { ok: false; error: unknown; attempts: number };

/**
 * Kafka transport strategy for `@nestjs/microservices`, built on `@platformatic/kafka`.
 *
 * Drop-in for the built-in kafkajs transport: `@MessagePattern` handlers answer request-reply
 * messages (same `kafka_*` headers and `<pattern>.reply` topics), `@EventPattern` handlers
 * receive events, and `@Ctx()` yields a `KafkaContext`.
 *
 * @example
 * const app = await NestFactory.createMicroservice(AppModule, {
 *   strategy: new KafkaTransportServer({
 *     client: { clientId: 'orders', bootstrapBrokers: ['localhost:9092'] },
 *     consumer: { groupId: 'orders' },
 *   }),
 * });
 * await app.listen();
 */
export class KafkaTransportServer
  extends Server<KafkaTransportServerEvents, KafkaTransportStatus>
  implements CustomTransportStrategy
{
  public override transportId = Transport.KAFKA;
  /** Event handler errors propagate to the consumer loop, as in the built-in transport. */
  public readonly propagatesEventHandlerErrors = true;

  protected override readonly logger = new Logger(KafkaTransportServer.name);
  protected consumer: TransportConsumer | null = null;
  protected producer: TransportProducer | null = null;
  protected stream: MessagesStream<Buffer, Buffer, string, Buffer> | null = null;
  protected readonly parser: KafkaParser;
  protected readonly postfixId: string;
  private consuming: Promise<void> | null = null;
  private closing = false;

  constructor(protected readonly options: KafkaTransportOptions) {
    super();
    this.parser = new KafkaParser(options.parser);
    this.postfixId = options.postfixId ?? DEFAULT_POSTFIX;
    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  public async listen(callback: (error?: unknown) => void): Promise<void> {
    try {
      const { client, consumer } = this.options;
      if (!consumer) {
        throw new KafkaTransportError('KafkaTransportServer requires `consumer.groupId`');
      }
      this.producer = createProducer(client, this.options.producer, this.postfixId);
      this.consumer = createConsumer(client, consumer, this.postfixId);
      this.registerEventListeners(this.consumer);

      const topics = await this.resolveTopics();
      if (topics.length === 0) {
        this.logger.warn(
          'No @MessagePattern / @EventPattern handlers registered; nothing to consume',
        );
      }
      await this.ensureTopics(this.consumer, topics);
      this.stream = await this.consumer.consume({
        topics,
        mode: consumer.mode ?? 'committed',
        fallbackMode: consumer.fallbackMode ?? 'latest',
        autocommit: consumer.autocommit ?? false,
        ...(consumer.maxFetches !== undefined ? { maxFetches: consumer.maxFetches } : {}),
        ...(consumer.onDeserializationError
          ? { onDeserializationError: consumer.onDeserializationError }
          : {}),
      });
      this._status$.next('connected');
      this.consuming = this.consumeLoop(this.stream, consumer.concurrency ?? 1);
      callback();
    } catch (error) {
      callback(error);
    }
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
      this._status$.next('disconnected');
    }
  }

  public override on(event: string, callback: (...args: unknown[]) => void): void {
    if (!this.consumer) {
      throw new KafkaTransportNotConnectedError('Call listen() before on()');
    }
    (this.consumer as unknown as { on(e: string, cb: (...args: unknown[]) => void): void }).on(
      event,
      callback,
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- signature fixed by Server
  public override unwrap<T>(): T {
    if (!this.consumer || !this.producer) {
      throw new KafkaTransportNotConnectedError('Call listen() before unwrap()');
    }
    const instances: KafkaTransportInstances = { consumer: this.consumer, producer: this.producer };
    return instances as T;
  }

  /**
   * Registers a handler. `RegExp` patterns are kept as `RegExp` keys (as Nest's built-in
   * transport does) and resolved against the existing topics at startup.
   */
  public override addHandler(
    pattern: unknown,
    callback: MessageHandler,
    isEventHandler = false,
    extras: Record<string, unknown> = {},
  ): void {
    if (!(pattern instanceof RegExp)) {
      super.addHandler(pattern, callback, isEventHandler, extras);
      return;
    }
    callback.isEventHandler = isEventHandler;
    callback.extras = extras;
    const handlers = this.handlersByKey();
    const existing = handlers.get(pattern);
    if (existing && isEventHandler) {
      let tail = existing;
      while (tail.next) {
        tail = tail.next;
      }
      tail.next = callback;
    } else {
      handlers.set(pattern, callback);
    }
  }

  public override getHandlerByPattern(pattern: string): MessageHandler | null {
    const direct = super.getHandlerByPattern(pattern);
    if (direct) {
      return direct;
    }
    const route = this.getRouteFromPattern(pattern);
    for (const [registered, handler] of this.handlersByKey()) {
      if (registered instanceof RegExp) {
        registered.lastIndex = 0;
        const matches = registered.test(route);
        registered.lastIndex = 0;
        if (matches) {
          return handler;
        }
      }
    }
    return null;
  }

  /**
   * Topics to subscribe to: one per string pattern, plus every existing topic matched by a
   * `RegExp` pattern (resolved once at startup against the cluster metadata; topics created
   * later are picked up on the next restart).
   */
  protected async resolveTopics(): Promise<string[]> {
    const topics = new Set<string>();
    const regexes: RegExp[] = [];
    for (const key of this.handlersByKey().keys()) {
      if (key instanceof RegExp) {
        regexes.push(key);
      } else {
        topics.add(key);
      }
    }
    if (regexes.length > 0) {
      for (const topic of await this.listTopics()) {
        // Reply topics carry responses, never requests: a broad RegExp must not subscribe to them.
        if (topic.endsWith(REPLY_TOPIC_SUFFIX)) {
          continue;
        }
        if (regexes.some((regex) => regex.test(topic))) {
          topics.add(topic);
        }
      }
    }
    return [...topics];
  }

  /** Lists the cluster topics (internal ones excluded) through a short-lived Admin client. */
  protected async listTopics(): Promise<string[]> {
    const admin = new Admin({
      ...this.options.client,
      clientId: `${this.options.client.clientId}${this.postfixId}-admin`,
    });
    try {
      return await admin.listTopics({ includeInternals: false });
    } finally {
      await admin.close();
    }
  }

  private handlersByKey(): Map<string | RegExp, MessageHandler> {
    const handlers: Map<string | RegExp, MessageHandler> = this.messageHandlers;
    return handlers;
  }

  protected async consumeLoop(
    stream: MessagesStream<Buffer, Buffer, string, Buffer>,
    concurrency: number,
  ): Promise<void> {
    const inFlight = new Set<Promise<void>>();
    try {
      for await (const message of stream) {
        const task = this.handleMessage(message).finally(() => inFlight.delete(task));
        inFlight.add(task);
        if (inFlight.size >= Math.max(1, concurrency)) {
          await Promise.race(inFlight);
        }
      }
    } catch (error) {
      if (!this.closing) {
        this.logger.error(
          'Consumer stream failed',
          error instanceof Error ? error.stack : String(error),
        );
        this._status$.next('disconnected');
      }
    } finally {
      await Promise.allSettled(inFlight);
    }
  }

  /** Dispatches one record to its handler, replies if it was a request, commits on success. */
  protected async handleMessage(raw: Message<Buffer, Buffer, string>): Promise<void> {
    const consumer = this.consumer;
    const producer = this.producer;
    if (!consumer || !producer) {
      return;
    }
    const message = this.parser.parse(raw);
    const handler = this.getHandlerByPattern(message.topic);
    if (!handler) {
      this.logger.warn(`No handler registered for topic "${message.topic}"`);
      await this.commit(raw, true);
      return;
    }

    const correlationId = this.headerAsString(message, KafkaHeaders.CORRELATION_ID);
    const replyTopic = this.headerAsString(message, KafkaHeaders.REPLY_TOPIC);
    const replyPartition = this.headerAsString(message, KafkaHeaders.REPLY_PARTITION);
    const context = createKafkaContext(message, consumer, producer, async () => {
      await raw.commit();
    });
    // Same convention as the built-in KafkaRequestDeserializer: the record value is the payload,
    // unless a custom deserializer was configured.
    const data: unknown = this.options.deserializer
      ? await this.customDeserialize(message)
      : decodePayload(message);

    if (handler.isEventHandler || !correlationId || !replyTopic) {
      const outcome = await this.withRetries(
        () => this.handleEvent(message.topic, { pattern: message.topic, data }, context),
        this.describe(message),
      );
      if (!outcome.ok) {
        await this.deadLetter(message, outcome.error, outcome.attempts);
      }
      await this.commit(raw);
      return;
    }

    const respond = async (response: WritePacket): Promise<void> => {
      const headers: Record<string, string> = { [KafkaHeaders.CORRELATION_ID]: correlationId };
      if (response.err !== undefined) {
        headers[KafkaHeaders.NEST_ERR] = JSON.stringify(this.serializeError(response.err));
      }
      if (response.isDisposed) {
        headers[KafkaHeaders.NEST_IS_DISPOSED] = '1';
      }
      const record = toWireRecord(replyTopic, response.response ?? null, headers);
      if (replyPartition !== undefined) {
        record.partition = Number(replyPartition);
      }
      await producer.send({ messages: [record] });
    };

    const outcome = await this.withRetries(async () => {
      const result: unknown = await this.runWithHooks(context, () => handler(data, context));
      const response$: Observable<unknown> = this.transformToObservable(result);
      await this.sendReplies(response$, respond);
    }, this.describe(message));
    if (!outcome.ok && outcome.error instanceof KafkaRetriableException) {
      // Retries exhausted: the caller got no reply. Keep the record for inspection.
      await this.deadLetter(message, outcome.error, outcome.attempts);
    }
    await this.commit(raw);
  }

  /**
   * Copies a failed record to its dead-letter topic (when `deadLetter` is configured) with the
   * `kafka_dlt-*` headers. A failure to produce it is logged; the source record is still
   * committed so the partition does not stall.
   */
  protected async deadLetter(
    message: KafkaTransportMessage,
    error: unknown,
    attempts: number,
  ): Promise<void> {
    const options = this.options.deadLetter;
    const producer = this.producer;
    if (!options || !producer) {
      return;
    }
    const record = buildDeadLetterRecord(message, { error, attempts }, options);
    try {
      await producer.send({ messages: [record] });
      this.logger.warn(`Dead-lettered ${this.describe(message)} to "${record.topic}"`);
    } catch (produceError) {
      this.logger.error(
        `Could not dead-letter ${this.describe(message)} to "${record.topic}"`,
        produceError instanceof Error ? produceError.stack : String(produceError),
      );
    }
  }

  /** `topic[partition]@offset`, for log lines. */
  protected describe(message: KafkaTransportMessage): string {
    return `${message.topic}[${String(message.partition)}]@${message.offset}`;
  }

  /** Streams every emission of the handler result as a reply; the last one is flagged disposed. */
  protected sendReplies(
    response$: Observable<unknown>,
    respond: (packet: WritePacket) => Promise<void>,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let last: Promise<void> = Promise.resolve();
      const sub = this.send(response$, (packet) => {
        // A retriable exception is not answered: it bubbles up so the record is re-processed.
        if (packet.err instanceof KafkaRetriableException) {
          sub.unsubscribe();
          reject(packet.err);
          return last;
        }
        last = last.then(() => respond(packet));
        if (packet.isDisposed || packet.err !== undefined) {
          void last.finally(() => {
            sub.unsubscribe();
            resolve();
          });
        }
        return last;
      });
    });
  }

  /**
   * Runs an event handler and waits for its result (including observable results) so that a
   * `KafkaRetriableException` reaches the retry loop, exactly as the built-in transport does.
   */
  public override async handleEvent(
    pattern: string,
    packet: ReadPacket,
    context: BaseRpcContext,
  ): Promise<void> {
    const handler = this.getHandlerByPattern(pattern);
    if (!handler) {
      this.logger.error(`No event handler registered for pattern "${pattern}"`);
      return;
    }
    await this.runWithHooks(context, async () => {
      const result: unknown = await handler(packet.data, context);
      if (isObservable(result)) {
        await lastValueFrom(result, { defaultValue: undefined });
      }
    });
  }

  /**
   * Re-runs `task` when it throws `KafkaRetriableException`, with exponential backoff. Resolves
   * with the final outcome; a failure is logged and never thrown.
   */
  protected async withRetries(task: () => Promise<void>, label: string): Promise<HandlerOutcome> {
    const attempts = this.options.retriableAttempts ?? DEFAULT_RETRIABLE_ATTEMPTS;
    let delay = this.options.retriableDelay ?? DEFAULT_RETRIABLE_DELAY;
    for (let attempt = 0; ; attempt++) {
      try {
        await task();
        return { ok: true };
      } catch (error) {
        if (!(error instanceof KafkaRetriableException) || attempt >= attempts) {
          this.logger.error(
            `Handler for ${label} failed${attempt > 0 ? ` after ${String(attempt + 1)} attempts` : ''}`,
            error instanceof Error ? error.stack : String(error),
          );
          return { ok: false, error, attempts: attempt + 1 };
        }
        await new Promise((r) => setTimeout(r, delay));
        delay *= 2;
      }
    }
  }

  protected async runWithHooks<T>(context: BaseRpcContext, run: () => Promise<T> | T): Promise<T> {
    // The start hook (graceful-shutdown tracking in Nest) receives `done` and is expected to call
    // it; if a custom hook does not, the handler still runs so no record is silently dropped.
    // Nest sets the hooks only when a listener (graceful shutdown tracking) asks for them, even
    // though the typings declare them as always present.
    const startHook = this.onProcessingStartHook as typeof this.onProcessingStartHook | undefined;
    const endHook = this.onProcessingEndHook as typeof this.onProcessingEndHook | undefined;
    let finished: Promise<T> | undefined;
    startHook?.(this.transportId, context, () => {
      finished = Promise.resolve(run());
      return finished;
    });
    finished ??= Promise.resolve(run());
    try {
      return await finished;
    } finally {
      endHook?.(this.transportId, context);
    }
  }

  /**
   * Asks the cluster for metadata of the topics with auto-creation enabled, so subscribing to a
   * topic nobody produced to yet works like it does with kafkajs. Disable with
   * `client.autocreateTopics: false` (topics must then exist).
   */
  protected async ensureTopics(consumer: TransportConsumer, topics: string[]): Promise<void> {
    if (topics.length === 0 || this.options.client.autocreateTopics === false) {
      return;
    }
    await consumer.metadata({ topics, autocreateTopics: true, forceUpdate: true });
  }

  /**
   * Commits the record unless the consumer autocommits or `commitMode` is `'manual'`.
   * `force` commits regardless of `commitMode` (records without a handler).
   */
  protected async commit(raw: Message<Buffer, Buffer, string>, force = false): Promise<void> {
    if (this.options.consumer?.autocommit || (this.options.commitMode === 'manual' && !force)) {
      return;
    }
    try {
      await raw.commit();
    } catch (error) {
      this.logger.warn(
        `Commit failed for ${raw.topic}[${String(raw.partition)}]@${String(raw.offset)}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /** Runs the user-provided deserializer (built-in transport contract) and returns its `data`. */
  protected async customDeserialize(message: KafkaTransportMessage): Promise<unknown> {
    if (!this.options.deserializer) {
      return decodePayload(message);
    }
    const packet = (await this.options.deserializer.deserialize(message, {
      channel: message.topic,
    })) as ReadPacket;
    return packet.data;
  }

  protected serializeError(error: unknown): unknown {
    if (error instanceof Error) {
      return { message: error.message, name: error.name };
    }
    return error;
  }

  protected registerEventListeners(consumer: TransportConsumer): void {
    consumer.on('consumer:group:rebalance', () => {
      this._status$.next('rebalancing');
    });
    consumer.on('consumer:group:join', () => {
      this._status$.next('connected');
    });
    consumer.on('client:broker:failed', (payload) => {
      this.logger.warn(`Broker connection failed: ${JSON.stringify(payload)}`);
    });
  }

  protected headerAsString(message: KafkaTransportMessage, name: string): string | undefined {
    const value = message.headers[name];
    if (value === undefined) {
      return undefined;
    }
    return Buffer.isBuffer(value) ? value.toString('utf8') : value;
  }
}
