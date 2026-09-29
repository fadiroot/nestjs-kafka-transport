import { EventEmitter } from 'node:events';

import { Logger } from '@nestjs/common';
import type { Message, MessagesStream } from '@platformatic/kafka';
import { firstValueFrom, lastValueFrom, toArray } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { KafkaTransportClient } from '../../src/client/kafka-transport.client.js';
import { KafkaReplyLostError } from '../../src/errors.js';
import type { KafkaTransportOptions } from '../../src/interfaces/options.js';
import { KafkaHeaders } from '../../src/wire/headers.js';
import type { WireRecord } from '../../src/wire/serialize.js';

// The factory module is replaced so the client talks to in-memory fakes instead of a broker.
const fakes = vi.hoisted(() => ({
  consumer: undefined as FakeConsumer | undefined,
  producer: undefined as FakeProducer | undefined,
}));
vi.mock('../../src/adapters/platformatic/factory.js', () => ({
  createConsumer: () => fakes.consumer,
  createProducer: () => fakes.producer,
}));

// The "disconnected" case logs an error on purpose; keep the test output clean.
Logger.overrideLogger(false);

type RawReply = Message<Buffer, Buffer, string>;
interface Waiter {
  resolve: (r: IteratorResult<RawReply>) => void;
  reject: (e: Error) => void;
}

/** Async-iterable reply stream the test feeds by hand. */
class FakeStream {
  private readonly queue: RawReply[] = [];
  private readonly waiters: Waiter[] = [];
  private ended: { error?: Error } | null = null;

  push(message: RawReply): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ value: message, done: false });
    } else {
      this.queue.push(message);
    }
  }

  fail(error: Error): void {
    this.ended = { error };
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(error);
    }
  }

  close(): Promise<void> {
    this.ended = {};
    for (const waiter of this.waiters.splice(0)) {
      waiter.resolve({ value: undefined, done: true });
    }
    return Promise.resolve();
  }

  [Symbol.asyncIterator](): AsyncIterator<RawReply> {
    return {
      next: () => {
        const queued = this.queue.shift();
        if (queued) {
          return Promise.resolve({ value: queued, done: false });
        }
        if (this.ended) {
          return this.ended.error !== undefined
            ? Promise.reject(this.ended.error)
            : Promise.resolve({ value: undefined, done: true });
        }
        return new Promise<IteratorResult<RawReply>>((resolve, reject) => {
          this.waiters.push({ resolve, reject });
        });
      },
    };
  }
}

class FakeConsumer extends EventEmitter {
  readonly stream = new FakeStream();
  metadata = vi.fn(() => Promise.resolve());
  consume = vi.fn(() =>
    Promise.resolve(this.stream as unknown as MessagesStream<Buffer, Buffer, string, Buffer>),
  );
  close = vi.fn(() => Promise.resolve());
}

class FakeProducer {
  readonly sent: WireRecord[] = [];
  failWith: Error | null = null;
  send = vi.fn((batch: { messages: WireRecord[] }) => {
    if (this.failWith) {
      return Promise.reject(this.failWith);
    }
    this.sent.push(...batch.messages);
    return Promise.resolve();
  });
  close = vi.fn(() => Promise.resolve());
}

const PATTERN = 'math.sum';
const REPLY_TOPIC = 'math.sum.reply';

function reply(
  correlationId: string,
  value: unknown,
  extra: Record<string, string> = {},
): RawReply {
  const headers = new Map<string, Buffer>([
    [KafkaHeaders.CORRELATION_ID, Buffer.from(correlationId)],
  ]);
  for (const [name, text] of Object.entries(extra)) {
    headers.set(name, Buffer.from(text));
  }
  return {
    topic: REPLY_TOPIC,
    partition: 0,
    offset: 1n,
    timestamp: 0n,
    key: null,
    value: Buffer.from(JSON.stringify(value)),
    headers,
    commit: () => Promise.resolve(),
  } as unknown as RawReply;
}

function header(record: WireRecord | undefined, name: string): string | undefined {
  return record?.headers.get(name)?.toString('utf8');
}

/** Resolves to the value or the rejection reason, so a pending promise never goes unhandled. */
function outcomeOf<T>(
  promise: Promise<T>,
): Promise<{ value?: T; error?: unknown; settled: boolean }> {
  return promise.then(
    (value) => ({ value, settled: true }),
    (error: unknown) => ({ error, settled: true }),
  );
}

const tick = (ms = 0): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
// Poll fast: several tests measure deadlines of a few tens of milliseconds.
const POLL = { interval: 5, timeout: 2_000 };

describe('KafkaTransportClient request tracking', () => {
  let consumer: FakeConsumer;
  let producer: FakeProducer;
  let client: KafkaTransportClient;

  function freshFakes(): void {
    consumer = new FakeConsumer();
    producer = new FakeProducer();
    fakes.consumer = consumer;
    fakes.producer = producer;
  }

  async function connect(
    partitions: number[] = [0, 1],
    options: Partial<KafkaTransportOptions> = {},
  ): Promise<KafkaTransportClient> {
    client = new KafkaTransportClient({
      client: { clientId: 'test', bootstrapBrokers: ['broker:9092'] },
      consumer: { groupId: 'test' },
      ...options,
    });
    client.subscribeToResponseOf(PATTERN);
    const connecting = client.connect();
    await vi.waitFor(() => {
      expect(consumer.listenerCount('consumer:group:join')).toBe(2);
    }, POLL);
    join(partitions);
    await connecting;
    return client;
  }

  function join(partitions: number[]): void {
    consumer.emit('consumer:group:join', {
      assignments: partitions.length ? [{ topic: REPLY_TOPIC, partitions }] : [],
    });
  }

  async function lastSent(): Promise<WireRecord> {
    await vi.waitFor(() => {
      expect(producer.sent.length).toBeGreaterThan(0);
    }, POLL);
    return producer.sent[producer.sent.length - 1]!;
  }

  beforeEach(freshFakes);

  afterEach(async () => {
    await client.close();
  });

  it('stamps the operation id and keeps it across retries while the correlation id changes', async () => {
    await connect();
    const first = firstValueFrom(client.send(PATTERN, [1, 2], { operationId: 'op-1' }));
    const firstRecord = await lastSent();
    expect(header(firstRecord, KafkaHeaders.OPERATION_ID)).toBe('op-1');
    expect(header(firstRecord, KafkaHeaders.REPLY_PARTITION)).toBe('0');
    consumer.stream.push(
      reply(
        header(firstRecord, KafkaHeaders.CORRELATION_ID)!,
        { sum: 3 },
        { [KafkaHeaders.NEST_IS_DISPOSED]: '1' },
      ),
    );
    expect(await first).toEqual({ sum: 3 });

    const second = firstValueFrom(client.send(PATTERN, [1, 2], { operationId: 'op-1' }));
    await vi.waitFor(() => {
      expect(producer.sent).toHaveLength(2);
    }, POLL);
    const secondRecord = producer.sent[1]!;
    expect(header(secondRecord, KafkaHeaders.OPERATION_ID)).toBe('op-1');
    expect(header(secondRecord, KafkaHeaders.CORRELATION_ID)).not.toBe(
      header(firstRecord, KafkaHeaders.CORRELATION_ID),
    );
    consumer.stream.push(
      reply(
        header(secondRecord, KafkaHeaders.CORRELATION_ID)!,
        { sum: 3 },
        { [KafkaHeaders.NEST_IS_DISPOSED]: '1' },
      ),
    );
    expect(await second).toEqual({ sum: 3 });
  });

  it('uses the client-level generator when a request carries no operation id, and no header otherwise', async () => {
    await connect([0], { generateOperationId: () => 'generated' });
    void outcomeOf(firstValueFrom(client.send(PATTERN, [1])));
    expect(header(await lastSent(), KafkaHeaders.OPERATION_ID)).toBe('generated');
    await client.close();

    freshFakes();
    await connect([0]);
    void outcomeOf(firstValueFrom(client.send(PATTERN, [1])));
    expect(header(await lastSent(), KafkaHeaders.OPERATION_ID)).toBeUndefined();
  });

  it('fails a request whose reply partition was reassigned, and keeps the ones still owned', async () => {
    await connect([0, 1]);
    const request = outcomeOf(firstValueFrom(client.send(PATTERN, [1], { operationId: 'op-2' })));
    const record = await lastSent();
    const correlationId = header(record, KafkaHeaders.CORRELATION_ID)!;
    expect(header(record, KafkaHeaders.REPLY_PARTITION)).toBe('0');

    // Partition 0 is still ours after this rebalance: nothing happens.
    join([0, 2]);
    await tick(10);
    expect(client.getConsumerAssignments()).toEqual({ [REPLY_TOPIC]: 0 });

    // Partition 0 moved to another member: the reply can never reach us.
    join([2]);
    const { error } = await request;
    expect(error).toBeInstanceOf(KafkaReplyLostError);
    expect(error).toMatchObject({
      name: 'KafkaReplyLostError',
      reason: 'rebalance',
      pattern: PATTERN,
      correlationId,
      operationId: 'op-2',
    });
    expect((error as Error).message).toContain('outcome is unknown');

    // A reply that shows up anyway is ignored instead of crashing the reply loop.
    consumer.stream.push(
      reply(correlationId, { sum: 1 }, { [KafkaHeaders.NEST_IS_DISPOSED]: '1' }),
    );
    await tick(10);
    expect(client.status).toBeDefined();
  });

  it('leaves requests sent without a known reply partition alone on rebalance', async () => {
    await connect([]);
    expect(client.getConsumerAssignments()).toEqual({});
    const request = outcomeOf(firstValueFrom(client.send(PATTERN, [1])));
    const record = await lastSent();
    expect(header(record, KafkaHeaders.REPLY_PARTITION)).toBeUndefined();
    join([1]);
    await tick(10);
    consumer.stream.push(
      reply(
        header(record, KafkaHeaders.CORRELATION_ID)!,
        { sum: 1 },
        { [KafkaHeaders.NEST_IS_DISPOSED]: '1' },
      ),
    );
    expect(await request).toEqual({ value: { sum: 1 }, settled: true });
  });

  it('fails with reason "timeout" when the first reply misses the deadline, and forgets the request', async () => {
    await connect();
    const request = outcomeOf(firstValueFrom(client.send(PATTERN, [1], { timeout: 30 })));
    const record = await lastSent();
    const { error } = await request;
    expect(error).toBeInstanceOf(KafkaReplyLostError);
    expect(error).toMatchObject({
      reason: 'timeout',
      correlationId: header(record, KafkaHeaders.CORRELATION_ID),
    });
    const internals = client as unknown as {
      inFlight: Map<string, unknown>;
      routingMap: Map<string, unknown>;
    };
    expect(internals.inFlight.size).toBe(0);
    expect(internals.routingMap.size).toBe(0);
  });

  it('applies the client-level requestTimeout, which a per-request timeout of 0 disables', async () => {
    await connect([0], { requestTimeout: 30 });
    const timedOut = outcomeOf(firstValueFrom(client.send(PATTERN, [1])));
    const unlimited = outcomeOf(firstValueFrom(client.send(PATTERN, [2], { timeout: 0 })));
    await vi.waitFor(() => {
      expect(producer.sent).toHaveLength(2);
    }, POLL);
    await tick(80);
    expect((await timedOut).error).toMatchObject({ reason: 'timeout' });

    const record = producer.sent[1]!;
    consumer.stream.push(
      reply(
        header(record, KafkaHeaders.CORRELATION_ID)!,
        { sum: 2 },
        { [KafkaHeaders.NEST_IS_DISPOSED]: '1' },
      ),
    );
    expect(await unlimited).toEqual({ value: { sum: 2 }, settled: true });
  });

  it('clears the deadline on the first reply so later emissions of a stream still arrive', async () => {
    await connect([0], { requestTimeout: 150 });
    const values = lastValueFrom(client.send(PATTERN, [1]).pipe(toArray()));
    const correlationId = header(await lastSent(), KafkaHeaders.CORRELATION_ID)!;
    consumer.stream.push(reply(correlationId, { n: 1 }));
    // Well past the deadline: the first reply must have cancelled it.
    await tick(300);
    consumer.stream.push(reply(correlationId, { n: 2 }, { [KafkaHeaders.NEST_IS_DISPOSED]: '1' }));
    expect(await values).toEqual([{ n: 1 }, { n: 2 }]);
  });

  it('reports a produce failure as the producer error: the request never left', async () => {
    await connect();
    producer.failWith = new Error('broker down');
    const request = outcomeOf(firstValueFrom(client.send(PATTERN, [1], { timeout: 1000 })));
    const { error } = await request;
    expect(error).not.toBeInstanceOf(KafkaReplyLostError);
    expect(error).toMatchObject({ message: 'broker down' });
    const internals = client as unknown as { inFlight: Map<string, unknown> };
    expect(internals.inFlight.size).toBe(0);
  });

  it('settles pending requests with reason "closed" when the client closes', async () => {
    await connect();
    const request = outcomeOf(firstValueFrom(client.send(PATTERN, [1])));
    await lastSent();
    await client.close();
    expect((await request).error).toMatchObject({ name: 'KafkaReplyLostError', reason: 'closed' });
  });

  it('settles pending requests with reason "disconnected" when the reply stream fails', async () => {
    await connect();
    const request = outcomeOf(firstValueFrom(client.send(PATTERN, [1])));
    await lastSent();
    consumer.stream.fail(new Error('connection reset'));
    expect((await request).error).toMatchObject({
      name: 'KafkaReplyLostError',
      reason: 'disconnected',
    });
    await vi.waitFor(() => {
      expect(client.status).toBeDefined();
    }, POLL);
  });

  it('still rejects unsubscribed patterns synchronously, without touching the producer', async () => {
    await connect();
    const { error } = await outcomeOf(
      firstValueFrom(client.send('math.unknown', [1], { operationId: 'x' })),
    );
    expect(error).toMatchObject({ name: 'KafkaReplyTopicNotSubscribedError' });
    expect(producer.sent).toHaveLength(0);
  });
});
