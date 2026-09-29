import { Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { firstValueFrom, lastValueFrom, toArray } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { KafkaTransportClient, KafkaTransportServer } from '../../src/index.js';
import { BROKERS, sleep, uniqueId } from './fixtures/broker.js';
import { MathController, received } from './fixtures/math.controller.js';

/* eslint-disable @typescript-eslint/no-extraneous-class -- Nest modules are declarative */
@Module({ controllers: [MathController] })
class AppModule {}
/* eslint-enable @typescript-eslint/no-extraneous-class */

describe('request-reply and events over @platformatic/kafka', () => {
  const run = uniqueId('e2e');
  let app: INestMicroservice | undefined;
  let client: KafkaTransportClient | undefined;
  const c = (): KafkaTransportClient => {
    if (!client) {
      throw new Error('client not connected');
    }
    return client;
  };

  beforeAll(async () => {
    const kafkaClient = new KafkaTransportClient({
      client: { clientId: `${run}-client`, bootstrapBrokers: BROKERS, autocreateTopics: true },
      consumer: {
        groupId: `${run}-client-group`,
        sessionTimeout: 10_000,
        heartbeatInterval: 1_000,
      },
    });
    client = kafkaClient;
    for (const pattern of [
      'math.sum.sync.kafka.message',
      'math.sum.sync.array',
      'math.sum.sync.string',
      'math.sum.sync.number',
      'math.stream',
      'math.fail',
      'math.sum.sync.regex.one',
      'math.operation',
      'math.slow',
    ]) {
      kafkaClient.subscribeToResponseOf(pattern);
    }
    await kafkaClient.connect();
    // The RegExp pattern is resolved against existing topics at startup: create it first.
    await kafkaClient
      .unwrap<{
        producer: {
          metadata: (o: { topics: string[]; autocreateTopics: boolean }) => Promise<unknown>;
        };
      }>()
      .producer.metadata({ topics: ['math.sum.sync.regex.one'], autocreateTopics: true });

    app = await NestFactory.createMicroservice(AppModule, {
      strategy: new KafkaTransportServer({
        client: { clientId: `${run}-server`, bootstrapBrokers: BROKERS, autocreateTopics: true },
        consumer: {
          groupId: `${run}-server-group`,
          sessionTimeout: 10_000,
          heartbeatInterval: 1_000,
        },
        retriableDelay: 10,
      }),
      logger: ['error', 'warn'],
    });
    await app.listen();
    // Let the server's consumer group settle before the first request.
    await sleep(1_500);
  });

  afterAll(async () => {
    await client?.close();
    await app?.close();
  });

  it('answers a request sent as a full record (key + value)', async () => {
    const result = await firstValueFrom(
      c().send<number>('math.sum.sync.kafka.message', {
        key: '1',
        value: { numbers: [1, 2, 3] },
      }),
    );
    expect(result).toBe(6);
    const ctx = received.contexts.at(-1)!;
    expect(ctx.getTopic()).toBe('math.sum.sync.kafka.message');
    expect(typeof ctx.getPartition()).toBe('number');
    expect(ctx.getMessage()).toMatchObject({ key: '1', value: { numbers: [1, 2, 3] } });
  });

  it('answers requests whose payload is an array, a string or a number', async () => {
    expect(await firstValueFrom(c().send<number>('math.sum.sync.array', [4, 5]))).toBe(9);
    expect(await firstValueFrom(c().send<number>('math.sum.sync.string', '1,2,3'))).toBe(6);
    expect(await firstValueFrom(c().send<number>('math.sum.sync.number', 21))).toBe(42);
  });

  it('routes a topic to a regular-expression pattern', async () => {
    expect(
      await firstValueFrom(c().send<number>('math.sum.sync.regex.one', { numbers: [1, 1] })),
    ).toBe(20);
  });

  it('streams every emission of an observable handler, in order', async () => {
    const values = await lastValueFrom(
      c().send<number>('math.stream', { count: 3 }).pipe(toArray()),
    );
    expect(values).toEqual([1, 2, 3]);
  });

  it('propagates handler errors to the caller', async () => {
    await expect(firstValueFrom(c().send('math.fail', {}))).rejects.toMatchObject({
      message: 'boom',
    });
  });

  it('rejects a request whose reply topic was not subscribed', async () => {
    await expect(firstValueFrom(c().send('math.unknown', {}))).rejects.toMatchObject({
      name: 'KafkaReplyTopicNotSubscribedError',
    });
  });

  it('delivers events to @EventPattern handlers with a KafkaContext', async () => {
    await lastValueFrom(c().emit('notify', { hello: 'world' }));
    await expect
      .poll(() => received.events, { timeout: 10_000 })
      .toContainEqual({ hello: 'world' });
    const ctx = received.contexts.at(-1)!;
    expect(ctx.getTopic()).toBe('notify');
    expect(ctx.getConsumer()).toBeDefined();
    expect(ctx.getProducer()).toBeDefined();
  });

  it('re-runs an event handler that throws KafkaRetriableException', async () => {
    await lastValueFrom(c().emit('notify.retriable', { failUntil: 2 }));
    await expect.poll(() => received.events, { timeout: 10_000 }).toContainEqual({ retriable: 3 });
  });

  it('hands the operation id to the handler and keeps it identical across retries', async () => {
    const send = () =>
      firstValueFrom(
        c().send<{ operationId: string | null }>('math.operation', {}, { operationId: 'op-42' }),
      );
    expect(await send()).toEqual({ operationId: 'op-42' });
    expect(await send()).toEqual({ operationId: 'op-42' });
    expect(await firstValueFrom(c().send('math.operation', {}))).toEqual({ operationId: null });
  });

  it('fails with KafkaReplyLostError when the reply misses the deadline', async () => {
    await expect(
      firstValueFrom(c().send('math.slow', { ms: 1_500 }, { timeout: 300 })),
    ).rejects.toMatchObject({
      name: 'KafkaReplyLostError',
      reason: 'timeout',
      pattern: 'math.slow',
    });
  });

  it('exposes the underlying clients through unwrap()', () => {
    const { consumer, producer } = c().unwrap<{ consumer: unknown; producer: unknown }>();
    expect(consumer).toBeDefined();
    expect(producer).toBeDefined();
    expect(Object.keys(c().getConsumerAssignments())).toContain('math.sum.sync.array.reply');
  });
});
