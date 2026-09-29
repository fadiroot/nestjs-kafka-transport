import { Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Consumer, stringDeserializer, type Message } from '@platformatic/kafka';
import { lastValueFrom } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  KafkaHeaders,
  KafkaReplyLostError,
  KafkaTransportClient,
  KafkaTransportHealthIndicator,
  KafkaTransportServer,
  type KafkaTransportInstances,
} from '../../src/index.js';
import { BROKERS, ensureTopics, sleep, uniqueId } from './fixtures/broker.js';
import { DeliveryController, delivery } from './fixtures/delivery.controller.js';

/* eslint-disable @typescript-eslint/no-extraneous-class -- Nest modules are declarative */
@Module({ controllers: [DeliveryController] })
class AppModule {}
/* eslint-enable @typescript-eslint/no-extraneous-class */

type DeadLetter = Message<Buffer, Buffer, string>;

/** Reads every record of `topic` from the beginning until `count` arrived (or the timeout). */
async function readAll(topic: string, count: number, timeoutMs = 20_000): Promise<DeadLetter[]> {
  const consumer = new Consumer<Buffer, Buffer, string, Buffer>({
    clientId: uniqueId('reader'),
    groupId: uniqueId('reader'),
    bootstrapBrokers: BROKERS,
    autocommit: true,
    deserializers: {
      key: (b?: Buffer) => b,
      value: (b?: Buffer) => b,
      headerKey: stringDeserializer,
      headerValue: (b?: Buffer) => b,
    },
  });
  const records: DeadLetter[] = [];
  const stream = await consumer.consume({ topics: [topic], mode: 'earliest' });
  const timer = setTimeout(() => void stream.close(), timeoutMs);
  try {
    for await (const message of stream) {
      records.push(message);
      if (records.length >= count) {
        break;
      }
    }
  } finally {
    clearTimeout(timer);
    await stream.close();
    await consumer.close();
  }
  return records;
}

const header = (message: DeadLetter, name: string): string | undefined =>
  message.headers.get(name)?.toString('utf8');

describe('commit modes, dead-letter topics and health', () => {
  const run = uniqueId('delivery');
  const topics = ['delivery.manual', 'delivery.broken', 'delivery.flaky'];
  let app: INestMicroservice | undefined;
  let server: KafkaTransportServer | undefined;
  let client: KafkaTransportClient | undefined;
  const c = (): KafkaTransportClient => {
    if (!client) {
      throw new Error('client not connected');
    }
    return client;
  };

  beforeAll(async () => {
    await ensureTopics([
      ...topics,
      `delivery.broken.${run}.dlq`,
      `delivery.flaky.${run}.dlq`,
      'delivery.flaky.reply',
    ]);
    const kafkaClient = new KafkaTransportClient({
      client: { clientId: `${run}-client`, bootstrapBrokers: BROKERS, autocreateTopics: true },
      consumer: {
        groupId: `${run}-client-group`,
        sessionTimeout: 10_000,
        heartbeatInterval: 1_000,
      },
      requestTimeout: 8_000,
    });
    kafkaClient.subscribeToResponseOf('delivery.flaky');
    client = kafkaClient;
    await kafkaClient.connect();

    server = new KafkaTransportServer({
      client: { clientId: `${run}-server`, bootstrapBrokers: BROKERS, autocreateTopics: true },
      consumer: {
        groupId: `${run}-server-group`,
        sessionTimeout: 10_000,
        heartbeatInterval: 1_000,
      },
      commitMode: 'manual',
      deadLetter: { includeStackTrace: true, topic: (topic) => `${topic}.${run}.dlq` },
      retriableAttempts: 2,
      retriableDelay: 10,
    });
    app = await NestFactory.createMicroservice(AppModule, {
      strategy: server,
      logger: ['error'],
    });
    await app.listen();
    await sleep(1_500);
  });

  afterAll(async () => {
    await client?.close();
    await app?.close();
  });

  it('in manual mode, only records whose handler called ctx.commit() are committed', async () => {
    await lastValueFrom(
      c().emit('delivery.manual', { key: 'a', value: { id: 'a', commit: false } }),
    );
    await lastValueFrom(
      c().emit('delivery.manual', { key: 'a', value: { id: 'b', commit: true } }),
    );
    await expect.poll(() => delivery.committed, { timeout: 10_000 }).toEqual(['b']);
    // Both records were handled; the committed offset covers the second one only after b committed.
    expect(delivery.handled).toEqual(
      expect.arrayContaining([
        { id: 'a', commit: false },
        { id: 'b', commit: true },
      ]),
    );
    const { consumer } = server!.unwrap<KafkaTransportInstances>();
    const offsets = await consumer.listCommittedOffsets({
      topics: [{ topic: 'delivery.manual', partitions: [0, 1, 2] }],
    });
    const committed = offsets.get('delivery.manual') ?? [];
    // Same key: both records landed on one partition; its committed offset is at least 2 (past b).
    expect(committed.some((offset) => offset >= 2n)).toBe(true);
  });

  it('dead-letters an event whose handler throws a non-retriable error, with kafka_dlt-* headers', async () => {
    await lastValueFrom(
      c().emit('delivery.broken', {
        key: 'k-1',
        value: { id: 'x' },
        headers: { 'x-trace': 'trace-1' },
      }),
    );
    const [record] = await readAll(`delivery.broken.${run}.dlq`, 1);
    expect(record).toBeDefined();
    expect(record!.key.toString('utf8')).toBe('k-1');
    expect(JSON.parse(record!.value.toString('utf8'))).toEqual({ id: 'x' });
    expect(header(record!, 'x-trace')).toBe('trace-1');
    expect(header(record!, KafkaHeaders.DLT_ORIGINAL_TOPIC)).toBe('delivery.broken');
    expect(header(record!, KafkaHeaders.DLT_EXCEPTION_FQCN)).toBe('RpcError');
    expect(header(record!, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).toBe('cannot process x');
    expect(header(record!, KafkaHeaders.DELIVERY_ATTEMPT)).toBe('1');
    expect(Number(header(record!, KafkaHeaders.DLT_ORIGINAL_OFFSET))).toBeGreaterThanOrEqual(0);
  });

  it('dead-letters a request whose retriable retries are exhausted (the caller sees a lost reply)', async () => {
    const error = await lastValueFrom(c().send('delivery.flaky', { id: 'y' })).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(KafkaReplyLostError);
    const [record] = await readAll(`delivery.flaky.${run}.dlq`, 1);
    expect(record).toBeDefined();
    expect(header(record!, KafkaHeaders.DLT_EXCEPTION_FQCN)).toBe('KafkaRetriableException');
    expect(header(record!, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).toBe('still failing y');
    expect(header(record!, KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).toContain('still failing y');
    expect(header(record!, KafkaHeaders.DELIVERY_ATTEMPT)).toBe('3'); // 1 + 2 retries
    expect(header(record!, KafkaHeaders.CORRELATION_ID)).toBeDefined();
  });

  it('reports the server and the client as healthy, with a broker probe', async () => {
    const forServer = new KafkaTransportHealthIndicator(server!);
    const forClient = new KafkaTransportHealthIndicator(c());
    await expect(forServer.isHealthy('kafka-server', { probe: true })).resolves.toMatchObject({
      'kafka-server': { status: 'up', transport: 'connected', brokers: 1 },
    });
    await expect(forClient.isHealthy('kafka-client')).resolves.toMatchObject({
      'kafka-client': { status: 'up', transport: 'connected' },
    });
  });
});
