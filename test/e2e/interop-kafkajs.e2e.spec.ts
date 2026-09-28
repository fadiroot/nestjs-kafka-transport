import { Controller, Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  ClientKafka,
  EventPattern,
  MessagePattern,
  Payload,
  Transport,
} from '@nestjs/microservices';
import { firstValueFrom, lastValueFrom } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { KafkaTransportClient, KafkaTransportServer } from '../../src/index.js';
import { BROKERS, ensureTopics, sleep, uniqueId } from './fixtures/broker.js';

/**
 * Mixed deployments: a service on the built-in kafkajs transport and a service on this transport
 * must be able to exchange requests, replies and events in both directions.
 */
const seen: unknown[] = [];

@Controller()
class EchoController {
  @MessagePattern('interop.echo')
  echo(@Payload() data: { text: string }): { echoed: string } {
    return { echoed: data.text };
  }

  @EventPattern('interop.event')
  event(@Payload() data: unknown): void {
    seen.push(data);
  }
}

/* eslint-disable @typescript-eslint/no-extraneous-class -- Nest modules are declarative */
@Module({ controllers: [EchoController] })
class EchoModule {}
/* eslint-enable @typescript-eslint/no-extraneous-class */

describe('interoperability with the built-in kafkajs transport', () => {
  const run = uniqueId('interop');
  const kafkajsLogLevel = 1; // ERROR
  let platformaticServer: INestMicroservice | undefined;
  let kafkajsServer: INestMicroservice | undefined;
  let kafkajsClient: ClientKafka | undefined;
  let platformaticClient: KafkaTransportClient | undefined;

  beforeAll(async () => {
    await ensureTopics(['interop.echo', 'interop.echo.reply', 'interop.event']);

    // Service A: this transport.
    platformaticServer = await NestFactory.createMicroservice(EchoModule, {
      strategy: new KafkaTransportServer({
        client: { clientId: `${run}-a`, bootstrapBrokers: BROKERS, autocreateTopics: true },
        consumer: { groupId: `${run}-a`, sessionTimeout: 10_000, heartbeatInterval: 1_000 },
      }),
      logger: ['error'],
    });
    await platformaticServer.listen();

    // Client on kafkajs (the built-in ClientKafka) talking to service A.
    kafkajsClient = new ClientKafka({
      client: { clientId: `${run}-kjs-client`, brokers: BROKERS, logLevel: kafkajsLogLevel },
      consumer: { groupId: `${run}-kjs-client`, sessionTimeout: 10_000, heartbeatInterval: 1_000 },
    });
    kafkajsClient.subscribeToResponseOf('interop.echo');
    await kafkajsClient.connect();
    await sleep(2_000);
  });

  afterAll(async () => {
    await kafkajsClient?.close();
    await platformaticClient?.close();
    await platformaticServer?.close();
    await kafkajsServer?.close();
  });

  it('kafkajs ClientKafka → this transport: request-reply', async () => {
    const result = await firstValueFrom(
      kafkajsClient!.send<{ echoed: string }>('interop.echo', { text: 'from kafkajs' }),
    );
    expect(result).toEqual({ echoed: 'from kafkajs' });
  });

  it('kafkajs ClientKafka → this transport: event', async () => {
    await lastValueFrom(kafkajsClient!.emit('interop.event', { via: 'kafkajs' }));
    await expect.poll(() => seen, { timeout: 10_000 }).toContainEqual({ via: 'kafkajs' });
  });

  it('this transport client → built-in kafkajs ServerKafka: request-reply and event', async () => {
    // Service B: the built-in transport, in its own consumer group and with its own patterns
    // (same controller, different group, so both services receive the request; use a fresh
    // client that only knows service B's reply partition).
    kafkajsServer = await NestFactory.createMicroservice(EchoModule, {
      transport: Transport.KAFKA,
      options: {
        client: { clientId: `${run}-b`, brokers: BROKERS, logLevel: kafkajsLogLevel },
        consumer: { groupId: `${run}-b`, sessionTimeout: 10_000, heartbeatInterval: 1_000 },
      },
      logger: ['error'],
    });
    await kafkajsServer.listen();
    // Stop service A so only the kafkajs service answers.
    await platformaticServer?.close();
    platformaticServer = undefined;

    platformaticClient = new KafkaTransportClient({
      client: { clientId: `${run}-p-client`, bootstrapBrokers: BROKERS, autocreateTopics: true },
      consumer: { groupId: `${run}-p-client`, sessionTimeout: 10_000, heartbeatInterval: 1_000 },
    });
    platformaticClient.subscribeToResponseOf('interop.echo');
    await platformaticClient.connect();
    await sleep(2_000);

    const result = await firstValueFrom(
      platformaticClient.send<{ echoed: string }>('interop.echo', { text: 'from platformatic' }),
    );
    expect(result).toEqual({ echoed: 'from platformatic' });

    await lastValueFrom(platformaticClient.emit('interop.event', { via: 'platformatic' }));
    await expect.poll(() => seen, { timeout: 10_000 }).toContainEqual({ via: 'platformatic' });
  });
});
