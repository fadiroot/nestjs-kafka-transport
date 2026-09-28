# Getting started

## Requirements

- Node.js ≥ 22.22 or ≥ 24.6 (required by `@platformatic/kafka`)
- `@nestjs/microservices` 10 or 11
- A Kafka cluster (Apache Kafka 3.x/4.x, Redpanda, MSK, Confluent Cloud, …)

## Install

```bash
npm install nestjs-kafka-transport @platformatic/kafka
```

`@nestjs/common`, `@nestjs/core`, `@nestjs/microservices` and `rxjs` are peer dependencies you already have in a Nest project.

## A microservice

```ts
// main.ts
import { NestFactory } from '@nestjs/core';
import { KafkaTransportServer } from 'nestjs-kafka-transport';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice(AppModule, {
    strategy: new KafkaTransportServer({
      client: { clientId: 'orders', bootstrapBrokers: ['localhost:9092'] },
      consumer: { groupId: 'orders' },
    }),
  });
  await app.listen();
}
bootstrap();
```

```ts
// orders.controller.ts
import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, MessagePattern, Payload } from '@nestjs/microservices';

@Controller()
export class OrdersController {
  @MessagePattern('order.total')
  total(@Payload() order: { items: { price: number }[] }, @Ctx() ctx: KafkaContext): number {
    console.log(`${ctx.getTopic()}[${ctx.getPartition()}]`);
    return order.items.reduce((sum, item) => sum + item.price, 0);
  }

  @EventPattern('order.created')
  created(@Payload() order: { id: string }): void {
    // fire-and-forget
  }
}
```

The same handlers work unchanged with the built-in kafkajs transport; only `main.ts` changes.

## A hybrid application (HTTP + Kafka)

```ts
const app = await NestFactory.create(AppModule);
app.connectMicroservice({
  strategy: new KafkaTransportServer({
    client: { clientId: 'api', bootstrapBrokers: ['localhost:9092'] },
    consumer: { groupId: 'api' },
  }),
});
await app.startAllMicroservices();
await app.listen(3000);
```

## A client

```ts
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { firstValueFrom } from 'rxjs';
import { KafkaTransportClient } from 'nestjs-kafka-transport';

@Injectable()
export class OrdersGateway implements OnModuleInit, OnModuleDestroy {
  private readonly client = new KafkaTransportClient({
    client: { clientId: 'gateway', bootstrapBrokers: ['localhost:9092'] },
    consumer: { groupId: 'gateway' },
  });

  async onModuleInit() {
    this.client.subscribeToResponseOf('order.total'); // one per request pattern
    await this.client.connect();
  }

  onModuleDestroy() {
    return this.client.close();
  }

  total(order: unknown) {
    return firstValueFrom(this.client.send<number>('order.total', order));
  }

  created(order: unknown) {
    this.client.emit('order.created', order);
  }
}
```

You can also register it with `ClientsModule.register([{ name: 'ORDERS', customClass: KafkaTransportClient, options: {...} }])` and inject it with `@Inject('ORDERS')`.

## Payloads

- Objects and arrays are JSON-encoded. Numbers, booleans and `null` are JSON-encoded too and stamped with a content-type header so they come back with their original type (the built-in transport returns them as strings).
- Strings and `Buffer`s are sent as-is.
- To control the record, pass `{ key, value, headers, partition }`: `client.emit('order.created', { key: order.id, value: order })`. Records with the same key land on the same partition and are processed in order.

## Next

- [Migrating from the built-in transport](./migration.md)
- [Configuration reference](./configuration.md)
- [Delivery guarantees, commits and retries](./delivery.md)
