<p align="center">
  <h1 align="center">nestjs-kafka-transport</h1>
  <p align="center">Kafka for NestJS microservices, without kafkajs.</p>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/nestjs-kafka-transport"><img alt="npm" src="https://img.shields.io/npm/v/nestjs-kafka-transport.svg"></a>
  <a href="https://github.com/fadiroot/nestjs-kafka-transport/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/fadiroot/nestjs-kafka-transport/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://codecov.io/gh/fadiroot/nestjs-kafka-transport"><img alt="coverage" src="https://codecov.io/gh/fadiroot/nestjs-kafka-transport/branch/main/graph/badge.svg"></a>
  <img alt="node" src="https://img.shields.io/node/v/nestjs-kafka-transport.svg">
  <a href="./LICENSE"><img alt="license" src="https://img.shields.io/npm/l/nestjs-kafka-transport.svg"></a>
</p>

A production-grade Kafka transport for `@nestjs/microservices`, built on [`@platformatic/kafka`](https://github.com/platformatic/kafka) (a modern, pure-JavaScript client maintained by Node.js core contributors) instead of the unmaintained `kafkajs`.

It is a **drop-in replacement** for the built-in Kafka transport: same decorators, same `KafkaContext`, same request-reply headers and reply topics. A service on this transport can talk to a service still on the kafkajs transport, so you can migrate one service at a time.

## Why

- `kafkajs` has had no maintainer activity for years ([nestjs/nest#13223](https://github.com/nestjs/nest/issues/13223)); NestJS core keeps it for backwards compatibility.
- The alternatives either wrap a client without being a real transport (no `@MessagePattern` request-reply) or are copy-paste snippets without reconnection, typing or tests.
- Teams running Kafka in production keep re-implementing the same things: retries with backoff, dead-letter topics, batch consumption, manual commits, health checks, metrics.

## Features

|                                                                                         | built-in kafkajs transport | nestjs-kafka-transport |
| --------------------------------------------------------------------------------------- | -------------------------- | ---------------------- |
| `@MessagePattern` request-reply, `@EventPattern` events                                 | ✅                         | ✅ wire-compatible     |
| `KafkaContext` (`getMessage`, `getTopic`, `getPartition`, `getConsumer`, `getProducer`) | ✅                         | ✅                     |
| `KafkaRetriableException` redelivery                                                    | ✅                         | ✅                     |
| Custom serializers / deserializers, Schema Registry passthrough                         | ✅                         | ✅                     |
| Maintained client, Node 22/24, Kafka 3.x and 4.x (KRaft)                                | ❌                         | ✅                     |
| Typed options, no `any` in the public API                                               | partial                    | ✅                     |
| Retry policy with backoff, dead-letter topics (`kafka_dlt-*` headers)                   | ❌                         | v0.2                   |
| Manual commits and at-least-once semantics                                              | partial                    | v0.2                   |
| Terminus health indicator (broker reachability, consumer lag)                           | ❌                         | v0.2                   |
| Batch consumption with per-partition ordering                                           | ❌                         | v0.3                   |
| Prometheus metrics, OpenTelemetry spans                                                 | ❌                         | v0.3                   |

## Install

```bash
npm install nestjs-kafka-transport @platformatic/kafka
# peer dependencies: @nestjs/common @nestjs/core @nestjs/microservices rxjs
```

Requires Node.js ≥ 22.22 (or ≥ 24.6), the minimum of `@platformatic/kafka`.

## Quick start

Server (a microservice or a hybrid HTTP app):

```ts
import { NestFactory } from '@nestjs/core';
import { KafkaTransportServer } from 'nestjs-kafka-transport';

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new KafkaTransportServer({
    client: { clientId: 'orders', bootstrapBrokers: ['localhost:9092'] },
    consumer: { groupId: 'orders-consumer' },
  }),
});
await app.listen();
```

Handlers are the ones you already have:

```ts
@Controller()
export class OrdersController {
  @MessagePattern('order.total') // request-reply
  total(@Payload() order: Order, @Ctx() ctx: KafkaContext) {
    return order.items.reduce((sum, i) => sum + i.price, 0);
  }

  @EventPattern('order.created') // fire-and-forget
  created(@Payload() order: Order) {
    /* ... */
  }
}
```

Client:

```ts
import { KafkaTransportClient } from 'nestjs-kafka-transport';

const client = new KafkaTransportClient({
  client: { clientId: 'gateway', bootstrapBrokers: ['localhost:9092'] },
  consumer: { groupId: 'gateway-consumer' },
});
client.subscribeToResponseOf('order.total');
await client.connect();

const total = await firstValueFrom(client.send<number>('order.total', order));
client.emit('order.created', order);
```

## Migrating from the built-in transport

Replace `transport: Transport.KAFKA, options: {...}` with `strategy: new KafkaTransportServer({...})` and `ClientKafka` with `KafkaTransportClient`. Options map one-to-one; the [migration guide](./docs/migration.md) lists every kafkajs option and its equivalent. Because reply topics (`<pattern>.reply`) and headers (`kafka_correlationId`, `kafka_replyTopic`, `kafka_replyPartition`, `kafka_nest-err`) are unchanged, mixed deployments work during the migration.

## Documentation

- [Getting started](./docs/getting-started.md)
- [Migration from the kafkajs transport](./docs/migration.md)
- [Request-reply](./docs/request-reply.md) · [Events](./docs/events.md)
- [Configuration reference](./docs/configuration.md)
- [Delivery guarantees, commits and retries](./docs/delivery.md)
- [Observability](./docs/observability.md) · [Health checks](./docs/health.md)
- [Troubleshooting](./docs/troubleshooting.md)
- [API reference](https://fadiroot.github.io/nestjs-kafka-transport/api/)

## Status

Pre-1.0. The v0.1 line covers the built-in transport's feature set; see the [roadmap](./docs/roadmap.md). Feedback from production users is what drives priorities: open an issue or a discussion.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Every change ships with a test; e2e tests run against a real broker in Docker.

## License

[MIT](./LICENSE) © Fadi Romdhan
