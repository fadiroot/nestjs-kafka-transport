# Migrating from the built-in kafkajs transport

The wire format is unchanged, so you can migrate one service at a time: a service on this transport talks to services still on `Transport.KAFKA`, in both directions. The e2e suite (`test/e2e/interop-kafkajs.e2e.spec.ts`) exercises exactly that.

## Server

Before:

```ts
const app = await NestFactory.createMicroservice(AppModule, {
  transport: Transport.KAFKA,
  options: {
    client: { clientId: 'orders', brokers: ['localhost:9092'] },
    consumer: { groupId: 'orders' },
  },
});
```

After:

```ts
const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new KafkaTransportServer({
    client: { clientId: 'orders', bootstrapBrokers: ['localhost:9092'] },
    consumer: { groupId: 'orders' },
  }),
});
```

## Client

`ClientKafka` → `KafkaTransportClient`; `subscribeToResponseOf`, `send`, `emit`, `connect`, `close`, `unwrap` keep their names.

## Option mapping

| kafkajs transport option                                             | nestjs-kafka-transport                                           | Notes                                                  |
| -------------------------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------ |
| `client.brokers`                                                     | `client.bootstrapBrokers`                                        | strings `host:port`                                    |
| `client.clientId`                                                    | `client.clientId`                                                |                                                        |
| `client.ssl`                                                         | `client.tls`                                                     | Node `tls.ConnectionOptions`                           |
| `client.sasl`                                                        | `client.sasl`                                                    | PLAIN, SCRAM-SHA-256/512, OAUTHBEARER                  |
| `client.connectionTimeout`                                           | `client.timeout`                                                 |                                                        |
| `client.retry`                                                       | `client.retries`, `client.retryDelay`                            |                                                        |
| `consumer.groupId`                                                   | `consumer.groupId`                                               |                                                        |
| `consumer.sessionTimeout` / `heartbeatInterval` / `rebalanceTimeout` | same names                                                       | milliseconds                                           |
| `consumer.maxBytes` / `minBytes` / `maxWaitTimeInMs`                 | `consumer.maxBytes` / `minBytes` / `maxWaitTime`                 |                                                        |
| `consumer.allowAutoTopicCreation`                                    | `client.autocreateTopics`                                        | default `true`                                         |
| `subscribe.fromBeginning`                                            | `consumer.mode: 'earliest'`                                      | default: resume from the committed offset, else latest |
| `run.autoCommit`                                                     | `consumer.autocommit`                                            | default: commit after each successfully handled record |
| `run.partitionsConsumedConcurrently`                                 | `consumer.concurrency`                                           | records of one partition stay in order                 |
| `producer.allowAutoTopicCreation`                                    | `client.autocreateTopics`                                        |                                                        |
| `producer.idempotent` / `send.acks` / `send.compression`             | `producer.idempotent` / `producer.acks` / `producer.compression` |                                                        |
| `postfixId`                                                          | `postfixId`                                                      | same defaults (`-server`, `-client`)                   |
| `serializer` / `deserializer`                                        | same                                                             | same contract                                          |
| `parser.keepBinary`                                                  | `parser.keepBinary`                                              |                                                        |
| `producerOnlyMode`                                                   | `producerOnlyMode`                                               |                                                        |

Not carried over: `client.logLevel` / `logCreator` (use Nest's `Logger`), `run.eachBatch` (batches arrive in v0.3 as a first-class feature).

## Behaviour differences

- Numbers, booleans and `null` payloads keep their type between two services on this transport (a `kafka_nest-content-type` header is added; kafkajs peers ignore it and see the same strings they always did).
- Offsets are committed after the handler finishes (at-least-once). With the built-in transport, kafkajs auto-commits on an interval. Set `consumer.autocommit: true` to get interval commits back.
- `KafkaContext.getHeartbeat()` is a no-op: `@platformatic/kafka` heartbeats on its own thread of work, so long handlers no longer get the group kicked.
- `KafkaContext.getConsumer()` / `getProducer()` return `@platformatic/kafka` instances, not kafkajs ones. Code that reached into kafkajs APIs through the context needs adapting.
- RegExp patterns are resolved against the topics that exist at startup (kafkajs re-evaluated them on metadata refresh). Create topics before the service starts, or restart it.
