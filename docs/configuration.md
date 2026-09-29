# Configuration reference

All options are typed (`KafkaTransportOptions`) and documented inline; your editor shows the same text. Anything not listed here is passed straight to `@platformatic/kafka` and documented in its [consumer](https://github.com/platformatic/kafka/blob/main/docs/consumer.md) and [producer](https://github.com/platformatic/kafka/blob/main/docs/producer.md) docs.

## `client` (connection, shared by consumer and producer)

| Option             | Type                                        | Default  | Description                                               |
| ------------------ | ------------------------------------------- | -------- | --------------------------------------------------------- |
| `clientId`         | `string`                                    | required | Reported to brokers; `postfixId` is appended              |
| `bootstrapBrokers` | `string[]`                                  | required | `host:port` list                                          |
| `tls`              | `tls.ConnectionOptions`                     | –        | TLS                                                       |
| `sasl`             | `{ mechanism, username, password }` / OAuth | –        | SASL                                                      |
| `timeout`          | ms                                          | 5000     | request timeout                                           |
| `retries`          | number \| boolean                           | 3        | broker-call retries                                       |
| `retryDelay`       | ms                                          | 1000     | delay between retries                                     |
| `metadataMaxAge`   | ms                                          | 300000   | metadata refresh                                          |
| `autocreateTopics` | boolean                                     | true     | let the broker create missing topics on subscribe/produce |
| `metrics`          | `{ client, registry }`                      | –        | Prometheus via `prom-client`                              |

## `consumer`

| Option                                                                                           | Type                                    | Default       | Description                                                            |
| ------------------------------------------------------------------------------------------------ | --------------------------------------- | ------------- | ---------------------------------------------------------------------- |
| `groupId`                                                                                        | `string`                                | required      | consumer group; `postfixId` appended                                   |
| `mode`                                                                                           | `'committed' \| 'latest' \| 'earliest'` | `'committed'` | where to start                                                         |
| `fallbackMode`                                                                                   | `'latest' \| 'earliest' \| 'fail'`      | `'latest'`    | when there is no committed offset                                      |
| `autocommit`                                                                                     | boolean \| ms                           | `false`       | `false`: commit after each handled record; `true`/ms: interval commits |
| `concurrency`                                                                                    | number                                  | 1             | records processed in parallel across partitions                        |
| `sessionTimeout`                                                                                 | ms                                      | 60000         |                                                                        |
| `heartbeatInterval`                                                                              | ms                                      | 3000          |                                                                        |
| `rebalanceTimeout`                                                                               | ms                                      | 60000         |                                                                        |
| `maxBytes`, `minBytes`, `maxWaitTime`, `maxBytesPerPartition`, `highWaterMark`, `isolationLevel` |                                         |               | fetch tuning                                                           |
| `groupInstanceId`                                                                                | string                                  | –             | static membership                                                      |
| `partitionAssigner`                                                                              | function                                | round-robin   | custom assignment (the client sets its own for reply topics)           |
| `onDeserializationError`                                                                         | function                                | fail          | what to do with undecodable records                                    |

## `producer`

| Option            | Type                                              | Default        |
| ----------------- | ------------------------------------------------- | -------------- |
| `acks`            | -1, 0, 1                                          | -1 (all)       |
| `compression`     | `'none' \| 'gzip' \| 'snappy' \| 'lz4' \| 'zstd'` | none           |
| `idempotent`      | boolean                                           | false          |
| `partitioner`     | function                                          | murmur2 by key |
| `transactionalId` | string                                            | –              |

## Transport-level

| Option                         | Type                               | Default               | Description                                                                                            |
| ------------------------------ | ---------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------ |
| `postfixId`                    | string                             | `-server` / `-client` | suffix for `clientId` and `groupId`                                                                    |
| `serializer` / `deserializer`  | Nest `Serializer` / `Deserializer` | –                     | same contract as the built-in transport                                                                |
| `parser.keepBinary`            | boolean                            | false                 | leave record values as `Buffer`                                                                        |
| `producerOnlyMode`             | boolean                            | false                 | client without consumer (`send()` unavailable)                                                         |
| `retriableAttempts`            | number                             | 3                     | server: in-process re-runs on `KafkaRetriableException`                                                |
| `retriableDelay`               | ms                                 | 200                   | server: first retry delay, doubles each time                                                           |
| `commitMode`                   | `'auto' \| 'manual'`               | `'auto'`              | server: `'manual'` commits only when the handler calls `ctx.commit()`                                  |
| `deadLetter.topic`             | string \| `(topic) => string`      | `'.dlq'`              | server: enables dead-lettering; suffix or naming function for the dead-letter topic                    |
| `deadLetter.includeStackTrace` | boolean                            | false                 | server: add `kafka_dlt-exception-stacktrace` to dead letters                                           |
| `requestTimeout`               | ms                                 | –                     | client: deadline for the first reply of `send()`; past it the request fails with `KafkaReplyLostError` |
| `generateOperationId`          | `() => string`                     | –                     | client: operation id for requests that pass none                                                       |

## `send()` options (client)

`client.send(pattern, data, options?)` accepts, on top of the built-in signature:

| Option        | Type   | Description                                                                                                      |
| ------------- | ------ | ---------------------------------------------------------------------------------------------------------------- |
| `operationId` | string | key of the logical operation, sent as `kafka_nest-operation-id`; reuse it on retries so handlers can deduplicate |
| `timeout`     | ms     | deadline for the first reply of this request; overrides `requestTimeout`, `0` disables it                        |

See [delivery.md](./delivery.md#lost-replies-telling-not-executed-from-unknown) for when a request fails with `KafkaReplyLostError` and how to retry safely.
