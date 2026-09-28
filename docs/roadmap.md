# Roadmap

- **0.1** (current): drop-in server and client, request-reply, events, RegExp patterns, retriable retries, per-record commits, kafkajs interoperability, typed options.
- **0.2**: retry topics and dead-letter topics with `kafka_dlt-*` headers, manual commits (`ctx.commit()`), reconnection events, `@nestjs/terminus` health indicator.
- **0.3**: batch handlers, per-partition concurrency controls, Prometheus metrics, OpenTelemetry spans (`messaging.*` conventions), Schema Registry helpers.
- **1.0**: frozen API, Node 22/24 and Kafka 3.x/4.x support matrix, Confluent (librdkafka) adapter behind the same interface.

Open an issue to vote for or add an item.
