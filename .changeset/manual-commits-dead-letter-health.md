---
'nestjs-kafka-transport': minor
---

Manual commits, dead-letter topics and a Terminus health indicator.

- `commitMode: 'manual'` (server): offsets move only when the handler calls `ctx.commit()` on its `KafkaTransportContext`; records without a handler are still committed so the partition does not stall.
- `deadLetter` (server): a record whose handler exhausted its `KafkaRetriableException` retries, or an event whose handler threw any other error, is copied to `<topic>.dlq` (suffix or naming function configurable) with the original key, value and headers plus `kafka_dlt-original-*`, `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, optional `kafka_dlt-exception-stacktrace` and `kafka_deliveryAttempt`, before the source offset is committed. `buildDeadLetterRecord` and `deadLetterTopicOf` are exported.
- `KafkaTransportHealthIndicator` for `@nestjs/terminus` (optional peer dependency): `isHealthy(key, { probe, timeout })` reports the transport status of a server or client and can probe the broker for cluster metadata; throws Terminus' `HealthCheckError` when the package is installed, `KafkaTransportHealthError` otherwise.
