---
'nestjs-kafka-transport': minor
---

Initial release: `KafkaTransportServer` and `KafkaTransportClient` for `@nestjs/microservices` on `@platformatic/kafka`, wire-compatible with the built-in kafkajs transport (request-reply headers, `<pattern>.reply` topics, parser rules). Request-reply with a reply-partition assigner, events, RegExp patterns, `KafkaRetriableException` retries with backoff, per-record commits, typed options, and a JSON content-type header so primitives round-trip.
