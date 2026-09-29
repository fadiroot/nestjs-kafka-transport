---
'nestjs-kafka-transport': minor
---

Request-reply: report a lost reply as an unknown outcome, and carry a stable operation key across retries (#3).

- `send()` now fails with `KafkaReplyLostError` (`reason`: `rebalance` | `timeout` | `closed` | `disconnected`) when the request was produced but its reply can no longer reach the client: the reply partition was reassigned to another instance, the deadline passed, or the client closed / lost its reply stream. Any other error still means the request never left and is safe to retry.
- New client options `requestTimeout` (deadline for the first reply) and `generateOperationId`; new `send(pattern, data, { operationId, timeout })` options.
- New `kafka_nest-operation-id` header (`KafkaHeaders.OPERATION_ID`) that stays the same across retries of one logical operation while each attempt keeps its own correlation id. Handlers read it with `ctx.getOperationId()` on the new `KafkaTransportContext` (a `KafkaContext` subclass; existing handlers keep compiling). The header is ignored by the built-in kafkajs transport, so mixed deployments keep working.
- `close()` now settles pending requests instead of leaving their observables hanging.
