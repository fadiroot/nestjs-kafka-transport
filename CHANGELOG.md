# nestjs-kafka-transport

## 0.2.0

### Minor Changes

- [#4](https://github.com/fadiroot/nestjs-kafka-transport/pull/4) [`456082a`](https://github.com/fadiroot/nestjs-kafka-transport/commit/456082a7737d3e79ab217ee5e202f3848bcf32d3) Thanks [@fadiroot](https://github.com/fadiroot)! - Request-reply: report a lost reply as an unknown outcome, and carry a stable operation key across retries ([#3](https://github.com/fadiroot/nestjs-kafka-transport/issues/3)).
  
  - `send()` now fails with `KafkaReplyLostError` (`reason`: `rebalance` | `timeout` | `closed` | `disconnected`) when the request was produced but its reply can no longer reach the client: the reply partition was reassigned to another instance, the deadline passed, or the client closed / lost its reply stream. Any other error still means the request never left and is safe to retry.
  - New client options `requestTimeout` (deadline for the first reply) and `generateOperationId`; new `send(pattern, data, { operationId, timeout })` options.
  - New `kafka_nest-operation-id` header (`KafkaHeaders.OPERATION_ID`) that stays the same across retries of one logical operation while each attempt keeps its own correlation id. Handlers read it with `ctx.getOperationId()` on the new `KafkaTransportContext` (a `KafkaContext` subclass; existing handlers keep compiling). The header is ignored by the built-in kafkajs transport, so mixed deployments keep working.
  - `close()` now settles pending requests instead of leaving their observables hanging.

## 0.1.1

### Patch Changes

- [`f8a286f`](https://github.com/fadiroot/nestjs-kafka-transport/commit/f8a286f54baa2029fc7248ec393fd20ec2926806) Thanks [@fadiromdhan3](https://github.com/fadiromdhan3)! - Expose `package.json` through the package exports so tooling can read the installed version.

## 0.1.0

### Minor Changes

- [`f34e291`](https://github.com/fadiroot/nestjs-kafka-transport/commit/f34e291cfb1eb10d51122e952d7c113366a9db57) Thanks [@fadiromdhan3](https://github.com/fadiromdhan3)! - Initial release: `KafkaTransportServer` and `KafkaTransportClient` for `@nestjs/microservices` on `@platformatic/kafka`, wire-compatible with the built-in kafkajs transport (request-reply headers, `<pattern>.reply` topics, parser rules). Request-reply with a reply-partition assigner, events, RegExp patterns, `KafkaRetriableException` retries with backoff, per-record commits, typed options, and a JSON content-type header so primitives round-trip.
