# nestjs-kafka-transport

## 0.1.1

### Patch Changes

- [`f8a286f`](https://github.com/fadiroot/nestjs-kafka-transport/commit/f8a286f54baa2029fc7248ec393fd20ec2926806) Thanks [@fadiromdhan3](https://github.com/fadiromdhan3)! - Expose `package.json` through the package exports so tooling can read the installed version.

## 0.1.0

### Minor Changes

- [`f34e291`](https://github.com/fadiroot/nestjs-kafka-transport/commit/f34e291cfb1eb10d51122e952d7c113366a9db57) Thanks [@fadiromdhan3](https://github.com/fadiromdhan3)! - Initial release: `KafkaTransportServer` and `KafkaTransportClient` for `@nestjs/microservices` on `@platformatic/kafka`, wire-compatible with the built-in kafkajs transport (request-reply headers, `<pattern>.reply` topics, parser rules). Request-reply with a reply-partition assigner, events, RegExp patterns, `KafkaRetriableException` retries with backoff, per-record commits, typed options, and a JSON content-type header so primitives round-trip.
