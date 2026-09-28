# Troubleshooting

**`No reply topic subscription for pattern "x"`** — call `client.subscribeToResponseOf('x')` before `connect()` for every pattern used with `send()`.

**A request never resolves** — the reply topic exists but the client is not consuming it (not connected yet, or `producerOnlyMode`), or the server is not subscribed to the request topic (check `groupId` and that the service started after the topic existed for RegExp patterns).

**`Unknown topic` at startup** — `client.autocreateTopics` is `false` and the topic does not exist. Create it, or leave auto-creation on in development.

**Handlers run twice** — at-least-once delivery after a crash or a rebalance. Make handlers idempotent.

**Consumer keeps rebalancing** — a handler blocks the event loop longer than `sessionTimeout` (heartbeats cannot be sent). Move CPU-heavy work off the loop, or raise `sessionTimeout` / lower `concurrency`.

**Numbers arrive as strings** — the producer is the built-in kafkajs transport (it sends `6` as `"6"`). Between two services on this transport primitives keep their type.

**Where are the kafkajs `consumer.on(...)` events?** — `server.on(event, cb)` and `client.on(event, cb)` forward `@platformatic/kafka` events: `consumer:group:join`, `consumer:group:rebalance`, `consumer:heartbeat:stalled`, `client:broker:failed`, … Status changes are also on `server.status` / `client.status` observables.
