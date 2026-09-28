# Delivery guarantees, commits and retries

## Commits

By default the transport commits a record's offset **after** its handler finished (and, for requests, after the reply was produced). A crash in the middle re-delivers the record to the next consumer: **at-least-once**. Make handlers idempotent (use the message key or an id in the payload).

`consumer.autocommit: true` (or an interval in ms) switches to interval commits like kafkajs: faster, but a crash can lose in-flight records (**at-most-once** for those).

## Ordering

Records of one partition are processed in order. `consumer.concurrency` only overlaps records from different partitions.

## Retries

Throw `KafkaRetriableException` from a handler (or from an interceptor/filter) to re-run it in-process with exponential backoff: `retriableAttempts` (default 3) and `retriableDelay` (default 200 ms, doubling). For requests, no reply is sent while retrying; after the last attempt the failure is logged and the record is committed so the partition is not blocked.

Any other error is answered to the caller (requests) or logged (events), and the record is committed.

Retry topics and dead-letter topics (`<topic>.retry.N`, `<topic>.dlq` with `kafka_dlt-*` headers) arrive in v0.2.

## Request-reply

- The client stamps `kafka_correlationId`, `kafka_replyTopic` (`<pattern>.reply`) and `kafka_replyPartition` on the request.
- The client owns at least one partition of every reply topic it subscribed to (custom partition assigner), and the server produces the reply straight to that partition.
- Replies carry `kafka_correlationId`, `kafka_nest-err` when the handler failed, and `kafka_nest-is-disposed` on the last reply of an observable result.
- If the client's reply consumer is rebalanced while a request is in flight, the reply may land on a partition the client no longer owns and the request times out; retry at the caller.
