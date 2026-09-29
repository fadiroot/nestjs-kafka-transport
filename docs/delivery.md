# Delivery guarantees, commits and retries

## Commits

By default the transport commits a record's offset **after** its handler finished (and, for requests, after the reply was produced). A crash in the middle re-delivers the record to the next consumer: **at-least-once**. Make handlers idempotent (use the message key or an id in the payload).

`consumer.autocommit: true` (or an interval in ms) switches to interval commits like kafkajs: faster, but a crash can lose in-flight records (**at-most-once** for those).

### Manual commits

`commitMode: 'manual'` hands the decision to the handler: nothing is committed unless it calls `ctx.commit()` on the `KafkaTransportContext` it received through `@Ctx()`. Use it when the offset must only move after a side effect succeeded, or when you want to batch commits yourself.

```ts
new KafkaTransportServer({ client, consumer, commitMode: 'manual' });

@EventPattern('order.created')
async created(@Payload() order: Order, @Ctx() ctx: KafkaTransportContext) {
  await this.orders.save(order);
  await ctx.commit(); // only after the write succeeded
}
```

Records without a handler are still committed so the partition does not stall. A record whose handler returns (or fails) without committing stays uncommitted: after a restart or rebalance it is delivered again.

## Ordering

Records of one partition are processed in order. `consumer.concurrency` only overlaps records from different partitions.

## Retries

Throw `KafkaRetriableException` from a handler (or from an interceptor/filter) to re-run it in-process with exponential backoff: `retriableAttempts` (default 3) and `retriableDelay` (default 200 ms, doubling). For requests, no reply is sent while retrying; after the last attempt the failure is logged and the record is committed so the partition is not blocked.

Any other error is answered to the caller (requests) or logged (events), and the record is committed.

## Dead-letter topics

With `deadLetter` configured, a record that cannot be processed is copied to its dead-letter topic before its offset is committed, so nothing is lost and the partition keeps moving:

```ts
new KafkaTransportServer({
  client,
  consumer,
  deadLetter: { topic: '.dlq', includeStackTrace: false }, // both are the defaults
});
```

| Situation                                                        | Dead-lettered? | Caller / log                                |
| ---------------------------------------------------------------- | -------------- | ------------------------------------------- |
| event handler throws `KafkaRetriableException` until exhausted   | yes            | logged                                      |
| event handler throws any other error                             | yes            | logged                                      |
| request handler throws `KafkaRetriableException` until exhausted | yes            | no reply; caller gets `KafkaReplyLostError` |
| request handler throws any other error                           | no             | answered with `kafka_nest-err`              |

The dead letter keeps the original key, value and headers and adds the same headers Spring Kafka and `@nestjs/microservices` define (`KafkaHeaders.DLT_*`): `kafka_dlt-original-topic`, `-partition`, `-offset`, `-timestamp`, `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace` (when `includeStackTrace` is on) and `kafka_deliveryAttempt`. `topic` accepts a suffix (default `.dlq`) or a function `(sourceTopic) => string`.

What lands in the exception headers depends on what reaches the transport. Nest's RPC exception filter runs first: a `KafkaRetriableException` arrives as is (class name and message), an `RpcException` as the `{ status, message }` of its `getError()` (reported as `RpcError` with that message), and any other exception as `{ status: 'error', message: 'Internal server error' }` while the original is logged by `RpcExceptionsHandler`. Throw `RpcException` (or a subclass) with a descriptive error when the dead letter must explain itself. A failure to produce the dead letter is logged and the source record is still committed.

Retry topics (`<topic>.retry.N` with delays) are planned; today retries are in-process with backoff.

## Health checks

`KafkaTransportHealthIndicator` reports a server or client for `@nestjs/terminus` (an optional peer dependency): `up` while the transport's last status is `connected` or `rebalancing`, `down` when it is `disconnected` or no status arrived within `timeout` (default 2 s). `probe: true` also fetches cluster metadata through the producer and reports the broker count.

```ts
@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly kafka: KafkaTransportHealthIndicator, // new KafkaTransportHealthIndicator(client)
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([() => this.kafka.isHealthy('kafka', { probe: true })]);
  }
}
```

An unhealthy check is reported the way the installed Terminus expects, so `HealthCheckService.check()` fails either way: Terminus 11+ gets the `down` result back, Terminus 10 gets its `HealthCheckError` thrown, and without Terminus a `KafkaTransportHealthError` carrying the same `causes` is thrown.

## Request-reply

- The client stamps `kafka_correlationId`, `kafka_replyTopic` (`<pattern>.reply`) and `kafka_replyPartition` on the request.
- The client owns at least one partition of every reply topic it subscribed to (custom partition assigner), and the server produces the reply straight to that partition.
- Replies carry `kafka_correlationId`, `kafka_nest-err` when the handler failed, and `kafka_nest-is-disposed` on the last reply of an observable result.

### Lost replies: telling "not executed" from "unknown"

A request can be produced and handled while its reply never reaches the client: the client's reply consumer was rebalanced and the reply partition now belongs to another instance, the client closed, its reply stream failed, or the deadline passed. The server still ran the handler and committed the record. A plain timeout would make that case indistinguishable from "the handler never ran", and a naive retry could execute a non-idempotent write twice.

`send()` therefore reports every such case as a **`KafkaReplyLostError`** (`reason`: `'rebalance'`, `'timeout'`, `'closed'` or `'disconnected'`, plus `pattern`, `correlationId` and `operationId`). The rule for callers:

| Error from `send()`                                   | Meaning                               | Safe to retry as is? |
| ----------------------------------------------------- | ------------------------------------- | -------------------- |
| `KafkaReplyLostError`                                 | request produced, **outcome unknown** | only if idempotent   |
| anything else (producer error, unsubscribed pattern…) | request never left                    | yes                  |
| `kafka_nest-err` reply (handler threw)                | handler ran and failed                | depends on the error |

Set a deadline with `requestTimeout` on the client or `timeout` on one request; without one, `send()` waits forever (as the built-in transport does) and only rebalances, `close()` and stream failures produce the error. A rebalance that leaves this client owning the stamped partition does not fail anything; a reply that arrives after a request was declared lost is ignored.

### Operation ids: idempotent retries

To retry safely after a `KafkaReplyLostError`, give the logical operation a stable key. It travels in the `kafka_nest-operation-id` header and stays the same across retries, while each attempt still gets its own `kafka_correlationId`:

```ts
const client = new KafkaTransportClient({
  client: { clientId: 'gateway', bootstrapBrokers },
  consumer: { groupId: 'gateway' },
  requestTimeout: 10_000,
  // optional default for requests that pass none:
  generateOperationId: () => randomUUID(),
});

async function pay(order: Order): Promise<Receipt> {
  const operationId = `pay:${order.id}`;
  for (let attempt = 1; ; attempt++) {
    try {
      return await firstValueFrom(client.send<Receipt>('order.pay', order, { operationId }));
    } catch (error) {
      if (!(error instanceof KafkaReplyLostError) || attempt === 3) {
        throw error;
      }
    }
  }
}
```

Handlers read it through `KafkaTransportContext` (a `KafkaContext` with extras) and deduplicate:

```ts
@MessagePattern('order.pay')
async pay(@Payload() order: Order, @Ctx() ctx: KafkaTransportContext): Promise<Receipt> {
  const operationId = ctx.getOperationId(); // undefined for callers that sent none
  if (operationId) {
    const done = await this.receipts.findByOperation(operationId);
    if (done) return done; // second attempt of the same payment: answer, do not charge again
  }
  const receipt = await this.payments.charge(order);
  if (operationId) await this.receipts.save(operationId, receipt);
  return receipt;
}
```

The header is specific to this transport; services on the built-in kafkajs transport ignore it, so mixed deployments keep working. A server-side dedupe hook (`onDuplicate(operationId)` backed by a user-provided store) is planned for v0.2.
