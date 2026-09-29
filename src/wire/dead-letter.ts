import type { KafkaTransportMessage } from '../context/kafka.context.js';
import type { KafkaDeadLetterOptions } from '../interfaces/options.js';
import { toWireValue } from './buffers.js';
import { deadLetterTopicOf, KafkaHeaders } from './headers.js';
import type { WireRecord } from './serialize.js';

/** What the dead-letter record describes about the failure. */
export interface DeadLetterFailure {
  /** The error thrown by the handler (last attempt). */
  error: unknown;
  /** Number of delivery attempts made in-process before giving up. */
  attempts: number;
}

/**
 * Builds the record produced to the dead-letter topic for a failed source record.
 *
 * The original key, value and headers are copied as they arrived, and the failure is described
 * with the same `kafka_dlt-*` headers Spring Kafka and `@nestjs/microservices` define:
 * original topic / partition / offset / timestamp, exception class name and message, the stack
 * trace when enabled, and `kafka_deliveryAttempt`.
 */
export function buildDeadLetterRecord(
  message: KafkaTransportMessage,
  failure: DeadLetterFailure,
  options: KafkaDeadLetterOptions = {},
): WireRecord {
  const headers = new Map<string, Buffer>();
  for (const [name, value] of Object.entries(message.headers)) {
    headers.set(name, toBuffer(value));
  }
  const { error } = failure;
  const { name, text } = describeError(error);
  headers.set(KafkaHeaders.DLT_ORIGINAL_TOPIC, toBuffer(message.topic));
  headers.set(KafkaHeaders.DLT_ORIGINAL_PARTITION, toBuffer(String(message.partition)));
  headers.set(KafkaHeaders.DLT_ORIGINAL_OFFSET, toBuffer(message.offset));
  headers.set(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP, toBuffer(message.timestamp ?? ''));
  headers.set(KafkaHeaders.DLT_EXCEPTION_FQCN, toBuffer(name));
  headers.set(KafkaHeaders.DLT_EXCEPTION_MESSAGE, toBuffer(text));
  if (options.includeStackTrace && error instanceof Error && error.stack) {
    headers.set(KafkaHeaders.DLT_EXCEPTION_STACKTRACE, toBuffer(error.stack));
  }
  headers.set(KafkaHeaders.DELIVERY_ATTEMPT, toBuffer(String(failure.attempts)));

  const record: WireRecord = {
    topic: deadLetterTopicOf(message.topic, options.topic),
    value: toBuffer(toWireValue(message.value)),
    headers,
  };
  if (message.key !== undefined && message.key !== null) {
    record.key = toBuffer(toWireValue(message.key));
  }
  return record;
}

/**
 * Class name and message of what the handler threw. Nest's RPC exception filter runs before the
 * transport sees the error: a `KafkaRetriableException` arrives as is, an `RpcException` as the
 * `{ status, message }` object of its `getError()`, and any other exception as
 * `{ status: 'error', message: 'Internal server error' }` (the original is logged by
 * `RpcExceptionsHandler`). Such objects are reported as `RpcError`.
 */
function describeError(error: unknown): { name: string; text: string } {
  if (error instanceof Error) {
    return { name: error.constructor.name || error.name, text: error.message };
  }
  if (typeof error === 'object' && error !== null && 'message' in error) {
    return { name: 'RpcError', text: String(error.message) };
  }
  return { name: typeof error, text: String(error) };
}

function toBuffer(value: string | Buffer): Buffer {
  return Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
}
