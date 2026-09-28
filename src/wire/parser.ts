import { isBufferLike } from './buffers.js';

/** Options of {@link KafkaParser}. Mirrors `KafkaParserConfig` of `@nestjs/microservices`. */
export interface KafkaParserConfig {
  /**
   * Keep the record value as a `Buffer` instead of decoding it. Keys and headers are still
   * decoded. Useful with Schema Registry payloads or custom deserializers.
   * @defaultValue false
   */
  keepBinary?: boolean;
}

/** A record as delivered by the broker client, before parsing. */
export interface RawKafkaRecord {
  key?: Buffer | string | null | undefined;
  value: Buffer | string | null | undefined;
  headers?: Map<string, Buffer | string> | Record<string, Buffer | string | undefined> | undefined;
  topic: string;
  partition: number;
  offset: bigint | number | string;
  timestamp?: bigint | number | string | undefined;
}

/** A record after parsing: JSON decoded where it looked like JSON, headers as plain strings. */
export interface ParsedKafkaRecord<TValue = unknown, TKey = unknown> {
  key: TKey | null;
  value: TValue | null;
  headers: Record<string, string | Buffer>;
  topic: string;
  partition: number;
  offset: string;
  timestamp: string | undefined;
}

/**
 * Decodes raw Kafka records the way the built-in `@nestjs/microservices` transport does, so
 * handlers written against `KafkaContext` keep receiving the same shapes:
 *
 * - values and keys that start with `{` or `[` are JSON-parsed; anything else is a string;
 * - a value whose first byte is `0` is a Confluent Schema Registry payload and is passed through
 *   untouched as a `Buffer`;
 * - headers are decoded to strings (binary-safe: a non-UTF-8 header stays a `Buffer`).
 *
 * @example
 * const parser = new KafkaParser();
 * parser.parse({ topic: 't', partition: 0, offset: 1n, value: Buffer.from('{"a":1}') }).value
 * // { a: 1 }
 */
export class KafkaParser {
  private readonly keepBinary: boolean;

  constructor(config: KafkaParserConfig = {}) {
    this.keepBinary = config.keepBinary ?? false;
  }

  /** Parses one record. The input is not mutated (the client may reuse it for retries). */
  public parse<TValue = unknown, TKey = unknown>(
    record: RawKafkaRecord,
  ): ParsedKafkaRecord<TValue, TKey> {
    const headers: Record<string, string | Buffer> = {};
    if (record.headers instanceof Map) {
      for (const [name, raw] of record.headers) {
        headers[name] = this.decodeHeader(raw);
      }
    } else if (record.headers) {
      for (const [name, raw] of Object.entries(record.headers)) {
        if (raw !== undefined) {
          headers[name] = this.decodeHeader(raw);
        }
      }
    }

    return {
      key:
        record.key === undefined || record.key === null ? null : (this.decode(record.key) as TKey),
      value: this.keepBinary
        ? ((record.value ?? null) as TValue | null)
        : (this.decode(record.value) as TValue | null),
      headers,
      topic: record.topic,
      partition: record.partition,
      offset: String(record.offset),
      timestamp: record.timestamp === undefined ? undefined : String(record.timestamp),
    };
  }

  /**
   * Decodes a single value with the JSON / Schema Registry rules described on the class.
   * Exposed so custom deserializers can reuse it.
   */
  public decode(value: Buffer | string | null | undefined): unknown {
    if (value === undefined || value === null) {
      return null;
    }
    if (isBufferLike(value)) {
      // A leading zero byte is the Confluent wire-format magic byte: the payload is a
      // Schema Registry record and must not be interpreted as text.
      if (value.length > 0 && value.readUInt8(0) === 0) {
        return value;
      }
    }
    const text = isBufferLike(value) ? value.toString('utf8') : value;
    const first = text.charAt(0);
    if (first === '{' || first === '[') {
      try {
        return JSON.parse(text) as unknown;
      } catch {
        // Not JSON after all: fall through and hand the raw string to the handler.
      }
    }
    return text;
  }

  private decodeHeader(raw: Buffer | string): string | Buffer {
    if (!isBufferLike(raw)) {
      return raw;
    }
    const text = raw.toString('utf8');
    // Round-trip check: if the bytes were not valid UTF-8, keep the buffer.
    return Buffer.from(text, 'utf8').equals(raw) ? text : raw;
  }
}
