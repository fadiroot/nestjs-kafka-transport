import { firstValueFrom, timeout, type Observable } from 'rxjs';

import { KafkaTransportError } from '../errors.js';
import type { KafkaTransportStatus } from '../interfaces/options.js';

/** The part of a `KafkaTransportServer` / `KafkaTransportClient` the indicator reads. */
export interface KafkaHealthTarget {
  status: Observable<KafkaTransportStatus>;
  unwrap(): unknown;
}

/** Options of {@link KafkaTransportHealthIndicator.isHealthy}. */
export interface KafkaHealthOptions {
  /**
   * Also ask the broker for cluster metadata, proving the connection is alive right now and not
   * only the last known status.
   * @defaultValue false
   */
  probe?: boolean;
  /**
   * Milliseconds to wait for the status and for the probe.
   * @defaultValue 2000
   */
  timeout?: number;
}

/** Details reported under the indicator key. */
export interface KafkaHealthDetails {
  status: 'up' | 'down';
  /** Last status emitted by the transport, or `'unknown'` when none arrived in time. */
  transport: KafkaTransportStatus | 'unknown';
  /** Number of brokers in the cluster metadata, when `probe` was requested. */
  brokers?: number;
  message?: string;
}

/** Shape of `@nestjs/terminus`' `HealthIndicatorResult`, without depending on the package. */
export type KafkaHealthResult = Record<string, KafkaHealthDetails>;

/** Thrown by {@link KafkaTransportHealthIndicator.isHealthy} when `@nestjs/terminus` is absent. */
export class KafkaTransportHealthError extends KafkaTransportError {
  public override readonly name = 'KafkaTransportHealthError';

  constructor(
    message: string,
    public readonly causes: KafkaHealthResult,
  ) {
    super(message);
  }
}

interface MetadataProbe {
  metadata(options: { topics: string[]; autocreateTopics: boolean }): Promise<{
    brokers: Map<number, unknown>;
  }>;
}

const DEFAULT_TIMEOUT = 2_000;

/**
 * Health indicator for `@nestjs/terminus`, reporting whether a transport instance is connected.
 *
 * `@nestjs/terminus` is an optional peer dependency: when it is installed, an unhealthy check
 * throws its `HealthCheckError` so `HealthCheckService.check()` aggregates it; otherwise a
 * `KafkaTransportHealthError` with the same `causes` is thrown.
 *
 * @example
 * @Controller('health')
 * export class HealthController {
 *   constructor(
 *     private readonly health: HealthCheckService,
 *     private readonly kafka: KafkaTransportHealthIndicator,
 *   ) {}
 *
 *   @Get()
 *   @HealthCheck()
 *   check() {
 *     return this.health.check([() => this.kafka.isHealthy('kafka', { probe: true })]);
 *   }
 * }
 *
 * // providers: [{ provide: KafkaTransportHealthIndicator, useFactory: () => new KafkaTransportHealthIndicator(client) }]
 */
export class KafkaTransportHealthIndicator {
  constructor(private readonly target: KafkaHealthTarget) {}

  public async isHealthy(
    key = 'kafka',
    options: KafkaHealthOptions = {},
  ): Promise<KafkaHealthResult> {
    const wait = options.timeout ?? DEFAULT_TIMEOUT;
    const transport = await this.currentStatus(wait);
    const details: KafkaHealthDetails = { status: 'up', transport };
    if (transport === 'disconnected' || transport === 'unknown') {
      details.status = 'down';
      details.message = transport === 'unknown' ? 'no status reported' : 'transport disconnected';
      throw await this.failure(key, details);
    }
    if (options.probe) {
      try {
        details.brokers = await this.probe(wait);
      } catch (error) {
        details.status = 'down';
        details.message = error instanceof Error ? error.message : String(error);
        throw await this.failure(key, details);
      }
    }
    return { [key]: details };
  }

  protected async currentStatus(wait: number): Promise<KafkaTransportStatus | 'unknown'> {
    try {
      return await firstValueFrom(this.target.status.pipe(timeout(wait)));
    } catch {
      return 'unknown';
    }
  }

  protected async probe(wait: number): Promise<number> {
    const { producer } = this.target.unwrap() as { producer: MetadataProbe };
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new KafkaTransportError(`metadata probe timed out after ${String(wait)} ms`));
      }, wait);
    });
    try {
      const metadata = await Promise.race([
        producer.metadata({ topics: [], autocreateTopics: false }),
        deadline,
      ]);
      return metadata.brokers.size;
    } finally {
      clearTimeout(timer);
    }
  }

  protected async failure(key: string, details: KafkaHealthDetails): Promise<Error> {
    const causes: KafkaHealthResult = { [key]: details };
    const message = `${key} check failed: ${details.message ?? details.transport}`;
    try {
      // Resolved at runtime only: `@nestjs/terminus` is an optional peer dependency.
      const specifier = '@nestjs/terminus';
      const terminus = (await import(specifier)) as {
        HealthCheckError: new (message: string, causes: unknown) => Error;
      };
      return new terminus.HealthCheckError(message, causes);
    } catch {
      return new KafkaTransportHealthError(message, causes);
    }
  }
}
