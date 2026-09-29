import { ReplaySubject, Subject } from 'rxjs';
import { describe, expect, it } from 'vitest';

import {
  KafkaTransportHealthError,
  KafkaTransportHealthIndicator,
  type KafkaHealthResult,
  type KafkaHealthTarget,
} from '../../src/health/kafka-health.indicator.js';
import type { KafkaTransportStatus } from '../../src/interfaces/options.js';

function target(
  status: KafkaTransportStatus | undefined,
  metadata?: () => Promise<{ brokers: Map<number, unknown> }>,
): KafkaHealthTarget {
  const status$ = new ReplaySubject<KafkaTransportStatus>(1);
  if (status) {
    status$.next(status);
  }
  return {
    status: status$,
    unwrap: () => ({
      producer: { metadata: metadata ?? (() => Promise.reject(new Error('no probe'))) },
    }),
  };
}

/**
 * How a `down` result surfaces depends on the installed `@nestjs/terminus`: 11+ gets the result
 * back, 10 gets its `HealthCheckError`, none gets `KafkaTransportHealthError`. Returns the causes
 * whichever applies here.
 */
async function unhealthy(run: () => Promise<KafkaHealthResult>): Promise<KafkaHealthResult> {
  const terminus = (await import('@nestjs/terminus').catch(() => null)) as {
    HealthIndicatorService?: unknown;
  } | null;
  if (terminus?.HealthIndicatorService) {
    const result = await run();
    expect(Object.values(result).every((d) => d.status === 'down')).toBe(true);
    return result;
  }
  const error = await run().catch((e: unknown) => e);
  expect(error).toBeInstanceOf(terminus ? Error : KafkaTransportHealthError);
  return (error as { causes: KafkaHealthResult }).causes;
}

describe('KafkaTransportHealthIndicator', () => {
  it('reports up when the transport is connected', async () => {
    const indicator = new KafkaTransportHealthIndicator(target('connected'));
    await expect(indicator.isHealthy('kafka')).resolves.toEqual({
      kafka: { status: 'up', transport: 'connected' },
    });
  });

  it('treats a rebalance as up', async () => {
    const indicator = new KafkaTransportHealthIndicator(target('rebalancing'));
    await expect(indicator.isHealthy()).resolves.toEqual({
      kafka: { status: 'up', transport: 'rebalancing' },
    });
  });

  it('fails with the causes when the transport is disconnected', async () => {
    const indicator = new KafkaTransportHealthIndicator(target('disconnected'));
    const causes = await unhealthy(() => indicator.isHealthy('bus'));
    expect(causes).toEqual({
      bus: { status: 'down', transport: 'disconnected', message: 'transport disconnected' },
    });
  });

  it('fails when no status arrives within the timeout', async () => {
    const indicator = new KafkaTransportHealthIndicator(target(undefined));
    const causes = await unhealthy(() => indicator.isHealthy('kafka', { timeout: 20 }));
    expect(causes.kafka).toMatchObject({ status: 'down', transport: 'unknown' });
  });

  it('probes the broker on request and reports the broker count', async () => {
    const indicator = new KafkaTransportHealthIndicator(
      target('connected', () =>
        Promise.resolve({
          brokers: new Map([
            [1, {}],
            [2, {}],
          ]),
        }),
      ),
    );
    await expect(indicator.isHealthy('kafka', { probe: true })).resolves.toEqual({
      kafka: { status: 'up', transport: 'connected', brokers: 2 },
    });
  });

  it('fails when the probe errors or hangs past the timeout', async () => {
    const failing = new KafkaTransportHealthIndicator(
      target('connected', () => Promise.reject(new Error('metadata refused'))),
    );
    const refused = await unhealthy(() => failing.isHealthy('kafka', { probe: true }));
    expect(refused.kafka).toMatchObject({ status: 'down', message: 'metadata refused' });

    const never = new Subject<{ brokers: Map<number, unknown> }>();
    const hanging = new KafkaTransportHealthIndicator(
      target('connected', () => new Promise((resolve) => never.subscribe(resolve))),
    );
    const timedOut = await unhealthy(() =>
      hanging.isHealthy('kafka', { probe: true, timeout: 20 }),
    );
    expect(timedOut.kafka?.message).toContain('timed out');
  });
});
