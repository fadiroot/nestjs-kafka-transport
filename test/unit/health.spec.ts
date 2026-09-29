import { ReplaySubject, Subject } from 'rxjs';
import { describe, expect, it } from 'vitest';

import {
  KafkaTransportHealthError,
  KafkaTransportHealthIndicator,
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
 * With `@nestjs/terminus` installed the indicator throws its `HealthCheckError`, otherwise
 * `KafkaTransportHealthError`; both carry `causes`. Assert on whichever applies here.
 */
async function unhealthy(error: unknown): Promise<{ causes: Record<string, unknown> }> {
  const terminus = await import('@nestjs/terminus').catch(() => null);
  if (terminus) {
    expect(error).toBeInstanceOf(terminus.HealthCheckError);
  } else {
    expect(error).toBeInstanceOf(KafkaTransportHealthError);
  }
  return error as { causes: Record<string, unknown> };
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
    const error = await unhealthy(await indicator.isHealthy('bus').catch((e: unknown) => e));
    expect(error.causes).toEqual({
      bus: { status: 'down', transport: 'disconnected', message: 'transport disconnected' },
    });
  });

  it('fails when no status arrives within the timeout', async () => {
    const indicator = new KafkaTransportHealthIndicator(target(undefined));
    const error = await unhealthy(
      await indicator.isHealthy('kafka', { timeout: 20 }).catch((e: unknown) => e),
    );
    expect(error.causes.kafka).toMatchObject({ status: 'down', transport: 'unknown' });
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
    const error = await failing.isHealthy('kafka', { probe: true }).catch((e: unknown) => e);
    expect((error as KafkaTransportHealthError).causes.kafka).toMatchObject({
      status: 'down',
      message: 'metadata refused',
    });

    const never = new Subject<{ brokers: Map<number, unknown> }>();
    const hanging = new KafkaTransportHealthIndicator(
      target('connected', () => new Promise((resolve) => never.subscribe(resolve))),
    );
    const timedOut = await hanging
      .isHealthy('kafka', { probe: true, timeout: 20 })
      .catch((e: unknown) => e);
    expect((error as KafkaTransportHealthError).causes.kafka?.status).toBe('down');
    expect((timedOut as KafkaTransportHealthError).causes.kafka?.message).toContain('timed out');
  });
});
