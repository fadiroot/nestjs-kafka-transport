/** Broker list for e2e tests: `KAFKA_BROKERS=host:port[,host:port]`, default localhost:9092. */
export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

/** Unique suffix so parallel/repeated runs never share topics or consumer groups. */
export function uniqueId(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Creates topics (3 partitions) so clients that cannot auto-create them can subscribe at once. */
export async function ensureTopics(topics: string[], clientId = uniqueId('admin')): Promise<void> {
  const { Admin } = await import('@platformatic/kafka');
  const admin = new Admin({ clientId, bootstrapBrokers: BROKERS });
  try {
    const existing = new Set(await admin.listTopics({ includeInternals: false }));
    const missing = topics.filter((topic) => !existing.has(topic));
    if (missing.length > 0) {
      await admin.createTopics({
        topics: missing.map((topic) => ({ topic, partitions: 3, replicas: 1 })),
      });
    }
  } finally {
    await admin.close();
  }
}
