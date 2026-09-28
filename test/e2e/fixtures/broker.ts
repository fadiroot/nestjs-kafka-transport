/** Broker list for e2e tests: `KAFKA_BROKERS=host:port[,host:port]`, default localhost:9092. */
export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

/** Unique suffix so parallel/repeated runs never share topics or consumer groups. */
export function uniqueId(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
