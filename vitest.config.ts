import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      {
        test: { name: 'unit', include: ['test/unit/**/*.spec.ts'], environment: 'node' },
      },
      {
        test: {
          name: 'e2e',
          include: ['test/e2e/**/*.e2e.spec.ts'],
          environment: 'node',
          testTimeout: 60_000,
          hookTimeout: 120_000,
          fileParallelism: false,
        },
      },
    ],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['src/index.ts', 'src/interfaces/**', 'src/**/*.d.ts'],
      // Measured over unit + e2e. Raised as the retry/DLQ and batch features land with their tests.
      thresholds: { lines: 80, functions: 80, branches: 70, statements: 80 },
      reporter: ['text', 'lcov'],
    },
  },
});
