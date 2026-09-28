import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'node22',
  tsconfig: 'tsconfig.build.json',
  external: [
    '@nestjs/common',
    '@nestjs/core',
    '@nestjs/microservices',
    '@platformatic/kafka',
    'rxjs',
    'reflect-metadata',
  ],
});
