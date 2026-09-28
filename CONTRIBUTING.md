# Contributing

Thanks for helping. Bug reports with a reproduction and small, focused pull requests are the fastest way to get changes in.

## Development setup

Requirements: Node.js ≥ 22.22 (or ≥ 24.6), pnpm 10, Docker (for the Kafka broker used by the e2e tests).

```bash
pnpm install
docker compose up -d kafka          # single-node Kafka (KRaft) on localhost:9092
pnpm test                           # unit tests, no broker needed
pnpm test:e2e                       # e2e tests against the broker
pnpm typecheck && pnpm lint && pnpm format
```

`KAFKA_BROKERS` overrides the broker list for e2e tests (default `localhost:9092`).

## Guidelines

- Every fix ships with a test that fails before and passes after the change.
- No `any` in `src/`. Public symbols carry TSDoc with an example.
- Keep wire compatibility with the built-in `@nestjs/microservices` Kafka transport (headers, reply topics, parser behaviour) unless a change is explicitly documented as breaking.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): `fix(server): ...`, `feat(client): ...`, `docs: ...`.
- Add a changeset (`pnpm changeset`) for anything users can notice.

## Release

Releases are automated: merging to `main` with pending changesets opens a version PR; merging that PR publishes to npm with provenance.
