# Basic example

Two processes: a microservice answering `order.total` and consuming `order.created`, and a client.

```bash
docker compose up -d kafka        # from the repository root
pnpm exec tsx examples/basic/server.ts
pnpm exec tsx examples/basic/client.ts   # in another terminal
```
