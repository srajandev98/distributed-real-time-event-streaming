# Examples

## basic-usage.ts

End-to-end example showing:

- connect
- produce
- consume
- join
- heartbeat
- sync
- commit
- offset
- set partition role
- leave
- close

## admin-usage.ts

Operator/control-plane example showing:

- create topic
- register broker
- broker heartbeat
- set partition leader
- get metadata snapshot

Run steps:

1. Start FLUX broker from `core/`:

```bash
cd ../../../core
go run ./cmd/broker
```

2. Build SDK and run app example:

```bash
cd ../packages/sdk/typescript
pnpm install
pnpm run build
npx tsc --module commonjs --target es2020 --outDir examples/dist examples/basic-usage.ts
node examples/dist/basic-usage.js
```

3. Run admin example:

```bash
npx tsc --module commonjs --target es2020 --outDir examples/dist examples/admin-usage.ts
node examples/dist/admin-usage.js
```

Optional env vars for admin example:

- `FLUX_ADMIN_BROKER_ID` (default: `1`)
