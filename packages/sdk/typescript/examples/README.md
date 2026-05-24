# Examples

## basic-usage.ts

End-to-end example showing:

- admin control-plane APIs:
  - create topic
  - register broker
  - broker heartbeat
  - set partition leader
  - get metadata snapshot
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

Run steps:

1. Start FLUX broker from `core/`:

```bash
cd ../../../core
go run ./cmd/broker
```

2. Build SDK and run example:

```bash
cd ../packages/sdk/typescript
pnpm install
pnpm run build
npx tsc --module commonjs --target es2020 --outDir examples/dist examples/basic-usage.ts
node examples/dist/basic-usage.js
```

Optional env vars for admin section:

- `FLUX_ADMIN_BROKER_ID` (default: `1`)
