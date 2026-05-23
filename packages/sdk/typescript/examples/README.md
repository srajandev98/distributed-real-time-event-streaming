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

Run steps:

1. Start RTES broker from `core/`:

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
