# Examples

## basic-usage.js

End-to-end example showing:

- connect
- produce
- consume
- join
- commit
- offset
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
node examples/basic-usage.js
```
