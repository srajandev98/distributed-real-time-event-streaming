# real-time-event-streaming monorepo

This monorepo contains:

- `apps/core`: Go-based RTES broker (core app)
- `packages/sdk/typescript`: TypeScript SDK for Node.js clients

## Layout

```text
real-time-event-streaming/
├── apps/
│   └── core/
└── packages/
    └── sdk/
        ├── typescript/
        └── python/
```

## Core App

```bash
cd apps/core
go run ./cmd/broker
```

Core app docs:

- `apps/core/README.md`

## TypeScript SDK

```bash
cd packages/sdk/typescript
pnpm install
pnpm run build
```

SDK docs:

- `packages/sdk/typescript/README.md`
