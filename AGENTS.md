# AGENTS.md — Context for LLM agents working with this repository

This file is a fact sheet for AI coding assistants. It is intentionally
exhaustive and boring: no marketing, only contracts, invariants, and pitfalls.
Read this before editing any file.

## What this package is

`asynq-client` is an **insert-only producer** for the Go task queue
[asynq](https://github.com/hibiken/asynq). It lets Node.js / TypeScript / Bun
services enqueue tasks that an **existing Go `asynq.Server` fleet consumes**.

It is NOT:
- a task consumer / worker (no lease, no retry scheduling, no recovery)
- a scheduler (`asynq.Scheduler` equivalent)
- a queue inspector (use `asynqmon` for that)

## The one-sentence architecture

The Go asynq client and server never talk to each other — both only talk to
Redis. This package replicates the **client half byte-for-byte** (protobuf
schema, Lua scripts, key layout, defaults), so the Go server cannot tell a
Node-produced task apart from a Go-produced one.

```
Node.js (this lib) --enqueue--> Redis (Lua scripts) <--pull-- Go asynq.Server
```

## Repository layout

| Path | Role | Edit rules |
|---|---|---|
| `src/client.ts` | Public API: `Client.enqueue()` | Only file with business logic worth editing |
| `src/scripts.ts` | Lua scripts for Redis | **DO NOT reformat or "fix". Byte-identical copies of Go `internal/rdb/rdb.go`.** |
| `src/keys.ts` | Redis key builders | Must stay 1:1 with Go `internal/base/base.go` |
| `src/message.ts` | protobuf encode/decode | Field mapping must match `proto/asynq.proto` |
| `proto/asynq.proto` | Wire schema | **Vendored from asynq v0.26.0. Never renumber fields.** |
| `src/index.ts` | Public exports | Keep in sync when adding exports |
| `test/unit.test.ts` | Pure unit tests (no Redis needed) | |
| `test/integration.test.ts` | Tests against a real Redis | Needs `ASYNQ_TEST_REDIS_URL` or localhost:6379 |
| `integration/` | End-to-end interop proof: real Go `asynq.Server` consumes Node-produced tasks | Run `integration/run-interop-test.sh` after ANY protocol-affecting change |
| `examples/basic.mts` | Runnable usage example | Keep compiling |

## Public API contract

```typescript
import Redis from 'ioredis';
import { Client } from 'asynq-client';

const client = new Client(redis);           // redis: ioredis instance, caller-owned
const id: string = await client.enqueue(taskType, payload, options?);
```

- `taskType: string` — must be non-empty; must exactly match the string
  registered on the Go side (`mux.HandleFunc("email:send", ...)`).
- `payload: unknown` — JSON-serialized into `TaskMessage.payload`
  (`JSON.stringify(payload ?? null)`). The Go handler receives raw JSON bytes.
- Returns the task ID (random UUID unless `options.taskId` is given).
- The Client never closes the passed Redis connection.

### `EnqueueOptions` (all optional)

| Option | Type | Default | Semantics |
|---|---|---|---|
| `queue` | `string` | `"default"` | Target queue. Non-blank. |
| `maxRetry` | `number` | `25` | Max retries. Negative → 0. |
| `taskId` | `string` | random UUID | Custom ID. Duplicate IDs throw `ErrTaskIdConflict`. |
| `timeout` | `number` (seconds) | `0` | Per-task timeout. |
| `deadline` | `Date` | none | Absolute deadline. |
| `uniqueTTL` | `number` (seconds) | none | Dedupe by `md5(type+payload)` within TTL. Duplicates throw `ErrDuplicateTask`. |
| `processAt` | `Date` | none | Scheduled delivery time. |
| `processIn` | `number` (seconds) | none | Delay shorthand. `processAt` wins if both set. |
| `retention` | `number` (seconds) | `0` | Keep completed task this long; 0 = delete on done. |
| `group` | `string` | none | Aggregation group key (Go server needs a `GroupAggregator`). |
| `headers` | `Record<string,string>` | `{}` | Extra metadata on the message. |

### Routing precedence inside `enqueue()`

1. `group` set → aggregation group (`state=aggregating`)
2. else `processAt`/`processIn` set → scheduled set (`state=scheduled`)
3. else → pending list (`state=pending`)

### Errors

| Class | Thrown when |
|---|---|
| `ErrDuplicateTask` | unique lock (`uniqueTTL`) already held |
| `ErrTaskIdConflict` | task ID already exists in Redis |
| `Error` (plain) | blank queue name, Redis errors, encode failures |

### Invariants the Go side relies on (do not break)

1. If neither `timeout` nor `deadline` is set, this library writes
   `timeout = 1800` (30 min) into the message — same as the Go client's
   `defaultTimeout`. The Go processor logs an internal error otherwise.
2. `retry` defaults to `25`.
3. Unique key format: `asynq:{queue}:unique:<type>:<md5-hex-of-payload>`.
4. All Redis keys embed the `{queue}` hash tag (Redis Cluster slot affinity).
5. `pending_since` is Unix **nanoseconds** (BigInt math, sent as string);
   group/schedule scores are Unix **seconds**.

## Pinned upstream

- asynq **v0.26.0** (`internal/proto/asynq.proto`, `internal/rdb/rdb.go`,
  `internal/base/base.go`, `client.go`).
- asynq is pre-1.0: upstream may change the wire format. After bumping the Go
  dependency anywhere in the consuming system, re-run the interop test.

## Runtime & environment

- Node.js ≥ 18 (developed on 24) or Bun ≥ 1.0 (verified on Bun 1.4.2).
- ESM only (`"type": "module"`). CommonJS consumers need Node ≥ 22
  (`require(esm)`) or a bundler.
- Redis ≥ 4 (tested on 7). Works against Redis Cluster for the producer paths.
- Dependencies: `ioredis`, `protobufjs`. No native bindings.

## How to verify changes

```bash
npm install
npm run build                                   # tsc must pass
docker run -d --name asynq-test-redis -p 6379:6379 redis:7-alpine
npm test                                        # unit + Redis integration
./integration/run-interop-test.sh               # needs docker + go
```

No docker? Point tests at any Redis:
`ASYNQ_TEST_REDIS_URL=redis://host:6379 npm test` (the interop script still
needs docker + Go; skip it if unavailable, but say so in your summary).

## Known pitfalls (learned from real failures)

1. **Reading tasks back**: `msg` is binary protobuf. ioredis `hgetall`
   corrupts high bytes — always `hgetallBuffer`, then `decodeMessage()`.
2. **Do not reformat `src/scripts.ts`.** Lua text is compared by hash in
   spirit; keep it byte-identical to upstream. Comments around it may change.
3. **`uuid` package is intentionally not a dependency** — use
   `node:crypto.randomUUID()`.
4. When adding a task option, mirror Go's `client.go` `composeOptions`
   semantics, not your own intuition.

## Conventions for contributing (LLM-specific)

- Public API changes require: README API table update, this file's contract
  section update, and a new test in `test/integration.test.ts`.
- Any change touching `scripts.ts`, `keys.ts`, `message.ts`, or `proto/`
  requires running `integration/run-interop-test.sh` and pasting the `PASS`
  line in the commit message body.
- Keep README bilingual (English first, 中文 second) when editing docs.
