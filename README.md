# asynq-node

Insert-only **Node.js / TypeScript producer** for the [asynq](https://github.com/hibiken/asynq) task queue (Go).

Push tasks from your Node.js services into the same Redis-backed queues your Go `asynq.Server` fleet consumes — with **zero changes on the Go side**.

[中文文档](#中文文档)

---

## Why

`asynq` is an excellent Go task queue, but it only ships a Go client. If part of your stack is Node.js (API gateway, webhook receiver, Next.js backend, …), you previously had two bad options: call an internal Go HTTP service just to enqueue a task, or run a second queue system.

`asynq-node` takes a third path: it writes task messages to Redis in **byte-identical wire format** — the same protobuf encoding, the same Lua scripts, the same key layout as the official Go client. To the Go server, a Node-produced task is indistinguishable from a Go-produced one.

```
┌──────────────────┐         ┌────────┐         ┌──────────────────┐
│  Node.js service │ ──────▶ │ Redis  │ ◀────── │  Go asynq.Server │
│  (this library)  │  insert │ + Lua  │  pull   │  (unchanged)     │
└──────────────────┘         └────────┘         └──────────────────┘
```

## Features

- **Immediate tasks** — `LPUSH` into the pending list
- **Scheduled tasks** — `ZADD` into the scheduled set (`processAt` / `processIn`)
- **Unique tasks** — dedupe by `md5(type + payload)` with TTL
- **Aggregation groups** — stage tasks into a group for Go-side aggregation
- **All task options** — `maxRetry`, `timeout`, `deadline`, `retention`, `headers`, custom task ID
- **Wire-compatible defaults** — same default timeout (30 min), retry count (25), and queue (`default`) as the Go client
- **Type-safe** — written in TypeScript, ships `.d.ts`

## Install

```bash
npm install asynq-node ioredis
```

> Not on npm yet? Install straight from GitHub: `npm install github:forwaard/asynq-node`

## Quick start

```typescript
import Redis from 'ioredis';
import { Client } from 'asynq-node';

const redis = new Redis(process.env.REDIS_URL!);
const client = new Client(redis);

// Immediate task
await client.enqueue('email:send', { to: 'user@example.com', subject: 'Hi' });

// Scheduled 1 hour from now
await client.enqueue('report:generate', { userId: 42 }, { processIn: 3600 });

// Unique within 5 minutes — duplicates throw ErrDuplicateTask
await client.enqueue('sync:account', { accountId: 'acct_1' }, { uniqueTTL: 300 });

// Full options
await client.enqueue('image:resize', { path: '/tmp/a.jpg' }, {
  queue: 'critical',     // target queue (default: "default")
  maxRetry: 3,           // default: 25
  timeout: 30,           // seconds; default: 1800 when no deadline either
  deadline: new Date(Date.now() + 3600_000),
  retention: 600,        // keep completed task for 10 min
  headers: { 'x-trace-id': 'abc' },
  taskId: 'my-fixed-id', // otherwise a random UUID
});
```

The Go consumer registers handlers as usual — nothing to change:

```go
mux := asynq.NewServeMux()
mux.HandleFunc("email:send", handleEmail)      // matches the type string
srv := asynq.NewServer(redisOpt, asynq.Config{Concurrency: 10})
srv.Run(mux)
```

## How compatibility works

The Go server never talks to the Go client directly — both talk to Redis. This library replicates the client half exactly:

| Piece | Source of truth | This library |
|---|---|---|
| Message encoding | `internal/proto/asynq.proto` | vendored at [`proto/asynq.proto`](proto/asynq.proto), encoded with `protobufjs` |
| Redis key layout | `internal/base/base.go` | [`src/keys.ts`](src/keys.ts), incl. `{queue}` hash tags |
| Atomic enqueue scripts | `internal/rdb/rdb.go` | [`src/scripts.ts`](src/scripts.ts) — **byte-identical copies** |
| Client defaults | `client.go` | same defaults: retry 25, timeout 30 min when unset |

Pinned against **asynq v0.26.0**. Because asynq is `v0.x`, its wire format could change in a future release — check the compatibility note below before upgrading your Go dependency.

## Verifying the interop yourself

The repo includes an end-to-end test that starts a **real Go `asynq.Server`**, enqueues tasks from Node, and asserts the Go side consumed them:

```bash
# needs: docker, go, node
docker run -d --name asynq-test-redis -p 6379:6379 redis:7-alpine
npm install
npm test                              # unit + redis integration tests
./integration/run-interop-test.sh     # Node producer -> Go consumer e2e
```

Expected output ends with:

```
PASS: Go asynq server consumed all 4 Node-produced tasks
```

## Reading task state back (inspection)

If you read task hashes from Redis yourself (e.g. to build tooling), use `hgetallBuffer`, **not** `hgetall` — the `msg` field is binary protobuf, and ioredis' string mode corrupts high bytes:

```typescript
const hash = await redis.hgetallBuffer(`asynq:{default}:t:${id}`);
const msg = decodeMessage(hash.msg as Buffer); // exported helper
```

## Not included (by design)

This is a **producer only**. The following stay on the Go side (or [asynqmon](https://github.com/hibiken/asynqmon) for inspection):

- Workers / consumers (lease, heartbeat, retry scheduling, crash recovery)
- Periodic-task scheduler (`asynq.Scheduler`)
- Web UI / CLI

If you need a native Node.js queue with workers, look at [BullMQ](https://docs.bullmq.io/) or [River](https://riverqueue.com) instead — this library exists purely to bridge Node producers into an existing Go asynq deployment.

## Compatibility & risks

- Tested against **asynq v0.26.0** (Go 1.24+), Redis ≥ 4 (works on Redis 7).
- asynq is pre-1.0; if upstream changes the protobuf schema or Lua scripts, this library must be updated in lockstep. Pin your asynq version and re-run `./integration/run-interop-test.sh` after upgrading.
- Lua scripts are compatible with Redis Cluster (all keys share the `{queue}` hash tag), though asynq itself notes some *other* scripts may not be cluster-safe.

## License

MIT. The vendored `proto/asynq.proto` and the Lua scripts in `src/scripts.ts` are copied from [hibiken/asynq](https://github.com/hibiken/asynq) (MIT, © Kentaro Hibino).

---

## 中文文档

`asynq-node` 是 Go 任务队列 [asynq](https://github.com/hibiken/asynq) 的 **Node.js 纯生产者客户端**：从 Node 服务往 Redis 里推任务，由现有的 Go `asynq.Server` 集群消费，**Go 侧零改动**。

### 原理

asynq 的 Go 客户端与服务端从不直接通信——两边都只操作 Redis。本库精确复刻了客户端这一半：

- 消息体用 protobuf 编码（`proto/asynq.proto` 从官方仓库原样复制）
- 写入走与官方**字节一致**的 Lua 脚本（`src/scripts.ts`）
- Redis key 布局、默认值（重试 25 次、默认超时 30 分钟）与 Go 客户端完全一致

所以对 Go 服务端来说，Node 推的任务和 Go 推的任务没有任何区别。

### 用法

```typescript
import Redis from 'ioredis';
import { Client } from 'asynq-node';

const client = new Client(new Redis(process.env.REDIS_URL!));

// 立即执行
await client.enqueue('email:send', { to: 'user@example.com' });

// 1 小时后执行
await client.enqueue('report:generate', { userId: 42 }, { processIn: 3600 });

// 5 分钟内去重，重复会抛 ErrDuplicateTask
await client.enqueue('sync:account', { accountId: 'acct_1' }, { uniqueTTL: 300 });
```

Go 消费端照常注册 handler 即可（任务 type 字符串两边对齐）：

```go
mux.HandleFunc("email:send", handleEmail)
```

### 自测

```bash
docker run -d --name asynq-test-redis -p 6379:6379 redis:7-alpine
npm install
npm test                            # 单元测试 + Redis 集成测试
./integration/run-interop-test.sh   # Node 生产 → Go 真实消费 端到端验证
```

### 注意事项

- 本库**只含生产者**。Worker、租约、崩溃恢复、周期任务调度都留在 Go 侧。
- 从 Redis 读回任务内容时务必用 `hgetallBuffer`（`msg` 字段是二进制 protobuf，普通 `hgetall` 会损坏高位字节）。
- 当前对齐 **asynq v0.26.0**。asynq 还是 v0.x，升级 Go 依赖版本后请重跑互操作测试。
