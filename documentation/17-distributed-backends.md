# Redis and PostgreSQL Backends

Remote implementations retain the same core interfaces while adding typed
driver failures. Callers can swap in-process and distributed implementations
without changing their coordination logic.

## Redis

Install the adapter and inject any client satisfying `RedisClient`:

```bash
bun add @perfect/redis ioredis
```

```ts
import Redis from "ioredis";
import { RedisRef, type RedisClient } from "@perfect/redis";

const redis = new Redis("redis://localhost:6379") as unknown as RedisClient;
const counter = await RedisRef.make({ redis, key: "counter", initial: 0 }).run();
const value = await counter.updateAndGet((n) => n + 1).run();
```

All driver failures surface as `Throws<RedisError>` and can be handled with
`.catchTag("RedisError", ...)`.

### Coordination

Redis implementations include `Ref`, `SubscriptionRef`/`Signal`, `Deferred`,
weighted `Semaphore`, `Latch`, `Barrier`, `Queue`, `PubSub`, `Singleflight`,
`RateLimiter`, `Throttle`, `CircuitBreaker`, `CacheStore`, `StateBackend`, and
partitioned topology state.

Atomic transitions use Lua or compare-and-set. Blocking list operations use
duplicated connections so fiber interruption can close a blocked command.
Multi-key primitives derive related keys with a shared Redis Cluster hash tag.

### Messaging

- `RedisStream<T>` is durable: consumer groups, replay, manual ack/nack,
  claiming abandoned deliveries, keyed publication, metrics, and optional
  dead-letter recovery.
- `RedisChannel<T>` implements non-durable `Streamable`/`Sinkable` Pub/Sub,
  including pattern subscriptions.
- `RedisPubSub<T>` is the distributed implementation of the core PubSub-style
  concurrency interface.

Pub/Sub buffers are bounded (default 1,024). A slow subscriber fails with a
typed `RedisError` for `pubsub.overflow` rather than growing memory without
bound. Redis Stream consumption is at-least-once; apply `Stream.retry`
explicitly when reconnect behavior is appropriate.

## PostgreSQL

```bash
bun add @perfect/postgres drizzle-orm postgres
```

The root package exports `PgQueue`, `PgChangeStream`, `PgRateLimiter`,
`PgThrottle`, `PgSingleflight`, `PgRef`, `PgLeaderElection`, and durable state
backends. `createPostgresDb(connectionString)` is the postgres-js convenience;
the public `DrizzleDb` type also accepts other Drizzle PostgreSQL drivers.

| Adapter | Role |
| --- | --- |
| `PgQueue` | durable work queue with `FOR UPDATE SKIP LOCKED`, delayed publication, manual ack/nack, archive mode, metrics, and explicit requeue management |
| `PgChangeStream` | LISTEN/NOTIFY wakeups with offset-based polling replay so notifications are not the durability boundary |
| `PgRateLimiter` / `PgThrottle` | cross-instance admission and pacing |
| `PgSingleflight` | one leader execution per distributed key |
| `PgRef` | transactional shared reference |
| `PgLeaderElection` | session advisory-lock leadership with explicit release |
| `PgStateBackend` | durable keyed state and checkpoint storage |
| `PgPartitionedStateBackend` | fenced topology leases plus atomic state, progress, checkpoint, and dedupe commits |

Driver failures stay typed as `Throws<PostgresError>`. Table/schema creation
helpers are exported for applications that manage migrations explicitly.

### PGMQ

PGMQ lives under a separate export:

```ts
import { createPostgresDb } from "@perfect/postgres";
import { PgmqQueue } from "@perfect/postgres/pgmq";

const db = createPostgresDb(process.env.DATABASE_URL!);
const jobs = await PgmqQueue.create<{ userId: string; sequence: number }>(
  db,
  "jobs",
  { fifo: true, defaultPollIntervalMs: 100 },
);

await jobs.publish({ userId: "u-1", sequence: 1 }, { group: "u-1" }).run();
const [job] = await jobs.subscribeAck().take(1).toArray().orDie().run();
await job!.ack().run();
```

With `fifo: true`, the recommended partial index is created and the `group`
publish option provides per-identity ordering: while one worker holds message
1 for group X, another worker cannot claim message 2 for X, but can continue
with another group.

Schema validation is optional. `onSchemaError` can be `"throw"`, `"skip"`, or
`"dlq"`; the loud typed failure is the default.

The `@perfect/postgres/pgmq` subpath also exports the low-level PGMQ operations
for queue creation, batched send, read/pop, delete/archive, visibility changes,
metrics, notifications, and FIFO index creation. Use those when the
`Streamable`/`Sinkable`/`Acknowledgeable` wrapper is not the desired boundary.

## Transactional topology delivery

`PgPartitionedStateBackend` stores partition-scoped state, source progress,
checkpoints, dedupe records, owner leases, and fencing epochs. When a
`PgmqQueue` source, `PgmqQueue` sink, and this state backend share the same
Drizzle database object, `@perfect/topology` can atomically publish output,
commit state/progress, and delete/archive the source message in one PostgreSQL
transaction.

Other combinations—Kafka, Redis Streams, mixed databases—remain explicitly
at-least-once. Make their sinks idempotent and derive dedupe retention from the
source replay horizon.

## Next

- [Stateful topologies](./18-topologies.md)
- [Resilience and coordination](./11-resilience-and-coordination.md)
