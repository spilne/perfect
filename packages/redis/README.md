# @perfect/redis

Distributed Redis implementations of the concurrency and coordination contracts in
`@perfect/core`, plus durable Redis Streams and Pub/Sub connectors for
`@perfect/core/connect`. Coordination operations and connector subscription streams expose
`Throws<RedisError>` in `Eff<A, S>`; driver failures are recoverable with
`.catchTag("RedisError", ...)` instead of becoming defects.

## Install

```bash
bun add @perfect/redis ioredis
```

The package is driver-agnostic. Inject any client implementing `RedisClient`; ioredis is
used by the integration suite but is not bundled at runtime.

## Example

```ts
import { run, succeed } from "@perfect/core";
import Redis from "ioredis";
import { RedisRef, type RedisClient } from "@perfect/redis";

const driver = new Redis("redis://localhost:6379");
const redis = driver as unknown as RedisClient;

const value = await run(
  RedisRef.make({ redis, key: "counter", initial: 0 })
    .flatMap((counter) => counter.updateAndGet((n) => n + 1))
    .catchTag("RedisError", () => succeed(0)),
);
```

## Implementations

- `RedisRef` and `RedisSubscriptionRef` (`RedisSignal` alias)
- `RedisDeferred`
- `RedisQueue` and `RedisPubSub`, including `PSUBSCRIBE` pattern streams
- `RedisSemaphore`, `RedisLatch`, and `RedisBarrier`
- `RedisSingleflight`
- `RedisRateLimiter` and `RedisThrottle`
- `RedisCircuitBreaker`
- `RedisCacheStore`
- `RedisStateBackend` — durable keyed state and atomic checkpoints for
  `Stream.statefulMap` and `@perfect/topology`
- `RedisPartitionedStateBackend` — topology/stage/partition namespaces,
  server-time leases, fencing epochs, and one-Lua state/progress/dedupe commits
- `RedisStream` — durable consumer groups, replay, manual acknowledgement, claiming,
  keyed publish, and metrics
- `RedisChannel` — non-durable Pub/Sub implementing `Streamable` and `Sinkable`, with
  `subscribePattern()` for pattern streams

Mutations that need cross-process consistency use Lua compare-and-set or atomic server-side
state transitions. Blocking list operations use dedicated duplicated connections so fiber
interruption can close the blocked connection.

## Durable messaging

```ts
import { run } from "@perfect/core";
import { RedisStream } from "@perfect/redis";

const events = RedisStream.make<{ id: string }>({
  redis,
  stream: "events",
  group: "indexer",
  recovery: {
    minIdleMs: 30_000,
    maxDeliveries: 5,
    deadLetterStream: "events-dlq",
  },
});

await events.publish({ id: "e-1" }, { key: "account-1" });

const envelopes = await run(
  events
    .subscribeAck({ offset: { type: "earliest" } })
    .take(1)
    .toArray()
    .orDie(),
);
await envelopes[0]!.ack();
```

`RedisStream.subscribe()` auto-acknowledges each Redis read batch. Use
`subscribeAck()` for at-least-once processing. Configure `recovery` to reclaim abandoned
deliveries with `XAUTOCLAIM`; messages that reach `maxDeliveries` are copied to the dead-letter
stream and acknowledged in the source group. `recoverPending()` exposes the same mechanism for
manual recovery. Stream and Pub/Sub subscription
failures surface as `RedisError`; apply `Stream.retry(...)` explicitly when reconnect or retry
behavior is appropriate for the application.

Pub/Sub subscriptions use a bounded buffer (1,024 messages by default). A subscriber that cannot
keep up fails with `RedisError` operation `pubsub.overflow` instead of consuming unbounded memory;
set `bufferCapacity` on `RedisPubSub` or `RedisChannel` for the workload. Multi-key Lua primitives
derive keys with a shared Redis Cluster hash tag, while preserving a caller-provided `{tag}`.
`processedRetentionMs` optionally bounds source-record dedupe history; set it no shorter than the
source system's replay/retention horizon.
