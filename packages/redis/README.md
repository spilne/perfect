# @perfect/redis

Distributed Redis implementations of the concurrency and coordination contracts in
`@perfect/core`, plus durable Redis Streams and Pub/Sub connectors for
`@perfect/core/connect`. Coordination operations expose `Throws<RedisError>` in
`Eff<A, S>`; driver failures are recoverable with `.catchTag("RedisError", ...)` instead
of becoming defects.

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
});

await events.publish({ id: "e-1" }, { key: "account-1" });

const envelopes = await run(
  events
    .subscribeAck({ offset: { type: "earliest" } })
    .take(1)
    .toArray(),
);
await envelopes[0]!.ack();
```

`RedisStream.subscribe()` auto-acknowledges each Redis read batch. Use
`subscribeAck()` for at-least-once processing, and `claimPending()` to recover messages
left in the pending entries list by an inactive consumer.
