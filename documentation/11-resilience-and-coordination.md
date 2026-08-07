# Resilience + Coordination Primitives

Eight interface-first primitives for the patterns you actually need in
production: rate limiting, circuit breaking, request deduplication,
broadcast, reactive state, and resource pooling.

All are **interface-first** — the in-process implementation ships with
`@perfect/core`, but distributed backends (Redis, Postgres, etc.) can
implement the same `Eff`-typed interface and slot in via Layer.

## CircuitBreaker

Classic 3-state breaker. While **Open**, calls reject fast with a typed
`CircuitOpen` error instead of running the protected effect. Transitions
to **HalfOpen** after `resetTimeoutMs`, and to **Closed** on the first
success.

<!-- @embed packages/core/examples/14-primitives.ts#circuit-breaker -->
```ts
import { succeed, CircuitBreaker, type Eff, type Throws } from "@perfect/core";

// 3-state breaker — Closed → Open after `failureThreshold` failures.
// While Open, calls reject fast with typed `CircuitOpen` error.
const cb = CircuitBreaker.make<string>({
  failureThreshold: 3,
  resetTimeoutMs: 1000,
});

// Wrap any effect with .protect()
const safeCall = (n: number): Eff<number, Throws<string | { _tag: "CircuitOpen" }>> =>
  cb.protect(succeed(n * 2));

console.log(await (safeCall(21) as any).run()); // → 42
```
<!-- @end -->

| | |
|---|---|
| `CircuitBreaker.make({ failureThreshold, resetTimeoutMs, isFailure? })` | construct |
| `cb.protect(eff)` | wrap with breaker semantics |
| `cb.state` / `cb.failures` | inspection |
| `cb.reset()` | force back to Closed |

**Defects don't trip the breaker** — only typed `Throws<E>` failures do.
Pass `isFailure` to filter further (e.g. only count 5xx, not 4xx).

## Singleflight

Concurrent calls with the same key share **one** execution. Useful for
cache-stampede protection ("ten requests hit a cold cache, only one
should query the DB").

<!-- @embed packages/core/examples/14-primitives.ts#singleflight -->
```ts
import { eff, sleep, all, Singleflight } from "@perfect/core";

// Deduplicate concurrent calls with the same key — leader runs the work
// once, followers wait and receive the same result.
const sf = Singleflight.make();
let fetchCount = 0;
const fetchUser = (id: number) =>
  sf.do(
    `user:${id}`,
    eff(function* () {
      yield* sleep(10);
      fetchCount++;
      return { id, name: `user-${id}` };
    }),
  );

// Five concurrent fetches for the same user → one execution
const users = await all([
  fetchUser(7),
  fetchUser(7),
  fetchUser(7),
  fetchUser(7),
  fetchUser(7),
]).run();
console.log(fetchCount); // → 1
console.log(users[0]!.id); // → 7
```
<!-- @end -->

| | |
|---|---|
| `Singleflight.make()` | construct |
| `sf.do(key, eff)` | dedupe by key |

**No caching** — once the eff settles, the key is cleared so the next
call re-runs. For caching, use `cached` / `cachedBy` (see [Cache](./12-utilities.md#cachestore)).

## RateLimiter

Three strategies — `slidingWindow`, `fixedWindow`, `tokenBucket`. Each
has fail-fast (`tryAcquire` / `acquire`) and blocking (`acquireWaiting`)
modes.

<!-- @embed packages/core/examples/14-primitives.ts#rate-limiter -->
```ts
import { eff, RateLimiter } from "@perfect/core";

// Three strategies. tryAcquire returns boolean; acquireWaiting blocks.
const rl = await RateLimiter.tokenBucket({ limit: 5, windowMs: 1000 }).run();

// Try 10 acquires — first 5 succeed, rest get false
const attempts = await (
  eff(function* () {
    const results: boolean[] = [];
    for (let i = 0; i < 10; i++) results.push(yield* rl.tryAcquire);
    return results;
  }) as any
).run();
console.log(attempts.filter(Boolean).length); // → 5
```
<!-- @end -->

| Strategy | When to use |
|---|---|
| `slidingWindow` | most accurate; true rate over a moving window |
| `fixedWindow` | simpler, allows bursts at window boundaries |
| `tokenBucket` | smooth rate with a configurable burst capacity |

| | |
|---|---|
| `rl.tryAcquire` | non-blocking, returns boolean |
| `rl.acquire` | fails with typed `RateLimitExceeded` (with `retryAfterMs`) |
| `rl.acquireWaiting` | blocks until a slot opens |
| `rl.withLimit(eff)` / `rl.withLimitWaiting(eff)` | wrap an effect |
| `rl.remaining` / `rl.resetAt` / `rl.nextSlotIn` | inspection |

`Throttle` is an alias for "always-blocking RateLimiter" — see
`Throttle.make({ permits, windowMs })`.

## Latch

`CountDownLatch` — N parties decrement, awaiters release when count hits
zero. Single-shot.

<!-- @embed packages/core/examples/14-primitives.ts#latch -->
```ts
import { eff, sync, sleep, fork, join, Latch } from "@perfect/core";

// CountDownLatch — N parties decrement, awaiters release when count hits 0.
const events: string[] = [];
await (
  eff(function* () {
    const ready = yield* Latch.make({ count: 3 });

    // Awaiter blocks until the 3 parties have arrived
    const watcher = yield* fork(
      ready.await.flatMap(() =>
        sync(() => {
          events.push("released");
        }),
      ),
    );

    // Three workers count down at different times
    yield* fork(sleep(10).flatMap(() => ready.countDown));
    yield* fork(sleep(20).flatMap(() => ready.countDown));
    yield* fork(sleep(30).flatMap(() => ready.countDown));

    yield* join(watcher);
  }) as any
).run();
console.log(events); // → ["released"]
```
<!-- @end -->

| | |
|---|---|
| `Latch.make({ count })` | construct |
| `latch.countDown` / `latch.countDownBy(n)` | decrement |
| `latch.await` | block until count = 0 |
| `latch.remaining` | inspection |

## Barrier

`CyclicBarrier` — N parties block until all have arrived, then all
proceed simultaneously. Useful for coordinated worker startup or
multi-phase tests.

<!-- @embed packages/core/examples/14-primitives.ts#barrier -->
```ts
import { eff, sleep, fork, join, Barrier } from "@perfect/core";

// CyclicBarrier — N parties block until all have arrived, then all proceed.
const arrived: number[] = [];
await (
  eff(function* () {
    const barrier = yield* Barrier.make({ parties: 3 });
    const party = (n: number) =>
      eff(function* () {
        yield* sleep(n * 5); // each party arrives at different times
        yield* barrier.await; // blocks until all 3 are here
        arrived.push(n);
      });
    const f1 = yield* fork(party(1));
    const f2 = yield* fork(party(2));
    const f3 = yield* fork(party(3));
    yield* join(f1);
    yield* join(f2);
    yield* join(f3);
  }) as any
).run();
console.log(arrived.sort()); // → [1, 2, 3]
```
<!-- @end -->

| | |
|---|---|
| `Barrier.make({ parties })` | construct |
| `barrier.await` | arrive AND block until all parties have arrived |
| `barrier.arrived` | how many have arrived so far |

## PubSub

Broadcast channel. Every subscriber sees every message via its own queue.

<!-- @embed packages/core/examples/14-primitives.ts#pubsub -->
```ts
import { eff, sync, sleep, fork, join, PubSub } from "@perfect/core";

// Broadcast channel — every subscriber gets every message.
const seen: number[][] = [[], [], []];
await (
  eff(function* () {
    const pubsub = yield* PubSub.unbounded<number>();
    const subA = yield* pubsub.subscribe;
    const subB = yield* pubsub.subscribe;
    const subC = yield* pubsub.subscribe;
    const fA = yield* fork(
      subA.take(3).forEach((n) =>
        sync(() => {
          seen[0]!.push(n);
        }),
      ),
    );
    const fB = yield* fork(
      subB.take(3).forEach((n) =>
        sync(() => {
          seen[1]!.push(n);
        }),
      ),
    );
    const fC = yield* fork(
      subC.take(3).forEach((n) =>
        sync(() => {
          seen[2]!.push(n);
        }),
      ),
    );
    yield* sleep(5); // let subscribers register
    yield* pubsub.publish(1);
    yield* pubsub.publish(2);
    yield* pubsub.publish(3);
    yield* join(fA);
    yield* join(fB);
    yield* join(fC);
  }) as any
).run();
console.log(seen[0]); // → [1, 2, 3]
console.log(seen[1]); // → [1, 2, 3]
console.log(seen[2]); // → [1, 2, 3]
```
<!-- @end -->

| | |
|---|---|
| `PubSub.bounded(capacity)` / `PubSub.unbounded()` | construct |
| `ps.publish(value)` | broadcast |
| `ps.subscribe` | get a `Stream<T>` (also Eff — wraps queue allocation) |
| `ps.shutdown()` | close all subscriber streams |
| `ps.subscriberCount` | inspection |

## SubscriptionRef

A `Ref<A>` that also exposes a change `Stream<A>`. The stream emits the
**current value** first, then every subsequent set/update.

<!-- @embed packages/core/examples/14-primitives.ts#subscription-ref -->
```ts
import { eff, sync, sleep, fork, join, SubscriptionRef } from "@perfect/core";

// Ref<A> + change Stream — reactive cell. `changes` emits current value
// first, then every subsequent set/update.
const observed: string[] = [];
await (
  eff(function* () {
    const config = yield* SubscriptionRef.make("v1");
    const stream = yield* config.changes;
    const reader = yield* fork(
      stream.take(3).forEach((v) =>
        sync(() => {
          observed.push(v);
        }),
      ),
    );
    yield* sleep(5);
    yield* config.set("v2");
    yield* config.update((v) => `${v}-patched`);
    yield* join(reader);
  }) as any
).run();
console.log(observed); // → ["v1", "v2", "v2-patched"]
```
<!-- @end -->

| | |
|---|---|
| `SubscriptionRef.make(initial)` | construct |
| `ref.get` / `ref.set(v)` / `ref.update(f)` | normal Ref ops |
| `ref.changes` | get a `Stream<A>` of state transitions |

## Pool

Generic resource pool with bounded capacity, reuse, and blocking
acquires. The pool reuses a previously-released resource if available;
otherwise creates a new one (up to `size`). At capacity, acquires block
until a release frees a slot.

<!-- @embed packages/core/examples/14-primitives.ts#pool -->
```ts
import { eff, sync, Pool } from "@perfect/core";

// Resource pool with reuse. Acquire blocks at capacity, hands off to
// waiters on release.
let connId = 0;
const conns: number[] = [];
await (
  eff(function* () {
    const pool = yield* Pool.make({
      acquire: sync(() => ({ id: ++connId })),
      release: () => sync(() => undefined),
      size: 2,
    });

    // Use the pool 5 times sequentially — each call reuses the same conn
    for (let i = 0; i < 5; i++) {
      yield* pool.use((c) =>
        sync(() => {
          conns.push(c.id);
        }),
      );
    }
  }) as any
).run();
// All 5 ops used conn id=1 (reuse)
console.log(conns); // → [1, 1, 1, 1, 1]
```
<!-- @end -->

| | |
|---|---|
| `Pool.make({ acquire, release, size, validate? })` | construct |
| `pool.use(fn)` | acquire → run → auto-release (LIFO release ordering) |
| `pool.shutdown()` | release idle, reject pending waiters with `PoolClosed` |
| `pool.inUse` / `pool.idle` / `pool.size` | inspection |

`validate?: (r) => Eff<boolean, never>` runs before handing a reused
resource to a caller — failed validation discards it (calls `release`)
and acquires fresh.

## Pitfalls

- **Defects don't retry / don't trip CircuitBreaker.** Use `fail()` for
  expected failures, not `throw`.
- **Singleflight followers wait synchronously on the leader.** If the
  leader hangs, all followers hang. Combine with `timeout` / `race` to
  cap.
- **PubSub's slow-consumer policy is "block".** A slow subscriber blocks
  publishers when its queue fills (bounded). Use `unbounded` for
  fire-and-forget at the cost of memory.
- **Pool's `validate` runs ONLY on reuse.** A fresh `acquire` is trusted.
- **All primitives are interface-first** — distributed backends (Redis,
  etc.) drop in as Layer-injected swaps. Look for `@perfect-ext/*`
  packages or roll your own.

## Next

- [Utilities — Duration, CacheStore](./12-utilities.md)
- [Resources and scopes](./07-resources-and-scopes.md)
