# Resilience + Coordination Primitives

Interface-first primitives for production coordination: rate limiting,
circuit breaking, request deduplication, weighted permits, queues, broadcast,
reactive state, barriers, latches, deferred values, and resource pooling.

All are **interface-first** — the in-process implementation ships with
`@spilne/perfect-core`, but distributed backends (Redis, Postgres, etc.) can
implement the same `Eff`-typed interface and slot in via Layer.

The backend effect parameter defaults to `never` for in-process use. Remote
implementations keep their failures visible, such as `Throws<RedisError>` or
`Throws<PostgresError>`, without changing callers that depend on the shared
interface.

## Queue

A queue passes values from producers to consumers. `Queue.bounded(n)` holds
at most `n` values: `offer` waits while it is full and `take` waits while it
is empty, so a fast producer can't run far ahead. `Queue.unbounded()` never
makes `offer` wait.

<!-- @embed packages/core/examples/16-basic-primitives.ts#queue -->

```ts
import { eff, fork, join, Queue } from "@spilne/perfect-core";

// A bounded queue: offer waits while the queue is full, take waits while it
// is empty. close() means "no more values"; takers then fail with QueueClosed.
const received = await eff(function* () {
  const queue = yield* Queue.bounded<number>(2);

  const producer = yield* fork(
    eff(function* () {
      for (const n of [1, 2, 3, 4]) yield* queue.offer(n);
      yield* queue.close();
    }),
  );

  const got: number[] = [];
  for (let i = 0; i < 4; i++) got.push(yield* queue.take());
  yield* join(producer);
  return got;
})
  .orDie()
  .run();
console.log(received); // → [1, 2, 3, 4]
```

<!-- @end -->

| API | What it does |
| --- | --- |
| `Queue.bounded<A>(n)` / `Queue.unbounded<A>()` | create a queue |
| `q.offer(a)` / `q.offerAll(as)` | add values, waiting for room |
| `q.take()` | remove one value, waiting for one to arrive |
| `q.takeAll()` | remove everything that is there now, without waiting |
| `q.close()` | no more values: waiting `offer`s fail, and `take` fails with `QueueClosed` once the queue is empty |
| `q.size` / `q.isClosed` / `q.awaitClose` | inspect the queue, or wait until it is closed |

`Stream.fromQueue(q)` turns a queue into a stream that ends when the queue
is closed.

## Semaphore

A semaphore limits how many things run at once. Each task takes a permit
and gives it back when done, even if it fails or is interrupted.

<!-- @embed packages/core/examples/16-basic-primitives.ts#semaphore -->

```ts
import { eff, sync, sleep, all, Semaphore } from "@spilne/perfect-core";

// At most 2 tasks run at the same time; the others wait for a permit.
let running = 0;
let mostAtOnce = 0;
const task = (ms: number) =>
  sync(() => {
    running++;
    mostAtOnce = Math.max(mostAtOnce, running);
  })
    .flatMap(() => sleep(ms))
    .flatMap(() => sync(() => void running--));

await eff(function* () {
  const permits = yield* Semaphore.make(2);
  yield* all([10, 10, 10, 10].map((ms) => permits.withPermit(task(ms))));
}).run();
console.log(mostAtOnce); // → 2
```

<!-- @end -->

| API | What it does |
| --- | --- |
| `Semaphore.make(n)` | `n` permits |
| `s.withPermit(eff)` | take a permit, run `eff`, give it back |
| `s.withPermits(k, eff)` | the same with `k` permits, for heavier tasks |
| `s.acquire()` / `s.release()` | manual control; prefer `withPermit` |
| `s.available` | permits free right now |

## Ref

A `Ref` holds one value that many fibers can read and change. Each update
happens as one step, so concurrent updates don't get lost.

<!-- @embed packages/core/examples/16-basic-primitives.ts#ref -->

```ts
import { eff, all, Ref } from "@spilne/perfect-core";

// A Ref is a mutable cell that fibers can share safely.
const finalCount = await eff(function* () {
  const counter = yield* Ref.make(0);
  yield* all(Array.from({ length: 100 }, () => counter.update((n) => n + 1)));
  // modify returns a value and sets a new state in one step
  const before = yield* counter.modify((n) => [n, 0] as [number, number]);
  return [before, yield* counter.get];
}).run();
console.log(finalCount); // → [100, 0]
```

<!-- @end -->

| API | What it does |
| --- | --- |
| `Ref.make(initial)` | create |
| `ref.get` / `ref.set(a)` | read / replace |
| `ref.update(f)` | change the value with `f` |
| `ref.modify(f)` | `f` returns `[result, newValue]`: change the value and get a result in one step |
| `ref.getAndSet(a)` / `ref.getAndUpdate(f)` / `ref.updateAndGet(f)` | change it and get the old or new value |

## Deferred

A `Deferred` is a value that is set once, later. Fibers that `await` it wait
until it is set. Use it to signal "this is ready" from one fiber to another.

<!-- @embed packages/core/examples/16-basic-primitives.ts#deferred -->

```ts
import { eff, sleep, fork, join, Deferred } from "@spilne/perfect-core";

// A Deferred is a value that will be set once, later. Fibers that await it
// wait until someone sets it.
const answer = await eff(function* () {
  const ready = yield* Deferred.make<string>();
  const waiter = yield* fork(ready.await);
  yield* sleep(10);
  yield* ready.succeed("done"); // returns false if it was already set
  return yield* join(waiter);
}).run();
console.log(answer); // → "done"
```

<!-- @end -->

| API | What it does |
| --- | --- |
| `Deferred.make<A, E>()` | create, empty |
| `d.succeed(a)` / `d.fail(e)` | set it; returns `false` if it was already set |
| `d.await` | wait for the value (or fail with `e`) |
| `d.isDone` | has it been set? |

## CircuitBreaker

A breaker has three states (`cb.state`):

- `"closed"`: calls run normally. After `failureThreshold` failures in a
  row, the breaker opens.
- `"open"`: calls fail right away with a typed `CircuitOpen` error, without
  running the protected effect.
- `"half-open"`: after `resetTimeoutMs` the breaker lets **one** trial call
  through. If it succeeds, the breaker is `"closed"` again; if it fails, it
  goes back to `"open"` and the timer starts over. Other calls made during the trial fail with
  `CircuitOpen`.

<!-- @embed packages/core/examples/14-primitives.ts#circuit-breaker -->

```ts
import { succeed, CircuitBreaker, type Eff, type Throws } from "@spilne/perfect-core";

// 3-state breaker — Closed → Open after `failureThreshold` failures.
// While Open, calls reject fast with typed `CircuitOpen` error.
const cb = CircuitBreaker.make<string>({
  failureThreshold: 3,
  resetTimeoutMs: 1000,
});

// Wrap any effect with .protect()
const safeCall = (n: number): Eff<number, Throws<string | { _tag: "CircuitOpen" }>> =>
  cb.protect(succeed(n * 2));

console.log(await safeCall(21).orDie().run()); // → 42
```

<!-- @end -->

|                                                                         |                             |
| ----------------------------------------------------------------------- | --------------------------- |
| `CircuitBreaker.make({ failureThreshold, resetTimeoutMs, isFailure? })` | construct                   |
| `cb.protect(eff)`                                                       | wrap with breaker semantics |
| `cb.state` / `cb.failures`                                              | effectful inspection        |
| `cb.reset()`                                                            | force back to Closed        |

**Defects don't trip the breaker** — only typed `Throws<E>` failures do.
Pass `isFailure` to filter further (e.g. only count 5xx, not 4xx).

## Singleflight

Concurrent calls with the same key share **one** execution. Useful for
cache-stampede protection ("ten requests hit a cold cache, only one
should query the DB").

<!-- @embed packages/core/examples/14-primitives.ts#singleflight -->

```ts
import { eff, sleep, all, Singleflight } from "@spilne/perfect-core";

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
const users = await all([fetchUser(7), fetchUser(7), fetchUser(7), fetchUser(7), fetchUser(7)])
  .orDie()
  .run();
console.log(fetchCount); // → 1
console.log(users[0]!.id); // → 7
```

<!-- @end -->

|                       |               |
| --------------------- | ------------- |
| `Singleflight.make()` | construct     |
| `sf.do(key, eff)`     | dedupe by key |

**No caching** — once the eff settles, the key is cleared so the next
call re-runs. For caching, use [`cached` / `cachedBy`](./12-utilities.md#cached-and-cachedby).

## RequestResolver

Code that looks things up one at a time (a customer for each order) makes
one query per item. `RequestResolver` collects the `get` calls that happen
together, such as the fibers of one `all`, and runs a single `load` for all
their keys, each key once:

<!-- @embed packages/core/examples/16-basic-primitives.ts#request-resolver -->

```ts
import { sync, all, RequestResolver } from "@spilne/perfect-core";

// Lookups made together become one load: here three orders need their
// customers, and the load runs once, for the two distinct ids.
const queries: string[][] = [];
const CustomerById = RequestResolver.make({
  load: (ids: readonly string[]) =>
    sync(() => {
      queries.push([...ids]); // e.g. select * from customers where id = any($1)
      return new Map(ids.map((id) => [id, { id, name: `customer ${id}` }] as const));
    }),
});

const orders = [{ customer: "c1" }, { customer: "c2" }, { customer: "c1" }];
const customers = await all(orders.map((order) => CustomerById.get(order.customer))).run();
assertEq(
  customers.map((customer) => customer?.name),
  ["customer c1", "customer c2", "customer c1"],
);
console.log(queries); // → [["c1", "c2"]]
```

<!-- @end -->

- `load(keys)` returns a `Map` from key to value; a key it leaves out gives
  `undefined`.
- If `load` fails, every caller in that batch fails with the same error,
  which stays in the type.
- `windowMs` also waits that long for more calls, which helps when requests
  arrive a little apart. `maxBatchSize` caps the keys per load.
- The load runs with the services of the caller that started the batch, and
  keeps going if that caller is interrupted.

`Singleflight` shares one run of the *same* request; `RequestResolver` turns
*different* requests into one load. They combine well with `cachedBy`.

## RateLimiter

Three strategies — `slidingWindow`, `fixedWindow`, `tokenBucket`. Each
has fail-fast (`tryAcquire` / `acquire`) and blocking (`acquireWaiting`)
modes.

<!-- @embed packages/core/examples/14-primitives.ts#rate-limiter -->

```ts
import { eff, RateLimiter } from "@spilne/perfect-core";

// Three strategies. tryAcquire returns boolean; acquireWaiting blocks.
const rl = await RateLimiter.tokenBucket({ limit: 5, windowMs: 1000 }).orDie().run();

// Try 10 acquires — first 5 succeed, rest get false
const attempts = await eff(function* () {
  const results: boolean[] = [];
  for (let i = 0; i < 10; i++) results.push(yield* rl.tryAcquire);
  return results;
})
  .orDie()
  .run();
console.log(attempts.filter(Boolean).length); // → 5
```

<!-- @end -->

| Strategy        | When to use                                    |
| --------------- | ---------------------------------------------- |
| `slidingWindow` | most accurate; true rate over a moving window  |
| `fixedWindow`   | simpler, allows bursts at window boundaries    |
| `tokenBucket`   | smooth rate with a configurable burst capacity |

|                                                  |                                                            |
| ------------------------------------------------ | ---------------------------------------------------------- |
| `rl.tryAcquire`                                  | non-blocking, returns boolean                              |
| `rl.acquire`                                     | fails with typed `RateLimitExceeded` (with `retryAfterMs`) |
| `rl.acquireWaiting`                              | blocks until a slot opens                                  |
| `rl.withLimit(eff)` / `rl.withLimitWaiting(eff)` | wrap an effect                                             |
| `rl.remaining` / `rl.resetAt` / `rl.nextSlotIn`  | inspection                                                 |

`Throttle` is an alias for "always-blocking RateLimiter" — see
`Throttle.make({ permits, windowMs })`.

## Latch

`CountDownLatch` — N parties decrement, awaiters release when count hits
zero. Single-shot.

<!-- @embed packages/core/examples/14-primitives.ts#latch -->

```ts
import { eff, sync, sleep, fork, join, Latch } from "@spilne/perfect-core";

// CountDownLatch — N parties decrement, awaiters release when count hits 0.
const events: string[] = [];
await eff(function* () {
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
})
  .orDie()
  .run();
console.log(events); // → ["released"]
```

<!-- @end -->

|                                            |                       |
| ------------------------------------------ | --------------------- |
| `Latch.make({ count })`                    | construct             |
| `latch.countDown` / `latch.countDownBy(n)` | decrement             |
| `latch.await`                              | block until count = 0 |
| `latch.remaining`                          | inspection            |

## Barrier

`CyclicBarrier` — N parties block until all have arrived, then all
proceed simultaneously. Useful for coordinated worker startup or
multi-phase tests.

<!-- @embed packages/core/examples/14-primitives.ts#barrier -->

```ts
import { eff, sleep, fork, join, Barrier } from "@spilne/perfect-core";

// CyclicBarrier — N parties block until all have arrived, then all proceed.
const arrived: number[] = [];
await eff(function* () {
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
})
  .orDie()
  .run();
console.log(arrived.sort()); // → [1, 2, 3]
```

<!-- @end -->

|                             |                                                 |
| --------------------------- | ----------------------------------------------- |
| `Barrier.make({ parties })` | construct                                       |
| `barrier.await`             | arrive AND block until all parties have arrived |
| `barrier.arrived`           | how many have arrived so far                    |

## PubSub

Broadcast channel. Every subscriber sees every message via its own queue.

<!-- @embed packages/core/examples/14-primitives.ts#pubsub -->

```ts
import { eff, sync, sleep, fork, join, PubSub } from "@spilne/perfect-core";

// Broadcast channel — every subscriber gets every message.
const seen: number[][] = [[], [], []];
await eff(function* () {
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
})
  .orDie()
  .run();
console.log(seen[0]); // → [1, 2, 3]
console.log(seen[1]); // → [1, 2, 3]
console.log(seen[2]); // → [1, 2, 3]
```

<!-- @end -->

|                                                   |                                                       |
| ------------------------------------------------- | ----------------------------------------------------- |
| `PubSub.bounded(capacity)` / `PubSub.unbounded()` | construct                                             |
| `ps.publish(value)`                               | broadcast                                             |
| `ps.subscribe`                                    | effect that allocates a subscription and returns its stream |
| `ps.shutdown()`                                   | close all subscriber streams                          |
| `ps.subscriberCount`                              | inspection                                            |

## SubscriptionRef

A `Ref<A>` that also exposes a change `Stream<A>`. The stream emits the
**current value** first, then every subsequent set/update.

<!-- @embed packages/core/examples/14-primitives.ts#subscription-ref -->

```ts
import { eff, sync, sleep, fork, join, SubscriptionRef } from "@spilne/perfect-core";

// Ref<A> + change Stream — reactive cell. `changes` emits current value
// first, then every subsequent set/update.
const observed: string[] = [];
await eff(function* () {
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
})
  .orDie()
  .run();
console.log(observed); // → ["v1", "v2", "v2-patched"]
```

<!-- @end -->

|                                            |                                        |
| ------------------------------------------ | -------------------------------------- |
| `SubscriptionRef.make(initial)`            | construct                              |
| `ref.get` / `ref.set(v)` / `ref.update(f)` | normal Ref ops                         |
| `ref.changes`                              | get a `Stream<A>` of state transitions |

## Pool

Generic resource pool with bounded capacity, reuse, and blocking
acquires. The pool reuses a previously-released resource if available;
otherwise creates a new one (up to `size`). At capacity, acquires block
until a release frees a slot.

<!-- @embed packages/core/examples/14-primitives.ts#pool -->

```ts
import { eff, sync, Pool } from "@spilne/perfect-core";

// Resource pool with reuse. Acquire blocks at capacity, hands off to
// waiters on release.
let connId = 0;
const conns: number[] = [];
await eff(function* () {
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
})
  .orDie()
  .run();
// All 5 ops used conn id=1 (reuse)
console.log(conns); // → [1, 1, 1, 1, 1]
```

<!-- @end -->

|                                                    |                                                        |
| -------------------------------------------------- | ------------------------------------------------------ |
| `Pool.make({ acquire, release, size, validate? })` | construct                                              |
| `pool.use(fn)`                                     | acquire → run → auto-release (LIFO release ordering)   |
| `pool.shutdown()`                                  | release idle, reject pending waiters with `PoolClosed` |
| `pool.inUse` / `pool.idle` / `pool.size`           | inspection                                             |

`validate?: (r) => Eff<boolean, never>` runs before handing a reused
resource to a caller — failed validation discards it (calls `release`)
and acquires fresh. Resources are returned on success, failure and interrupt,
also when a use is interrupted while validating or while releasing a rejected
resource.

`acquire` runs interruptibly, so an interrupted use can cancel a slow connect.
The pool cannot tell a cancellable create from one that goes on producing a
resource, such as a promise that resolves anyway: a resource that `acquire`
produces just as its use is interrupted never reaches the pool and is not
released. Keeping it would mean running every create to completion, holding a
slot for connects nobody waits for.

## Pitfalls

- **Defects don't retry / don't trip CircuitBreaker.** Use `fail()` for
  expected failures, not `throw`.
- **Singleflight followers suspend until the leader settles.** If the
  leader hangs, all followers hang. Combine with `timeout` / `race` to
  cap.
- **PubSub's slow-consumer policy is "block".** A slow subscriber blocks
  publishers when its queue fills (bounded). Use `unbounded` for
  fire-and-forget at the cost of memory.
- **Pool's `validate` runs ONLY on reuse.** A fresh `acquire` is trusted.
- **All primitives are interface-first** — their backend effect parameter defaults to
  `never`, while distributed implementations retain typed failures. `@spilne/perfect-redis`
  provides Redis-backed implementations with `Throws<RedisError>`.

## Distributed implementations

| Shared capability | Redis | PostgreSQL |
| --- | --- | --- |
| `Ref` | `RedisRef` / `RedisSubscriptionRef` | `PgRef` |
| `Queue` | `RedisQueue` | `PgQueue`, `PgmqQueue` |
| `PubSub` | `RedisPubSub`, `RedisChannel` | `PgChangeStream` (LISTEN/NOTIFY + polling) |
| `Semaphore`, `Latch`, `Barrier`, `Deferred` | Redis implementations | — |
| `RateLimiter`, `Throttle`, `Singleflight` | Redis implementations | PostgreSQL implementations |
| `CircuitBreaker`, `CacheStore` | Redis implementations | — |
| `StateBackend` / partitioned topology state | Redis implementations | PostgreSQL implementations |
| Leader election | — | `PgLeaderElection` |

Use [`@spilne/perfect-redis`](./17-distributed-backends.md#redis) for the broadest
distributed primitive set. Use
[`@spilne/perfect-postgres`](./17-distributed-backends.md#postgresql) when state,
queue acknowledgement, and sink publication need one PostgreSQL transaction.

## Next

- [Utilities — Duration, CacheStore](./12-utilities.md)
- [Resources and scopes](./07-resources-and-scopes.md)
- [Distributed backends](./17-distributed-backends.md)
