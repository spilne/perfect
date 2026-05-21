// Concurrency primitives: CircuitBreaker, Singleflight, RateLimiter,
// Latch, Barrier, PubSub, SubscriptionRef, Pool.
//
// Run: bun packages/core/examples/14-primitives.ts

import {
  eff, succeed, fail, sync, sleep, fork, join, all, run,
  CircuitBreaker, Singleflight, RateLimiter, Latch, Barrier,
  PubSub, SubscriptionRef, Pool,
  type Eff, type Throws,
} from "../src";
import { assertEq } from "./_assert";

// >>> example: circuit-breaker
// 3-state breaker — Closed → Open after `failureThreshold` failures.
// While Open, calls reject fast with typed `CircuitOpen` error.
const cb = CircuitBreaker.make<string>({
  failureThreshold: 3,
  resetTimeoutMs: 1000,
});

// Wrap any effect with .protect()
const safeCall = (n: number): Eff<number, Throws<string | { _tag: "CircuitOpen" }>> =>
  cb.protect(succeed(n * 2));

assertEq(await (safeCall(21) as any).run(), 42);
// <<< example

// >>> example: singleflight
// Deduplicate concurrent calls with the same key — leader runs the work
// once, followers wait and receive the same result.
const sf = Singleflight.make();
let fetchCount = 0;
const fetchUser = (id: number) =>
  sf.do(`user:${id}`, eff(function* () {
    yield* sleep(10);
    fetchCount++;
    return { id, name: `user-${id}` };
  }));

// Five concurrent fetches for the same user → one execution
const users = await all([fetchUser(7), fetchUser(7), fetchUser(7), fetchUser(7), fetchUser(7)]).run();
assertEq(fetchCount, 1);
assertEq(users[0]!.id, 7);
// <<< example

// >>> example: rate-limiter
// Three strategies. tryAcquire returns boolean; acquireWaiting blocks.
const rl = await RateLimiter.tokenBucket({ limit: 5, windowMs: 1000 }).run();

// Try 10 acquires — first 5 succeed, rest get false
const attempts = await (eff(function* () {
  const results: boolean[] = [];
  for (let i = 0; i < 10; i++) results.push(yield* rl.tryAcquire);
  return results;
}) as any).run();
assertEq(attempts.filter(Boolean).length, 5);
// <<< example

// >>> example: latch
// CountDownLatch — N parties decrement, awaiters release when count hits 0.
const events: string[] = [];
await (eff(function* () {
  const ready = yield* Latch.make({ count: 3 });

  // Awaiter blocks until the 3 parties have arrived
  const watcher = yield* fork(
    ready.await.flatMap(() => sync(() => { events.push("released"); })),
  );

  // Three workers count down at different times
  yield* fork(sleep(10).flatMap(() => ready.countDown));
  yield* fork(sleep(20).flatMap(() => ready.countDown));
  yield* fork(sleep(30).flatMap(() => ready.countDown));

  yield* join(watcher);
}) as any).run();
assertEq(events, ["released"]);
// <<< example

// >>> example: barrier
// CyclicBarrier — N parties block until all have arrived, then all proceed.
const arrived: number[] = [];
await (eff(function* () {
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
  yield* join(f1); yield* join(f2); yield* join(f3);
}) as any).run();
assertEq(arrived.sort(), [1, 2, 3]);
// <<< example

// >>> example: pubsub
// Broadcast channel — every subscriber gets every message.
const seen: number[][] = [[], [], []];
await (eff(function* () {
  const pubsub = yield* PubSub.unbounded<number>();
  const subA = yield* pubsub.subscribe;
  const subB = yield* pubsub.subscribe;
  const subC = yield* pubsub.subscribe;
  const fA = yield* fork(subA.take(3).forEach((n) => sync(() => { seen[0]!.push(n); })));
  const fB = yield* fork(subB.take(3).forEach((n) => sync(() => { seen[1]!.push(n); })));
  const fC = yield* fork(subC.take(3).forEach((n) => sync(() => { seen[2]!.push(n); })));
  yield* sleep(5); // let subscribers register
  yield* pubsub.publish(1);
  yield* pubsub.publish(2);
  yield* pubsub.publish(3);
  yield* join(fA); yield* join(fB); yield* join(fC);
}) as any).run();
assertEq(seen[0], [1, 2, 3]);
assertEq(seen[1], [1, 2, 3]);
assertEq(seen[2], [1, 2, 3]);
// <<< example

// >>> example: subscription-ref
// Ref<A> + change Stream — reactive cell. `changes` emits current value
// first, then every subsequent set/update.
const observed: string[] = [];
await (eff(function* () {
  const config = yield* SubscriptionRef.make("v1");
  const stream = yield* config.changes;
  const reader = yield* fork(stream.take(3).forEach((v) => sync(() => { observed.push(v); })));
  yield* sleep(5);
  yield* config.set("v2");
  yield* config.update((v) => `${v}-patched`);
  yield* join(reader);
}) as any).run();
assertEq(observed, ["v1", "v2", "v2-patched"]);
// <<< example

// >>> example: pool
// Resource pool with reuse. Acquire blocks at capacity, hands off to
// waiters on release.
let connId = 0;
const conns: number[] = [];
await (eff(function* () {
  const pool = yield* Pool.make({
    acquire: sync(() => ({ id: ++connId })),
    release: () => sync(() => undefined),
    size: 2,
  });

  // Use the pool 5 times sequentially — each call reuses the same conn
  for (let i = 0; i < 5; i++) {
    yield* pool.use((c) => sync(() => { conns.push(c.id); }));
  }
}) as any).run();
// All 5 ops used conn id=1 (reuse)
assertEq(conns, [1, 1, 1, 1, 1]);
// <<< example
