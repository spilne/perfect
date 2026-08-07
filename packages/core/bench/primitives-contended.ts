// Contended primitives bench — measures real-world scenarios where multiple
// fibers race on the primitive. Hot-path (single-fiber) numbers live in
// `primitives.ts`; this file is for the slow path (waiters, contention).
//
// Run: bun packages/core/bench/primitives-contended.ts

import { group, bench, run as mitataRun } from "mitata";
import {
  eff,
  succeed,
  sleep,
  fork,
  join,
  all,
  run,
  Deferred,
  Queue,
  Semaphore,
  CircuitBreaker,
  Singleflight,
  RateLimiter,
  Latch,
  Barrier,
  PubSub,
} from "../src";

// ── Queue: producers + consumers ──────────────────────────────────

group("Queue: 4 producers × 250 + 4 consumers × 250 (bounded 8)", () => {
  bench("perfect Queue (contended)", async () => {
    await run(
      eff(function* () {
        const q = yield* Queue.bounded<number>(8);
        const producer = (start: number) =>
          eff(function* () {
            for (let i = 0; i < 250; i++) yield* q.offer(start + i);
          });
        const consumer = eff(function* () {
          for (let i = 0; i < 250; i++) yield* q.take();
        });
        // Fork sequentially — `all([fork, fork, ...])` runs the forks in
        // child fibers, which then get interrupted when the all() completes.
        const p1 = yield* fork(producer(0));
        const p2 = yield* fork(producer(1000));
        const p3 = yield* fork(producer(2000));
        const p4 = yield* fork(producer(3000));
        const c1 = yield* fork(consumer);
        const c2 = yield* fork(consumer);
        const c3 = yield* fork(consumer);
        const c4 = yield* fork(consumer);
        yield* join(p1);
        yield* join(p2);
        yield* join(p3);
        yield* join(p4);
        yield* join(c1);
        yield* join(c2);
        yield* join(c3);
        yield* join(c4);
      }) as any,
    );
  });
});

// ── Semaphore: contended permits ──────────────────────────────────

group("Semaphore: 16 fibers × 100 acquire/release (4 permits)", () => {
  bench("perfect Semaphore.withPermit", async () => {
    await run(
      eff(function* () {
        const sem = yield* Semaphore.make(4);
        const worker = eff(function* () {
          for (let i = 0; i < 100; i++) yield* sem.withPermit(succeed(i));
        });
        const fibers: any[] = [];
        for (let i = 0; i < 16; i++) fibers.push(yield* fork(worker));
        for (const f of fibers) yield* join(f);
      }) as any,
    );
  });
});

// ── Deferred: 1 producer + 50 awaiters ────────────────────────────

group("Deferred: 1 producer + 50 awaiters", () => {
  bench("perfect Deferred (50 awaiters)", async () => {
    await run(
      eff(function* () {
        const d = yield* Deferred.make<number>();
        const awaiter = eff(function* () {
          yield* d.await;
        });
        const fibers: any[] = [];
        for (let i = 0; i < 50; i++) fibers.push(yield* fork(awaiter));
        yield* sleep(0); // let awaiters register
        yield* d.succeed(42);
        for (const f of fibers) yield* join(f);
      }) as any,
    );
  });
});

// ── CircuitBreaker: 100 concurrent .protect (closed, all success) ─

group("CircuitBreaker: 100 concurrent .protect (closed, all success)", () => {
  bench("perfect CircuitBreaker", async () => {
    await run(
      eff(function* () {
        const cb = CircuitBreaker.make({ failureThreshold: 1000, resetTimeoutMs: 10_000 });
        yield* all(Array.from({ length: 100 }, (_, i) => cb.protect(succeed(i))));
      }) as any,
    );
  });
});

// ── Singleflight: 100 callers / 5 distinct keys ───────────────────

group("Singleflight: 100 callers / 5 distinct keys (20× dedup factor)", () => {
  bench("perfect Singleflight.do", async () => {
    await run(
      eff(function* () {
        const sf = Singleflight.make();
        // Each call: key picked round-robin from 5; sleep then succeed
        const work = (i: number) =>
          sf.do(
            `k${i % 5}`,
            sleep(1).flatMap(() => succeed(i)),
          );
        yield* all(Array.from({ length: 100 }, (_, i) => work(i)));
      }) as any,
    );
  });

  bench("naive: bare effect (no dedup)", async () => {
    await run(
      eff(function* () {
        yield* all(Array.from({ length: 100 }, (_, i) => sleep(1).flatMap(() => succeed(i))));
      }) as any,
    );
  });
});

// ── RateLimiter: 100 concurrent tryAcquire ─────────────────────────

group("RateLimiter: 100 concurrent tryAcquire (limit 30)", () => {
  const benchStrat = (name: string, factory: (opts: { limit: number; windowMs: number }) => any) =>
    bench(name, async () => {
      await run(
        eff(function* () {
          const rl = yield* factory({ limit: 30, windowMs: 10_000 });
          yield* all(Array.from({ length: 100 }, () => rl.tryAcquire));
        }) as any,
      );
    });

  benchStrat("perfect sliding-window", RateLimiter.slidingWindow);
  benchStrat("perfect token-bucket", RateLimiter.tokenBucket);
  benchStrat("perfect fixed-window", RateLimiter.fixedWindow);
});

// ── Latch: 100 awaiters + 1 countDown ─────────────────────────────

group("Latch: 100 awaiters + countDown(100)", () => {
  bench("perfect Latch", async () => {
    await run(
      eff(function* () {
        const latch = yield* Latch.make({ count: 1 });
        const fibers: any[] = [];
        for (let i = 0; i < 100; i++) fibers.push(yield* fork(latch.await));
        yield* sleep(0);
        yield* latch.countDown;
        for (const f of fibers) yield* join(f);
      }) as any,
    );
  });
});

// ── Barrier: 32-party coordination ─────────────────────────────────

group("Barrier: 32-party coordination", () => {
  bench("perfect Barrier", async () => {
    await run(
      eff(function* () {
        const barrier = yield* Barrier.make({ parties: 32 });
        const fibers: any[] = [];
        for (let i = 0; i < 32; i++) fibers.push(yield* fork(barrier.await));
        for (const f of fibers) yield* join(f);
      }) as any,
    );
  });
});

// ── PubSub: 1 publisher + 10 subscribers + 200 messages ────────────

group("PubSub: 1 publisher + 10 subscribers + 200 messages", () => {
  bench("perfect PubSub", async () => {
    await run(
      eff(function* () {
        const ps = yield* PubSub.bounded<number>(1024);
        const subs: any[] = [];
        for (let i = 0; i < 10; i++) {
          const stream = yield* ps.subscribe;
          subs.push(yield* fork(stream.take(200).drain()));
        }
        yield* sleep(1); // let subscribers register
        for (let i = 0; i < 200; i++) yield* ps.publish(i);
        for (const f of subs) yield* join(f);
      }) as any,
    );
  });
});

await mitataRun();
