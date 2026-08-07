// Concurrency primitives bench.
//
// One group per primitive. Each compares Perfect to:
//   - effect-ts equivalent (where one exists)
//   - naive baseline (raw JS / closure / Promise)
//
// Goal: surface any unexpected overhead and prove our primitives are in
// the same ballpark as effect-ts. If we drift dramatically slower, the
// bench tells us before users do.
//
// Run: bun packages/core/bench/primitives.ts

import { group, bench, run as mitataRun } from "mitata";
import {
  eff,
  succeed,
  run,
  runSync,
  Ref,
  Deferred,
  Queue,
  Semaphore,
  CircuitBreaker,
  Singleflight,
  RateLimiter,
  Latch,
  Barrier,
  PubSub,
  SubscriptionRef,
} from "../src";
import {
  Effect,
  Ref as EffRef,
  Deferred as EffDeferred,
  Queue as EffQueue,
  PubSub as EffPubSub,
} from "effect";

const N = 1000;

// ── Ref ─────────────────────────────────────────────────────────────

group(`Ref — get/set × ${N}`, () => {
  bench("perfect Ref (single fiber)", () => {
    runSync(
      eff(function* () {
        const r = yield* Ref.make(0);
        for (let i = 0; i < N; i++) {
          yield* r.set(i);
          yield* r.get;
        }
      }) as any,
    );
  });

  bench("effect Ref + runSync", () => {
    Effect.runSync(
      Effect.gen(function* () {
        const r = yield* EffRef.make(0);
        for (let i = 0; i < N; i++) {
          yield* EffRef.set(r, i);
          yield* EffRef.get(r);
        }
      }),
    );
  });

  bench("naive: closure variable + assign/read", () => {
    let v = 0;
    for (let i = 0; i < N; i++) {
      v = i;
      void v;
    }
  });
});

// ── Deferred ────────────────────────────────────────────────────────

group(`Deferred — make + succeed + await × ${N}`, () => {
  bench("perfect Deferred (single fiber)", async () => {
    await run(
      eff(function* () {
        for (let i = 0; i < N; i++) {
          const d = yield* Deferred.make<number>();
          yield* d.succeed(i);
          yield* d.await;
        }
      }) as any,
    );
  });

  bench("effect Deferred", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        for (let i = 0; i < N; i++) {
          const d = yield* EffDeferred.make<number>();
          yield* EffDeferred.succeed(d, i);
          yield* EffDeferred.await(d);
        }
      }),
    );
  });

  bench("naive: Promise resolve + await", async () => {
    for (let i = 0; i < N; i++) {
      let resolve: (n: number) => void;
      const p = new Promise<number>((r) => {
        resolve = r;
      });
      resolve!(i);
      await p;
    }
  });
});

// ── Queue ───────────────────────────────────────────────────────────

const Q_N = 500;
group(`Queue — offer + take × ${Q_N}`, () => {
  bench("perfect Queue (unbounded, single fiber)", async () => {
    await run(
      eff(function* () {
        const q = yield* Queue.unbounded<number>();
        for (let i = 0; i < Q_N; i++) yield* q.offer(i);
        for (let i = 0; i < Q_N; i++) yield* q.take();
      }) as any,
    );
  });

  bench("effect Queue (unbounded)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const q = yield* EffQueue.unbounded<number>();
        for (let i = 0; i < Q_N; i++) yield* EffQueue.offer(q, i);
        for (let i = 0; i < Q_N; i++) yield* EffQueue.take(q);
      }),
    );
  });

  bench("naive: Array push/shift", () => {
    const a: number[] = [];
    for (let i = 0; i < Q_N; i++) a.push(i);
    for (let i = 0; i < Q_N; i++) a.shift();
  });
});

// ── Semaphore ───────────────────────────────────────────────────────

const S_N = 500;
group(`Semaphore — withPermit × ${S_N}`, () => {
  bench("perfect Semaphore.withPermit (single fiber)", async () => {
    await run(
      eff(function* () {
        const s = yield* Semaphore.make(10);
        for (let i = 0; i < S_N; i++) yield* s.withPermit(succeed(i));
      }) as any,
    );
  });

  bench("naive: counter mutex (no real semaphore)", async () => {
    let avail = 10;
    for (let i = 0; i < S_N; i++) {
      avail--;
      await Promise.resolve(i);
      avail++;
    }
  });
});

// ── CircuitBreaker ──────────────────────────────────────────────────

const CB_N = 500;
group(`CircuitBreaker — protect (closed, all success) × ${CB_N}`, () => {
  bench("perfect CircuitBreaker.protect (single fiber)", async () => {
    const cb = CircuitBreaker.make({ failureThreshold: 100, resetTimeoutMs: 1000 });
    await run(
      eff(function* () {
        for (let i = 0; i < CB_N; i++) yield* cb.protect(succeed(i));
      }) as any,
    );
  });

  bench("naive: try/catch around fn (no breaker)", async () => {
    let consecutive = 0;
    for (let i = 0; i < CB_N; i++) {
      try {
        await Promise.resolve(i);
        consecutive = 0;
      } catch {
        consecutive++;
      }
    }
    void consecutive;
  });
});

// ── Singleflight ────────────────────────────────────────────────────

const SF_N = 200;
group(`Singleflight — do (different keys, no contention) × ${SF_N}`, () => {
  bench("perfect Singleflight.do (single fiber)", async () => {
    const sf = Singleflight.make();
    await run(
      eff(function* () {
        for (let i = 0; i < SF_N; i++) yield* sf.do(`k${i}`, succeed(i));
      }) as any,
    );
  });

  bench("naive: bare effect (single fiber, no dedup)", async () => {
    await run(
      eff(function* () {
        for (let i = 0; i < SF_N; i++) yield* succeed(i);
      }) as any,
    );
  });
});

// ── RateLimiter ─────────────────────────────────────────────────────

const RL_N = 500;
group(`RateLimiter — sliding-window tryAcquire × ${RL_N}`, () => {
  const benchStrategy = (
    name: string,
    factory: (opts: { limit: number; windowMs: number }) => any,
  ) =>
    bench(name, async () => {
      await run(
        eff(function* () {
          const rl = yield* factory({ limit: 10000, windowMs: 1000 });
          for (let i = 0; i < RL_N; i++) yield* rl.tryAcquire;
        }) as any,
      );
    });

  benchStrategy("perfect RateLimiter.tryAcquire (sliding)", RateLimiter.slidingWindow);
  benchStrategy("perfect RateLimiter.tryAcquire (token-bucket)", RateLimiter.tokenBucket);
  benchStrategy("perfect RateLimiter.tryAcquire (fixed-window)", RateLimiter.fixedWindow);

  bench("naive: counter compare", () => {
    let count = 0;
    for (let i = 0; i < RL_N; i++) if (count < 10000) count++;
  });
});

// ── Latch ───────────────────────────────────────────────────────────

const L_N = 500;
group(`Latch — countDown all + await × ${L_N}`, () => {
  bench("perfect Latch (single fiber)", async () => {
    await run(
      eff(function* () {
        for (let i = 0; i < 50; i++) {
          const l = yield* Latch.make({ count: L_N / 50 });
          for (let j = 0; j < L_N / 50; j++) yield* l.countDown;
          yield* l.await;
        }
      }) as any,
    );
  });
});

// ── Barrier ─────────────────────────────────────────────────────────

group(`Barrier — single party rounds × 100`, () => {
  bench("perfect Barrier (1 party, 100 rounds, single fiber)", async () => {
    await run(
      eff(function* () {
        for (let i = 0; i < 100; i++) {
          const b = yield* Barrier.make({ parties: 1 });
          yield* b.await;
        }
      }) as any,
    );
  });
});

// ── PubSub ──────────────────────────────────────────────────────────

const PS_N = 200;
group(`PubSub — publish to N subscribers × ${PS_N}`, () => {
  bench("perfect PubSub.publish (5 subscribers, single fiber)", async () => {
    await run(
      eff(function* () {
        const ps = yield* PubSub.unbounded<number>();
        for (let i = 0; i < 5; i++) yield* ps.subscribe;
        for (let i = 0; i < PS_N; i++) yield* ps.publish(i);
      }) as any,
    );
  });

  bench("effect PubSub.publish (5 subscribers)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const ps = yield* EffPubSub.unbounded<number>();
        for (let i = 0; i < 5; i++) yield* EffPubSub.subscribe(ps);
        for (let i = 0; i < PS_N; i++) yield* EffPubSub.publish(ps, i);
      }),
    );
  });

  bench("naive: array of callbacks", () => {
    const subs: ((n: number) => void)[] = [];
    for (let i = 0; i < 5; i++) subs.push(() => {});
    for (let i = 0; i < PS_N; i++) for (const s of subs) s(i);
  });
});

// ── SubscriptionRef ─────────────────────────────────────────────────

const SR_N = 200;
group(`SubscriptionRef — set × ${SR_N}`, () => {
  bench("perfect SubscriptionRef.set (single fiber)", async () => {
    await run(
      eff(function* () {
        const ref = yield* SubscriptionRef.make(0);
        for (let i = 0; i < SR_N; i++) yield* ref.set(i);
      }) as any,
    );
  });

  bench("perfect Ref.set (no notification, single fiber)", async () => {
    await run(
      eff(function* () {
        const ref = yield* Ref.make(0);
        for (let i = 0; i < SR_N; i++) yield* ref.set(i);
      }) as any,
    );
  });
});

await mitataRun();
