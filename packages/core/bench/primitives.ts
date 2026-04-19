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
  eff, succeed, sync, run, runSync,
  Ref, Deferred, Queue, Semaphore,
  CircuitBreaker, Singleflight, RateLimiter, Latch, Barrier,
  PubSub, SubscriptionRef,
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
  bench("perfect Ref + runSync", () => {
    runSync(
      Ref.make(0).flatMap((r) => {
        let e: any = succeed(undefined);
        for (let i = 0; i < N; i++) e = e.flatMap(() => r.set(i)).flatMap(() => r.get);
        return e;
      }),
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
  bench("perfect Deferred", async () => {
    for (let i = 0; i < N; i++) {
      const d = await run(Deferred.make<number>());
      await run(d.succeed(i));
      await run(d.await);
    }
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
      const p = new Promise<number>((r) => { resolve = r; });
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
  bench("perfect Semaphore.withPermit", async () => {
    const s = await run(Semaphore.make(10));
    for (let i = 0; i < S_N; i++) await run(s.withPermit(succeed(i)));
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
  bench("perfect CircuitBreaker.protect", async () => {
    const cb = CircuitBreaker.make({ failureThreshold: 100, resetTimeoutMs: 1000 });
    for (let i = 0; i < CB_N; i++) await run(cb.protect(succeed(i)));
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
  bench("perfect Singleflight.do (unique keys)", async () => {
    const sf = Singleflight.make();
    for (let i = 0; i < SF_N; i++) await run(sf.do(`k${i}`, succeed(i)));
  });

  bench("naive: bare effect (no dedup)", async () => {
    for (let i = 0; i < SF_N; i++) await run(succeed(i));
  });
});

// ── RateLimiter ─────────────────────────────────────────────────────

const RL_N = 500;
group(`RateLimiter — sliding-window tryAcquire × ${RL_N}`, () => {
  bench("perfect RateLimiter.tryAcquire (sliding)", async () => {
    const rl = await run(
      RateLimiter.slidingWindow({ limit: 10000, windowMs: 1000 }),
    );
    for (let i = 0; i < RL_N; i++) await run(rl.tryAcquire);
  });

  bench("perfect RateLimiter.tryAcquire (token-bucket)", async () => {
    const rl = await run(RateLimiter.tokenBucket({ limit: 10000, windowMs: 1000 }));
    for (let i = 0; i < RL_N; i++) await run(rl.tryAcquire);
  });

  bench("perfect RateLimiter.tryAcquire (fixed-window)", async () => {
    const rl = await run(RateLimiter.fixedWindow({ limit: 10000, windowMs: 1000 }));
    for (let i = 0; i < RL_N; i++) await run(rl.tryAcquire);
  });

  bench("naive: counter compare", () => {
    let count = 0;
    for (let i = 0; i < RL_N; i++) if (count < 10000) count++;
  });
});

// ── Latch ───────────────────────────────────────────────────────────

const L_N = 500;
group(`Latch — countDown all + await × ${L_N}`, () => {
  bench("perfect Latch", async () => {
    for (let i = 0; i < 50; i++) {
      const l = await run(Latch.make({ count: L_N / 50 }));
      for (let j = 0; j < L_N / 50; j++) await run(l.countDown);
      await run(l.await);
    }
  });
});

// ── Barrier ─────────────────────────────────────────────────────────

group(`Barrier — single party rounds × 100`, () => {
  bench("perfect Barrier (1 party, 100 rounds)", async () => {
    for (let i = 0; i < 100; i++) {
      const b = await run(Barrier.make({ parties: 1 }));
      await run(b.await);
    }
  });
});

// ── PubSub ──────────────────────────────────────────────────────────

const PS_N = 200;
group(`PubSub — publish to N subscribers × ${PS_N}`, () => {
  bench("perfect PubSub.publish (5 subscribers)", async () => {
    const ps = await run(PubSub.unbounded<number>());
    for (let i = 0; i < 5; i++) await run(ps.subscribe);
    for (let i = 0; i < PS_N; i++) await run(ps.publish(i));
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
  bench("perfect SubscriptionRef.set", async () => {
    const ref = await run(SubscriptionRef.make(0));
    for (let i = 0; i < SR_N; i++) await run(ref.set(i));
  });

  bench("perfect Ref.set (no notification)", async () => {
    const ref = await run(Ref.make(0));
    for (let i = 0; i < SR_N; i++) await run(ref.set(i));
  });
});

await mitataRun();
