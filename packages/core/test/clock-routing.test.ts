// Time-gated primitives must read the Clock service, not Date.now() —
// a TestClock drives them deterministically with zero real waiting.

import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  provide,
  run,
  runSync,
  runExit,
  retry,
  Clock,
  TestClock,
  Cause,
} from "../src";
import { RateLimiter } from "../src/rate-limiter";
import { CircuitBreaker } from "../src/circuit-breaker";
import { CacheStore } from "../src/cache-store";

const tick = () => new Promise((r) => setTimeout(r, 0));

describe("RateLimiter under TestClock", () => {
  test("fixed window resets on virtual time, no real waiting", async () => {
    const c = new TestClock();

    const make = provide(RateLimiter.fixedWindow({ limit: 2, windowMs: 1000 }) as any, Clock, c);
    const limiter = await run(make as any);

    const acquire = () => run(provide((limiter as any).tryAcquire, Clock, c) as any);

    expect(await acquire()).toBe(true);
    expect(await acquire()).toBe(true);
    expect(await acquire()).toBe(false);

    c.advance(1001);
    expect(await acquire()).toBe(true);
  });

  test("token bucket refills on virtual time", async () => {
    const c = new TestClock();
    const limiter: any = await run(
      provide(RateLimiter.tokenBucket({ limit: 2, windowMs: 100 }) as any, Clock, c) as any,
    );
    const acquire = () => run(provide(limiter.tryAcquire, Clock, c) as any);

    expect(await acquire()).toBe(true);
    expect(await acquire()).toBe(true);
    expect(await acquire()).toBe(false);

    c.advance(100); // one full refill period
    expect(await acquire()).toBe(true);
  });
});

describe("CircuitBreaker under TestClock", () => {
  test("open → half-open transition follows virtual time", async () => {
    const c = new TestClock();
    const breaker = CircuitBreaker.make({ failureThreshold: 1, resetTimeoutMs: 5000 });

    const protectedFail = provide(breaker.protect(fail("boom")) as any, Clock, c);
    const protectedOk = provide(breaker.protect(succeed("ok")) as any, Clock, c);

    // trip it
    await expect(run(protectedFail as any)).rejects.toBe("boom");
    expect(runSync(breaker.state)).toBe("open");

    // still open — rejects fast with CircuitOpen
    const exit = await runExit(provide(breaker.protect(succeed("nope")) as any, Clock, c) as any);
    expect(exit._tag).toBe("Failure");

    // advance past the reset timeout: next protect probes (half-open) and closes
    c.advance(5001);
    expect(await run(protectedOk as any)).toBe("ok");
    expect(runSync(breaker.state)).toBe("closed");
  });
});

describe("CacheStore under TestClock", () => {
  test("TTL expiry follows virtual time", async () => {
    const c = new TestClock();
    const store = CacheStore.memory<string, number>({ ttlMs: 100 });

    await run(provide(store.set("k", 42) as any, Clock, c) as any);
    expect(await run(provide(store.get("k") as any, Clock, c) as any)).toBe(42);

    c.advance(101);
    expect(await run(provide(store.get("k") as any, Clock, c) as any)).toBeUndefined();
    expect(await run(provide(store.has("k") as any, Clock, c) as any)).toBe(false);
  });
});

describe("retry time budget under TestClock", () => {
  test("deadline anchors at run time and reads virtual time", async () => {
    const c = new TestClock();
    let attempts = 0;

    const failing = sync(() => {
      attempts++;
    }).flatMap(() => fail(`attempt-${attempts}`));

    const program = provide(
      retry(failing as any, { times: 100, delay: 60, timeBudgetMs: 100 }) as any,
      Clock,
      c,
    );

    // build long before "running" — budget must not start counting yet
    c.advance(10_000);

    const done = runExit(program as any);
    // drive the retry sleeps: each advance fires the pending sleep(60)
    for (let i = 0; i < 5; i++) {
      await tick();
      c.advance(60);
    }
    const exit = await done;

    expect(exit._tag).toBe("Failure");
    // attempts at t0, t60, t120 (virtual). The budget check after the t120
    // failure sees 120 >= 100 and stops — exactly 3 attempts. Were the
    // deadline anchored at build time (t=-10000), attempt 1 would already
    // be past budget and there would be no retries at all.
    expect(attempts).toBe(3);
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBe("attempt-3");
    }
  });
});

describe("stream time ops under TestClock", () => {
  test("throttle spaces emissions on virtual time", async () => {
    const { Stream } = await import("../src");
    const c = new TestClock();

    const done = run(provide((Stream.of(1, 2, 3) as any).throttle(100).toArray(), Clock, c) as any);
    // item 1 emits at t=0; items 2 and 3 wait on virtual sleeps
    for (let i = 0; i < 6; i++) {
      await tick();
      c.advance(100);
    }
    expect(await done).toEqual([1, 2, 3]);
    expect(c.now()).toBeLessThanOrEqual(600);
  });

  test("debounce emits after virtual quiet period", async () => {
    const { Stream, Queue } = await import("../src");
    const c = new TestClock();

    const program = (Queue.unbounded<number>() as any).flatMap((q: any) =>
      q
        .offer(1)
        .flatMap(() => q.offer(2))
        .flatMap(() => {
          const s = (Stream.fromQueue(q) as any).debounce(1000);
          return s.take(1).toArray();
        }),
    );

    const done = run(provide(program, Clock, c) as any);
    // let the consumer drain both offers and start its quiet-period wait
    await tick();
    await tick();
    c.advance(1001);
    expect(await done).toEqual([2]);
  });
});
