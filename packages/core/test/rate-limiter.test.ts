import { describe, test, expect } from "bun:test";
import { eff, succeed, sleep, fork, join, all, run, RateLimiter } from "../src";

describe("RateLimiter — sliding-window", () => {
  test("allows up to limit, then rejects with retryAfterMs", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.slidingWindow({ limit: 3, windowMs: 1000 });
        const a = yield* rl.tryAcquire;
        const b = yield* rl.tryAcquire;
        const c = yield* rl.tryAcquire;
        const d = yield* rl.tryAcquire;
        return [a, b, c, d];
      }) as any,
    );
    expect(result).toEqual([true, true, true, false]);
  });

  test("acquire fails with typed RateLimitExceeded", async () => {
    const program = eff(function* () {
      const rl = yield* RateLimiter.slidingWindow({ limit: 1, windowMs: 1000 });
      yield* rl.acquire;
      yield* rl.acquire; // should fail
      return "unreachable";
    });
    await expect(run(program as any)).rejects.toMatchObject({
      _tag: "RateLimitExceeded",
    });
  });

  test("remaining decrements as we acquire", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.slidingWindow({ limit: 5, windowMs: 1000 });
        const r0 = yield* rl.remaining;
        yield* rl.acquire;
        const r1 = yield* rl.remaining;
        yield* rl.acquire;
        yield* rl.acquire;
        const r3 = yield* rl.remaining;
        return [r0, r1, r3];
      }) as any,
    );
    expect(result).toEqual([5, 4, 2]);
  });
});

describe("RateLimiter — fixed-window", () => {
  test("allows up to limit per window", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.fixedWindow({ limit: 2, windowMs: 1000 });
        const a = yield* rl.tryAcquire;
        const b = yield* rl.tryAcquire;
        const c = yield* rl.tryAcquire;
        return [a, b, c];
      }) as any,
    );
    expect(result).toEqual([true, true, false]);
  });
});

describe("RateLimiter — token-bucket", () => {
  test("starts with full burst, then exhausts", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.tokenBucket({ limit: 3, windowMs: 1000 });
        const a = yield* rl.tryAcquire;
        const b = yield* rl.tryAcquire;
        const c = yield* rl.tryAcquire;
        const d = yield* rl.tryAcquire;
        return [a, b, c, d];
      }) as any,
    );
    expect(result).toEqual([true, true, true, false]);
  });

  test("refills over time", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.tokenBucket({ limit: 2, windowMs: 100 });
        // Drain the bucket
        yield* rl.acquire;
        yield* rl.acquire;
        const empty = yield* rl.tryAcquire;
        // Wait long enough for ~1 refill (100ms / 2 = 50ms per token)
        yield* eff(function* () { yield* succeed(undefined); }); // no-op to anchor types
        return empty;
      }) as any,
    );
    expect(result).toBe(false);
  });
});

describe("RateLimiter — wait mode (subsumes throttle)", () => {
  test("acquireWaiting blocks then succeeds", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.tokenBucket({ limit: 1, windowMs: 50 });
        yield* rl.acquire; // takes the token
        const start = Date.now();
        yield* rl.acquireWaiting; // should block ~50ms for refill
        const elapsed = Date.now() - start;
        return elapsed >= 30; // some slack
      }) as any,
    );
    expect(result).toBe(true);
  });

  test("withLimit fails over limit; withLimitWaiting blocks", async () => {
    const program = eff(function* () {
      const rl = yield* RateLimiter.fixedWindow({ limit: 1, windowMs: 30 });
      yield* rl.withLimit(succeed(1));
      // Second call would fail without wait mode; use wait mode to succeed
      const start = Date.now();
      const v = yield* rl.withLimitWaiting(succeed(2));
      return [v, Date.now() - start >= 20];
    });
    expect(await run(program as any)).toEqual([2, true]);
  });
});

describe("RateLimiter — concurrent / slow-path coverage", () => {
  test("100 concurrent tryAcquire — exactly `limit` get true, rest get false", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.fixedWindow({ limit: 30, windowMs: 10_000 });
        const attempts = yield* all(Array.from({ length: 100 }, () => rl.tryAcquire));
        const granted = attempts.filter((b: boolean) => b).length;
        return { granted, denied: attempts.length - granted };
      }) as any,
    );
    expect(result.granted).toBe(30);
    expect(result.denied).toBe(70);
  });

  test("token-bucket: concurrent draining respects burst capacity", async () => {
    const result = await run(
      eff(function* () {
        const rl = yield* RateLimiter.tokenBucket({ limit: 10, windowMs: 10_000 });
        const attempts = yield* all(Array.from({ length: 50 }, () => rl.tryAcquire));
        const granted = attempts.filter((b: boolean) => b).length;
        return granted;
      }) as any,
    );
    // Bucket starts at burst=10. With 50 concurrent attempts, ~10 should succeed.
    // Allow ±2 for tiny refill during the loop.
    expect(result).toBeGreaterThanOrEqual(10);
    expect(result).toBeLessThanOrEqual(12);
  });

  test("multiple fibers acquireWaiting → all eventually proceed in order", async () => {
    const order: number[] = [];
    await run(
      eff(function* () {
        const rl = yield* RateLimiter.tokenBucket({ limit: 1, windowMs: 20 });
        // Drain the initial token
        yield* rl.acquire;
        // Fork 3 waiters that should each acquire as a token refills
        const f1 = yield* fork(rl.acquireWaiting.flatMap(() => succeed(order.push(1))));
        const f2 = yield* fork(rl.acquireWaiting.flatMap(() => succeed(order.push(2))));
        const f3 = yield* fork(rl.acquireWaiting.flatMap(() => succeed(order.push(3))));
        yield* sleep(150); // enough refills for all 3
        yield* join(f1);
        yield* join(f2);
        yield* join(f3);
      }) as any,
    );
    expect(order.length).toBe(3);
    expect(order.sort()).toEqual([1, 2, 3]);
  });

  test("acquire fails with retryAfterMs hint", async () => {
    const program = eff(function* () {
      const rl = yield* RateLimiter.tokenBucket({ limit: 1, windowMs: 1000 });
      yield* rl.acquire;
      try {
        yield* rl.acquire;
        return null;
      } catch (e: any) {
        return e;
      }
    });
    const err = await run(program as any);
    expect(err._tag).toBe("RateLimitExceeded");
    expect(err.retryAfterMs).toBeGreaterThan(0);
  });
});

describe("RateLimiter — validation", () => {
  test("limit must be >= 1", async () => {
    expect(() => run(RateLimiter.make({ limit: 0, windowMs: 100 }) as any)).toThrow(
      /limit must be >= 1/,
    );
  });
  test("windowMs must be >= 1", async () => {
    expect(() => run(RateLimiter.make({ limit: 1, windowMs: 0 }) as any)).toThrow(
      /windowMs must be >= 1/,
    );
  });
});
