import { expect, test } from "bun:test";
import { Clock, TestClock, RateLimiter, provide, runSync, type Eff } from "../src";

test("sliding windows expire at the boundary without consuming slots on inspection", () => {
  const clock = new TestClock();
  const run = <A>(eff: Eff<A, never>) => runSync(provide(eff, Clock, clock));
  const limiter = run(RateLimiter.slidingWindow({ limit: 2, windowMs: 100 }));
  expect(run(limiter.tryAcquire)).toBe(true);
  clock.advance(50);
  expect(run(limiter.tryAcquire)).toBe(true);
  for (let i = 0; i < 10; i++) {
    expect(run(limiter.remaining)).toBe(0);
    expect(run(limiter.resetAt)).toBe(100);
    expect(run(limiter.nextSlotIn)).toBe(50);
  }
  expect(run(limiter.tryAcquire)).toBe(false);
  clock.advance(50);
  expect(run(limiter.remaining)).toBe(1);
  expect(run(limiter.nextSlotIn)).toBe(0);
  expect(run(limiter.tryAcquire)).toBe(true);
  clock.advance(50);
  expect(run(limiter.remaining)).toBe(1);
  clock.advance(50);
  expect(run(limiter.remaining)).toBe(2);
  expect(run(limiter.resetAt)).toBe(200);
});

test("sliding windows retain live entries across repeated queue compaction", () => {
  const clock = new TestClock();
  const run = <A>(eff: Eff<A, never>) => runSync(provide(eff, Clock, clock));
  const limiter = run(RateLimiter.slidingWindow({ limit: 4096, windowMs: 4096 }));
  const acquire = limiter.tryAcquire;
  for (let i = 0; i < 20_000; i++) {
    expect(run(acquire)).toBe(true);
    if (i >= 4095) {
      expect(run(acquire)).toBe(false);
      expect(run(limiter.nextSlotIn)).toBe(1);
    }
    clock.advance(1);
  }
  expect(run(limiter.remaining)).toBe(1);
});

test("sliding windows keep expiration ordered when wall time moves backwards", () => {
  let now = 50;
  const clock: Clock = { now: () => now, sleep: (ms) => new TestClock().sleep(ms) };
  const run = <A>(eff: Eff<A, never>) => runSync(provide(eff, Clock, clock));
  const limiter = run(RateLimiter.slidingWindow({ limit: 2, windowMs: 100 }));
  expect(run(limiter.tryAcquire)).toBe(true);
  now = 0;
  expect(run(limiter.tryAcquire)).toBe(true);
  expect(run(limiter.nextSlotIn)).toBe(100);
  now = 100;
  expect(run(limiter.remaining)).toBe(1);
  expect(run(limiter.resetAt)).toBe(150);
});
