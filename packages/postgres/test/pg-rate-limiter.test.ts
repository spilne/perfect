import { describe, it, expect } from "bun:test";
import { run } from "@spilne/perfect-core";
import { PgRateLimiter, slidingWindowDecision } from "../src/lib/pg-rate-limiter";
import { fakeDb } from "./fake-db";

// ---------------------------------------------------------------------------
// slidingWindowDecision — pure window math (fake-clock)
// ---------------------------------------------------------------------------

describe("slidingWindowDecision", () => {
  it("grants while under the limit", () => {
    const d = slidingWindowDecision({ count: 1, oldestTs: 0, now: 1000, limit: 2, windowMs: 100 });
    expect(d).toEqual({ granted: true, retryAfterMs: 0 });
  });

  it("rejects at the limit with retryAfter = oldest + window - now", () => {
    const now = 10_000;
    const oldest = now - 40; // oldest slot taken 40ms ago in a 100ms window
    const d = slidingWindowDecision({ count: 2, oldestTs: oldest, now, limit: 2, windowMs: 100 });
    expect(d.granted).toBe(false);
    expect(d.retryAfterMs).toBe(60);
  });

  it("clamps retryAfter to at least 1ms when the oldest slot is about to expire", () => {
    const now = 10_000;
    const oldest = now - 100; // exactly at window edge
    const d = slidingWindowDecision({ count: 2, oldestTs: oldest, now, limit: 2, windowMs: 100 });
    expect(d.granted).toBe(false);
    expect(d.retryAfterMs).toBe(1);
  });

  it("falls back to now when oldest is unknown", () => {
    const d = slidingWindowDecision({
      count: 3,
      oldestTs: null,
      now: 5000,
      limit: 3,
      windowMs: 250,
    });
    expect(d).toEqual({ granted: false, retryAfterMs: 250 });
  });
});

// ---------------------------------------------------------------------------
// PgRateLimiter — Eff contract wired over a scripted fake db
// ---------------------------------------------------------------------------

describe("PgRateLimiter (fake db)", () => {
  it("tryAcquire grants and inserts a slot when under the limit", async () => {
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("COUNT(*)")) return [{ cnt: "1" }];
      return [];
    });
    const rl = new PgRateLimiter({ db, key: "api", limit: 2, windowMs: 100 });

    const granted = await run(rl.tryAcquire);
    expect(granted).toBe(true);
    expect(fake.allSql).toContain("INSERT INTO perfect_rate_limit");
  });

  it("acquire fails with typed RateLimitExceeded when at the limit", async () => {
    const oldest = Date.now() - 40;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("COUNT(*)")) return [{ cnt: "2" }];
      if (sql.includes("MIN(ts)")) return [{ oldest: String(oldest) }];
      return [];
    });
    const rl = new PgRateLimiter({ db, key: "api", limit: 2, windowMs: 100 });

    const exit = await run(rl.acquire.either());
    expect(exit._tag).toBe("Left");
    if (exit._tag === "Left") {
      expect(exit.left._tag).toBe("RateLimitExceeded");
      expect(exit.left.retryAfterMs).toBeGreaterThanOrEqual(1);
      expect(exit.left.retryAfterMs).toBeLessThanOrEqual(100);
    }
    // Rejected — no slot row inserted
    expect(fake.allSql).not.toContain("INSERT INTO perfect_rate_limit (key, ts) VALUES");
  });

  it("remaining subtracts active slots from the limit", async () => {
    const { db } = fakeDb((sql) => {
      if (sql.includes("COUNT(*)")) return [{ cnt: "3", oldest: "0" }];
      return [];
    });
    const rl = new PgRateLimiter({ db, key: "api", limit: 5, windowMs: 100 });
    expect(await run(rl.remaining)).toBe(2);
  });

  it("nextSlotIn is 0 when a slot is free", async () => {
    const { db } = fakeDb((sql) => {
      if (sql.includes("COUNT(*)")) return [{ cnt: "0", oldest: null }];
      return [];
    });
    const rl = new PgRateLimiter({ db, key: "api", limit: 1, windowMs: 100 });
    expect(await run(rl.nextSlotIn)).toBe(0);
  });
});
