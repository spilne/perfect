// ---------------------------------------------------------------------------
// PgThrottle — Postgres-backed sliding-window throttle (blocks until a slot)
//
// Thin adapter over PgRateLimiter, mirroring how @perfect/core's Throttle
// wraps its in-process RateLimiter: acquire ≡ acquireWaiting, withPermit ≡
// withLimitWaiting. Adapted from promin's Promise-typed Throttle
// (acquireAsync/withPermitAsync + per-call resource suffix).
// ---------------------------------------------------------------------------

import type { Eff, Throttle, Throws } from "@perfect/core";
import { type DrizzleDb } from "./drizzle-db";
import { PgRateLimiter } from "./pg-rate-limiter";
import type { PostgresError } from "./postgres-error";

export interface PgThrottleConfig {
  db: DrizzleDb;
  key: string;
  permits: number;
  windowMs: number;
  table?: string;
}

export class PgThrottle implements Throttle<Throws<PostgresError>> {
  private readonly rl: PgRateLimiter;

  constructor(config: PgThrottleConfig) {
    this.rl = new PgRateLimiter({
      db: config.db,
      key: config.key,
      limit: config.permits,
      windowMs: config.windowMs,
      table: config.table ?? "perfect_throttle",
    });
  }

  /** Create and wait for the backing table to exist. */
  static async create(config: PgThrottleConfig): Promise<PgThrottle> {
    const t = new PgThrottle(config);
    await t.rl["_ensureReady"]();
    return t;
  }

  get acquire(): Eff<void, Throws<PostgresError>> {
    return this.rl.acquireWaiting;
  }

  get tryAcquire(): Eff<boolean, Throws<PostgresError>> {
    return this.rl.tryAcquire;
  }

  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<PostgresError>> {
    return this.rl.withLimitWaiting(eff);
  }

  get remaining(): Eff<number, Throws<PostgresError>> {
    return this.rl.remaining;
  }

  get nextSlotIn(): Eff<number, Throws<PostgresError>> {
    return this.rl.nextSlotIn;
  }
}
