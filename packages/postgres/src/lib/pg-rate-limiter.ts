// ---------------------------------------------------------------------------
// PgRateLimiter — Postgres-backed sliding-window rate limiter
//
// Implements @perfect/core's Eff-typed RateLimiter contract on top of a
// Promise-based Postgres driver: one row per granted slot, expired rows
// swept inside the acquire transaction. Driver rejections are typed
// PostgresError failures; over-limit is RateLimitExceeded.
//
// Adapted from promin's Promise-typed RateLimiter (acquireAsync/…): the
// per-call `resource` suffix is gone — perfect's contract keys the limiter
// once at construction. Create one PgRateLimiter per resource key instead.
// ---------------------------------------------------------------------------

import { fail, fromPromise, sleep, succeed, sync } from "@perfect/core";
import type { Eff, Throws, RateLimitExceeded, RateLimiter } from "@perfect/core";
import { sql } from "drizzle-orm";
import { type DrizzleDb, execRaw } from "./drizzle-db";
import { PostgresError, toPostgresError } from "./postgres-error";

export interface PgRateLimiterConfig {
  db: DrizzleDb;
  key: string;
  limit: number;
  windowMs: number;
  table?: string;
}

/**
 * Sliding-window decision math, pure — exported for fake-clock tests.
 * `oldestTs` is the MIN(ts) of active slots (null when the window is empty).
 */
export function slidingWindowDecision(params: {
  count: number;
  oldestTs: number | null;
  now: number;
  limit: number;
  windowMs: number;
}): { granted: boolean; retryAfterMs: number } {
  if (params.count < params.limit) return { granted: true, retryAfterMs: 0 };
  const oldest = params.oldestTs ?? params.now;
  return { granted: false, retryAfterMs: Math.max(1, oldest + params.windowMs - params.now) };
}

export class PgRateLimiter implements RateLimiter<Throws<PostgresError>> {
  private readonly db: DrizzleDb;
  private readonly key: string;
  private readonly limit: number;
  private readonly windowMs: number;
  private readonly table: string;
  private setupPromise: Promise<void> | null = null;

  constructor(config: PgRateLimiterConfig) {
    this.db = config.db;
    this.key = config.key;
    this.limit = config.limit;
    this.windowMs = config.windowMs;
    this.table = config.table ?? "perfect_rate_limit";
    this.setupPromise = this._setup();
    this.setupPromise.catch(() => {});
  }

  /** Create and wait for the backing table to exist. */
  static async create(config: PgRateLimiterConfig): Promise<PgRateLimiter> {
    const rl = new PgRateLimiter(config);
    await rl._ensureReady();
    return rl;
  }

  private async _setup(): Promise<void> {
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS ${this.table} (
          key TEXT NOT NULL,
          ts BIGINT NOT NULL
        )
      `),
    );
    await this.db.execute(
      sql.raw(`
        CREATE INDEX IF NOT EXISTS ${this.table}_key_ts_idx ON ${this.table} (key, ts)
      `),
    );
  }

  private async _ensureReady(): Promise<void> {
    if (this.setupPromise) {
      await this.setupPromise;
      this.setupPromise = null;
    }
  }

  /** Try to acquire once. Returns 0 if granted, or ms to wait until a slot opens. */
  private async _tryAcquireOnce(): Promise<number> {
    await this._ensureReady();
    const table = this.table;
    const key = this.key;
    const limit = this.limit;
    const windowMs = this.windowMs;

    let decision = { granted: false, retryAfterMs: 0 };

    await this.db.transaction(async (tx) => {
      const db = tx as DrizzleDb;

      // Delete expired entries
      await db.execute(
        sql`DELETE FROM ${sql.raw(table)} WHERE key = ${key} AND ts <= (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT - ${windowMs}`,
      );

      // Count active entries
      const countRows = await execRaw(
        db,
        sql`SELECT COUNT(*) AS cnt FROM ${sql.raw(table)} WHERE key = ${key}`,
      );
      const count = Number(countRows[0]?.cnt ?? 0);

      if (count < limit) {
        await execRaw(
          db,
          sql`INSERT INTO ${sql.raw(table)} (key, ts) VALUES (${key}, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT)`,
        );
        decision = { granted: true, retryAfterMs: 0 };
      } else {
        const oldestRows = await execRaw(
          db,
          sql`SELECT MIN(ts) AS oldest FROM ${sql.raw(table)} WHERE key = ${key}`,
        );
        const oldest = oldestRows[0]?.oldest != null ? Number(oldestRows[0].oldest) : null;
        decision = slidingWindowDecision({
          count,
          oldestTs: oldest,
          now: Date.now(),
          limit,
          windowMs,
        });
      }
    });

    return decision.granted ? 0 : decision.retryAfterMs;
  }

  private async _activeStats(): Promise<{ count: number; oldest: number | null }> {
    await this._ensureReady();
    const rows = await execRaw(
      this.db,
      sql`SELECT COUNT(*) AS cnt, MIN(ts) AS oldest FROM ${sql.raw(this.table)} WHERE key = ${this.key} AND ts > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT - ${this.windowMs}`,
    );
    return {
      count: Number(rows[0]?.cnt ?? 0),
      oldest: rows[0]?.oldest != null ? Number(rows[0].oldest) : null,
    };
  }

  // ---------------------------------------------------------------------------
  // RateLimiter (Eff-typed contract)
  // ---------------------------------------------------------------------------

  get acquire(): Eff<void, Throws<PostgresError> | Throws<RateLimitExceeded>> {
    return fromPromise(
      () => this._tryAcquireOnce(),
      (e) => toPostgresError("rateLimiter.acquire", e),
    ).flatMap((waitMs) =>
      waitMs === 0
        ? succeed(undefined)
        : fail<RateLimitExceeded>({ _tag: "RateLimitExceeded", retryAfterMs: waitMs }),
    );
  }

  get tryAcquire(): Eff<boolean, Throws<PostgresError>> {
    return fromPromise(
      () => this._tryAcquireOnce(),
      (e) => toPostgresError("rateLimiter.tryAcquire", e),
    ).map((waitMs) => waitMs === 0);
  }

  get acquireWaiting(): Eff<void, Throws<PostgresError>> {
    const loop = (): Eff<void, Throws<PostgresError>> =>
      fromPromise(
        () => this._tryAcquireOnce(),
        (e) => toPostgresError("rateLimiter.acquireWaiting", e),
      ).flatMap((waitMs) =>
        waitMs === 0 ? sync(() => undefined) : sleep(waitMs).flatMap(() => loop()),
      );
    return loop();
  }

  withLimit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<PostgresError> | Throws<RateLimitExceeded>> {
    return (this.acquire as any).flatMap(() => eff) as any;
  }

  withLimitWaiting<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<PostgresError>> {
    return (this.acquireWaiting as any).flatMap(() => eff) as any;
  }

  get remaining(): Eff<number, Throws<PostgresError>> {
    return fromPromise(
      () => this._activeStats(),
      (e) => toPostgresError("rateLimiter.remaining", e),
    ).map((s) => Math.max(0, this.limit - s.count));
  }

  get resetAt(): Eff<number, Throws<PostgresError>> {
    return fromPromise(
      () => this._activeStats(),
      (e) => toPostgresError("rateLimiter.resetAt", e),
    ).map((s) => (s.count === 0 || s.oldest === null ? Date.now() : s.oldest + this.windowMs));
  }

  get nextSlotIn(): Eff<number, Throws<PostgresError>> {
    return fromPromise(
      () => this._activeStats(),
      (e) => toPostgresError("rateLimiter.nextSlotIn", e),
    ).map((s) => {
      const d = slidingWindowDecision({
        count: s.count,
        oldestTs: s.oldest,
        now: Date.now(),
        limit: this.limit,
        windowMs: this.windowMs,
      });
      return d.granted ? 0 : d.retryAfterMs;
    });
  }
}
