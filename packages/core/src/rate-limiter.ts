// RateLimiter — three pluggable strategies + wait-mode (subsumes throttle).
//
// Strategies:
//   - sliding-window: true rate over moving window (most accurate)
//   - fixed-window:   simpler, allows bursts at window boundaries
//   - token-bucket:   smooth rate with burst capacity
//
// Modes:
//   - .acquire / .withLimit(eff)  — fail with RateLimitExceeded (typed)
//   - .acquireWaiting / .withLimitWaiting(eff) — block until token is available
//
// The wait-mode replaces what promin called Throttle.
//
// Eff-typed contract; in-process by default. Distributed RateLimiters
// (Redis-backed shared limits) implement the same interface.

import { type Eff, type Throws } from "./eff";
import { sync, fail, sleep } from "./constructors";
import { type Ref, Ref as RefNS } from "./ref";

export type RateLimitStrategy = "sliding-window" | "fixed-window" | "token-bucket";

export interface RateLimitExceeded {
  readonly _tag: "RateLimitExceeded";
  readonly retryAfterMs: number;
}

export interface RateLimiterOptions {
  /** Max calls per window (or token-bucket capacity). */
  readonly limit: number;
  /** Window duration in ms (also: token refill period). */
  readonly windowMs: number;
  /** Defaults to "sliding-window". */
  readonly strategy?: RateLimitStrategy;
}

export interface RateLimiter {
  /** Take one slot. Fails with RateLimitExceeded if over limit. */
  readonly acquire: Eff<void, Throws<RateLimitExceeded>>;
  /** Try to take a slot — returns true if obtained, false otherwise. */
  readonly tryAcquire: Eff<boolean, never>;
  /** Take one slot, BLOCKING until available (wait-mode / "throttle"). */
  readonly acquireWaiting: Eff<void, never>;
  /** Run an effect within the rate limit. Fails with RateLimitExceeded if over. */
  withLimit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<RateLimitExceeded>>;
  /** Run an effect within the rate limit, BLOCKING until a slot is free. */
  withLimitWaiting<A, S>(eff: Eff<A, S>): Eff<A, S>;
  /** Remaining capacity right now. */
  readonly remaining: Eff<number, never>;
  /** Approximate timestamp (ms since epoch) when limit fully resets. */
  readonly resetAt: Eff<number, never>;
  /** Milliseconds until the next slot opens (0 if a slot is available now). */
  readonly nextSlotIn: Eff<number, never>;
}

// ── Internal state shapes ──────────────────────────────────────────

type SlidingWindow = { _tag: "sliding-window"; timestamps: number[] };
type FixedWindow = { _tag: "fixed-window"; windowStart: number; count: number };
type TokenBucket = { _tag: "token-bucket"; tokens: number; lastRefill: number };
type State = SlidingWindow | FixedWindow | TokenBucket;

type AcquireResult = { _tag: "ok" } | { _tag: "rejected"; retryAfterMs: number };

// ── Strategy logic — pure functions over State + now → (result, nextState) ──

function tryAcquireState(
  s: State,
  now: number,
  limit: number,
  windowMs: number,
  dryRun: boolean,
): [AcquireResult, State] {
  switch (s._tag) {
    case "sliding-window": {
      const cutoff = now - windowMs;
      const active = s.timestamps.filter((t) => t > cutoff);
      if (active.length < limit) {
        const next = dryRun ? active : [...active, now];
        return [{ _tag: "ok" }, { _tag: "sliding-window", timestamps: next }];
      }
      const oldest = active[0]!;
      return [
        { _tag: "rejected", retryAfterMs: oldest + windowMs - now },
        { _tag: "sliding-window", timestamps: active },
      ];
    }
    case "fixed-window": {
      const windowEnd = s.windowStart + windowMs;
      if (now >= windowEnd) {
        return [{ _tag: "ok" }, { _tag: "fixed-window", windowStart: now, count: dryRun ? 0 : 1 }];
      }
      if (s.count < limit) {
        return [
          { _tag: "ok" },
          {
            _tag: "fixed-window",
            windowStart: s.windowStart,
            count: dryRun ? s.count : s.count + 1,
          },
        ];
      }
      return [{ _tag: "rejected", retryAfterMs: windowEnd - now }, s];
    }
    case "token-bucket": {
      const elapsed = now - s.lastRefill;
      const refillRate = limit / windowMs;
      const refilled = Math.min(limit, s.tokens + elapsed * refillRate);
      if (refilled >= 1) {
        return [
          { _tag: "ok" },
          {
            _tag: "token-bucket",
            tokens: dryRun ? refilled : refilled - 1,
            lastRefill: now,
          },
        ];
      }
      const retryAfterMs = Math.ceil((1 - refilled) / refillRate);
      return [
        { _tag: "rejected", retryAfterMs },
        { _tag: "token-bucket", tokens: refilled, lastRefill: now },
      ];
    }
  }
}

function computeRemaining(s: State, now: number, limit: number, windowMs: number): number {
  switch (s._tag) {
    case "sliding-window": {
      const active = s.timestamps.filter((t) => t > now - windowMs);
      return Math.max(0, limit - active.length);
    }
    case "fixed-window": {
      if (now >= s.windowStart + windowMs) return limit;
      return Math.max(0, limit - s.count);
    }
    case "token-bucket": {
      const elapsed = now - s.lastRefill;
      const refillRate = limit / windowMs;
      return Math.min(limit, Math.floor(s.tokens + elapsed * refillRate));
    }
  }
}

function computeResetAt(s: State, now: number, limit: number, windowMs: number): number {
  switch (s._tag) {
    case "sliding-window": {
      const active = s.timestamps.filter((t) => t > now - windowMs);
      if (active.length === 0) return now;
      return active[0]! + windowMs;
    }
    case "fixed-window":
      return s.windowStart + windowMs;
    case "token-bucket": {
      const elapsed = now - s.lastRefill;
      const refillRate = limit / windowMs;
      const current = Math.min(limit, s.tokens + elapsed * refillRate);
      if (current >= limit) return now;
      return now + (limit - current) / refillRate;
    }
  }
}

function makeInitialState(strategy: RateLimitStrategy, limit: number, now: number): State {
  switch (strategy) {
    case "sliding-window":
      return { _tag: "sliding-window", timestamps: [] };
    case "fixed-window":
      return { _tag: "fixed-window", windowStart: now, count: 0 };
    case "token-bucket":
      return { _tag: "token-bucket", tokens: limit, lastRefill: now };
  }
}

// ── In-process implementation ──────────────────────────────────────

class InProcessRateLimiter implements RateLimiter {
  constructor(
    private readonly limit: number,
    private readonly windowMs: number,
    private readonly state: Ref<State>,
  ) {}

  private get tryAcquireOnce(): Eff<AcquireResult, never> {
    return this.state.modify((s) => {
      const now = Date.now();
      const [result, next] = tryAcquireState(s, now, this.limit, this.windowMs, false);
      return [result, next];
    });
  }

  get acquire(): Eff<void, Throws<RateLimitExceeded>> {
    return (this.tryAcquireOnce as any).flatMap((r: AcquireResult) =>
      r._tag === "ok"
        ? sync(() => undefined)
        : (fail({ _tag: "RateLimitExceeded", retryAfterMs: r.retryAfterMs }) as any),
    ) as Eff<void, Throws<RateLimitExceeded>>;
  }

  get tryAcquire(): Eff<boolean, never> {
    return (this.tryAcquireOnce as any).map((r: AcquireResult) => r._tag === "ok");
  }

  get acquireWaiting(): Eff<void, never> {
    const loop = (): Eff<void, never> =>
      (this.tryAcquireOnce as any).flatMap((r: AcquireResult) =>
        r._tag === "ok" ? sync(() => undefined) : sleep(r.retryAfterMs).flatMap(() => loop()),
      );
    return loop();
  }

  withLimit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<RateLimitExceeded>> {
    return (this.acquire as any).flatMap(() => eff) as any;
  }

  withLimitWaiting<A, S>(eff: Eff<A, S>): Eff<A, S> {
    return (this.acquireWaiting as any).flatMap(() => eff) as any;
  }

  get remaining(): Eff<number, never> {
    return (this.state.get as any).map((s: State) =>
      computeRemaining(s, Date.now(), this.limit, this.windowMs),
    );
  }

  get resetAt(): Eff<number, never> {
    return (this.state.get as any).map((s: State) =>
      computeResetAt(s, Date.now(), this.limit, this.windowMs),
    );
  }

  get nextSlotIn(): Eff<number, never> {
    return (this.state.get as any).map((s: State) => {
      const now = Date.now();
      const [result] = tryAcquireState(s, now, this.limit, this.windowMs, true);
      return result._tag === "ok" ? 0 : result.retryAfterMs;
    });
  }
}

export const RateLimiter = {
  /** Default — sliding window (most accurate). */
  make(opts: RateLimiterOptions): Eff<RateLimiter, never> {
    if (opts.limit < 1) throw new Error("RateLimiter.make: limit must be >= 1");
    if (opts.windowMs < 1) throw new Error("RateLimiter.make: windowMs must be >= 1");
    const strategy = opts.strategy ?? "sliding-window";
    return RefNS.make<State>(makeInitialState(strategy, opts.limit, Date.now())).map(
      (state) => new InProcessRateLimiter(opts.limit, opts.windowMs, state) as RateLimiter,
    );
  },
  /** Convenience: sliding-window strategy. */
  slidingWindow(opts: { limit: number; windowMs: number }): Eff<RateLimiter, never> {
    return RateLimiter.make({ ...opts, strategy: "sliding-window" });
  },
  /** Convenience: fixed-window strategy. */
  fixedWindow(opts: { limit: number; windowMs: number }): Eff<RateLimiter, never> {
    return RateLimiter.make({ ...opts, strategy: "fixed-window" });
  },
  /** Convenience: token-bucket. limit = burst capacity, windowMs = refill period. */
  tokenBucket(opts: { limit: number; windowMs: number }): Eff<RateLimiter, never> {
    return RateLimiter.make({ ...opts, strategy: "token-bucket" });
  },
} as const;
