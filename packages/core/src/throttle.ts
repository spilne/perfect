// Throttle — N permits per sliding window, callers always BLOCK until a
// permit opens. This is a thin alias over RateLimiter (sliding-window +
// wait-mode). Provided for API familiarity (matches promin's Throttle).
//
// Functionally equivalent to:
//   const rl = RateLimiter.slidingWindow({ limit: permits, windowMs });
//   await run(rl.acquireWaiting);  // ≡ throttle.acquire
//
// Use Throttle when "always block" is the right semantic (e.g. respecting
// an external API's documented rate limit). Use RateLimiter directly when
// you want fail-fast OR wait-mode at different call sites.

import { type Eff } from "./eff";
import { type RateLimiter, RateLimiter as RateLimiterNS } from "./rate-limiter";

export interface Throttle<S = never> {
  /** Block until a permit is available. */
  readonly acquire: Eff<void, S>;
  /** Try to acquire a permit without blocking. */
  readonly tryAcquire: Eff<boolean, S>;
  /** Acquire then run (blocking). */
  withPermit<A, S2>(eff: Eff<A, S2>): Eff<A, S | S2>;
  /** Permits remaining in the current window. */
  readonly remaining: Eff<number, S>;
  /** Milliseconds until the next permit opens. */
  readonly nextSlotIn: Eff<number, S>;
}

class ThrottleAdapter implements Throttle {
  constructor(private readonly rl: RateLimiter) {}
  get acquire(): Eff<void, never> {
    return this.rl.acquireWaiting;
  }
  get tryAcquire(): Eff<boolean, never> {
    return this.rl.tryAcquire;
  }
  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S> {
    return this.rl.withLimitWaiting(eff);
  }
  get remaining(): Eff<number, never> {
    return this.rl.remaining;
  }
  get nextSlotIn(): Eff<number, never> {
    return this.rl.nextSlotIn;
  }
}

export const Throttle = {
  /** N permits per sliding window. */
  make(opts: { permits: number; windowMs: number }): Eff<Throttle, never> {
    return RateLimiterNS.slidingWindow({ limit: opts.permits, windowMs: opts.windowMs }).map(
      (rl) => new ThrottleAdapter(rl) as Throttle,
    );
  },
} as const;
