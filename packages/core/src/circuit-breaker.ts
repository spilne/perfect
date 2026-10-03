// CircuitBreaker — classic 3-state breaker.
//
//   Closed → Open: after `failureThreshold` consecutive failures
//   Open → HalfOpen: after `resetTimeoutMs` elapses
//   HalfOpen: exactly one call (the "probe") is let through to test the
//             dependency; other calls are rejected with CircuitOpen until
//             the probe finishes
//   HalfOpen → Closed: on first success
//   HalfOpen → Open: on failure; resets the timer
//
// While Open, calls reject fast with a typed `CircuitOpen` failure.
// Defects (uncaught throws) propagate as defects and DO NOT trip the
// breaker. Use `isFailure` to filter which typed failures count.
//
// The `CircuitBreaker` interface is the contract; this module ships an
// in-process implementation. Distributed implementations (Redis-backed,
// Postgres-backed, etc.) live in other @spilne/perfect-* or downstream packages —
// they implement the same interface and are a drop-in swap.

import { type Eff, type Throws } from "./eff.js";
import { ensuring, fail, sync } from "./constructors.js";
import { Clock } from "./clock.js";

export type CircuitState = "closed" | "open" | "half-open";

export interface CircuitOpen {
  readonly _tag: "CircuitOpen";
  readonly openedAt: number;
  readonly resetAtMs: number;
}

export interface CircuitBreakerOptions<E = unknown> {
  /** Consecutive failures before opening. */
  readonly failureThreshold: number;
  /** How long to stay open before transitioning to half-open (ms). */
  readonly resetTimeoutMs: number;
  /** Optional: only count typed failures matching this predicate. */
  readonly isFailure?: (e: E) => boolean;
}

/**
 * CircuitBreaker contract — implementations may be in-process (this module's
 * default) or distributed (Redis, Postgres, etc.) supplied by downstream
 * packages. Either way, callers depend on this interface.
 */
export interface CircuitBreaker<E = unknown, S = never> {
  /** Current state — `"closed" | "open" | "half-open"`. */
  readonly state: Eff<CircuitState, S>;
  /** Consecutive failures since last success. */
  readonly failures: Eff<number, S>;
  /**
   * Wrap an effect with breaker semantics.
   * If state is Open: rejects fast with CircuitOpen.
   * Otherwise: runs the effect; on success → close; on failure → count.
   */
  protect<A, S2>(eff: Eff<A, S2 | Throws<E>>): Eff<A, S | S2 | Throws<E | CircuitOpen>>;
  /** Manually reset to Closed. */
  reset(): Eff<void, S>;
}

// ── In-process implementation ──────────────────────────────────────

interface InternalState {
  state: CircuitState;
  consecutiveFailures: number;
  openedAt: number;
}

class InProcessCircuitBreaker<E> implements CircuitBreaker<E> {
  private internal: InternalState = {
    state: "closed",
    consecutiveFailures: 0,
    openedAt: 0,
  };

  // Time source for the plain (non-Eff) `state` getter. Updated to the
  // context Clock on every protect() run, so a breaker driven under a
  // TestClock reads consistent virtual time from the getter too. Falls
  // back to wall time before the first protect().
  private nowFn: () => number = () => Date.now();

  // True while the one half-open test call is running. Without this, every
  // call that arrived while half-open went through at once, so a dependency
  // that was just recovering got hit by the whole backlog.
  private probeInFlight = false;

  constructor(private readonly opts: CircuitBreakerOptions<E>) {}

  get state(): Eff<CircuitState, never> {
    return sync(() => {
      this.maybeTransitionToHalfOpen(this.nowFn());
      return this.internal.state;
    });
  }

  get failures(): Eff<number, never> {
    return sync(() => this.internal.consecutiveFailures);
  }

  protect<A, S>(eff: Eff<A, S | Throws<E>>): Eff<A, S | Throws<E | CircuitOpen>> {
    return (Clock.get as any).flatMap((clock: Clock) => {
      this.nowFn = () => clock.now();
      const openErr = this.checkOpen(clock.now());
      if (openErr !== null) return fail(openErr) as any;
      const isProbe = this.internal.state === "half-open";
      if (isProbe) {
        if (this.probeInFlight) return fail(this.openError()) as any;
        this.probeInFlight = true;
      }
      const guarded = (eff as any)
        .flatMap((value: A) => sync(() => this.recordSuccess()).map(() => value))
        .catch((e: E) =>
          sync(() => this.recordFailure(e, clock.now())).flatMap(() => fail(e) as any),
        );
      // However the probe ends (success, failure, defect, interrupt), the
      // next call may probe again if we are still half-open.
      return isProbe
        ? ensuring(
            guarded,
            sync(() => {
              this.probeInFlight = false;
            }),
          )
        : guarded;
    }) as any;
  }

  reset(): Eff<void, never> {
    return sync(() => {
      this.internal = { state: "closed", consecutiveFailures: 0, openedAt: 0 };
      this.probeInFlight = false;
    });
  }

  private maybeTransitionToHalfOpen(now: number): void {
    if (this.internal.state !== "open") return;
    if (now - this.internal.openedAt >= this.opts.resetTimeoutMs) {
      this.internal.state = "half-open";
    }
  }

  private checkOpen(now: number): CircuitOpen | null {
    this.maybeTransitionToHalfOpen(now);
    return this.internal.state === "open" ? this.openError() : null;
  }

  private openError(): CircuitOpen {
    return {
      _tag: "CircuitOpen",
      openedAt: this.internal.openedAt,
      resetAtMs: this.internal.openedAt + this.opts.resetTimeoutMs,
    };
  }

  private recordSuccess(): void {
    this.internal.state = "closed";
    this.internal.consecutiveFailures = 0;
    this.internal.openedAt = 0;
  }

  private recordFailure(e: E, now: number): void {
    if (this.opts.isFailure && !this.opts.isFailure(e)) return;
    if (this.internal.state === "half-open") {
      this.internal.state = "open";
      this.internal.openedAt = now;
      return;
    }
    this.internal.consecutiveFailures++;
    if (this.internal.consecutiveFailures >= this.opts.failureThreshold) {
      this.internal.state = "open";
      this.internal.openedAt = now;
    }
  }
}

// ── Namespace + factory ────────────────────────────────────────────

export const CircuitBreaker = {
  /**
   * Construct an in-process CircuitBreaker. Distributed backends (Redis,
   * Postgres) are provided by downstream packages — they expose their own
   * factory that returns the same `CircuitBreaker<E>` interface.
   */
  make<E = unknown>(opts: CircuitBreakerOptions<E>): CircuitBreaker<E> {
    if (opts.failureThreshold < 1) {
      throw new Error("CircuitBreaker: failureThreshold must be >= 1");
    }
    if (opts.resetTimeoutMs < 0) {
      throw new Error("CircuitBreaker: resetTimeoutMs must be >= 0");
    }
    return new InProcessCircuitBreaker<E>(opts);
  },
} as const;
