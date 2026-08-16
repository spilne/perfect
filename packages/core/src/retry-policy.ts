// Unified retry policy. Fluent builder over Schedule + additional metadata
// (predicates, hooks, time budget) that Schedule alone can't express.
//
// Usage:
//   retry(eff, RetryPolicy.exponential(100).withMaxRetries(5).withFullJitter())
//   retry(eff, RetryPolicy.recurs(3).whenError(e => e._tag === "Transient"))
//   retry(eff, RetryPolicy.spaced(1000).withTimeBudget(30_000).onRetry(d => Log.info(...)))

import { type Eff, Suspend, Op } from "./eff";
import { Cause } from "./cause";
import { sleep, succeed } from "./constructors";
import { Schedule, type RetryDetails } from "./schedule";

// ── Public: RetryDetails passed to onRetry hooks ────────────────────

export type { RetryDetails } from "./schedule";

// ── Internal policy config ──────────────────────────────────────────

interface PolicyImpl {
  readonly schedule: Schedule<unknown, unknown>;
  readonly whenError?: (error: unknown) => boolean;
  readonly whenCause?: (cause: Cause) => boolean;
  readonly onRetry?: (details: RetryDetails<Cause, unknown>) => Eff<void, unknown>;
  // Time budget is applied by intersecting a whileOutput(cumulativeDelay, < budget)
  // onto the schedule at build time. Stored so multiple withTimeBudget() calls
  // are idempotent (the last one wins rather than compounding).
  readonly timeBudgetMs?: number;
}

// ── RetryPolicy class ──────────────────────────────────────────────

export class RetryPolicy {
  private constructor(readonly impl: PolicyImpl) {}

  // ── Constructors (base schedules) ─────────────────────────────────

  /** Retry up to N times with zero delay between attempts. */
  static recurs(n: number): RetryPolicy {
    return new RetryPolicy({ schedule: Schedule.recurs(n) });
  }

  /** Retry forever with a fixed delay between attempts. */
  static spaced(ms: number): RetryPolicy {
    return new RetryPolicy({ schedule: Schedule.spaced(ms) });
  }

  /** Alias for spaced. */
  static constant(ms: number): RetryPolicy {
    return RetryPolicy.spaced(ms);
  }

  /** Exponential backoff: base * factor^attempt. */
  static exponential(base: number, factor = 2): RetryPolicy {
    return new RetryPolicy({ schedule: Schedule.exponential(base, factor) });
  }

  /** Fibonacci backoff: base * fib(attempt). Smoother than exponential. */
  static fibonacci(base: number): RetryPolicy {
    return new RetryPolicy({ schedule: Schedule.fibonacci(base) });
  }

  /** Linear backoff: base * (attempt + 1). */
  static linear(base: number): RetryPolicy {
    return new RetryPolicy({ schedule: Schedule.linear(base) });
  }

  /** Retry forever with zero delay. Combine with other builders. */
  static forever: RetryPolicy = new RetryPolicy({ schedule: Schedule.forever });

  /** Never retry. */
  static none: RetryPolicy = new RetryPolicy({ schedule: Schedule.recurs(0) });

  /** Wrap an existing Schedule. */
  static fromSchedule<In, Out>(s: Schedule<In, Out>): RetryPolicy {
    return new RetryPolicy({ schedule: s as Schedule<unknown, unknown> });
  }

  // ── Fluent modifiers ──────────────────────────────────────────────

  /** Cap the number of retries at n. Intersects with the current schedule. */
  withMaxRetries(n: number): RetryPolicy {
    return this.copy({ schedule: Schedule.intersect(this.impl.schedule, Schedule.recurs(n)) });
  }

  /** Cap individual delay durations at `ms`. */
  withMaxDelay(ms: number): RetryPolicy {
    return this.copy({ schedule: Schedule.maxDelay(this.impl.schedule, ms) });
  }

  /** Total wall-clock budget across all retries; fail once exceeded. */
  withTimeBudget(ms: number): RetryPolicy {
    // whileOutput on a cumulative-delay schedule terminates when total exceeds budget.
    const bounded = Schedule.whileOutput(
      Schedule.cumulativeDelay(this.impl.schedule),
      (total) => total < ms,
    );
    return this.copy({ schedule: bounded, timeBudgetMs: ms });
  }

  /** Equal jitter: random factor in [min, max], default ±20%. */
  withJitter(min = 0.8, max = 1.2): RetryPolicy {
    return this.copy({ schedule: Schedule.jittered(this.impl.schedule, min, max) });
  }

  /** Full jitter: each delay is random in [0, scheduled_delay]. */
  withFullJitter(): RetryPolicy {
    return this.copy({ schedule: Schedule.fullJitter(this.impl.schedule) });
  }

  /** Only retry when the typed error predicate returns true. */
  whenError<E>(pred: (error: E) => boolean): RetryPolicy {
    return this.copy({ whenError: pred as any });
  }

  /** Defect-aware: retry decisions based on the full Cause. */
  whenCause(pred: (cause: Cause) => boolean): RetryPolicy {
    return this.copy({ whenCause: pred });
  }

  /** Hook fired before each retry (and when giving up) — for logging/metrics. */
  onRetry(fn: (details: RetryDetails<Cause, unknown>) => Eff<void, unknown>): RetryPolicy {
    return this.copy({ onRetry: fn });
  }

  // ── Composition ────────────────────────────────────────────────────

  /** Intersect with another policy: both must agree to continue. */
  and(other: RetryPolicy): RetryPolicy {
    return this.copy({ schedule: Schedule.intersect(this.impl.schedule, other.impl.schedule) });
  }

  /** Union with another policy: either can keep going. */
  or(other: RetryPolicy): RetryPolicy {
    return this.copy({ schedule: Schedule.union(this.impl.schedule, other.impl.schedule) });
  }

  // ── internal ────────────────────────────────────────────────────────

  private copy(patch: Partial<PolicyImpl>): RetryPolicy {
    return new RetryPolicy({ ...this.impl, ...patch });
  }
}

// ── Applier ─────────────────────────────────────────────────────────

export function runRetry<A, S>(eff: Eff<A, S>, policy: RetryPolicy): Eff<A, S> {
  const { schedule, whenError, whenCause, onRetry } = policy.impl;

  function loop(state: any, attempts: number): Eff<A, S> {
    return new Suspend(Op.CatchAll, eff, (cause: Cause) => {
      // Defect/interrupt gate (unless whenCause explicitly allows):
      const firstFail = Cause.firstFail(cause);
      if (whenCause) {
        if (!whenCause(cause)) return new Suspend(Op.Fail, cause, null);
      } else {
        if (firstFail === null) return new Suspend(Op.Fail, cause, null);
        if (whenError && !whenError(firstFail.value)) {
          return new Suspend(Op.Fail, cause, null);
        }
      }

      const decision = schedule.step(cause as any, state);
      if (decision._tag === "Done") {
        if (onRetry) {
          return (
            onRetry({
              attempts: attempts + 1,
              upcomingDelayMs: 0,
              input: cause,
              output: decision.output,
              givingUp: true,
            }) as any
          ).flatMap(() => new Suspend(Op.Fail, cause, null)) as any;
        }
        return new Suspend(Op.Fail, cause, null);
      }

      const next = () => {
        const wait =
          decision.delay > 0 ? (sleep(decision.delay) as any) : (succeed(undefined) as any);
        return (wait as any).flatMap(() => loop(decision.state, attempts + 1));
      };
      if (onRetry) {
        return (
          onRetry({
            attempts: attempts + 1,
            upcomingDelayMs: decision.delay,
            input: cause,
            output: decision.output,
            givingUp: false,
          }) as any
        ).flatMap(next) as any;
      }
      return next();
    }) as any;
  }
  return loop(schedule.initial, 0);
}
