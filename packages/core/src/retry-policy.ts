// Unified retry policy. Fluent builder over Schedule + additional metadata
// (predicates, hooks, time budget) that Schedule alone can't express.
//
// Usage:
//   retry(eff, RetryPolicy.exponential(100).withMaxRetries(5).withFullJitter())
//   retry(eff, RetryPolicy.recurs(3).whenError(e => e._tag === "Transient"))
//   retry(eff, RetryPolicy.spaced(1000).withTimeBudget(30_000).onRetry(d => Log.info(...)))

import { type Eff, Suspend, Op } from "./eff";
import { Cause } from "./cause";
import { clockNow } from "./clock";
import { type RetryConfig, sleep, succeed } from "./constructors";
import { Schedule, type RetryDetails } from "./schedule";

// ── Public: RetryDetails passed to onRetry hooks ────────────────────

export type { RetryDetails } from "./schedule";

// ── Internal policy config ──────────────────────────────────────────

interface PolicyImpl {
  readonly schedule: Schedule<unknown, unknown>;
  readonly whenError?: (error: unknown) => boolean;
  readonly whenCause?: (cause: Cause) => boolean;
  readonly onRetry?: (details: RetryDetails<Cause, unknown>) => Eff<void, unknown>;
  /** Wall-clock deadline in ms, anchored when the effect RUNS. See
   *  {@link RetryPolicy.withWallClockBudget}. */
  readonly wallClockBudgetMs?: number;
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

  /** Exponential backoff: initial * factor^attempt. */
  static exponential(options: { readonly initial: number; readonly factor?: number }): RetryPolicy;
  static exponential(initial: number, factor?: number): RetryPolicy;
  static exponential(
    optionsOrInitial: number | { readonly initial: number; readonly factor?: number },
    factor = 2,
  ): RetryPolicy {
    const initial =
      typeof optionsOrInitial === "number" ? optionsOrInitial : optionsOrInitial.initial;
    const resolvedFactor =
      typeof optionsOrInitial === "number" ? factor : (optionsOrInitial.factor ?? 2);
    return new RetryPolicy({ schedule: Schedule.exponential(initial, resolvedFactor) });
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

  /**
   * Build a policy from the declarative {@link RetryConfig} dict — the shape
   * accepted by `retry(eff, { times, delay, … })`, `.retry()` and
   * `Stream.retry()`. This is the single translation of that sugar; there is
   * no second retry implementation behind it.
   *
   * `maxDelay` is applied both before and after jitter, matching the config's
   * documented "never exceeds maxDelay" contract.
   */
  static fromConfig<E>(config: RetryConfig<E>): RetryPolicy {
    const {
      times,
      delay = 0,
      backoff = "fixed",
      maxDelay = 30_000,
      when,
      jitter = false,
      timeBudgetMs,
    } = config;

    // Both branches make the FIRST retry wait `delay`: Schedule.exponential's
    // step 0 is `delay * factor^0`.
    let policy =
      backoff === "exponential" ? RetryPolicy.exponential(delay, 2) : RetryPolicy.spaced(delay);

    policy = policy.withMaxDelay(maxDelay);
    if (jitter) policy = policy.withJitter(0.5, 1.5).withMaxDelay(maxDelay);
    policy = policy.withMaxRetries(times);
    if (when !== undefined) policy = policy.whenError(when);
    if (timeBudgetMs !== undefined && Number.isFinite(timeBudgetMs)) {
      policy = policy.withWallClockBudget(timeBudgetMs);
    }
    return policy;
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

  /**
   * Cap the cumulative scheduled *delay* across retries — the time spent
   * sleeping, ignoring how long each attempt itself takes. Purely a function
   * of the schedule, so it needs no Clock.
   *
   * Use {@link withWallClockBudget} when the attempts' own runtime should
   * count against the budget too.
   */
  withTimeBudget(ms: number): RetryPolicy {
    const bounded = Schedule.whileOutput(
      Schedule.cumulativeDelay(this.impl.schedule),
      (total) => total < ms,
    );
    return this.copy({ schedule: bounded });
  }

  /**
   * Cap total wall-clock time across all attempts — sleeping *and* the time
   * each attempt spends running. The deadline is anchored via the Clock
   * service when the effect RUNS, not when the policy is built, so a policy
   * can be constructed once at module load and reused.
   *
   * Checked before each retry: if the deadline has passed, the last cause is
   * re-failed instead of sleeping again. An attempt already in flight is not
   * interrupted — compose with `timeout()` for that.
   */
  withWallClockBudget(ms: number): RetryPolicy {
    return this.copy({ wallClockBudgetMs: ms });
  }

  /** Randomize each delay by a factor in [min, max], default ±20%. */
  withJitter(min = 0.8, max = 1.2): RetryPolicy {
    return this.copy({ schedule: Schedule.jittered(this.impl.schedule, min, max) });
  }

  /** Equal jitter: randomize each delay into its upper half. */
  withEqualJitter(): RetryPolicy {
    return this.withJitter(0.5, 1);
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
  const { schedule, whenError, whenCause, onRetry, wallClockBudgetMs } = policy.impl;

  function loop(state: any, attempts: number, deadline: number): Eff<A, S> {
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

      const giveUp = (output: unknown, upcomingDelayMs: number) => {
        if (onRetry) {
          return (
            onRetry({
              attempts: attempts + 1,
              upcomingDelayMs,
              input: cause,
              output,
              givingUp: true,
            }) as any
          ).flatMap(() => new Suspend(Op.Fail, cause, null)) as any;
        }
        return new Suspend(Op.Fail, cause, null);
      };

      const decision = schedule.step(cause as any, state);
      if (decision._tag === "Done") return giveUp(decision.output, 0);

      const next = () => {
        const wait =
          decision.delay > 0 ? (sleep(decision.delay) as any) : (succeed(undefined) as any);
        return (wait as any).flatMap(() => loop(decision.state, attempts + 1, deadline));
      };
      const retryNow = () => {
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
      };

      if (deadline === Infinity) return retryNow();
      // Budget is consulted before sleeping — an attempt already in flight is
      // never interrupted by it.
      return (clockNow as any).flatMap((now: number) =>
        now >= deadline ? giveUp(decision.output, decision.delay) : retryNow(),
      );
    }) as any;
  }

  if (wallClockBudgetMs === undefined) return loop(schedule.initial, 0, Infinity);
  // Anchor at RUN time, not build time, so a module-level policy can be reused.
  return (clockNow as any).flatMap((start: number) =>
    loop(schedule.initial, 0, start + wallClockBudgetMs),
  ) as any;
}
