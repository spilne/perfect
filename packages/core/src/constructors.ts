import { Cause } from "./cause";
import { type Eff, type Throws, Suspend, Op } from "./eff";
import { Fiber } from "./fiber";
import { type Exit, Exit as ExitNS } from "./exit";

export function succeed<A>(value: A): Eff<A, never> {
  return new Suspend(Op.Succeed, value, null) as any;
}

export function fail<E>(error: E): Eff<never, Throws<E>> {
  return new Suspend(Op.Fail, Cause.fail(error), null) as any;
}

export function die(defect: unknown): Eff<never, never> {
  return new Suspend(Op.Fail, Cause.die(defect), null) as any;
}

/** Fail with a pre-existing Cause — useful for re-failing unchanged after
 *  inspecting it, without `mapErrorCause`'s transformation step. */
export function failCause<E = unknown>(cause: import("./cause").Cause<E>): Eff<never, any> {
  return new Suspend(Op.Fail, cause, null) as any;
}

export function sync<A>(f: () => A): Eff<A, never> {
  return new Suspend(Op.Sync, f, null) as any;
}

export function suspend<A, S>(f: () => Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.FlatMap, new Suspend(Op.Succeed, undefined, null), f) as any;
}

export function async<A, E = never>(
  register: (resume: (value: Eff<A, Throws<E>>) => void) => (() => void) | void,
): Eff<A, Throws<E>> {
  return new Suspend(Op.Async, register, null) as any;
}

// alias: fromPromise — same as tryPromise (matches the "from*" naming family).
export const fromPromise: typeof tryPromise = (...args: any[]) => (tryPromise as any)(...args);

/**
 * Marker tagged on the Suspend returned by tryPromise/fromPromise so the
 * thenable shim can detect "this is just a Promise wrapper" and skip the
 * fiber spawn — directly awaiting the underlying Promise instead.
 */
export const PROMISE_THUNK: unique symbol = Symbol.for("spilne/promise-thunk");
export const PROMISE_ON_REJECT: unique symbol = Symbol.for("spilne/promise-onReject");

export function tryPromise<A, E>(
  promise: () => Promise<A>,
  onReject: (e: unknown) => E,
): Eff<A, Throws<E>> {
  const eff = async<A, E>((resume) => {
    promise().then(
      (a) => resume(succeed(a) as any),
      (e) => resume(fail(onReject(e)) as any),
    );
  }) as any;
  // Tag for the thenable fast-path. Going through run() costs ~500 ns
  // (Suspend alloc + fiber spawn + register/resume cycle) on top of the
  // underlying Promise. For the bare `await tryPromise(p)` case (no further
  // Eff composition), skip that and await the Promise directly.
  eff[PROMISE_THUNK] = promise;
  eff[PROMISE_ON_REJECT] = onReject;
  return eff;
}

// ── Fiber constructors ─────────────────────────────────────────────

export function fork<A, S>(eff: Eff<A, S>): Eff<Fiber<A>, never> {
  return new Suspend(Op.Fork, eff, null) as any;
}

// Spawn a fiber that is NOT tied to the parent's lifecycle.
// Use for long-running background workers.
export function forkDaemon<A, S>(eff: Eff<A, S>): Eff<Fiber<A>, never> {
  return new Suspend(Op.ForkDaemon, eff, null) as any;
}

export function join<A>(fiber: Fiber<A>): Eff<A, Throws<Cause>> {
  return async<A, Cause>((resume) => {
    fiber.onComplete((result) => {
      if (result.ok) resume(succeed(result.value) as any);
      else resume(fail(result.cause) as any);
    });
  }) as any;
}

export function interrupt(fiber: Fiber): Eff<void, never> {
  return sync(() => fiber.interrupt());
}

// Wait for a fiber and get its Exit, never failing.
export function awaitFiber<A>(fiber: Fiber<A>): Eff<Exit<unknown, A>, never> {
  return async<Exit<unknown, A>>((resume) => {
    fiber.onComplete((result) => {
      resume(
        succeed(result.ok ? ExitNS.succeed(result.value) : ExitNS.failure(result.cause)) as any,
      );
    });
  }) as any;
}

// ── Interruption masking ───────────────────────────────────────────

export function uninterruptible<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.SetInterruptible, eff, false) as any;
}

export function interruptible<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.SetInterruptible, eff, true) as any;
}

// Explicit cooperative yield point — forces a scheduler reschedule.
export const yieldNow: Eff<void, never> = new Suspend(Op.YieldNow, null, null) as any;

// ── Timing ─────────────────────────────────────────────────────────

// sleep routes through the Clock service in the fiber context.
// Clock is seeded to a real default in runtime.ts, so no user provide() is
// needed. Tests swap in a TestClock via provide(eff, Clock, testClock).
const CLOCK_KEY = Symbol.for("spilne/svc/Clock");
export function sleep(ms: number): Eff<void, never> {
  // Op.FlatMap directly so we don't depend on the fluent prototype being installed.
  return new Suspend(Op.FlatMap, new Suspend(Op.GetCtx, CLOCK_KEY, null), (clock: any) =>
    clock.sleep(ms),
  ) as any;
}

export function delay<A, S>(eff: Eff<A, S>, ms: number): Eff<A, S> {
  return new Suspend(Op.FlatMap, sleep(ms), () => eff) as any;
}

// ── Race ───────────────────────────────────────────────────────────

// First settled (success OR failure) wins; losers are interrupted.
export function race<A, S>(effects: Eff<A, S>[]): Eff<A, S> {
  return new Suspend(Op.Race, effects, null) as any;
}

// Alias for race — named for symmetry with raceAll.
export function raceFirst<A, S>(effects: Eff<A, S>[]): Eff<A, S> {
  return race(effects);
}

// Race two effects and wrap the winner in Either — tells you who won.
// Accepts either positional args or a [left, right] tuple for consistency
// with the array form used by race / raceFirst / raceAll.
export function raceEither<A, S1, B, S2>(
  effects: [Eff<A, S1>, Eff<B, S2>],
): Eff<{ _tag: "Left"; left: A } | { _tag: "Right"; right: B }, S1 | S2>;
export function raceEither<A, S1, B, S2>(
  left: Eff<A, S1>,
  right: Eff<B, S2>,
): Eff<{ _tag: "Left"; left: A } | { _tag: "Right"; right: B }, S1 | S2>;
export function raceEither(...args: any[]): any {
  const [left, right] = Array.isArray(args[0]) ? args[0] : args;
  return race([
    (left as any).map((a: any) => ({ _tag: "Left" as const, left: a })),
    (right as any).map((b: any) => ({ _tag: "Right" as const, right: b })),
  ]) as any;
}

// Run all in parallel and collect their Exits — never fails, never interrupts siblings on failure.
export function raceAll<A, S>(
  effects: Eff<A, S>[],
): Eff<Exit<unknown, A>[], Exclude<S, Throws<any>>> {
  const wrapped = effects.map(
    (e) =>
      new Suspend(
        Op.CatchAll,
        new Suspend(Op.FlatMap, e as any, (a: any) => succeed(ExitNS.succeed(a))),
        (cause: any) => succeed(ExitNS.failure(cause)),
      ),
  );
  return new Suspend(Op.All, wrapped, null) as any;
}

export function timeoutOption<A, S>(eff: Eff<A, S>, ms: number): Eff<A | undefined, S> {
  return race([
    eff as any,
    new Suspend(Op.FlatMap, sleep(ms), () => succeed(undefined)) as any,
  ]) as any;
}

// Like timeout but with a custom typed error instead of Option<A>.
export function timeoutFail<A, S, E>(
  eff: Eff<A, S>,
  ms: number,
  onTimeout: () => E,
): Eff<A, S | Throws<E>> {
  return race([eff, new Suspend(Op.FlatMap, sleep(ms), () => fail(onTimeout())) as any]) as any;
}

// Race against a timer; timeout produces a typed failure.
export function timeout<A, S, E>(
  eff: Eff<A, S>,
  ms: number,
  onTimeout: () => E,
): Eff<A, S | Throws<E>> {
  return timeoutFail(eff, ms, onTimeout);
}

// ── Resource safety ────────────────────────────────────────────────

export function ensuring<A, S>(eff: Eff<A, S>, finalizer: Eff<void, never>): Eff<A, S> {
  return new Suspend(Op.Ensuring, eff, finalizer) as any;
}

// Run handler with the Exit of eff, then propagate eff's original outcome.
export function onExit<A, S, S2>(
  eff: Eff<A, S>,
  handler: (exit: Exit<unknown, A>) => Eff<void, S2>,
): Eff<A, S | S2> {
  const success = new Suspend(
    Op.FlatMap,
    eff,
    (a: any) => new Suspend(Op.FlatMap, handler(ExitNS.succeed(a)) as any, () => succeed(a)),
  );
  return new Suspend(
    Op.CatchAll,
    success,
    (cause: any) =>
      new Suspend(
        Op.FlatMap,
        handler(ExitNS.failure(cause)) as any,
        () => new Suspend(Op.Fail, cause, null),
      ),
  ) as any;
}

export function acquireRelease<A, S>(
  acquire: Eff<A, S>,
  release: (a: A) => Eff<void, never>,
): Eff<A, S> {
  return new Suspend(Op.AcqRel, acquire, release) as any;
}

export function scoped<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.Scoped, eff, null) as any;
}

// ── Retry ──────────────────────────────────────────────────────────

export interface RetryConfig<E = unknown> {
  times: number;
  delay?: number;
  backoff?: "fixed" | "exponential";
  maxDelay?: number;
  /**
   * Predicate on the typed error. Only retry when it returns true.
   * Defaults to always-true (retry all typed errors).
   * Defects and interrupts never retry — they indicate something
   * unexpected (bug, OOM) or a deliberate cancellation. Use
   * `retryAllCause` (or `RetryPolicy.whenCause`) if you explicitly
   * want defect-aware retry.
   */
  when?: (error: E) => boolean;
  /** Randomize each delay by ±50% to avoid thundering-herd patterns. */
  jitter?: boolean;
  /** Total wall-clock budget across all attempts; fail when exceeded. */
  timeBudgetMs?: number;
}

// Overload: pass either a new RetryPolicy class or the legacy config dict.
export function retry<A, S>(
  eff: Eff<A, S>,
  policy: import("./retry-policy").RetryPolicy,
): Eff<A, S>;
export function retry<A, S>(eff: Eff<A, S>, config: RetryConfig): Eff<A, S>;
export function retry<A, S>(
  eff: Eff<A, S>,
  policyOrConfig: RetryConfig | import("./retry-policy").RetryPolicy,
): Eff<A, S> {
  // Dispatch on shape — a RetryPolicy instance has `.impl`, the legacy config
  // has `.times` (and no `.impl`).
  const isPolicy = typeof (policyOrConfig as any).impl !== "undefined";
  if (isPolicy) {
    // Delegate to the unified applier. Imported lazily to avoid the circular
    // retry-policy.ts → constructors.ts → retry-policy.ts cycle on module load.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { runRetry } = require("./retry-policy") as typeof import("./retry-policy");
    return runRetry(eff, policyOrConfig as any);
  }
  return retryLegacy(eff, policyOrConfig as RetryConfig);
}

function retryLegacy<A, S>(eff: Eff<A, S>, policy: RetryConfig): Eff<A, S> {
  const {
    times,
    delay: baseDelay = 0,
    backoff = "fixed",
    maxDelay = 30_000,
    when,
    jitter = false,
    timeBudgetMs = Infinity,
  } = policy;

  const deadline = timeBudgetMs === Infinity ? Infinity : Date.now() + timeBudgetMs;

  function nextDelayMs(current: number): number {
    const base = backoff === "exponential" ? Math.min(current * 2, maxDelay) : current;
    if (!jitter) return base;
    // ±50% jitter, never below 0
    const factor = 0.5 + Math.random();
    return Math.max(0, Math.min(maxDelay, Math.floor(base * factor)));
  }

  function attempt(remaining: number, currentDelay: number): Eff<A, S> {
    return new Suspend(Op.CatchAll, eff, (cause: any) => {
      if (remaining <= 0) return new Suspend(Op.Fail, cause, null);
      if (Date.now() >= deadline) return new Suspend(Op.Fail, cause, null);

      // Only retry typed failures. Defects / interrupts propagate immediately.
      const f = Cause.firstFail(cause);
      if (f === null) return new Suspend(Op.Fail, cause, null);

      // Predicate: if supplied, decide per typed error
      if (when !== undefined && !when(f.value)) {
        return new Suspend(Op.Fail, cause, null);
      }

      const waitEff = currentDelay > 0 ? sleep(currentDelay) : succeed(undefined);
      const nextD = nextDelayMs(currentDelay);
      return new Suspend(Op.FlatMap, waitEff, () => attempt(remaining - 1, nextD));
    }) as any;
  }

  return attempt(times, baseDelay) as any;
}
