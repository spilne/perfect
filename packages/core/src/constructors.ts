import { Cause } from "./cause.js";
import type { WithError } from "./either.js";
import {
  type Eff,
  type ErrorsOf,
  type Throws,
  type InferValue,
  type InferEffects,
  Suspend,
  Op,
} from "./eff.js";
import { Fiber } from "./fiber.js";
import { type Exit, Exit as ExitNS } from "./exit.js";
import { RetryPolicy, runRetry as runRetryUnified } from "./retry-policy.js";

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
export function failCause<E = unknown>(
  cause: import("./cause.js").Cause<E>,
): Eff<never, Throws<E>> {
  return new Suspend(Op.Fail, cause, null) as any;
}

export function sync<A>(f: () => A): Eff<A, never> {
  return new Suspend(Op.Sync, f, null) as any;
}

export function suspend<A, S>(f: () => Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.FlatMap, new Suspend(Op.Succeed, undefined, null), f) as any;
}

/**
 * Suspend the fiber until `register` calls `resume` with the effect to continue
 * with. `register` may return a canceler, called if the fiber is interrupted
 * while it waits. A canceler that throws does not stop the interrupt: its
 * error joins the interrupted fiber's cause as a defect, also when the
 * canceler interrupts the fiber again first. So does an error thrown by
 * `register` after it interrupted its own fiber.
 *
 * `resume(value, onDiscard)` hands over something only one waiter may have,
 * such as a queue item or a permit. The fiber may be interrupted after the
 * resume but before it runs, or `resume` may arrive after the fiber stopped
 * waiting. In both cases `value` is not run and `onDiscard` is called exactly
 * once, so the caller can give the item to someone else. `onDiscard` is never
 * called once the fiber has started running `value`.
 *
 * If `onDiscard` throws when an interrupt discards the value, the error joins
 * the interrupted fiber's cause as a defect and the interrupt goes ahead. When
 * `resume` itself calls it, the error propagates to the caller of `resume`.
 */
export function async<A, E = never>(
  register: (
    resume: (value: Eff<A, Throws<E>>, onDiscard?: () => void) => void,
  ) => (() => void) | void,
): Eff<A, WithError<never, E>> {
  return new Suspend(Op.Async, register, null) as any;
}

// alias: fromPromise — same as tryPromise (matches the "from*" naming family).
export const fromPromise: typeof tryPromise = (...args: any[]) => (tryPromise as any)(...args);

/**
 * The promise behind each effect made by tryPromise/fromPromise, so the
 * thenable shim can detect "this is just a Promise wrapper" and skip the
 * fiber spawn — directly awaiting the underlying Promise instead.
 */
export const PROMISE_SOURCES = new WeakMap<
  object,
  { readonly promise: () => Promise<unknown>; readonly onReject: (e: unknown) => unknown }
>();

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
  // Remember the promise for the thenable fast path. Going through run()
  // costs ~500 ns (fiber spawn + register/resume cycle) on top of the
  // promise. For a bare `await tryPromise(p)` (no further composition) the
  // shim awaits the promise directly instead.
  //
  // This lives in a WeakMap rather than as extra properties on the node:
  // extra properties would give these nodes a different shape from every
  // other effect node, which slows down the interpreter's hot loop.
  PROMISE_SOURCES.set(eff, { promise, onReject });
  return eff;
}

// ── Fiber constructors ─────────────────────────────────────────────

export function fork<A, S>(
  eff: Eff<A, S>,
): Eff<Fiber<A, ErrorsOf<S>>, Exclude<S, Throws<unknown>>> {
  return new Suspend(Op.Fork, eff, null) as any;
}

// Spawn a fiber that is NOT tied to the parent's lifecycle.
// Use for long-running background workers.
export function forkDaemon<A, S>(
  eff: Eff<A, S>,
): Eff<Fiber<A, ErrorsOf<S>>, Exclude<S, Throws<unknown>>> {
  return new Suspend(Op.ForkDaemon, eff, null) as any;
}

/**
 * Wait for a fiber and take over its result. If the fiber failed, the
 * failure is raised here exactly as it was: its typed errors stay typed
 * (and show up in the type), a defect stays a defect, and an interrupt
 * stays an interrupt. (It used to arrive as one typed error holding the
 * raw Cause, so even defects could be caught with .catch.)
 */
export function join<A, E>(fiber: Fiber<A, E>): Eff<A, WithError<never, E>> {
  return async<A, Cause>((resume) =>
    // Returning the remover means a join that gives up stops listening.
    fiber.onComplete((result) => {
      if (result.ok) resume(succeed(result.value) as any);
      else resume(failCause(result.cause) as any);
    }),
  ) as any;
}

export function interrupt(fiber: Fiber): Eff<void, never> {
  return sync(() => fiber.interrupt());
}

// Wait for a fiber and get its Exit, never failing.
export function awaitFiber<A, E>(fiber: Fiber<A, E>): Eff<Exit<E, A>, never> {
  return async<Exit<E, A>>((resume) =>
    fiber.onComplete((result) => {
      resume(
        succeed(result.ok ? ExitNS.succeed(result.value) : ExitNS.failure(result.cause)) as any,
      );
    }),
  ) as any;
}

// ── Interruption masking ───────────────────────────────────────────

export function uninterruptible<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.SetInterruptible, eff, false) as any;
}

export function interruptible<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.SetInterruptible, eff, true) as any;
}

/**
 * Run the effect `f` builds uninterruptibly. `restore(eff)` runs `eff` with the
 * interruptibility in effect where the mask was entered: interruptible for an
 * ordinary caller, still uninterruptible when the mask itself runs inside a
 * finalizer or another uninterruptible region. `interruptible(eff)` would make
 * `eff` interruptible in both cases, so inside cleanup of an interrupted fiber
 * it would be interrupted at once.
 *
 * @example
 *   // Wait for a permit interruptibly, but register its release atomically.
 *   uninterruptibleMask((restore) =>
 *     acquireRelease(restore(waitForPermit), () => releasePermit),
 *   )
 */
export function uninterruptibleMask<A, S>(
  f: (restore: <B, S2>(eff: Eff<B, S2>) => Eff<B, S2>) => Eff<A, S>,
): Eff<A, S> {
  return new Suspend(
    Op.SetInterruptible,
    (wasInterruptible: boolean) =>
      f(<B, S2>(eff: Eff<B, S2>) => new Suspend(Op.SetInterruptible, eff, wasInterruptible) as any),
    false,
  ) as any;
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

// First settled (success OR failure) wins; losers are interrupted, and the race
// returns once they have finished.
// Generic over the tuple so heterogeneous arrays infer the UNION of their
// value/effect types instead of locking onto the first element.
export function race<E extends Eff<unknown, unknown>[]>(
  effects: [...E],
): Eff<InferValue<E[number]>, InferEffects<E[number]>> {
  return new Suspend(Op.Race, effects, null) as any;
}

/**
 * Run the effects at the same time and return the first one that SUCCEEDS.
 * The rest are interrupted.
 *
 * A failure does not end the race (that is the difference from `race`,
 * where the first effect to finish wins even if it failed). Only when every
 * effect has failed does this fail, with all the failures combined.
 */
export function raceSuccess<E extends Eff<unknown, unknown>[]>(
  effects: [...E],
): Eff<InferValue<E[number]>, InferEffects<E[number]>> {
  if (effects.length === 0) return die(new Error("raceSuccess: empty input")) as any;
  return suspend(() => {
    let remaining = effects.length;
    let failures: Cause | null = null;
    const contenders = effects.map(
      (effect) =>
        new Suspend(Op.CatchAll, effect, (cause: Cause) => {
          failures = failures === null ? cause : Cause.both(failures, cause);
          remaining--;
          // Others are still running: stay in the race without winning it.
          if (remaining > 0) return NEVER;
          return new Suspend(Op.Fail, failures, null);
        }),
    );
    return new Suspend(Op.Race, contenders, null) as Eff<unknown, unknown>;
  }) as any;
}

const NEVER: Eff<never, never> = new Suspend(Op.Async, () => {}, null) as any;

/** @deprecated Same as {@link race}. Use `race`. */
export function raceFirst<E extends Eff<unknown, unknown>[]>(
  effects: [...E],
): Eff<InferValue<E[number]>, InferEffects<E[number]>> {
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

/**
 * Run all effects at the same time and collect how each one ended (its
 * Exit), in input order. Never fails, and one failure doesn't stop the
 * others — like `Promise.allSettled`.
 */
export function allSettled<A, S>(
  effects: Eff<A, S>[],
): Eff<Exit<unknown, A>[], Exclude<S, Throws<unknown>>> {
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

/**
 * @deprecated The name was misleading: nothing races, every effect runs to
 * the end. Use {@link allSettled}, which is the same function.
 */
export const raceAll: typeof allSettled = allSettled;

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

export function ensuring<A, S, S2>(eff: Eff<A, S>, finalizer: Eff<void, S2>): Eff<A, S | S2> {
  return new Suspend(Op.Ensuring, eff, finalizer) as any;
}

// Run handler with the Exit of eff, then propagate eff's original outcome.
// The handler is a finalizer: it runs uninterruptibly, also when eff is
// interrupted, and its failure is added to the outcome like any finalizer's.
export function onExit<A, S, S2>(
  eff: Eff<A, S>,
  handler: (exit: Exit<unknown, A>) => Eff<void, S2>,
): Eff<A, S | S2> {
  return new Suspend(Op.Ensuring, eff, handler) as any;
}

export function acquireRelease<A, S, S2>(
  acquire: Eff<A, S>,
  release: (a: A) => Eff<void, S2>,
): Eff<A, S | S2> {
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
  /**
   * Total wall-clock budget across all attempts — sleeping and the time each
   * attempt spends running. Anchored when the effect runs. Fail when exceeded.
   */
  timeBudgetMs?: number;
}

// Two spellings, one implementation: the declarative config dict is
// translated by RetryPolicy.fromConfig and run by the same applier as a
// hand-built policy.
export function retry<A, S>(eff: Eff<A, S>, policy: RetryPolicy): Eff<A, S>;
export function retry<A, S>(eff: Eff<A, S>, config: RetryConfig): Eff<A, S>;
export function retry<A, S>(eff: Eff<A, S>, policyOrConfig: RetryConfig | RetryPolicy): Eff<A, S> {
  return runRetryUnified(
    eff,
    policyOrConfig instanceof RetryPolicy ? policyOrConfig : RetryPolicy.fromConfig(policyOrConfig),
  );
}
