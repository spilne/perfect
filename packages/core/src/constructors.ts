import { Cause } from "./cause"
import { type Eff, type Throws, Suspend, Op } from "./eff"
import { Fiber } from "./fiber"
import { type Exit, Exit as ExitNS } from "./exit"

export function succeed<A>(value: A): Eff<A, never> {
  return new Suspend(Op.Succeed, value, null) as any
}

export function fail<E>(error: E): Eff<never, Throws<E>> {
  return new Suspend(Op.Fail, Cause.fail(error), null) as any
}

export function die(defect: unknown): Eff<never, never> {
  return new Suspend(Op.Fail, Cause.die(defect), null) as any
}

export function sync<A>(f: () => A): Eff<A, never> {
  return new Suspend(Op.Sync, f, null) as any
}

export function suspend<A, S>(f: () => Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.FlatMap, new Suspend(Op.Succeed, undefined, null), f) as any
}

export function async<A, E = never>(
  register: (resume: (value: Eff<A, Throws<E>>) => void) => (() => void) | void,
): Eff<A, Throws<E>> {
  return new Suspend(Op.Async, register, null) as any
}

export function tryPromise<A, E>(
  promise: () => Promise<A>,
  onReject: (e: unknown) => E,
): Eff<A, Throws<E>> {
  return async<A, E>((resume) => {
    promise().then(
      (a) => resume(succeed(a) as any),
      (e) => resume(fail(onReject(e)) as any),
    )
  }) as any
}

// ── Fiber constructors ─────────────────────────────────────────────

export function fork<A, S>(eff: Eff<A, S>): Eff<Fiber<A>, never> {
  return new Suspend(Op.Fork, eff, null) as any
}

// Spawn a fiber that is NOT tied to the parent's lifecycle.
// Use for long-running background workers.
export function forkDaemon<A, S>(eff: Eff<A, S>): Eff<Fiber<A>, never> {
  return new Suspend(Op.ForkDaemon, eff, null) as any
}

export function join<A>(fiber: Fiber<A>): Eff<A, Throws<Cause>> {
  return async<A, Cause>((resume) => {
    fiber.onComplete((result) => {
      if (result.ok) resume(succeed(result.value) as any)
      else resume(fail(result.cause) as any)
    })
  }) as any
}

export function interrupt(fiber: Fiber): Eff<void, never> {
  return sync(() => fiber.interrupt())
}

// Wait for a fiber and get its Exit, never failing.
export function awaitFiber<A>(fiber: Fiber<A>): Eff<Exit<unknown, A>, never> {
  return async<Exit<unknown, A>>((resume) => {
    fiber.onComplete((result) => {
      resume(succeed(result.ok ? ExitNS.succeed(result.value) : ExitNS.failure(result.cause)) as any)
    })
  }) as any
}

// ── Interruption masking ───────────────────────────────────────────

export function uninterruptible<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.SetInterruptible, eff, false) as any
}

export function interruptible<A, S>(eff: Eff<A, S>): Eff<A, S> {
  return new Suspend(Op.SetInterruptible, eff, true) as any
}

// Explicit cooperative yield point — forces a scheduler reschedule.
export const yieldNow: Eff<void, never> =
  new Suspend(Op.YieldNow, null, null) as any

// ── Timing ─────────────────────────────────────────────────────────

export function sleep(ms: number): Eff<void, never> {
  return async<void>((resume) => {
    const id = setTimeout(() => resume(succeed(undefined) as any), ms)
    return () => clearTimeout(id)
  }) as any
}

export function delay<A, S>(eff: Eff<A, S>, ms: number): Eff<A, S> {
  return new Suspend(Op.FlatMap, sleep(ms), () => eff) as any
}

// ── Race ───────────────────────────────────────────────────────────

// First settled (success OR failure) wins; losers are interrupted.
export function race<A, S>(effects: Eff<A, S>[]): Eff<A, S> {
  return new Suspend(Op.Race, effects, null) as any
}

// Alias for race — named for symmetry with raceAll.
export function raceFirst<A, S>(effects: Eff<A, S>[]): Eff<A, S> {
  return race(effects)
}

// Race two effects and wrap the winner in Either — tells you who won.
export function raceEither<A, S1, B, S2>(
  left: Eff<A, S1>,
  right: Eff<B, S2>,
): Eff<{ _tag: "Left"; left: A } | { _tag: "Right"; right: B }, S1 | S2> {
  return race([
    (left as any).map((a: A) => ({ _tag: "Left" as const, left: a })),
    (right as any).map((b: B) => ({ _tag: "Right" as const, right: b })),
  ]) as any
}

// Run all in parallel and collect their Exits — never fails, never interrupts siblings on failure.
export function raceAll<A, S>(effects: Eff<A, S>[]): Eff<Exit<unknown, A>[], Exclude<S, Throws<any>>> {
  const wrapped = effects.map((e) =>
    new Suspend(
      Op.CatchAll,
      new Suspend(Op.FlatMap, e as any, (a: any) => succeed(ExitNS.succeed(a))),
      (cause: any) => succeed(ExitNS.failure(cause)),
    ),
  )
  return new Suspend(Op.All, wrapped, null) as any
}

export function timeoutOption<A, S>(
  eff: Eff<A, S>,
  ms: number,
): Eff<A | undefined, S> {
  return race([
    eff as any,
    new Suspend(Op.FlatMap, sleep(ms), () => succeed(undefined)) as any,
  ]) as any
}

// Like timeout but with a custom typed error instead of Option<A>.
export function timeoutFail<A, S, E>(
  eff: Eff<A, S>,
  ms: number,
  onTimeout: () => E,
): Eff<A, S | Throws<E>> {
  return race([
    eff,
    new Suspend(Op.FlatMap, sleep(ms), () => fail(onTimeout())) as any,
  ]) as any
}

// Race against a timer; timeout produces a typed failure.
export function timeout<A, S, E>(
  eff: Eff<A, S>,
  ms: number,
  onTimeout: () => E,
): Eff<A, S | Throws<E>> {
  return timeoutFail(eff, ms, onTimeout)
}

// ── Resource safety ────────────────────────────────────────────────

export function ensuring<A, S>(
  eff: Eff<A, S>,
  finalizer: Eff<void, never>,
): Eff<A, S> {
  return new Suspend(Op.Ensuring, eff, finalizer) as any
}

// Run handler with the Exit of eff, then propagate eff's original outcome.
export function onExit<A, S, S2>(
  eff: Eff<A, S>,
  handler: (exit: Exit<unknown, A>) => Eff<void, S2>,
): Eff<A, S | S2> {
  const success = new Suspend(Op.FlatMap, eff, (a: any) =>
    new Suspend(Op.FlatMap, handler(ExitNS.succeed(a)) as any, () => succeed(a)),
  )
  return new Suspend(Op.CatchAll, success, (cause: any) =>
    new Suspend(Op.FlatMap, handler(ExitNS.failure(cause)) as any, () =>
      new Suspend(Op.Fail, cause, null),
    ),
  ) as any
}

export function acquireRelease<A, S>(
  acquire: Eff<A, S>,
  release: (a: A) => Eff<void, never>,
): Eff<A, S> {
  return new Suspend(Op.AcqRel, acquire, release) as any
}

export function scoped<A, S>(
  eff: Eff<A, S>,
): Eff<A, S> {
  return new Suspend(Op.Scoped, eff, null) as any
}

// ── Retry ──────────────────────────────────────────────────────────

export interface RetryPolicy {
  times: number
  delay?: number
  backoff?: "fixed" | "exponential"
  maxDelay?: number
}

export function retry<A, S>(
  eff: Eff<A, S>,
  policy: RetryPolicy,
): Eff<A, S> {
  const { times, delay: baseDelay = 0, backoff = "fixed", maxDelay = 30_000 } = policy

  function attempt(remaining: number, currentDelay: number): Eff<A, S> {
    return new Suspend(Op.CatchAll, eff, (cause: any) => {
      if (remaining <= 0) return new Suspend(Op.Fail, cause, null)
      const waitEff = currentDelay > 0 ? sleep(currentDelay) : succeed(undefined)
      const nextDelay = backoff === "exponential"
        ? Math.min(currentDelay * 2, maxDelay)
        : currentDelay
      return new Suspend(Op.FlatMap, waitEff, () => attempt(remaining - 1, nextDelay))
    }) as any
  }

  return attempt(times, baseDelay) as any
}
