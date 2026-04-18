// Ported from promin's Pipeline: combinators that go beyond the core algebra.
// These are standalone functions, not methods — they take an Eff and return an Eff.

import { type Eff, type Throws, Suspend, Op } from "./eff"
import { Cause } from "./cause"
import { succeed, fail, sleep } from "./constructors"
import { all as allParallel } from "./combinators"

// ── trapError ──────────────────────────────────────────────────────
//
// Convert `instanceof X` defects (thrown Errors, Promise rejections) back into
// typed failures. Useful when code calls `sync(() => riskyThing())` that can
// throw domain exceptions you want to handle structurally rather than letting
// them propagate as `Cause.die`.

type Ctor<E> = new (...args: any[]) => E

export function trapError<A, S, Classes extends Ctor<any>[]>(
  eff: Eff<A, S>,
  ...classes: Classes
): Eff<A, S | Throws<InstanceType<Classes[number]>>> {
  return new Suspend(Op.CatchAll, eff, (cause: Cause) => {
    const defects = Cause.defects(cause)
    for (const d of defects) {
      for (const cls of classes) {
        if (d instanceof cls) return fail(d as any)
      }
    }
    // nothing matched — reraise the original cause
    return new Suspend(Op.Fail, cause, null)
  }) as any
}

// ── validate ───────────────────────────────────────────────────────
//
// Like `all`, but runs every effect to completion and accumulates EVERY failure
// into a single Cause.both tree. Returns the successful results array if every
// effect succeeded; otherwise fails with the combined cause.
//
// `all` fails-fast on first failure; `validate` reports all problems.

export function validate<const T extends readonly Eff<any, any>[]>(
  effects: T,
): Eff<
  { [K in keyof T]: T[K] extends Eff<infer A, any> ? A : never },
  T[number] extends Eff<any, infer S> ? S : never
> {
  if (effects.length === 0) return succeed([] as any) as any

  // Wrap each effect so it never fails — captures Exit-like { ok, value/cause }.
  const wrapped = effects.map((e) =>
    new Suspend(
      Op.CatchAll,
      new Suspend(Op.FlatMap, e as any, (v: any) => succeed({ ok: true as const, value: v })),
      (cause: Cause) => succeed({ ok: false as const, cause }),
    ),
  )

  return new Suspend(
    Op.FlatMap,
    allParallel(wrapped as any) as any,
    (results: Array<{ ok: true; value: any } | { ok: false; cause: Cause }>) => {
      const values: any[] = new Array(results.length)
      let combined: Cause | null = null
      for (let i = 0; i < results.length; i++) {
        const r = results[i]!
        if (r.ok) values[i] = r.value
        else combined = combined === null ? r.cause : Cause.both(combined, r.cause)
      }
      if (combined !== null) return new Suspend(Op.Fail, combined, null)
      return succeed(values)
    },
  ) as any
}

// ── hedged ─────────────────────────────────────────────────────────
//
// Race the same effect N times with a stagger delay between starts. The first
// to succeed wins; losers are interrupted. Useful for latency-sensitive IO
// where the P99 is much higher than the P50 (a delayed duplicate can save you
// a tail-latency spike).

export function hedged<A, S>(
  eff: Eff<A, S>,
  opts: { replicas: number; staggerMs: number },
): Eff<A, S> {
  const { replicas, staggerMs } = opts
  if (replicas < 1) throw new Error("hedged: replicas must be >= 1")
  if (replicas === 1) return eff
  // Build N copies, each delayed by i*staggerMs.
  const variants: Eff<A, S>[] = []
  for (let i = 0; i < replicas; i++) {
    if (i === 0) {
      variants.push(eff)
    } else {
      const d = i * staggerMs
      variants.push(
        new Suspend(Op.FlatMap, sleep(d), () => eff) as any,
      )
    }
  }
  return new Suspend(Op.Race, variants, null) as any
}

// ── retryAllCause ──────────────────────────────────────────────────
//
// Opt-in defect-aware retry. Unlike `retry` (which only retries typed
// failures), this calls `shouldRetry` with the full Cause — users can
// choose to retry on defects, interrupt, or any combination.
//
// Useful when wrapping libraries that throw Promise rejections via
// tryPromise / sync, and you've decided the class of defect is safe
// to retry.

export function retryAllCause<A, S>(
  eff: Eff<A, S>,
  opts: {
    shouldRetry: (cause: Cause) => boolean
    times: number
    delayMs?: number
    backoff?: "fixed" | "exponential"
    maxDelayMs?: number
  },
): Eff<A, S> {
  const { shouldRetry, times, delayMs = 0, backoff = "fixed", maxDelayMs = 30_000 } = opts

  function attempt(remaining: number, current: number): Eff<A, S> {
    return new Suspend(Op.CatchAll, eff, (cause: Cause) => {
      if (remaining <= 0) return new Suspend(Op.Fail, cause, null)
      if (!shouldRetry(cause)) return new Suspend(Op.Fail, cause, null)
      const waitEff = current > 0 ? sleep(current) : succeed(undefined)
      const next = backoff === "exponential" ? Math.min(current * 2, maxDelayMs) : current
      return new Suspend(Op.FlatMap, waitEff, () => attempt(remaining - 1, next))
    }) as any
  }
  return attempt(times, delayMs)
}

// ── pollUntil / pollUntilWithBackoff ───────────────────────────────

export interface PollTimeoutError {
  readonly _tag: "PollTimeoutError"
  readonly reason: "maxAttempts" | "maxDuration"
  readonly attempts: number
  readonly elapsedMs: number
}

function makePollTimeout(
  reason: PollTimeoutError["reason"],
  attempts: number,
  elapsedMs: number,
): PollTimeoutError {
  return { _tag: "PollTimeoutError", reason, attempts, elapsedMs }
}

export function pollUntil<A, S>(
  eff: Eff<A, S>,
  opts: {
    until: (value: A) => boolean
    intervalMs?: number
    maxAttempts?: number
    maxDurationMs?: number
  },
): Eff<A, S | Throws<PollTimeoutError>> {
  const { until, intervalMs = 1_000, maxAttempts = Infinity, maxDurationMs = Infinity } = opts
  const startNow = new Suspend(Op.Sync, () => Date.now(), null) as Eff<number, never>

  function step(attempt: number, started: number): Eff<A, S | Throws<PollTimeoutError>> {
    return new Suspend(Op.FlatMap, eff as any, (value: A) => {
      if (until(value)) return succeed(value)
      const now = Date.now()
      const elapsed = now - started
      if (attempt + 1 >= maxAttempts) return fail(makePollTimeout("maxAttempts", attempt + 1, elapsed))
      if (elapsed >= maxDurationMs) return fail(makePollTimeout("maxDuration", attempt + 1, elapsed))
      return new Suspend(Op.FlatMap, sleep(intervalMs), () => step(attempt + 1, started)) as any
    }) as any
  }

  return new Suspend(Op.FlatMap, startNow as any, (now: number) => step(0, now)) as any
}

export function pollUntilWithBackoff<A, S>(
  eff: Eff<A, S>,
  opts: {
    until: (value: A) => boolean
    initialIntervalMs?: number
    maxIntervalMs?: number
    maxAttempts?: number
    maxDurationMs?: number
  },
): Eff<A, S | Throws<PollTimeoutError>> {
  const {
    until,
    initialIntervalMs = 100,
    maxIntervalMs = 30_000,
    maxAttempts = Infinity,
    maxDurationMs = Infinity,
  } = opts

  const startNow = new Suspend(Op.Sync, () => Date.now(), null) as Eff<number, never>

  function step(attempt: number, currentInterval: number, started: number): Eff<A, S | Throws<PollTimeoutError>> {
    return new Suspend(Op.FlatMap, eff as any, (value: A) => {
      if (until(value)) return succeed(value)
      const now = Date.now()
      const elapsed = now - started
      if (attempt + 1 >= maxAttempts) return fail(makePollTimeout("maxAttempts", attempt + 1, elapsed))
      if (elapsed >= maxDurationMs) return fail(makePollTimeout("maxDuration", attempt + 1, elapsed))
      const nextInterval = Math.min(currentInterval * 2, maxIntervalMs)
      return new Suspend(Op.FlatMap, sleep(currentInterval), () => step(attempt + 1, nextInterval, started)) as any
    }) as any
  }

  return new Suspend(Op.FlatMap, startNow as any, (now: number) => step(0, initialIntervalMs, now)) as any
}
