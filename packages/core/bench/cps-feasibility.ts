// CPS feasibility check.
//
// Hand-rolled CPS-encoded flatMap chain vs:
//   - Our current interpreter (Suspend/Cont, 13 ns/op)
//   - Effect-ts (~? ns/op)
//   - Raw imperative loop (baseline)
//   - Naive function-call chain (no CPS)
//
// Goal: see whether CPS actually beats the interpreter on Bun's JIT.
// JVM JITs love CPS chains because they unbox / inline aggressively.
// V8/Bun may or may not.
//
// If CPS is dramatically faster: branch and integrate.
// If marginal: stop the line, the interpreter is already at the
// realistic floor for what JS can squeeze out.
//
// Run: bun packages/core/bench/cps-feasibility.ts

import { group, bench, run as mitataRun } from "mitata"
import { Effect } from "effect"
import { succeed as pSucceed, runSync as pRunSync, type Eff } from "../src"

// ── CPS encoding ───────────────────────────────────────────────────

type CPS<A> = (k: (a: A) => void) => void

const cpsSucceed = <A>(a: A): CPS<A> => (k) => k(a)
const cpsFlatMap = <A, B>(eff: CPS<A>, f: (a: A) => CPS<B>): CPS<B> =>
  (k) => eff((a) => f(a)(k))
const cpsMap = <A, B>(eff: CPS<A>, f: (a: A) => B): CPS<B> =>
  (k) => eff((a) => k(f(a)))

function runCPS<A>(eff: CPS<A>): A {
  let result!: A
  eff((a) => { result = a })
  return result
}

// ── Build chains ───────────────────────────────────────────────────

const N = 1000

function buildPerfect(): Eff<number, never> {
  let e: Eff<number, never> = pSucceed(0)
  for (let i = 0; i < N; i++) e = e.flatMap((x) => pSucceed(x + 1))
  return e
}

function buildEffect(): Effect.Effect<number> {
  let e: Effect.Effect<number> = Effect.succeed(0)
  for (let i = 0; i < N; i++) e = e.pipe(Effect.flatMap((x) => Effect.succeed(x + 1)))
  return e
}

function buildCPS(): CPS<number> {
  let e: CPS<number> = cpsSucceed(0)
  for (let i = 0; i < N; i++) e = cpsFlatMap(e, (x) => cpsSucceed(x + 1))
  return e
}

function buildCPSMap(): CPS<number> {
  // map-only — should be cheaper since no inner CPS allocation per step.
  let e: CPS<number> = cpsSucceed(0)
  for (let i = 0; i < N; i++) e = cpsMap(e, (x) => x + 1)
  return e
}

// Build once outside the timed region so we measure execution, not construction.
const perfectEff = buildPerfect()
const effectEff = buildEffect()
const cpsEff = buildCPS()
const cpsMapEff = buildCPSMap()

// ── Benchmarks ─────────────────────────────────────────────────────

group(`flatMap × ${N} — execution only (built once)`, () => {
  bench("imperative for-loop (baseline)", () => {
    let x = 0
    for (let i = 0; i < N; i++) x = x + 1
    return x
  })

  bench("naive function calls (no CPS)", () => {
    // Build N nested functions and call them once. This isn't really CPS,
    // it's just function-call cost — to compare with imperative.
    let f: (x: number) => number = (x) => x
    for (let i = 0; i < N; i++) {
      const prev = f
      f = (x) => prev(x + 1)
    }
    return f(0)
  })

  bench("hand-rolled CPS flatMap × N", () => runCPS(cpsEff))
  bench("hand-rolled CPS map × N",     () => runCPS(cpsMapEff))

  bench("perfect flatMap + runSync", () => pRunSync(perfectEff))
  bench("effect  flatMap + runSync", () => Effect.runSync(effectEff))
})

// Also include the build cost — for fairness when the program builds the
// chain on each call (typical pattern).
group(`flatMap × ${N} — build + execute each time`, () => {
  bench("CPS build + run", () => runCPS(buildCPS()))
  bench("perfect build + runSync", () => pRunSync(buildPerfect()))
  bench("effect  build + runSync", () => Effect.runSync(buildEffect()))
})

// Smaller chain — does fixed overhead dominate?
const N_SMALL = 10
const cpsEffSmall = (() => {
  let e: CPS<number> = cpsSucceed(0)
  for (let i = 0; i < N_SMALL; i++) e = cpsFlatMap(e, (x) => cpsSucceed(x + 1))
  return e
})()
const perfectEffSmall = (() => {
  let e: Eff<number, never> = pSucceed(0)
  for (let i = 0; i < N_SMALL; i++) e = e.flatMap((x) => pSucceed(x + 1))
  return e
})()
const effectEffSmall = (() => {
  let e: Effect.Effect<number> = Effect.succeed(0)
  for (let i = 0; i < N_SMALL; i++) e = e.pipe(Effect.flatMap((x) => Effect.succeed(x + 1)))
  return e
})()

group(`flatMap × ${N_SMALL} — short chain (fixed overhead matters)`, () => {
  bench("hand-rolled CPS", () => runCPS(cpsEffSmall))
  bench("perfect runSync", () => pRunSync(perfectEffSmall))
  bench("effect  runSync", () => Effect.runSync(effectEffSmall))
})

await mitataRun()
