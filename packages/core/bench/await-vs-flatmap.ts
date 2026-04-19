// Measure the cost of `await eff` vs composed `.flatMap` vs `await run(eff)`.
//
// Key takeaway we want to prove: `await eff` (thenable) is an escape hatch,
// not a composition primitive. Composed `.flatMap` inside a single run is
// an order of magnitude faster on hot loops.
//
// Run: bun packages/core/bench/await-vs-flatmap.ts

import { group, bench, run as mitataRun } from "mitata"
import { succeed, run, runSync, runSafe, runExit, type Eff } from "../src"

// ── single-shot ────────────────────────────────────────────────────

group("single effect", () => {
  bench("raw Promise.resolve(42)", async () => Promise.resolve(42))
  bench("await succeed(42)  (thenable)", async () => await succeed(42))
  bench("await run(succeed(42))  (explicit)", async () => await run(succeed(42)))
  bench("runSync(succeed(42))  (sync)", () => runSync(succeed(42)))
})

// ── chain of N effects ─────────────────────────────────────────────

for (const N of [10, 100, 1_000]) {
  group(`chain × ${N}`, () => {
    // Pre-build the chain once — measure per-run cost, not construction.
    const built = (() => {
      let e: Eff<number, never> = succeed(0)
      for (let i = 0; i < N; i++) e = e.flatMap((x) => succeed(x + 1))
      return e
    })()

    bench("flatMap chain + runSync (composed)", () => runSync(built))
    bench("flatMap chain + await (composed, thenable)", async () => await built)
    bench("flatMap chain + await run(...) (composed, explicit)", async () => await run(built))

    bench("await eff per step (fresh each time)", async () => {
      let x = 0
      for (let i = 0; i < N; i++) x = await succeed(x + 1)
      return x
    })

    bench("await run(eff) per step (explicit)", async () => {
      let x = 0
      for (let i = 0; i < N; i++) x = await run(succeed(x + 1))
      return x
    })
  })
}

// ── async effects (cant fast-path) ─────────────────────────────────

group("async effect — fast-path bypassed", () => {
  const sleepZero = (async (resolve: any) => setImmediate(() => resolve(succeed(42)))) as any
  // sleep(0) actually goes through Op.Async — await eff will spawn a fiber
  bench("await sleep(0)+succeed  (async: fiber)", async () => {
    const { sleep } = await import("../src")
    await sleep(0).flatMap(() => succeed(42))
  })
})

await mitataRun()
