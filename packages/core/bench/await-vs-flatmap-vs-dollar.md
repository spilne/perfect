# `await` vs `.flatMap` vs `eff($)` vs `eff(function*)` — comparison

Companion to [`await-vs-flatmap-vs-dollar.ts`](./await-vs-flatmap-vs-dollar.ts).

Run:

```bash
bun packages/core/bench/await-vs-flatmap-vs-dollar.ts
```

Compares four ways to write an effect pipeline against raw Promise baselines
across five realistic scenarios.

| Approach | what it does |
|---|---|
| **(A) composed `.flatMap`** | one fiber, all steps share state, built once |
| **(B) `await eff` per step** | one thenable fast-path per `await` (microtask per step) |
| **(C) `eff(($) => …)` source syntax** | compiles to (A) via `@perfect/transform` — needs build step |
| **(D) `eff(function* () { yield* … })`** | runtime driver, no build step, effect-ts `Effect.gen` style |

## Composite table (Bun, M-series, N=100, single run)

| Approach | pure × 100 | 1 sleep + 99 pure | 20 × sleep(0) | service × 100 | fail mid-chain |
|---|---:|---:|---:|---:|---:|
| `for` loop (no Promise) | **33 ns** | — | — | — | — |
| composed `.flatMap` + `run`/`runSync` | **1.45 µs** | 1.15 ms | **22.86 ms** | **3.03 µs** | **3.05 µs** |
| composed + `await` (thenable) | 1.51 µs | 1.17 ms | — | — | — |
| `eff($)` syntax (compiled at load) | 3.41 µs | — | — | — | — |
| **`eff(function*)`** (generator, no build) | **6.37 µs** | — | — | **9.34 µs** | **15.81 µs** |
| `Promise.then` chain | 2.85 µs | 1.16 ms | — | 4.93 µs | 3.06 µs |
| `await Promise` per step | 2.24 µs | 1.15 ms | 22.72 ms | — | — |
| `await eff` per step (thenable) | 12.84 µs | 1.19 ms | 22.83 ms | 41.52 µs | 14.63 µs |

## Per-step cost

Divide by N=100:

| Approach | ns/step |
|---|---:|
| `for` loop | 0.33 |
| composed `.flatMap` | 14.5 |
| `await Promise` | 22.4 |
| `Promise.then` | 28.5 |
| `eff($)` compiled | 34 |
| `eff(function*)` | **64** |
| `await eff` (thenable) | 128 |

## Interpretation

### Pure compute × 100
Composed `.flatMap` wins at 14.5 ns/step. The generator form is ~4× slower
(~64 ns/step) because every `yield*` reifies success/failure as a tagged sum
(`{ok: true, val}` / `{ok: false, err}`) so `try/catch` in the generator can
catch any cause. Still **2× faster than `await eff` per step**.

### 1 sleep + 99 pure
All approaches collapse to ~1.15 ms. Pipeline cost is in the noise once a
real timer enters the picture. Use whichever syntax reads best.

### 20 × sleep(0) (all-async)
`await`-per-step matches `composed + run` (~22.8 ms). Once every step yields,
microtask cost dominates and the thenable shim adds nothing on top.

### Service lookup × 100
Composed pays for 100 `c.add(x)` FlatMap nodes: 3.03 µs. Generator pays that
plus driver reification: 9.34 µs. Still vastly better than the naive
`await provide(Counter.get, …)` per step at **41.5 µs** — don't re-resolve
services inside loops in any syntax.

### Fail mid-chain + recover
Composed `.flatMap + .catch + .flatMap` is 3.05 µs — catchAll only wraps the
failing region. `eff(function*)` with `try/catch` pays the tagged-sum
reification on **every** step (15.8 µs), matching `await + try/catch` at 14.6
µs. For error recovery in hot paths, prefer composed `.catch()`.

## Takeaways

1. **Hot loops** → composed `.flatMap`. ~4× faster than `eff(function*)`.
2. **Real I/O** → any syntax works; pipeline cost is invisible next to the
   timer/network.
3. **`eff(function*)` is the Pareto sweet spot for new code**:
   - ~2× faster than `await eff` per step
   - No build step (unlike `eff($)`)
   - `try/catch` works natively
   - Same shape as effect-ts `Effect.gen`
4. **`eff($)` compiled** — best when you want the cleanest syntax *and* native
   speed. Costs the build pipeline.
5. **Services**: resolve **once** at the top of the block (with any syntax).
   Per-step lookups are 10–15× slower.
6. **Composed Eff beats raw `Promise` chains** (14.5 ns/step vs 28.5 ns/step)
   once the chain is built — one fiber walk vs one microtask per step.

## Why `eff($)` needs a build step (and `eff(function*)` doesn't)

`eff(($) => { const x = $(e); … })` is not a runtime construct. The TS source
is rewritten by `@perfect/transform` (SWC plugin / TS rewriter) into a
composed `.flatMap` chain before execution.

`eff(function* () { const x = yield* e; … })` **is** a runtime construct.
`Suspend` implements `[Symbol.iterator]`, so `yield* effect` yields the
effect itself; a driver in `syntax/generator.ts` threads values through
`gen.next(value)` and causes through `gen.throw(cause)` — all live at
runtime, no build tooling needed.

The rewriter currently only recognises `const x = $(e)` declaration form;
reassignments (`x = $(e)`) are not yet supported. The generator form has no
such restriction.

## When to pick which

| If you want… | Use |
|---|---|
| maximum speed, OK with nested lambdas | `.flatMap` |
| clean syntax, no build step, ~4× `.flatMap` cost is fine | `eff(function*)` |
| clean syntax AND native speed, willing to add SWC/TS plugin | `eff($)` |
| quick script, don't care about perf | `await eff` |
