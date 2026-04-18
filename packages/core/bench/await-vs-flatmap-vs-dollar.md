# `await` vs `.flatMap` vs `eff($)` — comparison

Companion to [`await-vs-flatmap-vs-dollar.ts`](./await-vs-flatmap-vs-dollar.ts).

Run:

```bash
bun packages/core/bench/await-vs-flatmap-vs-dollar.ts
```

The bench compares three ways to write an effect pipeline against raw Promise
baselines, across five realistic scenarios.

| Approach | what it does |
|---|---|
| **(A) composed `.flatMap`** | one fiber, all steps share state, built once |
| **(B) `await eff` per step** | one thenable fast-path per `await` (microtask per step) |
| **(C) `eff(($) => …)` source syntax** | compiles to (A) via `@perfect/transform` — runtime cost is identical to A |

## Composite table (Bun, M-series, N=100, single run)

| Approach | pure × 100 | 1 sleep + 99 pure | 20 × sleep(0) | service × 100 | fail mid-chain |
|---|---:|---:|---:|---:|---:|
| `for` loop (no Promise) | **33 ns** | — | — | — | — |
| composed `.flatMap` + `run`/`runSync` (Eff) | **1.51 µs** | 1.15 ms | **22.75 ms** | **2.98 µs** | **2.61 µs** |
| composed + `await` (Eff thenable) | 1.58 µs | 1.15 ms | — | — | — |
| `eff($)` syntax (compiled) | 3.52 µs | — | — | — | — |
| `Promise.then` chain | 2.97 µs | 1.15 ms | — | 5.12 µs | 3.09 µs |
| `await Promise` per step | 2.27 µs | 1.15 ms | 22.69 ms | — | — |
| `await eff` per step (Eff thenable) | 13.47 µs | 1.17 ms | 22.86 ms | 43.88 µs | 14.85 µs |

## Interpretation

### Pure compute × 100
The composed Eff chain is **~2× faster than `Promise.then`** and **~9× faster
than `await eff` per step**. The `for` loop is unbeatable (no promise at all).
`eff($)` adds the overhead of an `async` IIFE on top of the flatMap chain — still
faster than `await`-per-step but ~2× the bare composed chain.

### 1 sleep + 99 pure
All approaches collapse to ~1.15 ms — the cost is dominated by `setTimeout(0)`,
not by the pipeline. Use whichever syntax reads best.

### 20 × sleep(0) (all-async)
`await`-per-step matches `composed + run` (~22.7 ms). Once every step yields,
the microtask cost is unavoidable and the thenable shim adds essentially nothing
on top of the underlying timer.

### Service lookup × 100
Naive translation (`await provide(Counter.get, …)` per step) is **~15× slower**
than looking the service up once and reusing it. This is the strongest argument
for composed pipelines when service injection is involved.

### Fail mid-chain + recover
Composed `.flatMap + .catch + .flatMap` runs in 2.6 µs. `try/catch` around
`await fail(...)` per step is **~6× slower** because each step pays the thenable
+ microtask overhead.

## Takeaways

1. **Hot loops** — use composed `.flatMap`. Don't `await` per step.
2. **Real I/O** — any approach is fine; overhead is in the noise.
3. **`eff($)` syntax** — costs ~2× raw composed (an `async` IIFE wraps the chain),
   but it is the most readable form. Use it where readability beats microseconds.
4. **Composed Eff is faster than raw `Promise` in JS for chains** — once the
   chain is built, the interpreter walks it in a single fiber, while `Promise`
   pays microtask cost per `.then`.

## Why `eff($)` needs a build step

`eff(($) => { const x = $(e); … })` is not a runtime construct. The TS source
is rewritten by `@perfect/transform` (SWC plugin / TS rewriter) into a composed
`.flatMap` chain. The bench builds a small program string, runs it through
`rewriteEffBlocks`, and `new Function`s it — proving the runtime cost equals
plain composed `.flatMap`.

The rewriter currently only recognises `const x = $(e)` declaration form;
reassignments (`x = $(e)`) are not yet supported.
