# Comparison

How Perfect compares to effect-ts, RxJS, and plain Promises.

## vs effect-ts

Same model (typed errors, services, fibers, scopes, streams). Differences:

| | effect-ts | Perfect |
|---|---|---|
| Effect type | `Effect<A, E, R>` (3 params) | `Eff<A, S>` (2 params, S is flat union) |
| Composition | `pipe(eff, Effect.flatMap(f))` | `eff.flatMap(f)` (fluent) |
| Generator | `Effect.gen(function* () { yield* e })` | `eff(function* () { yield* e })` |
| Services | `Context.Tag<T>` (class) | `service<T>("Name")` (function) |
| Layer | `Layer<Out, E, In>` (class) | `Layer<Services, E>` (type alias over Eff) |
| Layer apply | `Effect.provide(eff, layer)` | `eff.with(layer)` |
| Layer chain | `Layer.merge(a, b)` / `Layer.provide` | `merge` / `.and` / `.provideTo` / `.with` (chains) |
| Bundle | larger surface, more constructors | leaner — reuses primitives where possible |

When to pick which:
- **effect-ts** — established ecosystem, more batteries (Schema, HttpApi,
  RPC, Cluster).
- **Perfect** — fluent API, flat union, lighter type ergonomics, a small
  runtime, and explicit syntactic styles. See the linked benchmark for current
  machine-specific measurements.

## vs RxJS

RxJS is **multi-shot** push streams. Perfect's `Eff` is **single-shot** pull.
Different semantics, different problems.

| | RxJS | Perfect |
|---|---|---|
| Cardinality | `Observable<T>` — 0..N values | `Eff<A>` — exactly 1 |
| Streams | first-class (everything is one) | `Stream<A>` is the multi-value variant |
| Cancellation | `subscription.unsubscribe()` | structured: scope/fiber boundaries |
| Errors | one terminal `error` channel | typed `Throws<E>` + structured `Cause` |
| DI | external | first-class via services |
| Resource management | `using` operator (limited) | `acquireRelease` + `scoped` (guaranteed) |

For an RxJS-like API on Perfect, use `Stream<A>` — but it's a pull model with
a fused interpreter, not a push model.

## vs plain Promises

Promises are great until you need:

| Need | Promise solution | Perfect solution |
|---|---|---|
| Typed errors | `try/catch` + cast `unknown` | `Throws<E>` in the type |
| Cancellation | `AbortController` (manual, viral) | structured fiber interrupts |
| Dependency injection | constructor injection or globals | `service<T>` + `provide` / `Layer` |
| Retry | hand-rolled | `RetryPolicy.exponential().withFullJitter()` |
| Resource cleanup | `try/finally` | `acquireRelease` + `scoped` |
| Concurrency limit | `Promise.all` chunked manually | `Semaphore`, `WorkerPool` |
| Race | `Promise.race` (winner only, others orphaned) | `race` (interrupts losers) |
| Testing time | mock `setTimeout` | `TestClock` |

Performance depends on runtime and hardware. The repository performance gate
tracks direct `.flatMap`, generator, Promise, and stream paths so regressions
are measured instead of frozen into documentation claims.

## When NOT to use Perfect

- Your codebase has no async at all → just use sync code.
- You need exactly one async call and never compose → `await fetch(...)` is fine.
- You want a battle-tested, mature ecosystem with dozens of integrations →
  use effect-ts (or wait until Perfect grows them).

## See also

- [Bench: which syntax to pick](../packages/core/bench/await-vs-flatmap-vs-dollar.md) — perf table
- [Bench: CPS feasibility](../packages/core/bench/cps-feasibility.ts) — why we don't use CPS
