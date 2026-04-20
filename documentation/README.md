# Perfect — documentation

A TypeScript effect runtime. Effects are values you build, compose, and run —
typed errors, dependency injection, structured concurrency, resource safety,
all tracked in the type system.

```ts
import { eff, succeed, run } from "@perfect/core";

const program = eff(function* () {
  const a = yield* succeed(21);
  const b = yield* succeed(2);
  return a * b;
});

console.log(await run(program)); // 42
```

## Guide

1. [Getting started](./01-getting-started.md) — install, first program, three syntaxes
2. [Effects](./02-effects.md) — `Eff<A, S>`, `Throws<E>`, `Needs<D>`
3. [Syntax](./03-syntax.md) — generator vs `.flatMap` vs `eff($)` rewriter
4. [Services and Layers](./04-services-and-layers.md) — typed dependency injection
5. [Error handling](./05-error-handling.md) — `.catch`, `.catchTag`, `Cause`, defects vs failures
6. [Concurrency](./06-concurrency.md) — fork, race, all, fibers
7. [Resources and scopes](./07-resources-and-scopes.md) — `acquireRelease`, `scoped`, `ensuring`
8. [Retry and schedule](./08-retry-and-schedule.md) — `RetryPolicy`, `Schedule`
9. [Streams](./09-streams.md) — fused, lazy, effect-typed sequences
10. [Testing](./10-testing.md) — `TestClock`, `TestRandom`, `TestConsole`
11. [Resilience + Coordination Primitives](./11-resilience-and-coordination.md) — `CircuitBreaker`, `Singleflight`, `RateLimiter`, `Latch`, `Barrier`, `PubSub`, `SubscriptionRef`, `Pool`
12. [Utilities](./12-utilities.md) — `Duration`, `CacheStore`

## Reference

- [Comparison vs effect-ts / RxJS / plain Promise](./comparison.md)
- [Bench: which syntax to pick](../packages/core/bench/await-vs-flatmap-vs-dollar.md)

## Examples

Every code snippet in these docs is extracted from a real, compilable file in
[`packages/core/examples/`](../packages/core/examples/). To run any of them:

```bash
bun packages/core/examples/01-hello.ts
```

To verify all examples still compile + run:

```bash
bun test packages/core/test/examples.test.ts
```

## How these docs work

Code blocks in `.md` files are auto-generated from `examples/` files via
`bun documentation/build.ts`. CI runs `bun documentation/build.ts --check` —
if anyone edits an example without rerunning the build, the docs go red.
This means the snippets you see are always the actual code that compiles
and runs.

If you're contributing: edit the `examples/` file, run `bun documentation:build`,
commit both.
