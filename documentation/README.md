# Perfect — documentation

A TypeScript effect runtime. Effects are values you build, compose, and run —
typed errors, dependency injection, structured concurrency, resource safety,
all tracked in the type system.

```ts
import { eff, succeed, run } from "@spilne/perfect-core";

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
6. [Concurrency](./06-concurrency.md) — fork, race, all, fibers, supervision
7. [Resources and scopes](./07-resources-and-scopes.md) — `acquireRelease`, `scoped`, finalizer causes
8. [Retry and schedule](./08-retry-and-schedule.md) — `RetryPolicy`, `Schedule`
9. [Streams](./09-streams.md) — fused, lazy, effect-typed streams, pipes, sinks
10. [Testing](./10-testing.md) — `TestClock`, `TestRandom`, `TestConsole`
11. [Resilience + Coordination Primitives](./11-resilience-and-coordination.md) — `CircuitBreaker`, `Singleflight`, `RateLimiter`, `Latch`, `Barrier`, `PubSub`, `SubscriptionRef`, `Pool`
12. [Utilities](./12-utilities.md) — `Duration`, `CacheStore`
13. [HTTP](./13-http.md) — typed-effect HTTP client, retry, streaming, mock
14. [HTTP — OpenTelemetry](./14-http-otel.md) — tracing middleware + W3C `traceparent` injection
15. [Observability](./15-observability.md) — structured logging, spans, metrics, general OTel bridge
16. [Messaging contracts and Kafka](./16-messaging.md) — connector capabilities, codecs, acknowledgements, drivers
17. [Redis and PostgreSQL backends](./17-distributed-backends.md) — distributed primitives, Redis Streams, PGMQ
18. [Stateful topologies](./18-topologies.md) — keyed state, windows, shuffle stages, delivery guarantees
19. [Package map](./19-packages.md) — every public package, adapter, internal validation workspace, and subpath export

## Reference

- [Comparison vs effect-ts / RxJS / plain Promise](./comparison.md)
- [Bench: which syntax to pick](../packages/core/bench/await-vs-flatmap-vs-dollar.md)

## Examples

The core tutorial examples embedded between `@embed` markers are extracted
from real, executable files in
[`packages/core/examples/`](../packages/core/examples/). To run one:

```bash
bun packages/core/examples/01-hello.ts
```

To verify all examples still compile + run:

```bash
bun test packages/core/test/examples.test.ts
```

## How these docs work

Embedded code blocks in `.md` files are generated from `examples/` via
`bun documentation/build.ts`. CI runs `bun documentation/build.ts --check` —
if anyone edits an embedded example without rebuilding, the docs go red.
Connector examples that require Redis, PostgreSQL, or Kafka are verified by
their package unit and opt-in real-service integration suites.

The documentation build also checks the [package map](./19-packages.md)
against every workspace manifest and requires every package to carry its own
README.

If you're contributing: edit the `examples/` file, run `bun documentation:build`,
commit both.
