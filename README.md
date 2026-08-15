# Perfect

A TypeScript effect runtime with cats-effect / ZIO-level ergonomics — typed
errors, typed dependencies, structured concurrency, and pull-based streams,
behind a fluent API with no `pipe()`.

```ts
import { eff, succeed, fail, provide, service, run } from "@perfect/core";

interface Db { findUser(id: string): Eff<User, Throws<NotFound>> }
const Db = service<Db>("Db");

const program = eff(function* () {
  const db = yield* Db.get;
  const user = yield* db.findUser("u1");
  return user.name;
}).catchTag("NotFound", () => succeed("anonymous"));

await run(provide(program, Db, liveDb));
```

> **Pre-release.** The packages are not on npm yet — clone the repo and use
> the Bun workspace. Install lines below describe the post-publish shape.

## Core ideas

- **`Eff<A, S>`** — an effect returning `A` with a flat union `S` of effect
  tags. `Throws<E>` marks a typed error (removed by `.catch()` /
  `.catchTag()`); `Needs<D>` marks a dependency (removed by `provide()`).
- **Fluent, not pipeable** — `.flatMap()`, `.map()`, `.catch()`, `.retry()`,
  `.timeout()` as methods. Three authoring styles: method chains, generators
  (`eff(function* () { yield* e })`), and compile-time `eff(($) => { … })`
  sugar via the SWC plugin.
- **Structured concurrency** — fibers with parent/child lifecycles, scopes
  with never-dropped finalizers, interruption that reaches every driver.
- **Deterministic time** — every time-gated primitive reads the `Clock`
  service; a `TestClock` drives retries, streams, and rate limiters in tests
  with zero real waiting.
- **Pull-based streams** — chunked, resource-safe, with structural
  backpressure and a full concurrency/time operator set.

## Packages

| Package | What it is |
|---|---|
| `@perfect/core` | The runtime: `Eff`, fibers, scheduler, `Stream`, concurrency primitives (Queue, Semaphore, CircuitBreaker, RateLimiter, Pool, PubSub, …), Layer DI, Clock/Random/Console/Logger/Tracer/Metrics services, and the `connect` contracts for messaging backends |
| `@perfect/http` | HTTP client: typed errors, retry, streaming (SSE/NDJSON), mock client, schema-library-agnostic validation |
| `@perfect/kafka` | Kafka backend for the `connect` contracts — offset-safe parallel commits, injected driver interface |
| `@perfect/topology` | Flink-style stream topology engine: windows, joins, stage planning, distributed runs over a shuffle transport |
| `@perfect/otel` | OpenTelemetry bridge for the core `Tracer` and `Metrics` services |
| `@perfect/http-otel` | HTTP-specific tracing middleware + W3C trace propagation for `@perfect/http` |
| `@perfect/transform` | Build-time compiler for the `eff(($) => …)` and `for { x <- e } yield` syntaxes (Bun plugin) |
| `@perfect/swc-plugin` | SWC WASM plugin — the canonical `eff(($) => …)` compiler for Next.js/Vite/anything SWC |

## Development

```bash
bun install
make ci          # fmt-check, lint, typecheck, tests, build, smoke, docs, perf gate
bun test --recursive packages/
```

Rust toolchain needed only for `bun run build:swc` (the SWC WASM plugin).
The full guide lives in `documentation/` (VitePress site).

## Status

Pre-1.0, developed in the open. The runtime core is extensively tested
(1,000+ tests, benchmarked against effect-ts) and the API surface is
stabilizing; expect breaking changes until the first npm release.

## License

MIT
