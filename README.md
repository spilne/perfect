# Perfect

A TypeScript effect runtime with cats-effect / ZIO-level ergonomics — typed
errors, typed dependencies, structured concurrency, and pull-based streams,
behind a fluent API with no `pipe()`.

```ts
import {
  TaggedError,
  eff,
  fail,
  provide,
  service,
  succeed,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";

interface User {
  name: string;
}

class NotFound extends TaggedError("NotFound")<{ id: string }>() {}

interface Db {
  findUser(id: string): Eff<User, Throws<NotFound>>;
}
const Db = service<Db>("Db");

const liveDb: Db = {
  findUser: (id) =>
    id === "u1"
      ? succeed({ name: "Ada" })
      : fail(new NotFound({ id })),
};

const program = eff(function* () {
  const db = yield* Db.get;
  const user = yield* db.findUser("u1");
  return user.name;
}).catchTag("NotFound", () => succeed("anonymous"));

console.log(await provide(program, Db, liveDb).run()); // Ada
```

> **Pre-release.** The packages are not on npm yet — clone the repo and use
> the Bun workspace. Install lines below describe the post-publish shape.

Try the [playground built into the guide](documentation/playground.md), or
[open its project in StackBlitz](https://stackblitz.com/fork/github/spilne/perfect/tree/main/templates/stackblitz?title=Perfect%20Playground).
The standalone starter is checked against the monorepo in CI and becomes directly
runnable when `@spilne/perfect-core` is published to npm.

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

| Package                       | What it is                                                                                                                                                                                                                                                  |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@spilne/perfect-core`               | The runtime: `Eff`, fibers, scheduler, `Stream`, concurrency primitives (Queue, Semaphore, CircuitBreaker, RateLimiter, Pool, PubSub, …), Layer DI, Clock/Random/Console/Logger/Tracer/Metrics services, and the `connect` contracts for messaging backends |
| `@spilne/perfect-http`               | HTTP client: typed errors, retry, streaming (SSE/NDJSON), mock client, schema-library-agnostic validation                                                                                                                                                   |
| `@spilne/perfect-kafka`              | Driver-neutral Kafka backend for the `connect` contracts — typed failures, offset-safe parallel commits, and a config builder                                                                                                                               |
| `@spilne/perfect-kafka-kafkajs`      | KafkaJS adapter for `@spilne/perfect-kafka` (Bun and Node.js)                                                                                                                                                                                                      |
| `@spilne/perfect-kafka-platformatic` | Platformatic Kafka adapter for `@spilne/perfect-kafka` (Node.js)                                                                                                                                                                                                   |
| `@spilne/perfect-postgres`           | Postgres coordination, LISTEN/NOTIFY streams, durable state, and PGMQ queues with optional FIFO message groups                                                                                                                                              |
| `@spilne/perfect-redis`              | Redis-backed concurrency primitives, durable state, Redis Streams, and bounded Pub/Sub connectors                                                                                                                                                           |
| `@spilne/perfect-topology`           | Flink-style stream topology engine: windows, joins, stage planning, distributed runs over a shuffle transport                                                                                                                                               |
| `@spilne/perfect-otel`               | OpenTelemetry bridge for the core `Tracer` and `Metrics` services                                                                                                                                                                                           |
| `@spilne/perfect-http-otel`          | HTTP-specific tracing middleware + W3C trace propagation for `@spilne/perfect-http`                                                                                                                                                                                |
| `@spilne/perfect-transform`          | Build-time compiler for the `eff(($) => …)` and `for { x <- e } yield` syntaxes (Bun plugin)                                                                                                                                                                |
| `@spilne/perfect-swc-plugin`         | SWC WASM plugin — the canonical `eff(($) => …)` compiler for Next.js/Vite/anything SWC                                                                                                                                                                      |

## Development

```bash
bun install
make ci          # fmt-check, lint, typecheck, tests, build, smoke, docs, perf gate
bun test --recursive packages/
```

### Node runtime test lane

Use this to validate Bun/TypeScript behavior on Node's `node:test` runner:

```bash
bun run test:node      # curated default set (known Node-incompatible cases are excluded)
bun run test:node:all  # run the full curated list, including known problematic tests
bun run test:node -- --list  # print exact files selected by the Node runner
bun run test:node:coverage  # run Node suite and print a basic function coverage summary
bun run test:node:coverage -- --coverage-dir=coverage/node-all # store V8 coverage JSON in a custom directory
bun run test:coverage     # run full Bun suite with coverage (lcov report in coverage/bun)
```

The `node-runtime` CI job runs this script in a Node matrix (`22.x`, `24.x`) on
macOS for manual/dispatch runs.

Rust toolchain needed only for `bun run build:swc` (the SWC WASM plugin).
The [full guide](documentation/README.md) covers the core runtime, HTTP,
observability, connector contracts, Kafka, Redis/PostgreSQL backends, and
stateful topologies. The [package map](documentation/19-packages.md) tracks
every public adapter, compiler integration, subpath export, and the private
real-service test workspace.
Package versioning and publication are documented in [`RELEASING.md`](RELEASING.md).

## Status

Pre-1.0, developed in the open. The runtime core is extensively tested
(1,000+ tests, benchmarked against effect-ts) and the API surface is
stabilizing; expect breaking changes until the first npm release.

## License

MIT
