# @spilne/perfect-core

The Perfect effect runtime — like effect-ts or ZIO, but with a flat union
type and a fluent API. `Eff<A, S>` is an effect producing `A`, where `S` is a
flat union of effect tags: `Throws<E>` for typed errors, `Needs<D>` for typed
dependencies. Compose with `.flatMap()`, `.map()`, `.catch()` — no `pipe()`.
Everything else in the Perfect stack (`@spilne/perfect-http`, `@spilne/perfect-kafka`,
`@spilne/perfect-topology`, …) is built on this package.

## Install

```bash
bun add @spilne/perfect-core
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

```ts
import { eff, succeed, fail, type Eff, type Throws } from "@spilne/perfect-core";

type Err = { _tag: "NotFound"; id: number };

const lookup = (id: number): Eff<string, Throws<Err>> =>
  id === 1 ? succeed("alice") : (fail({ _tag: "NotFound", id }) as Eff<never, Throws<Err>>);

const program = eff(function* () {
  const name = yield* lookup(1).catchTag("NotFound", (e) => succeed(`missing ${e.id}`));
  return `hello, ${name}`;
});

console.log(await program.run()); // → "hello, alice"
```

Handling an error removes it from the type: after `.catchTag("NotFound", …)`
the `Throws<Err>` is gone, so the compiler knows the program can't fail.

Services work the same way — a dependency is an effect tag until you provide it:

```ts
import { eff, succeed, service, provide, type Eff } from "@spilne/perfect-core";

interface Greeter {
  greet(name: string): Eff<string, never>;
}
const Greeter = service<Greeter>("Greeter");

const app = eff(function* () {
  const greeter = yield* Greeter.get;
  return yield* greeter.greet("world");
});

const wired = provide(app, Greeter, { greet: (name) => succeed(`hello, ${name}`) });
console.log(wired.runSync()); // → "hello, world"
```

## Three syntactic styles

All compile to the same fiber walk — pick by readability:

```ts
// Generator (recommended — no build step)
const a = eff(function* () {
  return (yield* succeed(21)) * 2;
});

// Composed .flatMap (fastest)
const b = succeed(21).flatMap((x) => succeed(x * 2));

// eff($) sugar (cleanest — needs @spilne/perfect-swc-plugin or @spilne/perfect-transform)
const c = eff(($) => {
  const x = $(succeed(21));
  return x * 2;
});
```

## Running

| Function        | When to use                                                   |
| --------------- | ------------------------------------------------------------- |
| `runSync(eff)`  | Sync only — throws if the effect suspends.                    |
| `run(eff)`      | Returns `Promise<A>`, rejects with squashed cause on failure. |
| `runExit(eff)`  | Returns `Promise<Exit<E, A>>` — never throws.                 |
| `runFiber(eff)` | Returns a `Fiber<A>` you can join, interrupt, race.           |

Each is also a fluent method: `program.run()`, `.runSync()`, `.runExit()`, `.runFiber()`.

## What's in the box

- **Constructors** — `succeed`, `fail`, `die`, `sync`, `suspend`, `async`,
  `tryPromise`, `fromPromise`
- **Errors** — `Throws<E>`, `.catch` / `.catchTag`, `Cause` (Fail | Die |
  Interrupt | composites), `Exit`, `TaggedError`
- **Services + Layers** — `service`, `provide`, `Layer` for memoized,
  dependency-ordered wiring
- **Concurrency** — `fork` / `forkDaemon`, `join`, `race` / `raceAll`,
  `all`, `timeout`, structured interruption, `Fiber` supervision
- **Resources** — `acquireRelease`, `scoped`, `ensuring`, `onExit`,
  `createGracefulShutdown`
- **Retry + schedule** — `retry`, `RetryPolicy`, `Schedule`, `repeat`,
  `repeatUntilWithBackoff`, `hedged`
- **Streams** — `Stream` / `Chunk` / `Sink` / `Pipes`: fused, lazy,
  effect-typed; lazy `fromAsyncIterable`, `mapAccumulate`, backend-powered
  `statefulMap`, ordered/unordered `parEvalMap`, `switchMap`, `exhaustMap`,
  `combineLatest`, `withLatest`, single-pass `broadcastThrough`, reliable
  `observe`/`takeUntil`, typed stream recovery, source-reacquiring `retryFrom`,
  Clock-driven time operators, and CSV/base64/binary pipes
- **Coordination** — `Ref`, `Deferred`, `Queue`, `Semaphore`, `Latch`,
  `Barrier`, `PubSub`, `SubscriptionRef`, `Pool`, `WorkerPool`
- **Resilience** — `CircuitBreaker`, `RateLimiter`, `Throttle`,
  `Singleflight`, `cached` / `CacheStore`
- **Observability** — `Logger`, `Tracer` / `withSpan`, `Metrics` (`Counter`,
  `Gauge`, `Histogram`) — bridge to OpenTelemetry via `@spilne/perfect-otel`
- **Testing** — `TestClock`, `TestRandom`, `TestConsole`, `TestLogger`,
  `TestTracer`, property testing with `Gen` / `forAll`
- **Utilities** — `Duration`, typeclasses (`Eq`, `Ord`, `Show`, `Monoid`)

## Subpath exports

| Import                         | Contents                                                                                                                                                                               |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@spilne/perfect-core`         | Everything above                                                                                                                                                                       |
| `@spilne/perfect-core/stream`  | `Stream`, `Chunk`, `Sink`, `Pipes` (also re-exported from root)                                                                                                                        |
| `@spilne/perfect-core/retry`   | `RetryPolicy`, `Schedule`, `retryWith`, and scheduled repetition                                                                                                                       |
| `@spilne/perfect-core/connect` | Queue-agnostic endpoint contracts (`Streamable`, `Sinkable`, `Envelope`, `Codec`, `OffsetTracker`, …) — implemented by `@spilne/perfect-kafka`, consumed by `@spilne/perfect-topology` |
| `@spilne/perfect-core/syntax`  | The `eff` comprehension entry point                                                                                                                                                    |
| `@spilne/perfect-core/worker`  | `WorkerPool`                                                                                                                                                                           |

## Links

- Repo: https://github.com/spilne/perfect
- Full guide: [`documentation/`](../../documentation/) — core, HTTP,
  observability, messaging, distributed backends, and topologies; embedded
  tutorial snippets are extracted from executable files in [`examples/`](./examples/)
- Syntax bench: [`bench/await-vs-flatmap-vs-dollar.md`](./bench/await-vs-flatmap-vs-dollar.md)
