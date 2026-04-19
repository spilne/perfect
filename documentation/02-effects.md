# Effects

The `Eff<A, S>` type is the heart of Perfect. Building one describes what you
*want* to happen; running it makes it happen. The `S` parameter is a flat
union of effect tags — typed errors (`Throws<E>`), service dependencies
(`Needs<D>`), and other markers — that the type system uses to keep you honest.

## The shape

```ts
type Eff<A, S = never> = ...;  // produces A; uses effects S
```

- `Eff<number, never>` — a pure effect that produces a number, no errors, no deps
- `Eff<User, Throws<NotFound>>` — produces a User, may fail with NotFound
- `Eff<Db, Needs<Config>>` — needs a Config service to produce a Db

## Constructors

| | |
|---|---|
| `succeed(a)` | wrap a pure value as `Eff<A, never>` |
| `sync(() => a)` | run a synchronous side-effect |
| `fail(e)` | typed failure, `Eff<never, Throws<E>>` |
| `die(e)` | unrecoverable defect (a bug); not in the typed channel |
| `async(register)` | bridge a callback-based async API |
| `tryPromise(() => p)` | bridge a Promise, errors caught into `Throws<unknown>` |
| `sleep(ms)` | suspend for `ms` according to the Clock service |

```ts
import { succeed, fail, sync, tryPromise } from "@perfect/core";

const a = succeed(42);                              // Eff<number, never>
const b = sync(() => Date.now());                   // Eff<number, never>
const c = fail("nope");                             // Eff<never, Throws<string>>
const d = tryPromise(() => fetch("/api/users"));    // Eff<Response, Throws<unknown>>
```

## Running

| | When to use |
|---|---|
| `runSync(eff)` | synchronous-only programs (throws if the effect suspends) |
| `run(eff)` | returns `Promise<A>`, rejects with squashed cause on failure |
| `runExit(eff)` | returns `Promise<Exit<E, A>>` — never throws; you switch on the exit |
| `runFiber(eff)` | returns a `Fiber<A>` you can join, interrupt, race externally |

Use `runExit` when you need to inspect the failure structure (typed error,
defect, interrupt) instead of catching squashed exceptions.

## Effect tags

The `S` channel is a union of opaque marker types — Perfect peels them off as
you handle them.

```ts
import type { Eff, Throws, Needs } from "@perfect/core";

declare const fetchUser: (id: number) => Eff<User, Throws<NotFound> | Needs<Db>>;

// .catch removes Throws<NotFound>
const safe = fetchUser(1).catch((_e) => succeed(defaultUser));
//    Eff<User, Needs<Db>>

// provide() removes Needs<Db>
const wired = provide(safe, Db, dbImpl);
//    Eff<User, never>

// Now run() is available — only effects with `S = never` can run.
await run(wired);
```

Calling `run` on an effect that still has unhandled `Throws` or `Needs`
shows a TypeScript error pointing at the unhandled tags.

## Pure values, side effects, and laziness

`succeed(v)` does *nothing* until run. `sync(() => v)` runs the lambda each
time the effect is executed. The runtime guarantees the lambda fires inside
the fiber, so any thrown exception becomes a defect (`Cause.Die`).

```ts
const lazy = sync(() => Math.random());

// Two runs → two different values
runSync(lazy); // e.g. 0.42
runSync(lazy); // e.g. 0.91
```

## Pitfalls

- **Don't `await` a Promise inside `sync`.** Use `tryPromise` to bridge.
- **`runSync` throws on Async/Fork/Sleep.** If you need them, use `run`.
- **Throwing inside a `sync` body is a defect, not a typed failure.** Use
  `fail()` for expected, recoverable errors. See [error handling](./05-error-handling.md).

## Next

- [Syntax — three styles compared](./03-syntax.md)
- [Services and Layers](./04-services-and-layers.md)
