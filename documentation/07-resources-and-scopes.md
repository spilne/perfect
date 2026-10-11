# Resources and Scopes

Anything you acquire — file handles, db connections, locks, subscriptions —
needs to be released. Perfect guarantees release runs on success, failure,
*and* interrupt via `acquireRelease` and `scoped`.

## acquireRelease

Pair an acquire effect with a release function. Release is registered with
the surrounding scope; nothing else changes about the program flow.

<!-- @embed packages/core/examples/08-resources.ts#acquire-release -->

```ts
import { sync, acquireRelease, scoped } from "@spilne/perfect-core";

// .acquireRelease(release) — fluent, pair an acquire with cleanup.
// .scoped() — define when the cleanup fires (the scope boundary).
const events: string[] = [];
const useFile = sync(() => {
  events.push("opened");
  return { read: () => "contents" };
})
  .acquireRelease(() =>
    sync(() => {
      events.push("closed");
    }),
  )
  .flatMap((file) => sync(() => file.read()))
  .scoped();

console.log(await useFile.run()); // → "contents"
console.log(events); // → ["opened", "closed"]
```

<!-- @end -->

`scoped(eff)` defines the scope boundary. When the scope ends, all
finalizers registered inside fire in LIFO order.

## When you need a scope, and `use`

Every fiber has a scope of its own, which ends with the fiber. A program
run with `run` doesn't need a `scoped(...)` around it: what it acquires is
released, in reverse order, before `run` returns, also when it fails. A
connection handler or any forked fiber is the same: its resources go when
the fiber does.

You need a scope only to release something **sooner**, before the fiber
ends: a connection per request, a file per job. `resource.use(f)` is the
short form: it acquires the resource, runs `f` with it, and releases it as
soon as `f` is done.

```ts
// Each report gets its own connection, closed as soon as it is sent.
const send = (report: Report) =>
  Tcp.connect(address, { codec }).use((connection) => connection.send(report));
```

`resource.use(f)` is `scoped(resource.flatMap(f))`; a function form,
`use(resource, f)`, is exported too.

## Release on failure

The release fires whether the inner effect succeeds or fails:

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/08-resources.ts#release-on-failure -->

```ts
import { eff, succeed, fail, sync, acquireRelease, scoped, type Eff, type Throws } from "@spilne/perfect-core";

// Release fires even when the inner effect fails.
const trace: string[] = [];
const safe = scoped(
  eff(function* () {
    yield* acquireRelease(
      sync(() => trace.push("acquire")),
      () =>
        sync(() => {
          trace.push("release");
        }),
    );
    yield* fail("crashed") as Eff<never, Throws<string>>;
    return "unreachable";
  }),
).catch((e) => succeed(`recovered: ${e}`));

console.log(await safe.run()); // → "recovered: crashed"
console.log(trace); // → ["acquire", "release"]
```

<!-- @end -->

:::

::: syntax chainable
<!-- @embed packages/core/examples/08-resources.ts#release-on-failure-flat -->

```ts
import { succeed, fail, sync, acquireRelease, scoped, type Eff, type Throws } from "@spilne/perfect-core";

// Same guarantee, chainable form — .acquireRelease + .scoped + .catch.
const traceFlat: string[] = [];
const safeFlat = sync(() => {
  traceFlat.push("acquire");
})
  .acquireRelease(() =>
    sync(() => {
      traceFlat.push("release");
    }),
  )
  .flatMap(() => fail("crashed") as Eff<never, Throws<string>>)
  .scoped()
  .catch((e) => succeed(`recovered: ${e}`));

console.log(await safeFlat.run()); // → "recovered: crashed"
console.log(traceFlat); // → ["acquire", "release"]
```

<!-- @end -->
:::

::::

## ensuring — try/finally for effects

When you don't have an acquire/release pair, just want a finalizer:

<!-- @embed packages/core/examples/08-resources.ts#ensuring -->

```ts
import { succeed, sync } from "@spilne/perfect-core";

// .ensuring(finalizer) — fluent try/finally for any effect.
let cleanedUp = false;
const tracked = succeed("done").ensuring(
  sync(() => {
    cleanedUp = true;
  }),
);

console.log(await tracked.run()); // → "done"
console.log(cleanedUp); // → true
```

<!-- @end -->

## Nesting

Multiple `acquireRelease` inside one `scoped` register multiple finalizers,
released in LIFO order:

```ts
scoped(
  eff(function* () {
    const a = yield* acquireRelease(openA, closeA);
    const b = yield* acquireRelease(openB, closeB);
    const c = yield* acquireRelease(openC, closeC);
    // ... use a, b, c
  }),
);
// closes c, then b, then a
```

## Finalizer failures

Finalizer failures are not swallowed. Perfect preserves them in the `Cause`
tree so diagnostics are deterministic:

| body result | finalizer result | final outcome |
|---|---|---|
| success | success | original success |
| success | failure/defect | finalizer failure |
| failure | success | original failure |
| failure | failure/defect | `Cause.Then(bodyCause, finalizerCause)` |

That means `runExit` can distinguish "the program failed" from "the program
failed, then cleanup also failed":

```ts
import { Cause, die, fail, runExit } from "@spilne/perfect-core";

const exit = await fail("body")
  .ensuring(die("release"))
  .runExit();

if (exit._tag === "Failure") {
  console.log(Cause.pretty(exit.cause));
  // → (Fail(body) ; Die(release))
}
```

`scoped(acquireRelease(...))` follows the same rule when a scope closes, and so
does a release registered without `scoped`, which runs when the fiber
completes.

Parallel children release first. `all`, `race`, `forEachPar`, `timeoutOption`
and the other combinators built on them wait for their children's finalizers before they
return, so a finalizer or scope around them runs after those finalizers have
finished. A child's finalizer failure joins the combinator's outcome with
`Cause.both` (see
[Structured teardown](./06-concurrency.md#structured-teardown)).

An interrupt adds `Interrupt` to the outcome. An interrupt that arrives while
a finalizer runs waits for it: interrupting `succeed(1).ensuring(release)`
while `release` fails with `e` ends as `(Fail(e) ; Interrupt)`, and while
`release` succeeds as `Interrupt`. A release in `scoped` or in the fiber's own
scope ends the same way. Error handlers
around an interrupted effect don't run (see
[Interruption and error handlers](./05-error-handling.md#interruption-and-error-handlers)),
so they can neither swallow the interrupt nor hide the finalizer failure.

## Scoped layers

Layers can register finalizers via `acquireRelease` — they fire when the
program built with `.with(layer)` ends. See
[Services and Layers](./04-services-and-layers.md#resources).

## `using` and `await using`

Resources and TypeScript's `using` declarations work together both ways.

`runDisposable(eff)` runs a scoped effect and keeps its scope open until
the result is disposed. In plain async code, `await using` releases its
resources when the block ends:

```ts
await using db = await runDisposable(openDatabase); // an acquireRelease
await db.value.query("select 1");
// released here, also when the block throws
```

It rejects, with nothing left open, if the effect fails. Disposing rejects
if a release fails. Like `run`, it takes `{ signal }`: aborting closes the
scope early.

`acquireDisposable(eff)` goes the other way. It takes an object that has
`[Symbol.asyncDispose]` or `[Symbol.dispose]` (a file handle, a
`DisposableStack`, a library client) and makes it a resource of the
surrounding scope:

```ts
const file = yield* acquireDisposable(tryPromise(() => fs.open(path), toError));
// file[Symbol.asyncDispose]() runs when the scope closes
```

## API summary

| API / concept | Behavior |
|---|---|
| `acquireRelease(acquire, release)` | pair acquire effect + release function |
| `scoped(eff)` | define scope boundary; finalizers fire on exit |
| `resource.use(f)` | acquire, run `f`, release right after (`scoped(resource.flatMap(f))`) |
| `ensuring(eff, finalizer)` | always-run finalizer (no acquire pair) |
| `onExit(eff, handler)` | finalizer that receives the Exit; the original outcome propagates |
| `runDisposable(eff)` | run a scoped effect, keep it open until disposed (`await using`) |
| `acquireDisposable(eff)` | a `Disposable` / `AsyncDisposable` as a resource of the scope |

## Pitfalls

- **Use `scoped` to choose the release boundary.** Without an explicit scope,
  the runtime registers release in a fiber-level scope and runs it when that
  fiber completes. An explicit scope can release resources earlier.
- **An acquire that waits needs `uninterruptibleMask`.** Wrap it so the wait
  can be cancelled with `restore(...)` while the release is still registered
  atomically once the resource is granted; `interruptible(...)` would fail at
  once when the acquire runs from cleanup of an interrupted fiber.
- **Release runs are uninterruptible.** If your release effect is slow, it
  will block scope exit. Make releases fast.
- **Release failures are visible.** Inspect them with `runExit` or `onExit`
  when cleanup failure is operationally meaningful. An interrupted fiber skips
  error handlers, so a plain `.catchAllCause` never sees the release failure of
  an interrupted fiber. `onExit` does, and so does a handler inside an
  uninterruptible region:
  `uninterruptibleMask((restore) => restore(eff).catchAllCause(handler))`. See
  [Interruption and error handlers](./05-error-handling.md#interruption-and-error-handlers).
- **`ensuring` doesn't acquire — just finalizes.** Use `acquireRelease` if
  you need acquire-then-release semantics.
- **Permits and pooled resources come back on interrupt.** `Semaphore.withPermit`
  and `Pool.use` return what they hold on success, failure and interrupt, also
  when the interrupt lands between the grant and the start of the body. See
  [Handoff to waiting fibers](./06-concurrency.md#handoff-to-waiting-fibers).
- **Cleanup belongs in finalizers, not error handlers.** An interrupted fiber
  skips `.catchAllCause`, `.tapErrorCause` and every other handler; `ensuring`,
  `acquireRelease` and `onExit` still run.

## Next

- [Retry and Schedule](./08-retry-and-schedule.md)
- [Streams](./09-streams.md)
