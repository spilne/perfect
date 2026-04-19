# Resources and Scopes

Anything you acquire — file handles, db connections, locks, subscriptions —
needs to be released. Perfect guarantees release runs on success, failure,
*and* interrupt via `acquireRelease` and `scoped`.

## acquireRelease

Pair an acquire effect with a release function. Release is registered with
the surrounding scope; nothing else changes about the program flow.

<!-- @embed packages/core/examples/08-resources.ts#acquire-release -->
```ts
// .acquireRelease(release) — fluent, pair an acquire with cleanup.
// .scoped() — define when the cleanup fires (the scope boundary).
const events: string[] = [];
const useFile = sync(() => {
  events.push("opened");
  return { read: () => "contents" };
})
  .acquireRelease(() => sync(() => { events.push("closed"); }))
  .flatMap((file) => sync(() => file.read()))
  .scoped();

assertEq(await run(useFile), "contents");
assertEq(events, ["opened", "closed"]);
```
<!-- @end -->

`scoped(eff)` defines the scope boundary. When the scope ends, all
finalizers registered inside fire in LIFO order.

## Release on failure

The release fires whether the inner effect succeeds or fails:

<!-- @embed packages/core/examples/08-resources.ts#release-on-failure -->
```ts
// Release fires even when the inner effect fails.
const trace: string[] = [];
const safe = scoped(
  eff(function* () {
    yield* acquireRelease(
      sync(() => trace.push("acquire")),
      () => sync(() => { trace.push("release"); }),
    );
    yield* (fail("crashed") as Eff<never, Throws<string>>);
    return "unreachable";
  }) as any,
).catch((e: any) => succeed(`recovered: ${e}`));

assertEq(await run(safe as any), "recovered: crashed");
assertEq(trace, ["acquire", "release"]);
```
<!-- @end -->

## ensuring — try/finally for effects

When you don't have an acquire/release pair, just want a finalizer:

<!-- @embed packages/core/examples/08-resources.ts#ensuring -->
```ts
// .ensuring(finalizer) — fluent try/finally for any effect.
let cleanedUp = false;
const tracked = succeed("done")
  .ensuring(sync(() => { cleanedUp = true; }));

assertEq(await run(tracked), "done");
assertEq(cleanedUp, true);
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

## Scoped layers

Layers can register finalizers via `acquireRelease` — they fire when the
program built with `.with(layer)` ends. See
[Services and Layers](./04-services-and-layers.md#resources).

## API summary

| | |
|---|---|
| `acquireRelease(acquire, release)` | pair acquire effect + release function |
| `scoped(eff)` | define scope boundary; finalizers fire on exit |
| `ensuring(eff, finalizer)` | always-run finalizer (no acquire pair) |
| `onExit(eff, handler)` | inspect Exit, then propagate original outcome |

## Pitfalls

- **`acquireRelease` outside `scoped` leaks.** Without a scope, there's
  nowhere to register the finalizer.
- **Release runs are uninterruptible.** If your release effect is slow, it
  will block scope exit. Make releases fast.
- **`ensuring` doesn't acquire — just finalizes.** Use `acquireRelease` if
  you need acquire-then-release semantics.

## Next

- [Retry and Schedule](./08-retry-and-schedule.md)
- [Streams](./09-streams.md)
