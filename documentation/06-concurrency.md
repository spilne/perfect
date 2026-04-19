# Concurrency

Structured concurrency via fibers — lightweight green threads scheduled
cooperatively. Forked fibers are tied to the parent's scope; if the parent
is interrupted, children are interrupted too.

## Fork and join

<!-- @embed packages/core/examples/07-concurrency.ts#fork-join -->
```ts
// .fork() spawns a fiber. join() awaits its result.
const forkExample = sleep(10)
  .flatMap(() => succeed(42))
  .fork()
  .flatMap((fiber) => join(fiber));

assertEq(await run(forkExample), 42);
```
<!-- @end -->

`fork(eff)` returns `Eff<Fiber<A>, never>`. The fiber starts immediately on
the next scheduler tick. `join(fiber)` awaits its result, threading typed
errors and interrupts back through.

## Race

`a.race(b)` — fluent two-way race. First to **succeed** wins, the loser is
interrupted:

<!-- @embed packages/core/examples/07-concurrency.ts#race-method -->
```ts
// .race(other) — fluent two-way race. First to succeed wins.
const fast = sleep(10).flatMap(() => succeed("fast"));
const slow = sleep(50).flatMap(() => succeed("slow"));

assertEq(await run(fast.race(slow)), "fast");
```
<!-- @end -->

For 3+ effects, use the variadic form:

<!-- @embed packages/core/examples/07-concurrency.ts#race-variadic -->
```ts
// race([...]) — variadic form for 3+ effects.
const winner = await run(
  race([
    sleep(30).flatMap(() => succeed("a")),
    sleep(10).flatMap(() => succeed("b")),
    sleep(20).flatMap(() => succeed("c")),
  ]),
);
assertEq(winner, "b");
```
<!-- @end -->

`raceFirst([a, b])` — first to **finish** wins (success OR failure).

## Parallel collection — `all`

`all(effects)` runs all effects in parallel. Accepts both **array** and
**object** form; if any fails, the rest are interrupted.

### Array form — `all([a, b, c])` → tuple

<!-- @embed packages/core/examples/07-concurrency.ts#all-parallel -->
```ts
// all() runs effects in parallel and collects their results.
const results = await run(
  all([
    sleep(10).flatMap(() => succeed("a")),
    sleep(20).flatMap(() => succeed("b")),
    sleep(30).flatMap(() => succeed("c")),
  ]),
);

assertEq(results, ["a", "b", "c"]);
```
<!-- @end -->

### Object form — `all({ a, b, c })` → record (named destructure)

<!-- @embed packages/core/examples/07-concurrency.ts#all-object -->
```ts
// all() also accepts an object — destructure named results.
const { user, posts, friends } = await run(
  all({
    user: sleep(10).flatMap(() => succeed({ id: 7, name: "alice" })),
    posts: sleep(20).flatMap(() => succeed([{ id: 1 }, { id: 2 }])),
    friends: sleep(15).flatMap(() => succeed(["bob", "carol"])),
  }),
);

assertEq(user, { id: 7, name: "alice" });
assertEq(posts, [{ id: 1 }, { id: 2 }]);
assertEq(friends, ["bob", "carol"]);
```
<!-- @end -->

## Daemons

`fork(eff)` ties the fiber to the parent scope — when the parent ends, the
fiber is interrupted. Use `forkDaemon(eff)` for long-running background work
that should outlive its spawning context.

```ts
import { forkDaemon, sleep, succeed } from "@perfect/core";

// Background job — keeps running after parent returns
forkDaemon(
  sleep(60_000).flatMap(() => succeed(console.log("tick"))),
);
```

## Interruption

Fibers are interruptible by default. `uninterruptible(eff)` marks a region as
non-cancellable (use sparingly — only for cleanup that must complete).

```ts
import { uninterruptible } from "@perfect/core";

// This block runs to completion even if interrupted
const safe = uninterruptible(criticalCleanup);
```

## API summary

| | |
|---|---|
| `fork(eff)` | spawn a fiber, scoped to parent |
| `forkDaemon(eff)` | spawn an unscoped fiber |
| `join(fiber)` | await fiber result |
| `interrupt(fiber)` | cancel a fiber |
| `awaitFiber(fiber)` | await Exit (never throws) |
| `race(effects[])` / `a.race(b)` | first success wins |
| `raceFirst(effects[])` / `a.raceFirst(b)` | first finish wins |
| `raceEither([a, b])` / `a.raceEither(b)` | returns `Either<A, B>` |
| `all(effects[])` | parallel + collect tuple |
| `all({ a, b })` | parallel + collect record |
| `uninterruptible(eff)` | block interruption |
| `interruptible(eff)` | restore interruptibility |
| `yieldNow` | give other fibers a turn |

## Pitfalls

- **`fork` doesn't auto-`join`.** If you want the value, you have to join.
- **`race` takes an array** — `race([a, b])`, not `race(a, b)`.
- **Daemons leak if you don't track them.** Hold onto the `Fiber` if you
  might need to cancel it.

## Next

- [Resources and scopes](./07-resources-and-scopes.md)
- [Streams](./09-streams.md)
