# Concurrency

Structured concurrency via fibers — lightweight green threads scheduled
cooperatively. Forked fibers are tied to the parent's scope; if the parent
is interrupted, children are interrupted too.

## Fork and join

<!-- @embed packages/core/examples/07-concurrency.ts#fork-join -->
```ts
import { succeed, sleep, join } from "@spilne/perfect-core";

// .fork() spawns a fiber. join() awaits its result.
const forkExample = sleep(10)
  .flatMap(() => succeed(42))
  .fork()
  .flatMap((fiber) => join(fiber));

console.log(await forkExample.run()); // → 42
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
import { succeed, sleep } from "@spilne/perfect-core";

// .race(other) — fluent two-way race. First to succeed wins.
const fast = sleep(10).flatMap(() => succeed("fast"));
const slow = sleep(50).flatMap(() => succeed("slow"));

console.log(await fast.race(slow).run()); // → "fast"
```
<!-- @end -->

For 3+ effects, use the variadic form:

<!-- @embed packages/core/examples/07-concurrency.ts#race-variadic -->
```ts
import { succeed, sleep, race } from "@spilne/perfect-core";

// race([...]) — variadic form for 3+ effects.
const winner = await race([
  sleep(30).flatMap(() => succeed("a")),
  sleep(10).flatMap(() => succeed("b")),
  sleep(20).flatMap(() => succeed("c")),
]).run();
console.log(winner); // → "b"
```
<!-- @end -->

`raceFirst([a, b])` — first to **finish** wins (success OR failure).

## Parallel collection — `all`

`all(effects)` runs all effects in parallel. Accepts both **array** and
**object** form; if any fails, the rest are interrupted.

### Array form — `all([a, b, c])` → tuple

<!-- @embed packages/core/examples/07-concurrency.ts#all-parallel -->
```ts
import { succeed, sleep, all } from "@spilne/perfect-core";

// all() runs effects in parallel and collects their results.
const results = await all([
  sleep(10).flatMap(() => succeed("a")),
  sleep(20).flatMap(() => succeed("b")),
  sleep(30).flatMap(() => succeed("c")),
]).run();

console.log(results); // → ["a", "b", "c"]
```
<!-- @end -->

### Object form — `all({ a, b, c })` → record (named destructure)

<!-- @embed packages/core/examples/07-concurrency.ts#all-object -->
```ts
import { succeed, sleep, all } from "@spilne/perfect-core";

// all() also accepts an object — destructure named results.
const { user, posts, friends } = await all({
  user: sleep(10).flatMap(() => succeed({ id: 7, name: "alice" })),
  posts: sleep(20).flatMap(() => succeed([{ id: 1 }, { id: 2 }])),
  friends: sleep(15).flatMap(() => succeed(["bob", "carol"])),
}).run();

console.log(user); // → { id: 7, name: "alice" }
console.log(posts); // → [{ id: 1 }, { id: 2 }]
console.log(friends); // → ["bob", "carol"]
```
<!-- @end -->

## Daemons

`fork(eff)` ties the fiber to the parent scope — when the parent ends, the
fiber is interrupted. Use `forkDaemon(eff)` for long-running background work
that should outlive its spawning context.

```ts
import { forkDaemon, sleep, succeed } from "@spilne/perfect-core";

// Background job — keeps running after parent returns
forkDaemon(
  sleep(60_000).flatMap(() => succeed(console.log("tick"))),
);
```

## Interruption

Fibers are interruptible by default. `uninterruptible(eff)` marks a region as
non-cancellable (use sparingly — only for cleanup that must complete).

```ts
import { uninterruptible } from "@spilne/perfect-core";

// This block runs to completion even if interrupted
const safe = uninterruptible(criticalCleanup);
```

Interruption is cooperative. A fiber observes it when it is running in an
interruptible region, resumes from an async boundary, or walks its
continuation stack. Finalizers registered by `ensuring` / `scoped` still run
during interruption, and async waiters unregister their interrupt handles so
late callbacks do not resume a cancelled fiber.

## Fiber status and supervision

`Fiber` exposes lightweight diagnostics for tests and debugging:

```ts
import { addFiberSupervisor, sleep } from "@spilne/perfect-core";

const stop = addFiberSupervisor({
  onStart: (fiber) => console.log("start", fiber.status),
  onFork: (_parent, child) => console.log("fork", child.snapshot()),
  onInterrupt: (fiber) => console.log("interrupt", fiber.childCount),
  onEnd: (fiber, result) => console.log("end", fiber.status, result.ok),
});

const fiber = await sleep(1_000).forkDaemon().run();
console.log(fiber.snapshot());
fiber.interrupt();
stop();
```

Available fiber diagnostics:

| | |
|---|---|
| `fiber.status` | `"ready"`, `"running"`, `"suspended"`, or `"done"` |
| `fiber.interrupted` | true when interrupted or pending interruption |
| `fiber.childCount` | number of structured children currently owned |
| `fiber.snapshot()` | stable `{ status, interrupted, childCount }` object |
| `fiber.childrenSnapshot()` | copy of currently owned child fibers |
| `addFiberSupervisor(hooks)` | observe fiber start/fork/interrupt/end events |

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
| `addFiberSupervisor(hooks)` | attach diagnostic fiber lifecycle hooks |

## Pitfalls

- **`fork` doesn't auto-`join`.** If you want the value, you have to join.
- **`race` takes an array** — `race([a, b])`, not `race(a, b)`.
- **Daemons leak if you don't track them.** Hold onto the `Fiber` if you
  might need to cancel it.
- **Supervisors are diagnostic hooks.** They should not contain application
  logic; exceptions thrown by hooks are ignored so supervision cannot perturb
  runtime semantics.

## Next

- [Resources and scopes](./07-resources-and-scopes.md)
- [Streams](./09-streams.md)
