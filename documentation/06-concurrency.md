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

console.log(await forkExample.orDie().run()); // → 42
```

<!-- @end -->

`fork(eff)` returns an effect producing `Fiber<A>`. It retains the child's
service requirements, but child failures are observed through the fiber.
The scheduler starts the child. `join(fiber)` awaits its result and exposes a
failed child as `Throws<Cause>`; use `awaitFiber(fiber)` to inspect its `Exit`
without adding a typed failure.

## Race

`a.race(b)` — fluent two-way race. First to **succeed** wins, the loser is
interrupted:

<!-- @embed packages/core/examples/07-concurrency.ts#race-method -->

```ts
import { succeed, sleep, race } from "@spilne/perfect-core";

// .race(other) — fluent two-way race. First to succeed wins.
const fast = sleep(10).flatMap(() => succeed("fast"));
const slow = sleep(50).flatMap(() => succeed("slow"));

console.log(await fast.race(slow).orDie().run()); // → "fast"
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
])
  .orDie()
  .run();
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
])
  .orDie()
  .run();

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
})
  .orDie()
  .run();

console.log(user); // → { id: 7, name: "alice" }
console.log(posts); // → [{ id: 1 }, { id: 2 }]
console.log(friends); // → ["bob", "carol"]
```

<!-- @end -->

## Structured teardown

`all`, `race` and the combinators built on them (`raceAll`, `raceEither`,
`validate`, `timeoutOption`, `timeoutFail`/`timeout`, `hedged`, `parZip`) do
not return while one of their children is still running. When a child fails,
when a race has its first result, or when the combinator itself is interrupted,
the remaining children are interrupted and the combinator waits until every
one of them has finished, finalizers included. Finalizers around the
combinator therefore run after its children's finalizers, never alongside
them:

```ts
import { all, ensuring, sync } from "@spilne/perfect-core";

// Interrupted, this logs "parent released" only after releaseA and releaseB
// have finished, even if they are asynchronous.
const program = ensuring(
  all([ensuring(taskA, releaseA), ensuring(taskB, releaseB)]),
  sync(() => console.log("parent released")),
);
```

The combinator's outcome:

| What happened | Outcome |
|---|---|
| every `all` child succeeded | the results |
| an `all` child failed | that child's failure |
| a `race` child settled first | its value or failure |
| the combinator was interrupted | the interrupt, joined with `Cause.both` to a child failure that had already stopped it |
| a child torn down in any of these cases failed with more than the interrupt (a finalizer died, say) | that failure is joined with `Cause.both` after the above |

A failure raised while a race loser is torn down fails the race even when its
winner succeeded, the same way a failing finalizer fails `ensuring`.

Waiting is the trade-off, as in ZIO and Effect: an uninterruptible child holds
up its combinator. `timeoutOption(uninterruptible(slow), 100)` returns only
once `slow` has finished, and a race loser blocked uninterruptibly on
something only the caller would provide never lets the race return. Keep
uninterruptible regions short, and move work that must outlive the
combinator into `forkDaemon`.

`fork` does not wait: a forked fiber is interrupted when its parent
completes, but the parent does not wait for it to finish.

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
during interruption, error handlers do not (an interrupted fiber cannot
recover; see [Interruption and error handlers](./05-error-handling.md#interruption-and-error-handlers)),
and async waiters unregister their interrupt handles.
A callback that could not be unregistered — a promise that settles late, a
child that finishes after its parent was interrupted — is ignored, so it
cannot resume a cancelled fiber or cut its finalizers short.

### Handoff to waiting fibers

`Queue`, `Semaphore` and `Pool` give an item, a permit or a resource straight
to the oldest fiber waiting for one. That fiber may be interrupted before it
gets to run, for example because its `take` just lost a race against a timer.
The runtime then hands the value back instead of dropping it:

| Primitive | Where a value given back goes |
|---|---|
| `Queue` (and `PubSub`, `SubscriptionRef`, `Stream.fromQueue`) | the next waiting taker, or the head of the queue |
| `Semaphore` | the semaphore, which serves its next waiter |
| `Pool` | the next waiter, or the idle list; after `shutdown()` it is released |
| `Stream.fromCallback` / `Stream.async` / `Stream.asyncChunks` | the next pull, or the head of the push buffer |

Every value is received once or given back once, never both. This also holds
when the waiter is uninterruptible (it keeps the value and sees the interrupt
afterwards) and when `scheduler.shutdown()` dropped its queued run (the value
is given back when the fiber is interrupted).

The guarantee ends where the waiting effect returns the value. An interrupt
that arrives after `take()` returned, before your code has used the value,
drops it like any other result. Use the value in the same step, as in
`queue.take().map(record)`, or inside `uninterruptible`. `Semaphore.withPermit`
and `Pool.use` already register their release in the step that receives the
permit or resource.

Two details follow from giving values back:

- A value given back to a bounded queue goes to its head even when the queue
  is full, so `size` can briefly exceed the capacity. Blocked offers wait
  until it is below the capacity again.
- A blocked `offer` is admitted when a `take` makes room. If that offer is
  interrupted before it runs, its value stays in the queue although the offer
  fails with the interrupt.

Your own `async` primitives get the same behavior by passing a second argument
to `resume`. `onDiscard` runs exactly once if the fiber does not run the value:

```ts
import { async, succeed } from "@spilne/perfect-core";

const takeSlot = async<number>((resume) => {
  const slot = slots.pop()!;
  resume(succeed(slot), () => slots.push(slot));
});
```

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

| API / concept | Behavior |
|---|---|
| `fiber.status` | `"ready"`, `"running"`, `"suspended"`, or `"done"` |
| `fiber.interrupted` | while running: an interrupt is pending or delivered; once done: the result is an interruption |
| `fiber.childCount` | number of structured children currently owned |
| `fiber.snapshot()` | stable `{ status, interrupted, childCount }` object |
| `fiber.childrenSnapshot()` | copy of currently owned child fibers |
| `addFiberSupervisor(hooks)` | observe fiber start/fork/interrupt/end events |

## API summary

| API / concept | Behavior |
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
