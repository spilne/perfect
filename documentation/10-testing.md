# Testing

Time, randomness, and IO are services in Perfect — which means tests can
swap in deterministic implementations.

## TestClock

Virtual time. `sleep(ms)` doesn't actually wait — you `advance(ms)` to fire
the sleep. Run with `provide(eff, Clock, testClock)`.

<!-- @embed packages/core/examples/12-testing.ts#test-clock -->

```ts
import { eff, sleep, provide, run, Clock, TestClock } from "@spilne/perfect-core";

// TestClock gives virtual time — `sleep` doesn't wait, you advance manually.
const clock = new TestClock();
const program = eff(function* () {
  const start = clock.now(); // 0
  yield* sleep(1000); // would be 1s in real time
  return clock.now() - start;
});

const result = run(provide(program, Clock, clock)); // a Promise, started now
await tick(); // let the program reach its sleep
clock.advance(1000); // fire the sleep
// 1000ms elapsed in virtual time, ~0ms real
console.log(await result); // → 1000
```

<!-- @end -->

The `tick()` helper (`Promise<void>` resolving on the next macrotask) lets
the fiber reach its next suspension before you advance. `run()` starts work
immediately, but earlier asynchronous operations may delay sleep registration.
Check `pendingCount` or use a synchronous test scheduler when you need an exact
registration boundary.

| API / concept | Behavior |
|---|---|
| `new TestClock(start = 0)` | construct with optional start time |
| `.now()` | current virtual time |
| `.advance(ms)` | move forward, fire eligible sleeps |
| `.setTime(t)` | jump to absolute time (must be ≥ now) |
| `.pendingCount` | sleeps still waiting |
| `.pendingDeadlines()` | their deadlines, sorted |

## TestRandom

Seeded PRNG for reproducibility:

<!-- @embed packages/core/examples/12-testing.ts#test-random -->

```ts
import { eff, provide, run, Random, TestRandom } from "@spilne/perfect-core";

// TestRandom — seeded for reproducibility.
const seeded = new TestRandom(42);
const guess = await provide(
  eff(function* () {
    const r = yield* Random.get;
    return yield* r.nextInt(100);
  }),
  Random,
  seeded,
).run();
// Deterministic output for seed=42 — re-running gives the same number.
const second = await provide(
  eff(function* () {
    const r = yield* Random.get;
    return yield* r.nextInt(100);
  }),
  Random,
  new TestRandom(42),
).run();
console.log(guess); // → second
```

<!-- @end -->

Use `random.setNextValues([0.1, 0.9])` to supply the next two floats in `[0, 1)`.
After that queue is consumed, generation resumes from the seeded PRNG.
`reseed(seed)` resets the generator and clears queued values.

## TestConsole

Captures `log` / `warn` / `error` calls instead of writing to stdout:

<!-- @embed packages/core/examples/12-testing.ts#test-console -->

```ts
import { eff, provide, run, Console, TestConsole } from "@spilne/perfect-core";

// TestConsole captures log output instead of writing to stdout.
const captured = new TestConsole();
await provide(
  eff(function* () {
    const c = yield* Console.get;
    yield* c.log("hello");
    yield* c.log("world");
    return undefined;
  }),
  Console,
  captured,
).run();
console.log(captured.logs()); // → ["hello", "world"]
```

<!-- @end -->

| API / concept | Behavior |
|---|---|
| `.logs()` | array of `log()` messages |
| `.warns()` | array of `warn()` messages |
| `.errors()` | array of `error()` messages |
| `.all()` | unified, with `level` tags |
| `.clear()` | reset captured state |
| `new TestConsole(["line 1", "line 2"])` / `.feed(...lines)` | lines that `readLine()` returns, in order (`undefined` when there are none left) |
| `.remainingInput()` | lines not read yet |

## Files, logs and spans

The other built-in services have test versions too:

| Service | Test version | Check with |
| --- | --- | --- |
| `FileSystem` | `new TestFileSystem({ "/path": "contents" })` | read the files back through the service |
| `ConfigProvider` | `new TestConfigProvider({ PORT: "8080" })` | the settings your code reads |
| `Logger` | `new TestLogger()` | `.entries`, `.messages`, `.atLevel("warn")`, `.clear()` |
| `Tracer` | `new TestTracer()` | `.finished` (ended spans, children first), `.find(name)`, `.clear()` |

See [Files](./12-utilities.md#filesystem) and
[Observability](./15-observability.md) for examples.

## Running fibers step by step — `SyncScheduler`

Fibers normally run on the default scheduler, which picks its own moment.
`SyncScheduler` only runs them when you call `flush()`, so a test decides
exactly when each step happens:

<!-- @embed packages/core/examples/17-runtime-utilities.ts#sync-scheduler -->

```ts
import { join, run, runFiber, sync, SyncScheduler } from "@spilne/perfect-core";

// SyncScheduler runs fibers only when you call flush(), which makes the
// order of steps in a test fully predictable.
const scheduler = new SyncScheduler();
const steps: string[] = [];
const fiber = runFiber(
  sync(() => steps.push("ran")),
  scheduler,
);
// nothing yet
console.log(steps); // → []
scheduler.flush();
console.log(steps); // → ["ran"]
console.log(await run(join(fiber))); // → 1
```

<!-- @end -->

Pass the scheduler to `runFiber(eff, scheduler)` or `run(eff, scheduler)`
for one effect. `setDefaultScheduler(scheduler)` changes it for everything
that doesn't pass one; set it back when the test ends.

## Property-based testing — `Gen` and `forAll`

Lightweight property testing built on the same Random service:

```ts
import { Gen, forAll, provide, Random, TestRandom } from "@spilne/perfect-core";

const positiveInts = Gen.int(1, 1000);

const property = forAll(positiveInts, 100, (n) => n + 0 === n);

// .orDie() because a failed property is a typed PropertyFailure, and run()
// only accepts effects whose errors are handled.
await provide(property, Random, new TestRandom(42)).orDie().run();
// passes for all 100 generated values, or rejects with the counterexample
```

No shrinking yet — counterexamples are reported as generated. For richer
property testing, integrate with fast-check and bridge through `Random`.

## Pitfalls

- **`runSync` doesn't work with TestClock + sleep.** Sleeps suspend; use
  `run` and remember to `await tick()` before `advance`.
- **TestClock advance order matters.** Advance fires sleeps with
  `deadline ≤ time` in deadline order — register everything first.

## Next

- [Comparison vs other libraries](./comparison.md)
