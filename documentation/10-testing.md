# Testing

Time, randomness, and IO are services in Perfect — which means tests can
swap in deterministic implementations.

## TestClock

Virtual time. `sleep(ms)` doesn't actually wait — you `advance(ms)` to fire
the sleep. Run with `provide(eff, Clock, testClock)`.

<!-- @embed packages/core/examples/12-testing.ts#test-clock -->
```ts
import { eff, sleep, provide, run, Clock, TestClock } from "@perfect/core";

// TestClock gives virtual time — `sleep` doesn't wait, you advance manually.
const clock = new TestClock();
const program = eff(function* () {
  const start = clock.now(); // 0
  yield* sleep(1000); // would be 1s in real time
  return clock.now() - start;
});

const fiber = run(provide(program, Clock, clock));
await tick(); // let the fiber register the sleep
clock.advance(1000); // fire the sleep
// 1000ms elapsed in virtual time, ~0ms real
console.log(await fiber); // → 1000
```
<!-- @end -->

The `tick()` helper (`Promise<void>` resolving on the next macrotask) lets
the fiber register its sleep before you advance — necessary because
`run()` returns synchronously while the fiber is still booting.

| | |
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
import { eff, provide, Random, TestRandom } from "@perfect/core";

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

You can also queue specific values for fully scripted tests — see
`packages/core/src/random.ts` for the full API.

## TestConsole

Captures `log` / `warn` / `error` calls instead of writing to stdout:

<!-- @embed packages/core/examples/12-testing.ts#test-console -->
```ts
import { eff, provide, Console, TestConsole } from "@perfect/core";

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

| | |
|---|---|
| `.logs()` | array of `log()` messages |
| `.warns()` | array of `warn()` messages |
| `.errors()` | array of `error()` messages |
| `.all()` | unified, with `level` tags |
| `.clear()` | reset captured state |

## Property-based testing — `Gen` and `forAll`

Lightweight property testing built on the same Random service:

```ts
import { Gen, forAll, run, provide, Random, TestRandom } from "@perfect/core";

const positiveInts = Gen.int(1, 1000);

const property = forAll(positiveInts, 100, (n) => n + 0 === n);

await run(provide(property, Random, new TestRandom(42)));
// passes for all 100 generated values, or fails with the counterexample
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
