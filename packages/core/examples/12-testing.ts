// Testing primitives — TestClock, TestRandom, TestConsole.
// Provide them as services to make time, randomness, and IO deterministic.
//
// Run: bun packages/core/examples/12-testing.ts

import {
  eff,
  sleep,
  provide,
  run,
  Clock,
  TestClock,
  Random,
  TestRandom,
  Console,
  TestConsole,
  TestRuntime,
  Log,
  RetryPolicy,
  fail,
  sync,
  type Eff,
  type Throws,
} from "../src";
import { assertEq } from "./_assert";

// Yield control so the fiber registers its next suspension before we advance.
const tick = () => new Promise((r) => setTimeout(r, 0));

// >>> example: test-clock
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
assertEq(await result, 1000); // 1000ms elapsed in virtual time, ~0ms real
// <<< example

// >>> example: test-random
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
assertEq(guess, second);
// <<< example

// >>> example: test-console
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
assertEq(captured.logs(), ["hello", "world"]);
// <<< example

// >>> example: test-runtime
// Every test service at once. runAdvancing moves the clock from one sleep
// to the next, so retries with backoff finish at once, in order.
const t = TestRuntime.make({ seed: 1 });
let attempts = 0;
const callApi: Eff<string, Throws<string>> = sync(() => ++attempts).flatMap((n) =>
  n < 3 ? Log.warn(`attempt ${n} failed`).flatMap(() => fail("unavailable")) : sync(() => "ok"),
);
const answer = await t.runAdvancing(
  callApi.retry(RetryPolicy.exponential({ initial: 1_000 }).withMaxRetries(5)).orDie(),
);
assertEq(answer, "ok");
assertEq(t.clock.now(), 3_000); // waited 1 s, then 2 s, in test time
assertEq(
  t.logger.atLevel("warn").map((entry) => entry.message),
  ["attempt 1 failed", "attempt 2 failed"],
);
// <<< example
