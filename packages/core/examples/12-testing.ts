// Testing primitives — TestClock, TestRandom, TestConsole.
// Provide them as services to make time, randomness, and IO deterministic.
//
// Run: bun packages/core/examples/12-testing.ts

import {
  eff, sleep, provide, run,
  Clock, TestClock, Random, TestRandom, Console, TestConsole,
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

const fiber = run(provide(program, Clock, clock));
await tick(); // let the fiber register the sleep
clock.advance(1000); // fire the sleep
assertEq(await fiber, 1000); // 1000ms elapsed in virtual time, ~0ms real
// <<< example

// >>> example: test-random
// TestRandom — seeded for reproducibility.
const seeded = new TestRandom(42);
const guess = await run(
  provide(
    eff(function* () {
      const r = yield* Random.get;
      return yield* r.nextInt(100);
    }),
    Random,
    seeded,
  ),
);
// Deterministic output for seed=42 — re-running gives the same number.
const second = await run(
  provide(
    eff(function* () {
      const r = yield* Random.get;
      return yield* r.nextInt(100);
    }),
    Random,
    new TestRandom(42),
  ),
);
assertEq(guess, second);
// <<< example

// >>> example: test-console
// TestConsole captures log output instead of writing to stdout.
const captured = new TestConsole();
await run(
  provide(
    eff(function* () {
      const c = yield* Console.get;
      yield* c.log("hello");
      yield* c.log("world");
      return undefined;
    }),
    Console,
    captured,
  ),
);
assertEq(captured.logs(), ["hello", "world"]);
// <<< example
