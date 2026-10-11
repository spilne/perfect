// Retry + Schedule. RetryPolicy is a fluent builder; only Throws<E> typed
// failures are retried — defects (real bugs from `throw`) are NOT.
//
// Run: bun packages/core/examples/09-retry-schedule.ts

import {
  eff,
  succeed,
  fail,
  sync,
  RetryPolicy,
  RetryDecision,
  Clock,
  Cron,
  Schedule,
  TestClock,
  provide,
  repeat,
  runFiber,
  type Eff,
  type Throws,
} from "../src";
import { assertEq } from "./_assert";

// >>> example: retry-config
// Inline config form — quickest setup. Fluent .retry() method.
let calls = 0;
const flaky: Eff<string, Throws<string>> = eff(function* () {
  calls++;
  if (calls < 3) yield* fail("still failing") as Eff<never, Throws<string>>;
  return "ok";
});

assertEq(await flaky.retry({ times: 5, delay: 5 }).orDie().run(), "ok");
assertEq(calls, 3);
// <<< example

// >>> example: retry-config-flat
// Same retry, chainable form — sync() + .flatMap, no generator.
let callsFlat = 0;
const flakyFlat: Eff<string, Throws<string>> = sync(() => ++callsFlat).flatMap((c) =>
  c < 3 ? (fail("still failing") as Eff<never, Throws<string>>) : succeed("ok"),
);

assertEq(await flakyFlat.retry({ times: 5, delay: 5 }).orDie().run(), "ok");
assertEq(callsFlat, 3);
// <<< example

// >>> example: retry-policy-fluent
// Fluent builder — composable, expressive.
calls = 0;
const policy = RetryPolicy.exponential({ initial: 5, factor: 2 })
  .withMaxRetries(4)
  .withFullJitter()
  .whenError((e: string) => e !== "fatal");

const flaky2: Eff<string, Throws<string>> = eff(function* () {
  calls++;
  if (calls < 3) yield* fail("transient") as Eff<never, Throws<string>>;
  return "recovered";
});

assertEq(await flaky2.retry(policy).orDie().run(), "recovered");
assertEq(calls, 3);
// <<< example

// >>> example: retry-on-cause-only
// Don't retry defects (real bugs) or interrupts — only typed failures.
const probablyABug = sync(() => {
  throw new Error("this is a defect, not a typed failure");
});

const failed = await probablyABug
  .retry(RetryPolicy.recurs(3))
  .catchAllCause((c) => succeed(`gave up: cause=${c._tag}`))
  .orDie()
  .run();
assertEq(failed, "gave up: cause=Die"); // no retries — defects don't retry
// <<< example

// >>> example: retry-all-by
// Retry typed failures and defects with per-outcome logic.
let unstable = 0;
const mayFail: Eff<string, Throws<string>> = eff(function* () {
  unstable++;
  if (unstable < 2) {
    throw new Error("ephemeral network issue");
  }
  if (unstable < 4) {
    yield* fail("transient typed bridge issue") as Eff<never, Throws<string>>;
  }
  return "ok";
});

const recovered = await mayFail
  .retryAllBy({
    maxRetries: 4,
    handle: (attempt) => {
      if (attempt._tag === "success") return RetryDecision.stop();
      return RetryDecision.retry();
    },
  })
  .orDie()
  .run();

assertEq(recovered, "ok");
// <<< example

// >>> example: cron
// A job on a cron expression: here every 15 minutes, in UTC. `repeat` runs
// it once right away, then at each matching time. (A TestClock stands in
// for the real clock, so the example doesn't take an hour.)
const clock = new TestClock(Date.parse("2026-01-01T10:07:00Z"));
const ranAt: string[] = [];
const report = sync(() => void ranAt.push(new Date(clock.now()).toISOString().slice(11, 16)));
const reports = runFiber(
  provide(
    repeat(
      report,
      Schedule.intersect(Schedule.cron("*/15 * * * *", { utc: true }), Schedule.recurs(3)),
    ),
    Clock,
    clock,
  ),
);
// Move the clock to 10:15, 10:30 and 10:45.
for (const minutes of [8, 15, 15]) {
  await new Promise((resolve) => setTimeout(resolve, 1));
  clock.advance(minutes * 60_000);
}
await reports.await();
assertEq(ranAt, ["10:07", "10:15", "10:30", "10:45"]);

// To run only at the matching times, wait for the next one first.
const nightly = Cron.parse("30 2 * * *"); // 02:30 every day, local time
const nextRun = new Date(nightly.next(Date.now()));
assertEq([nextRun.getHours(), nextRun.getMinutes()], [2, 30]);
// forever(nightly.untilNext.flatMap(() => backup))
// <<< example
