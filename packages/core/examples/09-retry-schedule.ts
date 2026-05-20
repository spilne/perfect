// Retry + Schedule. RetryPolicy is a fluent builder; only Throws<E> typed
// failures are retried — defects (real bugs from `throw`) are NOT.
//
// Run: bun packages/core/examples/09-retry-schedule.ts

import {
  eff, succeed, fail, sync, RetryPolicy, run, type Eff, type Throws,
} from "../src";
import { assertEq } from "./_assert";

// >>> example: retry-config
// Inline config form — quickest setup. Fluent .retry() method.
let calls = 0;
const flaky: Eff<string, Throws<string>> = eff(function* () {
  calls++;
  if (calls < 3) yield* (fail("still failing") as Eff<never, Throws<string>>);
  return "ok";
});

assertEq(await flaky.retry({ times: 5, delay: 5 }).run(), "ok");
assertEq(calls, 3);
// <<< example

// >>> example: retry-config-flat
// Same retry, chainable form — sync() + .flatMap, no generator.
let callsFlat = 0;
const flakyFlat: Eff<string, Throws<string>> = sync(() => ++callsFlat).flatMap((c) =>
  c < 3 ? (fail("still failing") as Eff<never, Throws<string>>) : succeed("ok"),
);

assertEq(await flakyFlat.retry({ times: 5, delay: 5 }).run(), "ok");
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
  if (calls < 3) yield* (fail("transient") as Eff<never, Throws<string>>);
  return "recovered";
});

assertEq(await flaky2.retry(policy).run(), "recovered");
assertEq(calls, 3);
// <<< example

// >>> example: retry-on-cause-only
// Don't retry defects (real bugs) or interrupts — only typed failures.
const probablyABug = sync(() => {
  throw new Error("this is a defect, not a typed failure");
}) as any;

const failed = await probablyABug
  .retry(RetryPolicy.recurs(3))
  .catchAllCause((c: any) => succeed(`gave up: cause=${c._tag}`))
  .run();
assertEq(failed, "gave up: cause=Die"); // no retries — defects don't retry
// <<< example
