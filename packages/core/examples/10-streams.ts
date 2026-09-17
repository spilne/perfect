// Streams: lazy, fused sequences with effect-typed pulls.
// Adjacent map/filter/tap ops fuse into a single chunk pass.
//
// Run: bun packages/core/examples/10-streams.ts

import { Stream, succeed } from "../src";
import { assertEq } from "./_assert";

// >>> example: stream-collect
// Build a stream from an array, transform, collect.
const collected = await Stream.fromArray([1, 2, 3, 4, 5])
  .map((x) => x * 10)
  .filter((x) => x > 20)
  .toArray()
  .run();

assertEq(collected, [30, 40, 50]);
// <<< example

// >>> example: stream-foreach
// forEach — apply an effect per element, return when stream exhausts.
const seen: number[] = [];
await Stream.range(1, 4)
  .forEach((n) => {
    seen.push(n);
    return succeed(undefined);
  })
  .run();
assertEq(seen, [1, 2, 3]);
// <<< example

// >>> example: stream-mapchunks
// take(n) — short-circuit after n elements (lazy: never produces beyond).
const first3 = await Stream.iterate(0, (n) => n + 1)
  .take(3)
  .toArray()
  .run();
assertEq(first3, [0, 1, 2]);
// <<< example

// >>> example: stream-merge-interrupt-after
// merge's background fibers belong to the stream, so pulls racing a timer
// (interruptAfter, timeout, takeUntil, …) don't stop them.
const ticks = await Stream.tick(10)
  .take(3)
  .merge(Stream.tick(15).take(2))
  .interruptAfter(1_000)
  .toArray()
  .run();
assertEq(ticks.length, 5);
// <<< example
