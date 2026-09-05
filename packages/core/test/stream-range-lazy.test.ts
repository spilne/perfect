import { expect, test } from "bun:test";
import { runSync } from "../src";
import { Stream } from "../src/stream";

test("range supports early termination without constructing the entire range", () => {
  expect(runSync(Stream.range(0, 1_000_000_000).take(1).toArray())).toEqual([0]);
  expect(runSync(Stream.range(0, Infinity).take(3).toArray())).toEqual([0, 1, 2]);
});

test("range preserves values across chunk boundaries and can be consumed again", () => {
  const stream = Stream.range(7, 30_007, 3);
  const expected = Array.from({ length: 10_000 }, (_, i) => 7 + i * 3);
  expect(runSync(stream.toArray())).toEqual(expected);
  expect(runSync(stream.toArray())).toEqual(expected);
  expect(runSync(Stream.range(5, 5).toArray())).toEqual([]);
  expect(runSync(Stream.range(10, 5).toArray())).toEqual([]);
});
