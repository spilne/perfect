import { expect, test } from "bun:test";
import { all, run, runSync, succeed, sync } from "../src";
import { Stream } from "../src/stream";

test("grouped accumulates small chunks without changing emitted groups", () => {
  const source = Stream.unfold(0, (i) => (i < 10_003 ? [i, i + 1] : null));
  const stream = source.grouped(4000);
  const groups = runSync(stream.toArray());
  expect(groups.map((group) => group.length)).toEqual([4000, 4000, 2003]);
  expect(groups.flatMap((group) => group.toArray())).toEqual(
    Array.from({ length: 10_003 }, (_, i) => i),
  );
  expect(runSync(stream.toArray()).map((group) => group.toArray())).toEqual(
    groups.map((group) => group.toArray()),
  );
});

test("concurrent consumers have independent grouping buffers", async () => {
  const stream = Stream.range(0, 17).rechunk(1).grouped(4);
  const consume = () => stream.toArray().map((groups) => groups.map((group) => group.toArray()));
  const results = await run(all([consume(), consume()]));
  expect(results[0]).toEqual([[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15], [16]]);
  expect(results[1]).toEqual(results[0]);
});

test("grouped preserves finalizers and empty streams", () => {
  let finalized = 0;
  const stream = Stream.repeat(succeed(1))
    .onFinalize(
      sync(() => {
        finalized++;
      }),
    )
    .grouped(5);
  expect(runSync(stream.take(1).toArray())[0]?.toArray()).toEqual([1, 1, 1, 1, 1]);
  expect(finalized).toBe(1);
  expect(runSync(Stream.empty().grouped(5).toArray())).toEqual([]);
  for (const size of [0, -1, 1.5, Infinity, NaN]) {
    expect(() => Stream.of(1).grouped(size)).toThrow(RangeError);
  }
});
