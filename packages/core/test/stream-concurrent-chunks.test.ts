import { describe, expect, test } from "bun:test";
import { Chunk, Stream, run, sleep, sync } from "../src";

describe("buffer moves whole chunks but still counts values", () => {
  test("keeps order and every value", async () => {
    const source = Stream.fromArray([1, 2, 3]).concat(Stream.fromArray([4, 5]));
    expect(await run(source.buffer(2).toArray())).toEqual([1, 2, 3, 4, 5]);
  });

  test("the producer never gets more than the capacity ahead", async () => {
    let produced = 0;
    let consumed = 0;
    let maxAhead = 0;
    const source = Stream.range(0, 200)
      .rechunk(7)
      .tap(() => {
        produced++;
        maxAhead = Math.max(maxAhead, produced - consumed);
      });
    await run(source.buffer(10).forEach(() => sleep(0).flatMap(() => sync(() => void consumed++))));
    expect(consumed).toBe(200);
    // The buffer holds 10, plus the chunk the consumer is working on and
    // the one the producer is holding while it waits for room.
    expect(maxAhead).toBeLessThanOrEqual(10 + 7 + 7);
  });
});

describe("groupWithin with chunks", () => {
  test("splits a chunk bigger than a group across groups", async () => {
    const groups = await run(
      Stream.fromChunk(Chunk.fromArray([1, 2, 3, 4, 5, 6, 7]))
        .groupWithin(3, 1_000)
        .map((c) => c.toArray())
        .toArray(),
    );
    expect(groups).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });
});

describe("debounce", () => {
  test("only the last value of a burst survives", async () => {
    const out = await run(Stream.fromArray([1, 2, 3]).debounce(5).toArray());
    expect(out).toEqual([3]);
  });
});

describe("observe", () => {
  test("passes the source through and runs the observer", async () => {
    const seen: number[] = [];
    const out = await run(
      Stream.range(0, 5)
        .observe((s) => s.tap((n) => seen.push(n)))
        .toArray(),
    );
    expect(out).toEqual([0, 1, 2, 3, 4]);
    expect(seen).toEqual([0, 1, 2, 3, 4]);
  });
});
