// Tests for Stream operator fusion — pure ops (map/filter/filterMap/tap)
// accumulate into a buffer and compile into a single chunk walk when flushed.
// Behaviour should be indistinguishable from non-fused chains.
//
// Test structure borrowed from promin's optimized-stream-pipeline.test.ts.

import { describe, test, expect } from "bun:test";
import { Stream, run } from "../src";

describe("Stream fusion — fusible operators", () => {
  test("map transforms each item", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .map((x) => x * 2)
        .toArray(),
    );
    expect(result).toEqual([2, 4, 6]);
  });

  test("chained maps compose in order", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .map((x) => x + 1)
        .map((x) => x * 10)
        .toArray(),
    );
    expect(result).toEqual([20, 30, 40]);
  });

  test("5 chained maps produce the same result as unfused", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .map((x) => x + 1)
        .map((x) => x * 2)
        .map((x) => x - 3)
        .map((x) => x * 10)
        .map((x) => x + 1)
        .toArray(),
    );
    expect(result).toEqual([11, 31, 51, 71, 91]);
  });

  test("filter keeps matching items", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .filter((x) => x > 3)
        .toArray(),
    );
    expect(result).toEqual([4, 5]);
  });

  test("filterMap keeps non-undefined values", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .filterMap((x) => (x % 2 === 0 ? x * 10 : undefined))
        .toArray(),
    );
    expect(result).toEqual([20, 40]);
  });

  test("tap runs side effects without changing values", async () => {
    const effects: number[] = [];
    const result = await run(
      Stream.of(1, 2, 3)
        .tap((x) => effects.push(x))
        .map((x) => x * 2)
        .toArray(),
    );
    expect(result).toEqual([2, 4, 6]);
    expect(effects).toEqual([1, 2, 3]);
  });

  test("mixed map+filter chain (correct order)", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5, 6)
        .map((x) => x * 2)
        .filter((x) => x > 5)
        .map((x) => x + 100)
        .toArray(),
    );
    expect(result).toEqual([106, 108, 110, 112]);
  });

  test("tap executes BEFORE map", async () => {
    const seen: number[] = [];
    await run(
      Stream.of(10, 20, 30)
        .tap((x) => seen.push(x))
        .map((x) => x + 1)
        .toArray(),
    );
    expect(seen).toEqual([10, 20, 30]);
  });
});

describe("Stream fusion — non-fusible ops flush the buffer", () => {
  test("take after a fused chain", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .map((x) => x * 10)
        .filter((x) => x > 10)
        .take(2)
        .toArray(),
    );
    expect(result).toEqual([20, 30]);
  });

  test("drop after a fused chain", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .map((x) => x * 10)
        .drop(2)
        .toArray(),
    );
    expect(result).toEqual([30, 40, 50]);
  });

  test("grouped after a fused chain", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .map((x) => x * 2)
        .grouped(2)
        .map((c) => c.toArray())
        .toArray(),
    );
    expect(result).toEqual([[2, 4], [6, 8], [10]]);
  });

  test("flatMap after a fused chain", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .map((x) => x + 1)
        .flatMap((x) => Stream.of(x, x))
        .toArray(),
    );
    expect(result).toEqual([2, 2, 3, 3, 4, 4]);
  });

  test("scan after a fused chain", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4)
        .filter((x) => x % 2 === 0)
        .scan(0, (acc, a) => acc + a)
        .toArray(),
    );
    expect(result).toEqual([0, 2, 6]);
  });

  test("zipWithIndex after a fused chain", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4)
        .filter((x) => x % 2 === 0)
        .zipWithIndex()
        .toArray(),
    );
    expect(result).toEqual([
      [2, 0],
      [4, 1],
    ]);
  });

  test("fused ops after a non-fusible op restart the buffer", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .map((x) => x * 2) // fused
        .take(3) // non-fusible — flush [map]
        .map((x) => x + 100) // new fused
        .filter((x) => x > 102) // append to new fused
        .toArray(),
    );
    expect(result).toEqual([104, 106]);
  });
});

describe("Stream fusion — terminals flush the buffer", () => {
  test("fold over a fused chain", async () => {
    const sum = await run(
      Stream.of(1, 2, 3, 4, 5)
        .filter((x) => x % 2 === 0)
        .map((x) => x * 10)
        .fold(0, (a, b) => a + b),
    );
    expect(sum).toBe(60);
  });

  test("drain flushes and consumes", async () => {
    const seen: number[] = [];
    await run(
      Stream.of(1, 2, 3)
        .tap((x) => seen.push(x))
        .drain(),
    );
    expect(seen).toEqual([1, 2, 3]);
  });

  test("count after a fused chain", async () => {
    const n = await run(
      Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .filter((x) => x > 3)
        .map((x) => x * 2)
        .count(),
    );
    expect(n).toBe(7);
  });

  test("head / last", async () => {
    const head = await run(
      Stream.of(1, 2, 3)
        .map((x) => x * 10)
        .head(),
    );
    const last = await run(
      Stream.of(1, 2, 3)
        .map((x) => x * 10)
        .last(),
    );
    expect(head).toBe(10);
    expect(last).toBe(30);
  });
});

describe("Stream fusion — correctness vs unfused", () => {
  test("fused result matches mapChunks reference", async () => {
    const input = Array.from({ length: 1000 }, (_, i) => i);

    const fused = await run(
      Stream.fromArray(input)
        .map((x) => x + 1)
        .filter((x) => x % 3 === 0)
        .map((x) => x * 2)
        .toArray(),
    );

    // Reference: compute the same transformation without a stream.
    const ref = input
      .map((x) => x + 1)
      .filter((x) => x % 3 === 0)
      .map((x) => x * 2);

    expect(fused).toEqual(ref);
  });

  test("single-element chunks (iterate) behave correctly", async () => {
    // iterate emits one-element chunks; fusion should still work across them.
    const result = await run(
      Stream.iterate(0, (n) => n + 1)
        .take(10)
        .filter((x) => x % 2 === 0)
        .map((x) => x * 5)
        .toArray(),
    );
    expect(result).toEqual([0, 10, 20, 30, 40]);
  });

  test("empty result after filter", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .filter(() => false)
        .toArray(),
    );
    expect(result).toEqual([]);
  });

  test("filter with 'drop' action inverts the predicate", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .filter((x) => x > 3, "drop")
        .toArray(),
    );
    expect(result).toEqual([1, 2, 3]);
  });

  test("unNone drops null and undefined", async () => {
    const s: any = Stream.of(1, null, 2, undefined, 3);
    const result = await run(s.unNone().toArray());
    expect(result).toEqual([1, 2, 3]);
  });

  test("unNone fuses with adjacent ops", async () => {
    const s: any = Stream.of(1, null, 2, undefined, 3);
    const result = await run(
      s
        .unNone()
        .map((x: number) => x * 10)
        .toArray(),
    );
    expect(result).toEqual([10, 20, 30]);
  });
});
