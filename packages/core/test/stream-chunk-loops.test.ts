import { describe, expect, test } from "bun:test";
import { Chunk, Stream, run } from "../src";

describe("Chunk iteration", () => {
  test("a whole chunk and a slice both iterate in order", () => {
    const whole = Chunk.fromArray([1, 2, 3, 4]);
    expect([...whole]).toEqual([1, 2, 3, 4]);
    expect([...whole.drop(1).take(2)]).toEqual([2, 3]);
    expect(Array.from(whole.drop(4))).toEqual([]);
  });
});

describe("fromIterable", () => {
  test("reads an infinite generator lazily", async () => {
    function* naturals() {
      let n = 0;
      while (true) yield n++;
    }
    expect(await run(Stream.fromIterable(naturals()).take(5).toArray())).toEqual([0, 1, 2, 3, 4]);
  });

  test("stopping early calls the iterator's return()", async () => {
    let closed = false;
    function* source() {
      try {
        for (let i = 0; i < 100; i++) yield i;
      } finally {
        closed = true;
      }
    }
    await run(Stream.fromIterable(source()).take(2).drain());
    expect(closed).toBe(true);
  });

  test("take(1) reads only one value", async () => {
    let read = 0;
    function* counted() {
      for (let i = 0; i < 100; i++) {
        read++;
        yield i;
      }
    }
    await run(Stream.fromIterable(counted()).take(1).drain());
    expect(read).toBe(1);
  });

  test("works for a Set and runs fresh each time", async () => {
    const set = new Set([1, 2, 3]);
    const eff = Stream.fromIterable(set).toArray();
    expect(await run(eff)).toEqual([1, 2, 3]);
    expect(await run(eff)).toEqual([1, 2, 3]);
  });
});

describe("unfold in batches", () => {
  test("gives the same values as before, across batch boundaries", async () => {
    const out = await run(Stream.unfold(0, (n) => (n < 10_000 ? [n, n + 1] : null)).toArray());
    expect(out).toHaveLength(10_000);
    expect(out[9_999]).toBe(9_999);
  });

  test("take(1) calls f once", async () => {
    let calls = 0;
    await run(
      Stream.unfold(0, (n) => {
        calls++;
        return [n, n + 1];
      })
        .take(1)
        .drain(),
    );
    expect(calls).toBe(1);
  });
});

describe("grouped and takeWhile across chunks", () => {
  test("grouped fills groups across chunk boundaries", async () => {
    const source = Stream.fromArray([1, 2, 3]).concat(Stream.fromArray([4, 5, 6, 7]));
    const groups = await run(
      source
        .grouped(3)
        .map((c) => c.toArray())
        .toArray(),
    );
    expect(groups).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });

  test("takeWhile stops inside a chunk", async () => {
    const out = await run(
      Stream.fromArray([1, 2, 3, 10, 4])
        .takeWhile((n) => n < 5)
        .toArray(),
    );
    expect(out).toEqual([1, 2, 3]);
  });
});
