import { describe, test, expect } from "bun:test";
import { run, Stream } from "../src";

describe("Stream.fromIterable", () => {
  test("consumes any iterable (Set)", async () => {
    expect(await run(Stream.fromIterable(new Set([1, 2, 3])).toArray())).toEqual([1, 2, 3]);
  });

  test("consumes a generator", async () => {
    function* gen() {
      yield "a";
      yield "b";
    }
    expect(await run(Stream.fromIterable(gen()).toArray())).toEqual(["a", "b"]);
  });

  test("empty iterable yields empty stream", async () => {
    expect(await run(Stream.fromIterable([]).toArray())).toEqual([]);
  });
});
