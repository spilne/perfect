import { describe, test, expect } from "bun:test";
import { run, Stream } from "../src";

describe("Stream.dropWhile", () => {
  test("drops the prefix matching the predicate, keeps the rest", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 1)
        .dropWhile((n: number) => n < 3)
        .toArray(),
    );
    expect(result).toEqual([3, 4, 1]);
  });

  test("drops across chunk boundaries", async () => {
    const s = Stream.of(1, 2).concat(Stream.of(3, 4));
    expect(await run(s.dropWhile((n: number) => n < 4).toArray())).toEqual([4]);
  });

  test("drops everything when the predicate always holds", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3)
          .dropWhile(() => true)
          .toArray(),
      ),
    ).toEqual([]);
  });

  test("drops nothing when the first element fails the predicate", async () => {
    expect(
      await run(
        Stream.of(5, 1, 2)
          .dropWhile((n: number) => n < 3)
          .toArray(),
      ),
    ).toEqual([5, 1, 2]);
  });
});
