import { describe, expect, test } from "bun:test";
import { RawStream, Stream, run } from "../src";

describe("RawStream", () => {
  test("fuses pure ops into a single pass over the source", () => {
    const seen: number[] = [];
    const result = RawStream.from([1, 2, 3, 4, 5, 6])
      .tap((n) => seen.push(n))
      .map((n) => n * 2)
      .filter((n) => n % 3 === 0)
      .toArray();

    expect(result).toEqual([6, 12]);
    // One pass: tap saw every source element exactly once, in order.
    expect(seen).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test("take after filter takes post-filter elements", () => {
    const result = RawStream.range(0, 100)
      .filter((n) => n % 10 === 0)
      .take(3)
      .toArray();
    expect(result).toEqual([0, 10, 20]);
  });

  test("is lazy — take short-circuits an infinite source", () => {
    let produced = 0;
    const result = RawStream.iterate(1, (n) => {
      produced++;
      return n * 2;
    })
      .take(5)
      .toArray();

    expect(result).toEqual([1, 2, 4, 8, 16]);
    // Only the elements needed to yield 5 values were ever produced.
    expect(produced).toBeLessThanOrEqual(5);
  });

  test("ops around a position-dependent op still fuse correctly", () => {
    const result = RawStream.range(1, 11)
      .map((n) => n + 1) // 2..11
      .drop(2) // 4..11
      .map((n) => n * 10)
      .filter((n) => n > 60)
      .toArray();
    expect(result).toEqual([70, 80, 90, 100, 110]);
  });

  test("takeWhile / dropWhile / flatMap / scan / concat", () => {
    expect(
      RawStream.range(0, 10)
        .takeWhile((n) => n < 4)
        .toArray(),
    ).toEqual([0, 1, 2, 3]);
    expect(
      RawStream.range(0, 6)
        .dropWhile((n) => n < 4)
        .toArray(),
    ).toEqual([4, 5]);
    expect(
      RawStream.of(1, 2)
        .flatMap((n) => [n, n * 10])
        .toArray(),
    ).toEqual([1, 10, 2, 20]);
    expect(
      RawStream.of(1, 2, 3)
        .scan(0, (a, b) => a + b)
        .toArray(),
    ).toEqual([0, 1, 3, 6]);
    expect(RawStream.of(1).concat(RawStream.of(2, 3)).toArray()).toEqual([1, 2, 3]);
  });

  test("range respects negative step and rejects zero", () => {
    expect(RawStream.range(5, 0, -2).toArray()).toEqual([5, 3, 1]);
    expect(() => RawStream.range(0, 5, 0)).toThrow(RangeError);
  });

  test("terminal operators return plain values, not effects", () => {
    const s = () => RawStream.range(1, 6);
    expect(s().fold(0, (a, b) => a + b)).toBe(15);
    expect(s().count()).toBe(5);
    expect(s().first()).toBe(1);
    expect(s().last()).toBe(5);
    expect(s().find((n) => n > 3)).toBe(4);
    expect(s().some((n) => n === 3)).toBe(true);
    expect(s().every((n) => n < 6)).toBe(true);
    expect(RawStream.empty<number>().first()).toBeUndefined();
  });

  test("is re-iterable when the source is", () => {
    const stream = RawStream.from([1, 2, 3]).map((n) => n * 2);
    expect(stream.toArray()).toEqual([2, 4, 6]);
    expect(stream.toArray()).toEqual([2, 4, 6]);
  });

  test("bridges into Stream via fromIterable", async () => {
    const raw = RawStream.range(0, 10)
      .filter((n) => n % 2 === 0)
      .map((n) => n * 3);

    expect(await run(Stream.fromIterable(raw).toArray())).toEqual([0, 6, 12, 18, 24]);
  });
});
