import { describe, test, expect } from "bun:test";
import { Chunk } from "../src";

describe("Chunk methods", () => {
  test("find returns the first match or undefined", () => {
    const c = Chunk.of(1, 2, 3, 4);
    expect(c.find((n) => n > 2)).toBe(3);
    expect(c.find((n) => n > 10)).toBeUndefined();
  });

  test("find respects slicing offsets", () => {
    const c = Chunk.of(1, 2, 3, 4).drop(2);
    expect(c.find((n) => n < 10)).toBe(3);
  });

  test("every checks all elements", () => {
    expect(Chunk.of(2, 4, 6).every((n) => n % 2 === 0)).toBe(true);
    expect(Chunk.of(2, 3, 6).every((n) => n % 2 === 0)).toBe(false);
    expect(Chunk.empty<number>().every(() => false)).toBe(true); // vacuous truth
  });

  test("some finds a matching element", () => {
    expect(Chunk.of(1, 2, 3).some((n) => n === 2)).toBe(true);
  });

  test("some when nothing matches returns false", () => {
    expect(Chunk.of(1, 2, 3).some((n) => n > 10)).toBe(false);
    expect(Chunk.empty<number>().some(() => true)).toBe(false);
    expect(Chunk.of(1, 2, 3).some((n) => n === 2)).toBe(true);
  });

  test("flatMap flattens per-element chunks", () => {
    const c = Chunk.of(1, 2, 3).flatMap((n) => Chunk.of(n, n * 10));
    expect(c.toArray()).toEqual([1, 10, 2, 20, 3, 30]);
  });

  test("flatMap to empty chunks yields empty", () => {
    expect(
      Chunk.of(1, 2)
        .flatMap(() => Chunk.empty<number>())
        .toArray(),
    ).toEqual([]);
  });

  test("forEach visits each element in order", () => {
    const seen: number[] = [];
    Chunk.of(1, 2, 3).forEach((n) => seen.push(n));
    expect(seen).toEqual([1, 2, 3]);
  });

  test("get indexes into the chunk (offset-aware)", () => {
    const c = Chunk.of(10, 20, 30);
    expect(c.get(0)).toBe(10);
    expect(c.get(2)).toBe(30);
    expect(c.drop(1).get(0)).toBe(20);
  });
});
