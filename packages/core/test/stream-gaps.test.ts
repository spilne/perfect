// Untested Stream ops (dropWhile, evalFilter, unfoldEffect, tick, fromIterable,
// fromChunk), Sink.contramap, and Chunk methods.

import { describe, test, expect } from "bun:test";
import { run, provide, succeed, sync, Clock, TestClock, Stream, Sink, Chunk } from "../src";

const tick = () => new Promise((r) => setTimeout(r, 0));

describe("Stream.dropWhile", () => {
  test("drops the prefix matching the predicate, keeps the rest", async () => {
    const result = await run(
      (Stream.of(1, 2, 3, 4, 1) as any).dropWhile((n: number) => n < 3).toArray(),
    );
    expect(result).toEqual([3, 4, 1]);
  });

  test("drops across chunk boundaries", async () => {
    const s = (Stream.of(1, 2) as any).concat(Stream.of(3, 4));
    expect(await run(s.dropWhile((n: number) => n < 4).toArray())).toEqual([4]);
  });

  test("drops everything when the predicate always holds", async () => {
    expect(await run((Stream.of(1, 2, 3) as any).dropWhile(() => true).toArray())).toEqual([]);
  });

  test("drops nothing when the first element fails the predicate", async () => {
    expect(await run((Stream.of(5, 1, 2) as any).dropWhile((n: number) => n < 3).toArray())).toEqual(
      [5, 1, 2],
    );
  });
});

describe("Stream.evalFilter", () => {
  test("keeps elements whose effectful predicate yields true", async () => {
    const result = await run(
      (Stream.range(1, 7) as any).evalFilter((n: number) => succeed(n % 2 === 0)).toArray(),
    );
    expect(result).toEqual([2, 4, 6]);
  });

  test("runs the predicate effect once per element", async () => {
    let calls = 0;
    const result = await run(
      (Stream.of(1, 2, 3) as any)
        .evalFilter((n: number) =>
          sync(() => {
            calls++;
            return n !== 2;
          }),
        )
        .toArray(),
    );
    expect(result).toEqual([1, 3]);
    expect(calls).toBe(3);
  });
});

describe("Stream.unfoldEffect", () => {
  test("unfolds from a seed using an effectful step", async () => {
    const result = await run(
      Stream.unfoldEffect(0, (n) => sync(() => (n < 4 ? ([n * 10, n + 1] as [number, number]) : null))).toArray(),
    );
    expect(result).toEqual([0, 10, 20, 30]);
  });

  test("null on the first step yields an empty stream", async () => {
    expect(await run(Stream.unfoldEffect(0, () => succeed(null)).toArray())).toEqual([]);
  });

  test("is lazy — steps beyond take(n) never run", async () => {
    let steps = 0;
    const result = await run(
      (
        Stream.unfoldEffect(0, (n) =>
          sync(() => {
            steps++;
            return [n, n + 1] as [number, number];
          }),
        ) as any
      )
        .take(3)
        .toArray(),
    );
    expect(result).toEqual([0, 1, 2]);
    expect(steps).toBe(3);
  });
});

describe("Stream.tick", () => {
  test("emits once per interval on virtual time", async () => {
    const c = new TestClock();
    const done = run(
      provide((Stream.tick(100) as any).take(3).toArray(), Clock, c) as any,
    );
    for (let i = 0; i < 5; i++) {
      await tick();
      c.advance(100);
    }
    const result = await done;
    expect(result).toEqual([undefined, undefined, undefined]);
    expect(c.now()).toBeLessThanOrEqual(500);
  });

  test("does not emit before the first interval elapses", async () => {
    const c = new TestClock();
    let emitted = 0;
    const done = run(
      provide(
        (Stream.tick(1000) as any).take(1).forEach(() =>
          sync(() => {
            emitted++;
          }),
        ),
        Clock,
        c,
      ) as any,
    );
    await tick();
    expect(emitted).toBe(0);
    c.advance(1000);
    await done;
    expect(emitted).toBe(1);
  });
});

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

describe("Stream.fromChunk", () => {
  test("emits the chunk's elements", async () => {
    expect(await run(Stream.fromChunk(Chunk.of(1, 2, 3)).toArray())).toEqual([1, 2, 3]);
  });

  test("empty chunk yields empty stream", async () => {
    expect(await run(Stream.fromChunk(Chunk.empty<number>()).toArray())).toEqual([]);
  });
});

describe("Sink.contramap", () => {
  test("adapts the sink's input type", async () => {
    const sumLengths = Sink.fold(0, (acc: number, n: number) => acc + n).contramap(
      (s: string) => s.length,
    );
    expect(await run((Stream.of("a", "bb", "ccc") as any).runSink(sumLengths))).toBe(6);
  });

  test("composes with sink map", async () => {
    const sink = Sink.collectAll<number>()
      .contramap((s: string) => s.length)
      .map((ns) => ns.join(","));
    expect(await run((Stream.of("x", "yy") as any).runSink(sink))).toBe("1,2");
  });
});

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
    expect(Chunk.of(1, 2).flatMap(() => Chunk.empty<number>()).toArray()).toEqual([]);
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
