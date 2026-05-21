import { describe, test, expect } from "bun:test";
import { succeed, fail, sync, sleep, run, runSync, Queue, Ref, Stream, Chunk } from "../src";

describe("Chunk", () => {
  test("fromArray + toArray", () => {
    const c = Chunk.fromArray([1, 2, 3]);
    expect(c.toArray()).toEqual([1, 2, 3]);
    expect(c.length).toBe(3);
  });

  test("map", () => {
    expect(
      Chunk.of(1, 2, 3)
        .map((x) => x * 2)
        .toArray(),
    ).toEqual([2, 4, 6]);
  });

  test("filter", () => {
    expect(
      Chunk.of(1, 2, 3, 4)
        .filter((x) => x % 2 === 0)
        .toArray(),
    ).toEqual([2, 4]);
  });

  test("take/drop (zero-copy slice)", () => {
    const c = Chunk.of(1, 2, 3, 4, 5);
    expect(c.take(3).toArray()).toEqual([1, 2, 3]);
    expect(c.drop(2).toArray()).toEqual([3, 4, 5]);
  });

  test("concat", () => {
    const a = Chunk.of(1, 2);
    const b = Chunk.of(3, 4);
    expect(a.concat(b).toArray()).toEqual([1, 2, 3, 4]);
  });

  test("reduce", () => {
    expect(Chunk.of(1, 2, 3).reduce(0, (a, b) => a + b)).toBe(6);
  });

  test("iterator", () => {
    expect([...Chunk.of(1, 2, 3)]).toEqual([1, 2, 3]);
  });

  test("empty", () => {
    expect(Chunk.empty().isEmpty).toBe(true);
    expect(Chunk.empty().length).toBe(0);
  });
});

describe("Stream constructors", () => {
  test("empty", async () => {
    expect(await run(Stream.empty().toArray())).toEqual([]);
  });

  test("succeed", async () => {
    expect(await run(Stream.succeed(42).toArray())).toEqual([42]);
  });

  test("of", async () => {
    expect(await run(Stream.of(1, 2, 3).toArray())).toEqual([1, 2, 3]);
  });

  test("fromArray", async () => {
    expect(await run(Stream.fromArray([10, 20, 30]).toArray())).toEqual([10, 20, 30]);
  });

  test("fromEffect", async () => {
    expect(await run(Stream.fromEffect(succeed(99)).toArray())).toEqual([99]);
  });

  test("fail", async () => {
    await expect(run(Stream.fail("boom").toArray())).rejects.toBe("boom");
  });

  test("range", async () => {
    expect(await run(Stream.range(0, 5).toArray())).toEqual([0, 1, 2, 3, 4]);
  });

  test("iterate", async () => {
    expect(
      await run(
        Stream.iterate(1, (x) => x * 2)
          .take(5)
          .toArray(),
      ),
    ).toEqual([1, 2, 4, 8, 16]);
  });

  test("unfold", async () => {
    const fibs = Stream.unfold([0, 1] as [number, number], ([a, b]) =>
      a > 20 ? null : [a, [b, a + b] as [number, number]],
    );
    expect(await run(fibs.toArray())).toEqual([0, 1, 1, 2, 3, 5, 8, 13]);
  });

  test("repeatValue + take", async () => {
    expect(await run(Stream.repeatValue("x").take(3).toArray())).toEqual(["x", "x", "x"]);
  });

  test("repeat effect + take", async () => {
    let n = 0;
    const s = Stream.repeat(sync(() => ++n)).take(4);
    expect(await run(s.toArray())).toEqual([1, 2, 3, 4]);
  });

  test("fromQueue", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q
        .offer(1)
        .flatMap(() => q.offer(2))
        .flatMap(() => q.offer(3))
        .flatMap(() => q.shutdown())
        .flatMap(() => Stream.fromQueue(q).toArray()),
    );
    expect(await run(program)).toEqual([1, 2, 3]);
  });

  test("bracket — resource released", async () => {
    const log: string[] = [];
    const s = Stream.bracket(
      sync(() => {
        log.push("acquire");
        return "conn";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    );
    expect(await run(s.toArray())).toEqual(["conn"]);
    expect(log).toEqual(["acquire", "release:conn"]);
  });
});

describe("Stream transforms", () => {
  test("map", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3)
          .map((x) => x * 10)
          .toArray(),
      ),
    ).toEqual([10, 20, 30]);
  });

  test("filter", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3, 4, 5)
          .filter((x) => x % 2 === 0)
          .toArray(),
      ),
    ).toEqual([2, 4]);
  });

  test("flatMap", async () => {
    const s = Stream.of(1, 2, 3).flatMap((x) => Stream.of(x, x * 10));
    expect(await run(s.toArray())).toEqual([1, 10, 2, 20, 3, 30]);
  });

  test("evalMap", async () => {
    const s = Stream.of(1, 2, 3).evalMap((x) => succeed(x * 100));
    expect(await run(s.toArray())).toEqual([100, 200, 300]);
  });

  test("take", async () => {
    expect(await run(Stream.of(1, 2, 3, 4, 5).take(3).toArray())).toEqual([1, 2, 3]);
  });

  test("drop", async () => {
    expect(await run(Stream.of(1, 2, 3, 4, 5).drop(2).toArray())).toEqual([3, 4, 5]);
  });

  test("takeWhile", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3, 4, 5)
          .takeWhile((x) => x < 4)
          .toArray(),
      ),
    ).toEqual([1, 2, 3]);
  });

  test("scan", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3)
          .scan(0, (a, b) => a + b)
          .toArray(),
      ),
    ).toEqual([0, 1, 3, 6]);
  });

  test("zipWithIndex", async () => {
    expect(await run(Stream.of("a", "b", "c").zipWithIndex().toArray())).toEqual([
      ["a", 0],
      ["b", 1],
      ["c", 2],
    ]);
  });

  test("changes (deduplicate consecutive)", async () => {
    expect(await run(Stream.of(1, 1, 2, 2, 2, 3, 1, 1).changes().toArray())).toEqual([
      1, 2, 3, 1,
    ]);
  });

  test("collect", async () => {
    const s = Stream.of(1, 2, 3, 4, 5).collect((x) => (x % 2 === 0 ? `even:${x}` : undefined));
    expect(await run(s.toArray())).toEqual(["even:2", "even:4"]);
  });

  test("tap", async () => {
    const log: number[] = [];
    await run(
      Stream.of(1, 2, 3)
        .tap((x) => log.push(x))
        .drain(),
    );
    expect(log).toEqual([1, 2, 3]);
  });

  test("grouped", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .grouped(2)
        .map((c) => c.toArray())
        .toArray(),
    );
    expect(result).toEqual([[1, 2], [3, 4], [5]]);
  });
});

describe("Stream combination", () => {
  test("concat", async () => {
    const s = Stream.of(1, 2).concat(Stream.of(3, 4));
    expect(await run(s.toArray())).toEqual([1, 2, 3, 4]);
  });

  test("zip", async () => {
    const s = Stream.of(1, 2, 3).zip(Stream.of("a", "b", "c"));
    expect(await run(s.toArray())).toEqual([
      [1, "a"],
      [2, "b"],
      [3, "c"],
    ]);
  });

  test("zip stops at shorter", async () => {
    const s = Stream.of(1, 2, 3, 4).zip(Stream.of("a", "b"));
    expect(await run(s.toArray())).toEqual([
      [1, "a"],
      [2, "b"],
    ]);
  });

  test("interleave", async () => {
    const s = Stream.of(1, 3, 5).interleave(Stream.of(2, 4, 6));
    expect(await run(s.toArray())).toEqual([1, 2, 3, 4, 5, 6]);
  });
});

describe("Stream error handling", () => {
  test("catch recovers", async () => {
    const s = Stream.of(1, 2)
      .concat(Stream.fail("oops"))
      .catch(() => Stream.of(99));
    expect(await run(s.toArray())).toEqual([1, 2, 99]);
  });
});

describe("Stream terminals", () => {
  test("fold", async () => {
    expect(await run(Stream.of(1, 2, 3).fold(0, (a, b) => a + b))).toBe(6);
  });

  test("forEach", async () => {
    const log: number[] = [];
    await run(
      Stream.of(1, 2, 3).forEach((x) =>
        sync(() => {
          log.push(x);
        }),
      ),
    );
    expect(log).toEqual([1, 2, 3]);
  });

  test("head", async () => {
    expect(await run(Stream.of(1, 2, 3).head())).toBe(1);
    expect(await run(Stream.empty().head())).toBeUndefined();
  });

  test("last", async () => {
    expect(await run(Stream.of(1, 2, 3).last())).toBe(3);
  });

  test("count", async () => {
    expect(await run(Stream.of(1, 2, 3, 4, 5).count())).toBe(5);
  });

  test("drain", async () => {
    let count = 0;
    await run(
      Stream.of(1, 2, 3)
        .tap(() => count++)
        .drain(),
    );
    expect(count).toBe(3);
  });
});

describe("Stream + services", () => {
  test("evalMap with service", async () => {
    const { service, provide } = await import("../src");

    interface Db {
      query(id: number): ReturnType<typeof succeed<string>>;
    }
    const Db = service<Db>("Db");

    const program = Stream.of(1, 2, 3)
      .evalMap((id) => Db.get.flatMap((db) => db.query(id)))
      .toArray();

    const result = await run(provide(program, Db, { query: (id) => succeed(`row-${id}`) }));
    expect(result).toEqual(["row-1", "row-2", "row-3"]);
  });
});

describe("Stream large / perf", () => {
  test("10K elements", async () => {
    const sum = await run(Stream.range(0, 10_000).fold(0, (a, b) => a + b));
    expect(sum).toBe(49_995_000);
  });

  test("chained operators", async () => {
    const result = await run(
      Stream.range(0, 1000)
        .filter((x) => x % 2 === 0)
        .map((x) => x * 3)
        .take(10)
        .toArray(),
    );
    expect(result).toEqual([0, 6, 12, 18, 24, 30, 36, 42, 48, 54]);
  });
});
