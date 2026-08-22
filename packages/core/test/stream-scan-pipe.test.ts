import { describe, expect, test } from "bun:test";
import { Clock, Pipe, Ref, Stream, TestClock, fail, provide, run, runExit, succeed } from "../src";

describe("Stream.scanEffect", () => {
  test("emits the seed then one accumulator per element", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .scanEffect(0, (acc, n) => succeed(acc + n))
        .toArray(),
    );
    expect(result).toEqual([0, 1, 3, 6]);
  });

  test("runs effects strictly in element order", async () => {
    const order: number[] = [];
    const result = await run(
      Stream.of(1, 2, 3, 4)
        .rechunk(2)
        .scanEffect(0, (acc, n) =>
          succeed(undefined).flatMap(() => {
            order.push(n);
            return succeed(acc + n);
          }),
        )
        .toArray(),
    );
    expect(order).toEqual([1, 2, 3, 4]);
    expect(result).toEqual([0, 1, 3, 6, 10]);
  });

  test("propagates a typed failure from the accumulator", async () => {
    const exit = await runExit(
      Stream.of(1, 2, 3)
        .scanEffect(0, (acc, n) => (n === 2 ? fail("boom") : succeed(acc + n)))
        .toArray() as any,
    );
    expect(exit._tag).toBe("Failure");
  });

  test("threads state that a plain scan could not — effects see a Ref", async () => {
    const program = Ref.make(0).flatMap((calls: any) =>
      Stream.of(5, 5, 5)
        .scanEffect(0, (acc, n) => calls.update((c: number) => c + 1).map(() => acc + n))
        .toArray()
        .flatMap((values: number[]) => calls.get.map((c: number) => ({ values, c }))),
    );
    expect(await run(program as any)).toEqual({ values: [0, 5, 10, 15], c: 3 });
  });

  test("is stack-safe across a large chunk", async () => {
    const n = 20_000;
    const result = await run(
      Stream.fromArray(Array.from({ length: n }, () => 1))
        .scanEffect(0, (acc, x) => succeed(acc + x))
        .last(),
    );
    expect(result).toBe(n);
  });

  test("works with a TestClock-driven effect", async () => {
    const clock = new TestClock();
    const program = provide(
      Stream.of(1, 2)
        .scanEffect(0, (acc, n) => succeed(acc + n))
        .toArray() as any,
      Clock,
      clock,
    );
    expect(await run(program as any)).toEqual([0, 1, 3]);
  });
});

describe("Pipe constructors", () => {
  test("map / filter / filterMap / scan build reusable stages", async () => {
    const evens = Pipe.filter<number>((n) => n % 2 === 0);
    const label = Pipe.map<number, string>((n) => `n=${n}`);

    expect(await run(Stream.range(0, 6).through(evens).through(label).toArray())).toEqual([
      "n=0",
      "n=2",
      "n=4",
    ]);

    expect(
      await run(
        Stream.of("1", "x", "3")
          .through(Pipe.filterMap<string, number>((s) => (/^\d+$/.test(s) ? Number(s) : undefined)))
          .toArray(),
      ),
    ).toEqual([1, 3]);

    expect(
      await run(
        Stream.of(1, 2, 3)
          .through(Pipe.scan<number, number>(0, (a, b) => a + b))
          .toArray(),
      ),
    ).toEqual([0, 1, 3, 6]);
  });

  test("a pipe is reusable across streams", async () => {
    const double = Pipe.map<number, number>((n) => n * 2);
    expect(await run(Stream.of(1, 2).through(double).toArray())).toEqual([2, 4]);
    expect(await run(Stream.of(3, 4).through(double).toArray())).toEqual([6, 8]);
  });

  test("evalMap pipe surfaces its own effects", async () => {
    const parse = Pipe.evalMap<string, number, any>((s) =>
      /^\d+$/.test(s) ? succeed(Number(s)) : fail(`bad: ${s}`),
    );
    expect(await run(Stream.of("1", "2").through(parse).toArray() as any)).toEqual([1, 2]);

    const exit = await runExit(Stream.of("1", "x").through(parse).toArray() as any);
    expect(exit._tag).toBe("Failure");
  });
});
