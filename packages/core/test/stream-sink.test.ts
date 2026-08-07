import { describe, test, expect } from "bun:test";
import { run, succeed, sync, Sink, Sinks, Stream } from "../src";

describe("Sink", () => {
  test("collectAll can be used as a reusable terminal postprocessor", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .map((n) => n * 2)
        .runSink(Sink.collectAll()),
    );

    expect(result).toEqual([2, 4, 6]);
  });

  test("namespace helpers build common sinks", async () => {
    const result = await run(
      Stream.of("a", "bb", "ccc").runSink(Sinks.fold(0, (n, s) => n + s.length)),
    );

    expect(result).toBe(6);
  });

  test("runSink preserves stream finalization", async () => {
    let finalized = 0;
    const stream = Stream.fromArray([1, 2, 3]).onFinalize(
      sync(() => {
        finalized++;
      }),
    );

    expect(await run(stream.runSink(Sinks.head()))).toBe(1);
    expect(finalized).toBe(1);
  });

  test("collectN short-circuits", async () => {
    let finalized = 0;
    const stream = Stream.range(0, 10).onFinalize(
      sync(() => {
        finalized++;
      }),
    );

    expect(await run(stream.runSink(Sinks.collectN(3)))).toEqual([0, 1, 2]);
    expect(finalized).toBe(1);
  });

  test("foldEffect folds in one pass", async () => {
    const result = await run(
      Stream.of(1, 2, 3).runSink(Sinks.foldEffect(0, (acc, n) => succeed(acc + n))),
    );

    expect(result).toBe(6);
  });

  test("forEachWhile stops after the first false result", async () => {
    const seen: number[] = [];

    await run(
      Stream.of(1, 2, 3, 4).runSink(
        Sinks.forEachWhile((n) =>
          sync(() => {
            seen.push(n);
            return n < 3;
          }),
        ),
      ),
    );

    expect(seen).toEqual([1, 2, 3]);
  });

  test("drainWith and fromEffect return effect results", async () => {
    expect(await run(Stream.of(1, 2).runSink(Sinks.drainWith(succeed("done"))))).toBe("done");
    expect(await run(Stream.of(1, 2).runSink(Sinks.fromEffect(succeed(42))))).toBe(42);
  });

  test("sink combinators compose", async () => {
    const sink = Sinks.fold(0, (acc: number, n: number) => acc + n)
      .contramap((s: string) => s.length)
      .map((n) => n * 2)
      .flatMap((n) => succeed(`total:${n}`));

    expect(await run(Stream.of("a", "bb").runSink(sink))).toBe("total:6");
  });
});
