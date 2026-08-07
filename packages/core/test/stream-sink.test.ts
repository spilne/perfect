import { describe, test, expect } from "bun:test";
import { run, sync, Sink, Sinks, Stream } from "../src";

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
});
