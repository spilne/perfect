import { describe, test, expect } from "bun:test";
import { run, Stream, Sink } from "../src";

describe("Sink.contramap", () => {
  test("adapts the sink's input type", async () => {
    const sumLengths = Sink.fold(0, (acc: number, n: number) => acc + n).contramap(
      (s: string) => s.length,
    );
    expect(await run(Stream.of("a", "bb", "ccc").runSink(sumLengths))).toBe(6);
  });

  test("composes with sink map", async () => {
    const sink = Sink.collectAll<number>()
      .contramap((s: string) => s.length)
      .map((ns) => ns.join(","));
    expect(await run(Stream.of("x", "yy").runSink(sink))).toBe("1,2");
  });
});
