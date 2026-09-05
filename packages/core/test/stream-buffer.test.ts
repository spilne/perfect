import { describe, test, expect } from "bun:test";
import { run, sync, Stream } from "../src";

describe("Stream.buffer", () => {
  test("passes all elements through in order", async () => {
    const result = await run(Stream.range(0, 20).rechunk(1).buffer(4).toArray());
    expect(result).toEqual(Array.from({ length: 20 }, (_, i) => i));
  });

  test("producer runs ahead of a slow consumer up to capacity", async () => {
    let produced = 0;
    const { sleep } = await import("../src");

    const source = Stream.unfoldEffect(0, (n) =>
      sync(() => {
        if (n >= 10) return null;
        produced++;
        return [n, n + 1] as [number, number];
      }),
    );

    const result = await run(
      source
        .buffer(5)
        .evalMap((x: number) => sleep(5).map(() => x))
        .take(2)
        .toArray(),
    );

    expect(result).toEqual([0, 1]);
    // while the consumer slept on the first items, the driver prefetched more
    expect(produced).toBeGreaterThan(2);
    expect(produced).toBeLessThanOrEqual(10);
  });

  test("propagates failures", async () => {
    const { runExit, Cause, fail: failFn } = await import("../src");
    const source = Stream.of(1, 2).concat(Stream.fromEffect(failFn("boom")));
    const exit = await runExit(source.buffer(4).toArray());
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBe("boom");
    }
  });

  test("stops the driver on early termination", async () => {
    let produced = 0;
    const infinite = Stream.unfoldEffect(0, (n) =>
      sync(() => {
        produced++;
        return [n, n + 1] as [number, number];
      }),
    );

    const result = await run(infinite.buffer(3).take(2).toArray());
    expect(result).toEqual([0, 1]);

    const after = produced;
    await new Promise((r) => setTimeout(r, 30));
    expect(produced).toBe(after);
  });
});
