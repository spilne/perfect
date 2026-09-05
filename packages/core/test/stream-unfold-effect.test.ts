import { describe, test, expect } from "bun:test";
import { run, succeed, sync, Stream } from "../src";

describe("Stream.unfoldEffect", () => {
  test("unfolds from a seed using an effectful step", async () => {
    const result = await run(
      Stream.unfoldEffect(0, (n) =>
        sync(() => (n < 4 ? ([n * 10, n + 1] as [number, number]) : null)),
      ).toArray(),
    );
    expect(result).toEqual([0, 10, 20, 30]);
  });

  test("null on the first step yields an empty stream", async () => {
    expect(await run(Stream.unfoldEffect(0, () => succeed(null)).toArray())).toEqual([]);
  });

  test("is lazy — steps beyond take(n) never run", async () => {
    let steps = 0;
    const result = await run(
      Stream.unfoldEffect(0, (n) =>
        sync(() => {
          steps++;
          return [n, n + 1] as [number, number];
        }),
      )
        .take(3)
        .toArray(),
    );
    expect(result).toEqual([0, 1, 2]);
    expect(steps).toBe(3);
  });
});
