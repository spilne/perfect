import { describe, test, expect } from "bun:test";
import { run, succeed, sync, Stream } from "../src";

describe("Stream.evalFilter", () => {
  test("keeps elements whose effectful predicate yields true", async () => {
    const result = await run(
      Stream.range(1, 7)
        .evalFilter((n: number) => succeed(n % 2 === 0))
        .toArray(),
    );
    expect(result).toEqual([2, 4, 6]);
  });

  test("runs the predicate effect once per element", async () => {
    let calls = 0;
    const result = await run(
      Stream.of(1, 2, 3)
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
