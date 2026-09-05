import { describe, test, expect } from "bun:test";
import { run, Stream } from "../src";

describe("Stream.parEvalMap ordering", () => {
  test("preserves input order under adversarial completion times", async () => {
    const { sleep } = await import("../src");
    // later items complete much faster than earlier ones
    const delays = [50, 5, 30, 1, 20, 2, 40, 3];
    const result = await run(
      Stream.fromArray(delays.map((d, i) => ({ d, i })))
        .rechunk(1)
        .parEvalMap(4, ({ d, i }) => sleep(d).map(() => i))
        .toArray(),
    );
    expect(result).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });
});
