import { describe, test, expect } from "bun:test";
import { Clock, Stream, TestClock, fail, provide, run, runFiber, sleep, succeed } from "../src";

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("Stream.parEvalMap ordering", () => {
  test("preserves input order under adversarial completion times", async () => {
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

  for (const concurrency of [1, 2, 8]) {
    test(`a source failure while workers are in flight fails with that cause (concurrency ${concurrency})`, async () => {
      const clock = new TestClock();
      const source = Stream.unfoldEffect(0, (n: number) =>
        n < 2 ? succeed([n, n + 1] as [number, number]) : sleep(5).flatMap(() => fail("boom")),
      );
      const fiber = runFiber(
        provide(
          source.parEvalMap(concurrency, (x) => sleep(40).map(() => x)).toArray(),
          Clock,
          clock,
        ),
      );
      for (let i = 0; i < 200 && fiber.status !== "done"; i++) {
        await tick();
        clock.advance(1);
      }

      expect(fiber.result).toEqual({ ok: false, cause: { _tag: "Fail", error: "boom" } });
    });
  }

  test("a source failure before a forked worker starts fails with that cause", async () => {
    const source = Stream.fromArray([1]).concat(Stream.fail("boom"));
    const fiber = runFiber(source.parEvalMap(2, (x) => succeed(x)).toArray());
    for (let i = 0; i < 20 && fiber.status !== "done"; i++) await tick();

    expect(fiber.result).toEqual({ ok: false, cause: { _tag: "Fail", error: "boom" } });
  });
});
