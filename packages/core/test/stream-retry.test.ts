import { describe, test, expect } from "bun:test";
import { Stream, RetryPolicy, succeed, fail, sync, run } from "../src";

describe("Stream.retry", () => {
  test("happy-path: no failure → retry is a no-op", async () => {
    const result = await run(
      Stream.of(1, 2, 3).retry(RetryPolicy.recurs(3)).runCollect(),
    );
    expect(result).toEqual([1, 2, 3]);
  });

  test("transient pull failure recovers within the retry budget", async () => {
    let attempts = 0;
    // Use Stream.suspend so each retry rebuilds the underlying stream.
    const flaky = Stream.suspend(() =>
      Stream.fromEffect(
        (sync(() => ++attempts) as any).flatMap((n: number) =>
          n < 3 ? fail(`attempt-${n}`) : succeed(99),
        ),
      ),
    );

    const result = await run(flaky.retry(RetryPolicy.recurs(5)).runCollect());
    expect(result).toEqual([99]);
    expect(attempts).toBe(3);
  });

  test("retry exhaustion surfaces the last failure", async () => {
    const eternal = Stream.fromEffect(fail("boom") as any);
    await expect(run(eternal.retry(RetryPolicy.recurs(2)).runCollect())).rejects.toBe("boom");
  });

  test("retry uses the supplied policy.delay (RetryConfig form)", async () => {
    let attempts = 0;
    const flaky = Stream.suspend(() =>
      Stream.fromEffect(
        (sync(() => ++attempts) as any).flatMap((n: number) =>
          n < 3 ? fail("nope") : succeed(7),
        ),
      ),
    );

    const start = Date.now();
    const result = await run(flaky.retry({ times: 5, delay: 5 }).runCollect());
    expect(result).toEqual([7]);
    expect(attempts).toBe(3);
    expect(Date.now() - start).toBeGreaterThanOrEqual(5);
  });

  test("retry only kicks in for typed failures, not defects", async () => {
    let attempts = 0;
    const flaky = Stream.fromEffect(
      sync(() => {
        attempts++;
        throw new Error("defect");
      }) as any,
    );
    await expect(run(flaky.retry(RetryPolicy.recurs(3)).runCollect())).rejects.toBeInstanceOf(Error);
    expect(attempts).toBe(1); // no retry on defect
  });
});
