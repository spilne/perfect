import { describe, expect, test } from "bun:test";
import { Cause, RetryPolicy, Stream, TaggedError, run, runExit, sync } from "../src";

class RetrySourceError extends TaggedError("RetrySourceError")<{
  readonly attempt: number;
}>() {}

describe("Stream.retryFrom", () => {
  test("reacquires sources that fail before their first emission", async () => {
    let builds = 0;
    let finalizers = 0;

    const values = await run(
      Stream.retryFrom(() => {
        const attempt = ++builds;
        const source =
          attempt < 3 ? Stream.fail(new RetrySourceError({ attempt })) : Stream.succeed(42);
        return source.onFinalize(sync(() => finalizers++));
      }, RetryPolicy.recurs(4)).toArray(),
    );

    expect(values).toEqual([42]);
    expect(builds).toBe(3);
    expect(finalizers).toBe(3);
  });

  test("restarts after a late failure and retains already emitted values", async () => {
    let builds = 0;
    let finalizers = 0;

    const values = await run(
      Stream.retryFrom(() => {
        const attempt = ++builds;
        const source =
          attempt === 1
            ? Stream.succeed(1).concat(Stream.fail(new RetrySourceError({ attempt })))
            : Stream.succeed(2);
        return source.onFinalize(sync(() => finalizers++));
      }, RetryPolicy.recurs(2)).toArray(),
    );

    expect(values).toEqual([1, 2]);
    expect(builds).toBe(2);
    expect(finalizers).toBe(2);
  });

  test("surfaces the last failure after exhausting the policy", async () => {
    let builds = 0;
    let finalizers = 0;
    const exit = await runExit(
      Stream.retryFrom(() => {
        const attempt = ++builds;
        return Stream.fail(new RetrySourceError({ attempt })).onFinalize(sync(() => finalizers++));
      }, RetryPolicy.recurs(2)).toArray(),
    );

    expect(builds).toBe(3);
    expect(finalizers).toBe(3);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toEqual(new RetrySourceError({ attempt: 3 }));
    }
  });

  test("finalizes the active attempt when downstream stops early", async () => {
    let finalizers = 0;
    const values = await run(
      Stream.retryFrom(
        () => Stream.repeatValue(1).onFinalize(sync(() => finalizers++)),
        RetryPolicy.recurs(2),
      )
        .take(1)
        .toArray(),
    );

    expect(values).toEqual([1]);
    expect(finalizers).toBe(1);
  });

  test("does not retry defects unless the policy opts into causes", async () => {
    let builds = 0;
    const exit = await runExit(
      Stream.retryFrom(() => {
        builds++;
        throw new Error("defect");
      }, RetryPolicy.recurs(3)).toArray(),
    );

    expect(builds).toBe(1);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.hasDie(exit.cause)).toBe(true);
  });
});
