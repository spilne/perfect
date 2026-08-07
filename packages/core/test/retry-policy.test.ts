import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  retry,
  RetryPolicy,
  run,
  Cause,
  Schedule,
  retryWith,
  type RetryDetails,
} from "../src";

describe("RetryPolicy — fluent builder", () => {
  test("recurs + withMaxRetries is the simple case", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap((n: number) =>
      n < 3 ? fail("try-again") : succeed("ok"),
    );
    const policy = RetryPolicy.recurs(5);
    expect(await run(retry(eff, policy))).toBe("ok");
    expect(attempts).toBe(3);
  });

  test("exponential + withMaxRetries + withMaxDelay composes via Schedule algebra", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("nope"));
    // small delays so we don't wait real seconds
    const policy = RetryPolicy.exponential(1).withMaxRetries(3).withMaxDelay(5);
    await expect(run(retry(eff, policy))).rejects.toBe("nope");
    expect(attempts).toBe(4); // 1 initial + 3 retries
  });

  test("fibonacci schedule produces 1, 1, 2, 3, 5, ...", () => {
    const s = Schedule.fibonacci(10);
    const delays: number[] = [];
    let state: any = s.initial;
    for (let i = 0; i < 5; i++) {
      const d = s.step(undefined, state);
      if (d._tag === "Done") break;
      delays.push(d.delay);
      state = d.state;
    }
    expect(delays).toEqual([10, 10, 20, 30, 50]);
  });

  test("fullJitter produces delays in [0, scheduled]", () => {
    const s = Schedule.fullJitter(Schedule.spaced(100));
    for (let i = 0; i < 30; i++) {
      const d = s.step(undefined, 0);
      if (d._tag === "Continue") {
        expect(d.delay).toBeGreaterThanOrEqual(0);
        expect(d.delay).toBeLessThanOrEqual(100);
      }
    }
  });

  test("cumulativeDelay output is running total", () => {
    const s = Schedule.cumulativeDelay(Schedule.spaced(100));
    const outputs: number[] = [];
    let state: any = s.initial;
    for (let i = 0; i < 4; i++) {
      const d = s.step(undefined, state);
      if (d._tag === "Done") break;
      outputs.push(d.output);
      state = d.state;
    }
    expect(outputs).toEqual([100, 200, 300, 400]);
  });

  test("withTimeBudget caps total retry duration", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("always"));
    // 10ms delay × 3 attempts = 30ms; budget 35ms just barely allows 3
    const policy = RetryPolicy.spaced(10).withTimeBudget(35);
    await expect(run(retry(eff, policy))).rejects.toBe("always");
    // 1 initial + some retries; stops when cumulative >= budget
    expect(attempts).toBeGreaterThanOrEqual(2);
    expect(attempts).toBeLessThanOrEqual(5);
  });

  test("whenError stops retrying on non-matching error", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap((n: number) =>
      fail(n === 1 ? "transient" : "fatal"),
    );

    const policy = RetryPolicy.recurs(5).whenError((e: string) => e === "transient");
    await expect(run(retry(eff, policy))).rejects.toBe("fatal");
    expect(attempts).toBe(2);
  });

  test("whenCause opts into defect-aware retry", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      if (attempts < 3) throw new Error("transient");
      return "ok";
    }) as any;

    const policy = RetryPolicy.recurs(5).whenCause((c) => Cause.hasDie(c));
    expect(await run(retry(eff, policy) as any)).toBe("ok");
    expect(attempts).toBe(3);
  });

  test("onRetry hook fires for each attempt and on giveup", async () => {
    const details: RetryDetails[] = [];
    const eff: any = fail("boom");

    const policy = RetryPolicy.recurs(2).onRetry(
      (d) =>
        sync(() => {
          details.push(d);
        }) as any,
    );
    await expect(run(retry(eff, policy))).rejects.toBe("boom");
    // 2 retries + 1 giving-up call
    expect(details.length).toBe(3);
    expect(details[0]!.givingUp).toBe(false);
    expect(details[1]!.givingUp).toBe(false);
    expect(details[2]!.givingUp).toBe(true);
    expect(details[0]!.attempts).toBe(1);
    expect(details[2]!.attempts).toBe(3);
  });

  test("and composes two policies — both must agree to continue", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("x"));

    // recurs(10) AND recurs(3) → 3 wins
    const policy = RetryPolicy.recurs(10).and(RetryPolicy.recurs(3));
    await expect(run(retry(eff, policy))).rejects.toBe("x");
    expect(attempts).toBe(4); // 1 initial + 3 retries
  });

  test("fromSchedule accepts a raw Schedule", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("x"));
    const policy = RetryPolicy.fromSchedule(Schedule.recurs(2));
    await expect(run(retry(eff, policy))).rejects.toBe("x");
    expect(attempts).toBe(3);
  });
});

describe("retryWith onRetry hook", () => {
  test("callback sees each attempt", async () => {
    const seen: RetryDetails[] = [];
    const eff: any = fail("nope");
    const program = retryWith(eff, Schedule.recurs(2), {
      onRetry: (d) =>
        sync(() => {
          seen.push(d);
        }) as any,
    });
    await expect(run(program)).rejects.toBe("nope");
    // 2 retries + 1 giveup
    expect(seen.length).toBe(3);
    expect(seen[seen.length - 1]!.givingUp).toBe(true);
  });
});

describe("legacy retry config still works", () => {
  test("old config dict is preserved for back-compat", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap((n: number) =>
      n < 3 ? fail("retry") : succeed("ok"),
    );
    expect(await run(retry(eff, { times: 5, delay: 0 }) as any)).toBe("ok");
    expect(attempts).toBe(3);
  });
});
