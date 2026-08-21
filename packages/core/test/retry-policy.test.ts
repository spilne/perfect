import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  sleep,
  provide,
  retry,
  RetryPolicy,
  run,
  runExit,
  Clock,
  TestClock,
  Cause,
  Schedule,
  retryWith,
  type RetryDetails,
} from "../src";

const tick = () => new Promise((r) => setTimeout(r, 0));

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

  test("exponential accepts fluent object options", () => {
    const schedule = RetryPolicy.exponential({ initial: 10, factor: 3 }).impl.schedule;
    const first = schedule.step(undefined, schedule.initial);
    expect(first._tag).toBe("Continue");
    if (first._tag !== "Continue") return;
    expect(first.delay).toBe(10);

    const second = schedule.step(undefined, first.state);
    expect(second._tag).toBe("Continue");
    if (second._tag !== "Continue") return;
    expect(second.delay).toBe(30);
  });

  test("withEqualJitter keeps delays in the upper half", () => {
    const schedule = RetryPolicy.spaced(100).withEqualJitter().impl.schedule;
    for (let i = 0; i < 30; i++) {
      const decision = schedule.step(undefined, schedule.initial);
      if (decision._tag === "Continue") {
        expect(decision.delay).toBeGreaterThanOrEqual(50);
        expect(decision.delay).toBeLessThanOrEqual(100);
      }
    }
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

describe("RetryPolicy.fromConfig — the config dict is sugar, not a second engine", () => {
  test("delay schedule matches the config: first retry waits `delay`, then doubles", async () => {
    const c = new TestClock();
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("nope"));

    const program = provide(
      retry(eff, { times: 3, delay: 100, backoff: "exponential" }) as any,
      Clock,
      c,
    );
    const done = runExit(program as any);

    // Delays are 100, 200, 400 — the same sequence the config dict has always
    // produced, now sourced from Schedule.exponential.
    const seen: number[] = [];
    for (const step of [100, 200, 400]) {
      await tick();
      seen.push(c.pendingDeadlines()[0]! - c.now());
      c.advance(step);
    }
    await done;

    expect(seen).toEqual([100, 200, 400]);
    expect(attempts).toBe(4); // initial + 3 retries
  });

  test("maxDelay caps fixed backoff too (it silently did not before)", async () => {
    const c = new TestClock();
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("nope"));

    const program = provide(
      retry(eff, { times: 1, delay: 5_000, backoff: "fixed", maxDelay: 250 }) as any,
      Clock,
      c,
    );
    const done = runExit(program as any);
    await tick();
    const waited = c.pendingDeadlines()[0]! - c.now();
    c.advance(waited);
    await done;

    expect(waited).toBe(250);
  });

  test("`when` predicate stops the retry loop", async () => {
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("fatal"));
    await expect(
      run(retry(eff, { times: 5, delay: 0, when: (e: string) => e === "transient" }) as any),
    ).rejects.toBe("fatal");
    expect(attempts).toBe(1);
  });
});

describe("RetryPolicy.withWallClockBudget", () => {
  test("counts attempt runtime, not just scheduled delay", async () => {
    const c = new TestClock();
    let attempts = 0;
    // Each attempt itself burns 40ms of virtual time before failing.
    const eff: any = sync(() => ++attempts)
      .flatMap(() => sleep(40))
      .flatMap(() => fail("nope"));

    const policy = RetryPolicy.spaced(10).withMaxRetries(100).withWallClockBudget(100);
    const done = runExit(provide(retry(eff, policy) as any, Clock, c) as any);

    // Drive: attempt runs (40) then sleeps (10) => 50ms per cycle.
    for (let i = 0; i < 8; i++) {
      await tick();
      const next = c.pendingDeadlines()[0];
      if (next === undefined) break;
      c.advance(next - c.now());
    }
    const exit = await done;

    expect(exit._tag).toBe("Failure");
    // t=40 (0 elapsed at check... 40 < 100) retry; t=90 -> 90 < 100 retry;
    // t=140 -> over budget, stop. withTimeBudget(100) would have allowed 10
    // retries here, since it only ever counts the 10ms sleeps.
    expect(attempts).toBe(3);
  });

  test("the deadline is anchored at run time, not build time", async () => {
    const c = new TestClock();
    const policy = RetryPolicy.spaced(60).withMaxRetries(100).withWallClockBudget(100);
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap(() => fail("nope"));

    // Simulate a policy built at module load, used much later.
    c.advance(10_000);

    const done = runExit(provide(retry(eff, policy) as any, Clock, c) as any);
    for (let i = 0; i < 5; i++) {
      await tick();
      c.advance(60);
    }
    const exit = await done;

    expect(exit._tag).toBe("Failure");
    expect(attempts).toBe(3);
  });
});
