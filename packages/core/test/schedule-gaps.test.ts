// Schedule combinators without prior coverage: union, whileInput, jittered,
// linear, once, forever — exercised both step-wise and through retryWith/repeat.

import { describe, test, expect } from "bun:test";
import { run, runExit, sync, suspend, fail, Ref, Schedule, retryWith, repeat, Cause } from "../src";

describe("Schedule.union", () => {
  test("takes the minimum delay of both schedules", () => {
    const sched = Schedule.union(Schedule.spaced(100), Schedule.spaced(30));
    const d1 = sched.step(null, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag === "Continue") expect(d1.delay).toBe(30);
  });

  test("continues while either schedule continues; done only when both are", () => {
    const sched = Schedule.union(Schedule.recurs(1), Schedule.recurs(2));
    const d1 = sched.step(null, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag !== "Continue") return;
    const d2 = sched.step(null, d1.state); // recurs(1) done, recurs(2) continues
    expect(d2._tag).toBe("Continue");
    if (d2._tag !== "Continue") return;
    const d3 = sched.step(null, d2.state); // both done
    expect(d3._tag).toBe("Done");
  });

  test("retryWith retries as long as either branch continues", async () => {
    let attempts = 0;
    const alwaysFail = sync(() => {
      attempts++;
      throw new Error("nope");
    });

    const exit = await runExit(
      retryWith(alwaysFail as any, Schedule.union(Schedule.recurs(1), Schedule.recurs(2))) as any,
    );
    expect(exit._tag).toBe("Failure");
    expect(attempts).toBe(3); // initial + 2 retries (recurs(2) is the longer branch)
  });
});

describe("Schedule.whileInput", () => {
  test("retries only while the error matches the predicate", async () => {
    let attempts = 0;
    const effect = suspend(() => {
      attempts++;
      return fail(attempts < 3 ? "retryable" : "fatal");
    });

    const sched = Schedule.whileInput(Schedule.forever, (e: unknown) => e === "retryable");
    const exit = await runExit(retryWith(effect as any, sched as any) as any);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBe("fatal");
    }
    expect(attempts).toBe(3); // "retryable" x2 retried, "fatal" stops it
  });

  test("step-wise: predicate failure yields Done", () => {
    const sched = Schedule.whileInput(Schedule.forever, (n: number) => n < 10);
    const d1 = sched.step(5, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag !== "Continue") return;
    const d2 = sched.step(10, d1.state);
    expect(d2._tag).toBe("Done");
  });
});

describe("Schedule.jittered", () => {
  test("delays stay within [min, max] factors of the base delay", () => {
    const sched = Schedule.jittered(Schedule.spaced(100), 0.5, 1.5);
    let state = sched.initial;
    for (let i = 0; i < 50; i++) {
      const d = sched.step(null, state);
      expect(d._tag).toBe("Continue");
      if (d._tag !== "Continue") return;
      expect(d.delay).toBeGreaterThanOrEqual(50);
      expect(d.delay).toBeLessThanOrEqual(150);
      state = d.state;
    }
  });

  test("default bounds are [0.8, 1.2]", () => {
    const sched = Schedule.jittered(Schedule.spaced(1000));
    let state = sched.initial;
    for (let i = 0; i < 50; i++) {
      const d = sched.step(null, state);
      if (d._tag !== "Continue") return;
      expect(d.delay).toBeGreaterThanOrEqual(800);
      expect(d.delay).toBeLessThanOrEqual(1200);
      state = d.state;
    }
  });

  test("passes Done through untouched", () => {
    const sched = Schedule.jittered(Schedule.recurs(0));
    const d = sched.step(null, sched.initial);
    expect(d._tag).toBe("Done");
  });
});

describe("Schedule.linear", () => {
  test("delays grow linearly: base, 2*base, 3*base, ...", () => {
    const sched = Schedule.linear(100);
    let state = sched.initial;
    const delays: number[] = [];
    for (let i = 0; i < 4; i++) {
      const d = sched.step(null, state);
      if (d._tag !== "Continue") break;
      delays.push(d.delay);
      state = d.state;
    }
    expect(delays).toEqual([100, 200, 300, 400]);
  });
});

describe("Schedule.once", () => {
  test("step-wise: one Continue, then Done", () => {
    const d1 = Schedule.once.step(null, Schedule.once.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag !== "Continue") return;
    expect(d1.delay).toBe(0);
    const d2 = Schedule.once.step(null, d1.state);
    expect(d2._tag).toBe("Done");
  });

  test("repeat with once runs the effect exactly twice", async () => {
    const program = Ref.make(0).flatMap((counter) =>
      repeat(
        counter.update((n) => n + 1),
        Schedule.once,
      ).flatMap(() => counter.get),
    );
    expect(await run(program as any)).toBe(2); // initial + one repeat
  });

  test("retryWith once allows a single retry", async () => {
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 2) throw new Error("first");
      return "ok";
    });
    expect(await run(retryWith(flaky as any, Schedule.once) as any)).toBe("ok");
    expect(attempts).toBe(2);
  });
});

describe("Schedule.forever", () => {
  test("step-wise: never Done, zero delay, output counts attempts", () => {
    let state = Schedule.forever.initial;
    for (let i = 0; i < 20; i++) {
      const d = Schedule.forever.step(null, state);
      expect(d._tag).toBe("Continue");
      if (d._tag !== "Continue") return;
      expect(d.delay).toBe(0);
      expect(d.output).toBe(i);
      state = d.state;
    }
  });

  test("retryWith forever retries until the effect succeeds", async () => {
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 5) throw new Error("nope");
      return "ok";
    });
    expect(await run(retryWith(flaky as any, Schedule.forever) as any)).toBe("ok");
    expect(attempts).toBe(5);
  });
});
