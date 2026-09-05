import { describe, test, expect } from "bun:test";
import { sync, run, Ref, Schedule, retryWith, repeat } from "../src";

describe("Schedule", () => {
  test("recurs(3) retries 3 times", async () => {
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 4) throw new Error("nope");
      return "ok";
    });

    expect(await run(retryWith(flaky, Schedule.recurs(5)))).toBe("ok");
    expect(attempts).toBe(4);
  });

  test("recurs(2) gives up after 2 retries", async () => {
    let attempts = 0;
    const always_fail = sync(() => {
      attempts++;
      throw new Error("always");
    });

    await expect(run(retryWith(always_fail, Schedule.recurs(2)))).rejects.toBeInstanceOf(Error);
    expect(attempts).toBe(3); // initial + 2 retries
  });

  test("spaced adds delay between retries", async () => {
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 2) throw new Error("nope");
      return "ok";
    });

    const start = Date.now();
    await run(retryWith(flaky, Schedule.intersect(Schedule.recurs(3), Schedule.spaced(30))));
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(25);
  });

  test("exponential backoff", () => {
    const sched = Schedule.exponential(100, 2);
    const d1 = sched.step(null, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag === "Continue") {
      expect(d1.delay).toBe(100);
      const d2 = sched.step(null, d1.state);
      if (d2._tag === "Continue") {
        expect(d2.delay).toBe(200);
        const d3 = sched.step(null, d2.state);
        if (d3._tag === "Continue") expect(d3.delay).toBe(400);
      }
    }
  });

  test("intersect combines schedules", () => {
    const sched = Schedule.intersect(Schedule.recurs(2), Schedule.spaced(100));
    const d1 = sched.step(null, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag === "Continue") {
      expect(d1.delay).toBe(100);
      const d2 = sched.step(null, d1.state);
      if (d2._tag === "Continue") {
        const d3 = sched.step(null, d2.state);
        expect(d3._tag).toBe("Done"); // recurs(2) = 2 retries then stop
      }
    }
  });

  test("maxDelay caps delay", () => {
    const sched = Schedule.maxDelay(Schedule.exponential(100, 2), 300);
    let state = sched.initial;
    for (let i = 0; i < 5; i++) {
      const d = sched.step(null, state);
      if (d._tag === "Continue") {
        expect(d.delay).toBeLessThanOrEqual(300);
        state = d.state;
      }
    }
  });

  test("repeat runs effect multiple times", async () => {
    const program = Ref.make(0).flatMap((counter) =>
      repeat(
        counter.update((n) => n + 1),
        Schedule.recurs(4),
      ).flatMap(() => counter.get),
    );
    expect(await run(program)).toBe(5); // initial + 4 repeats
  });
});
