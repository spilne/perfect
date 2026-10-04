import { describe, test, expect } from "bun:test";
import { succeed, fail, sync, sleep, all, Singleflight, type Eff, type Throws } from "../src";
import { runUnchecked } from "./run-unchecked";

// Singleflight.do() types even a never-failing effect as failing (with
// Throws<unknown> or Throws<never>), so runUnchecked() rejects every result here.

describe("Singleflight", () => {
  test("single call passes through", async () => {
    const sf = Singleflight.make();
    const result = await runUnchecked(sf.do("k", succeed(42)));
    expect(result).toBe(42);
  });

  test("concurrent calls with same key share one execution", async () => {
    const sf = Singleflight.make();
    let executions = 0;
    const work = sleep(20).flatMap(() =>
      sync(() => {
        executions++;
        return executions; // returns 1, 2, 3 across separate executions
      }),
    );

    const program = all([
      sf.do("user:1", work),
      sf.do("user:1", work),
      sf.do("user:1", work),
      sf.do("user:1", work),
      sf.do("user:1", work),
    ]);
    const results = await runUnchecked(program);
    // All should see the same value because work ran exactly once
    expect(executions).toBe(1);
    expect(results).toEqual([1, 1, 1, 1, 1]);
  });

  test("different keys execute independently", async () => {
    const sf = Singleflight.make();
    let executions = 0;
    const mk = (k: string) =>
      sf.do(
        k,
        sleep(10).flatMap(() =>
          sync(() => {
            executions++;
            return k;
          }),
        ),
      );
    const results = await runUnchecked(all([mk("a"), mk("b"), mk("c"), mk("a"), mk("b")]));
    expect(executions).toBe(3);
    expect(results).toEqual(["a", "b", "c", "a", "b"]);
  });

  test("key is cleared after success — next call re-executes", async () => {
    const sf = Singleflight.make();
    let executions = 0;
    const work = sync(() => ++executions);
    expect(await runUnchecked(sf.do("k", work))).toBe(1);
    expect(await runUnchecked(sf.do("k", work))).toBe(2);
    expect(await runUnchecked(sf.do("k", work))).toBe(3);
  });

  test("failure also fans out to all followers", async () => {
    const sf = Singleflight.make();
    const work = sleep(10).flatMap(() => fail("boom"));
    const results = await Promise.allSettled([
      runUnchecked(sf.do("k", work)),
      runUnchecked(sf.do("k", work)),
      runUnchecked(sf.do("k", work)),
    ]);
    for (const r of results) {
      expect(r.status).toBe("rejected");
      if (r.status === "rejected") expect(r.reason).toBe("boom");
    }
  });

  test("key is cleared after failure — next call re-tries", async () => {
    const sf = Singleflight.make();
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 3) throw "transient";
      return "ok";
    }) as Eff<string, Throws<string>>;

    // First two attempts fail (defects, but we use catch to observe)
    await expect(runUnchecked(sf.do("k", flaky))).rejects.toBeDefined();
    await expect(runUnchecked(sf.do("k", flaky))).rejects.toBeDefined();
    // Third succeeds — proves the key was cleared each time
    expect(await runUnchecked(sf.do("k", flaky))).toBe("ok");
    expect(attempts).toBe(3);
  });
});
