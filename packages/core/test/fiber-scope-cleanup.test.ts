import { describe, expect, test } from "bun:test";
import { acquireRelease, all, forkDaemon, run, runExit, succeed, sync, yieldNow } from "../src";
import type { Eff } from "../src";

// Resources acquired with acquireRelease outside any `scoped` belong to the
// fiber, and are released when the fiber's body ends. These tests pin how
// that cleanup runs.

function steps(count: number, onStep: () => void): Eff<void, never> {
  let eff: Eff<void, never> = succeed(undefined);
  for (let i = 0; i < count; i++) eff = eff.flatMap(() => sync(onStep));
  return eff;
}

describe("fiber-level scope cleanup", () => {
  test("a long cleanup lets other fibers run", async () => {
    const order: string[] = [];
    let cleanupSteps = 0;
    const program = forkDaemon(
      // Another fiber that wants to run while the cleanup is busy.
      yieldNow.flatMap(() => sync(() => void order.push(`other ran after ${cleanupSteps} steps`))),
    ).flatMap(() =>
      acquireRelease(succeed("resource"), () =>
        steps(20_000, () => void cleanupSteps++).flatMap(() =>
          sync(() => void order.push("cleanup done")),
        ),
      ),
    );
    await run(program);
    await run(yieldNow);
    // The other fiber got a turn before the cleanup finished.
    expect(order[0]).toMatch(/^other ran after \d+ steps$/);
    expect(order[0]).not.toBe("other ran after 20000 steps");
    expect(order).toContain("cleanup done");
  });

  test("deeply nested ensuring inside cleanup doesn't overflow the stack", async () => {
    let finalizers = 0;
    let release: Eff<void, never> = succeed(undefined);
    for (let i = 0; i < 20_000; i++) release = release.ensuring(sync(() => void finalizers++));
    const exit = await runExit(acquireRelease(succeed(1), () => release));
    expect(exit._tag).toBe("Success");
    expect(finalizers).toBe(20_000);
  });

  test("cleanup can run work in parallel", async () => {
    const released: number[] = [];
    const exit = await runExit(
      acquireRelease(succeed(1), () =>
        all([1, 2, 3].map((n) => sync(() => void released.push(n)))).map(() => undefined),
      ),
    );
    expect(exit._tag).toBe("Success");
    expect(released.sort()).toEqual([1, 2, 3]);
  });
});
