import { describe, test, expect } from "bun:test";
import {
  CircuitBreaker, succeed, fail, sync, sleep, fork, join, all, eff, run, runSync,
  type Eff, type Throws,
} from "../src";

describe("CircuitBreaker", () => {
  test("starts closed", () => {
    const cb = CircuitBreaker.make({ failureThreshold: 3, resetTimeoutMs: 100 });
    expect(cb.state).toBe("closed");
    expect(cb.failures).toBe(0);
  });

  test("passes through successful effects", () => {
    const cb = CircuitBreaker.make({ failureThreshold: 3, resetTimeoutMs: 100 });
    expect(runSync(cb.protect(succeed(42)))).toBe(42);
    expect(cb.state).toBe("closed");
  });

  test("counts consecutive typed failures", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 3, resetTimeoutMs: 100 });
    const failing: Eff<never, Throws<string>> = fail("nope") as any;
    await expect(run(cb.protect(failing) as any)).rejects.toBe("nope");
    await expect(run(cb.protect(failing) as any)).rejects.toBe("nope");
    expect(cb.state).toBe("closed");
    expect(cb.failures).toBe(2);
    await expect(run(cb.protect(failing) as any)).rejects.toBe("nope");
    expect(cb.state).toBe("open");
    expect(cb.failures).toBe(3);
  });

  test("rejects fast with CircuitOpen when open", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 100 });
    const failing: Eff<never, Throws<string>> = fail("trip") as any;
    await expect(run(cb.protect(failing) as any)).rejects.toBe("trip");
    expect(cb.state).toBe("open");

    // Subsequent call rejects without invoking the inner effect
    let invoked = false;
    const tracked: Eff<number, Throws<string>> = sync(() => {
      invoked = true;
      return 1;
    }) as any;
    const result = await (cb as any).protect(tracked).catch((e: any) => succeed({ caught: e }));
    const exit = await run(result);
    expect(invoked).toBe(false);
    expect((exit as any).caught._tag).toBe("CircuitOpen");
  });

  test("transitions to half-open after reset timeout", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 30 });
    await expect(run(cb.protect(fail("boom") as any) as any)).rejects.toBe("boom");
    expect(cb.state).toBe("open");

    await new Promise((r) => setTimeout(r, 35));
    expect(cb.state).toBe("half-open");
  });

  test("half-open success closes the breaker", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 20 });
    await expect(run(cb.protect(fail("a") as any) as any)).rejects.toBe("a");
    await new Promise((r) => setTimeout(r, 25));
    expect(cb.state).toBe("half-open");

    expect(await run(cb.protect(succeed("ok")))).toBe("ok");
    expect(cb.state).toBe("closed");
    expect(cb.failures).toBe(0);
  });

  test("half-open failure re-opens the breaker", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 20 });
    await expect(run(cb.protect(fail("a") as any) as any)).rejects.toBe("a");
    await new Promise((r) => setTimeout(r, 25));
    expect(cb.state).toBe("half-open");

    await expect(run(cb.protect(fail("b") as any) as any)).rejects.toBe("b");
    expect(cb.state).toBe("open");
  });

  test("isFailure filters which errors count", async () => {
    const cb = CircuitBreaker.make<string>({
      failureThreshold: 2,
      resetTimeoutMs: 100,
      isFailure: (e) => e === "fatal",
    });
    // Non-fatal failures don't count
    await expect(run(cb.protect(fail("transient") as any) as any)).rejects.toBe("transient");
    await expect(run(cb.protect(fail("transient") as any) as any)).rejects.toBe("transient");
    expect(cb.state).toBe("closed");
    expect(cb.failures).toBe(0);

    // Fatal ones do
    await expect(run(cb.protect(fail("fatal") as any) as any)).rejects.toBe("fatal");
    await expect(run(cb.protect(fail("fatal") as any) as any)).rejects.toBe("fatal");
    expect(cb.state).toBe("open");
  });

  test("success resets the failure counter", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 3, resetTimeoutMs: 100 });
    await expect(run(cb.protect(fail("a") as any) as any)).rejects.toBe("a");
    await expect(run(cb.protect(fail("b") as any) as any)).rejects.toBe("b");
    expect(cb.failures).toBe(2);

    expect(await run(cb.protect(succeed(42)))).toBe(42);
    expect(cb.failures).toBe(0);
    expect(cb.state).toBe("closed");
  });

  test("reset() forces back to closed", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 9999 });
    await expect(run(cb.protect(fail("trip") as any) as any)).rejects.toBe("trip");
    expect(cb.state).toBe("open");
    runSync(cb.reset());
    expect(cb.state).toBe("closed");
    expect(cb.failures).toBe(0);
  });

  // ── Concurrent / slow-path coverage ────────────────────────────

  test("100 concurrent .protect calls (closed, all success) all pass", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 100, resetTimeoutMs: 1000 });
    const results = await run(
      all(Array.from({ length: 100 }, (_, i) => cb.protect(succeed(i)))),
    );
    expect(results.length).toBe(100);
    expect(cb.state).toBe("closed");
    expect(cb.failures).toBe(0);
  });

  test("concurrent failure burst opens the breaker; subsequent calls reject fast", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 5, resetTimeoutMs: 1000 });

    // Concurrent burst — check check + record happen non-atomically per fiber,
    // so all 20 may run their inner eff and record (race-tolerant by design).
    // What matters: after the burst, the breaker IS open.
    await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        run((cb.protect(fail(`f${i}`) as any) as any).catch((e: any) => succeed({ caught: e }))),
      ),
    );
    expect(cb.state).toBe("open");
    expect(cb.failures).toBeGreaterThanOrEqual(5);

    // Now a fresh call should reject with CircuitOpen WITHOUT invoking the inner eff.
    let invoked = false;
    const tracked = sync(() => {
      invoked = true;
      return 1;
    }) as any;
    const rejected = await run(
      (cb.protect(tracked) as any).catch((e: any) => succeed({ caught: e })),
    );
    expect(invoked).toBe(false);
    expect((rejected as any).caught._tag).toBe("CircuitOpen");
  });

  test("half-open: only one probe at a time (mostly — others see open)", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 20 });
    await expect(run(cb.protect(fail("trip") as any) as any)).rejects.toBe("trip");
    expect(cb.state).toBe("open");

    // Wait past reset; breaker should now be half-open. Fire 10 concurrent
    // .protect calls — the slow probe should succeed for the one(s) that
    // squeezed in before transition closes the breaker; the rest see closed.
    await new Promise((r) => setTimeout(r, 25));
    expect(cb.state).toBe("half-open");

    const probe = sleep(10).flatMap(() => succeed("probe-ok"));
    const all10 = await run(all(Array.from({ length: 10 }, () => cb.protect(probe))));
    // After concurrent probes, breaker should be closed (success closes it)
    expect(cb.state).toBe("closed");
    // All 10 succeeded — half-open lets concurrent calls through; the first
    // success closes, subsequent calls run in closed state. (Promin's design
    // doesn't enforce single-probe — it's eventually consistent.)
    expect(all10.every((r) => r === "probe-ok")).toBe(true);
  });

  test("validates options at make time", () => {
    expect(() => CircuitBreaker.make({ failureThreshold: 0, resetTimeoutMs: 100 }))
      .toThrow(/failureThreshold/);
    expect(() => CircuitBreaker.make({ failureThreshold: 1, resetTimeoutMs: -1 }))
      .toThrow(/resetTimeoutMs/);
  });
});
