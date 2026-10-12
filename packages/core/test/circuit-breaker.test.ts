import { describe, test, expect } from "bun:test";
import {
  CircuitBreaker,
  succeed,
  fail,
  die,
  sync,
  sleep,
  all,
  run,
  runSync,
  type Eff,
  type Throws,
} from "../src";
import { runUnchecked } from "./run-unchecked";

describe("CircuitBreaker", () => {
  test("starts closed", () => {
    const cb = CircuitBreaker.make({ failureThreshold: 3, resetTimeoutMs: 100 });
    expect(runSync(cb.state)).toBe("closed");
    expect(runSync(cb.failures)).toBe(0);
  });

  test("passes through successful effects", () => {
    const cb = CircuitBreaker.make({ failureThreshold: 3, resetTimeoutMs: 100 });
    expect(runSync(cb.protect(succeed(42)).orDie())).toBe(42);
    expect(runSync(cb.state)).toBe("closed");
  });

  test("counts consecutive typed failures", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 3, resetTimeoutMs: 100 });
    const failing: Eff<never, Throws<string>> = fail("nope");
    await expect(runUnchecked(cb.protect(failing))).rejects.toBe("nope");
    await expect(runUnchecked(cb.protect(failing))).rejects.toBe("nope");
    expect(runSync(cb.state)).toBe("closed");
    expect(runSync(cb.failures)).toBe(2);
    await expect(runUnchecked(cb.protect(failing))).rejects.toBe("nope");
    expect(runSync(cb.state)).toBe("open");
    expect(runSync(cb.failures)).toBe(3);
  });

  test("rejects fast with CircuitOpen when open", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 100 });
    const failing: Eff<never, Throws<string>> = fail("trip");
    await expect(runUnchecked(cb.protect(failing))).rejects.toBe("trip");
    expect(runSync(cb.state)).toBe("open");

    // Subsequent call rejects without invoking the inner effect
    let invoked = false;
    const tracked: Eff<number, Throws<string>> = sync(() => {
      invoked = true;
      return 1;
    });
    const exit = await run(cb.protect(tracked).catch((e) => succeed({ caught: e })));
    expect(invoked).toBe(false);
    expect(exit).toMatchObject({ caught: { _tag: "CircuitOpen" } });
  });

  test("transitions to half-open after reset timeout", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 30 });
    await expect(runUnchecked(cb.protect(fail("boom")))).rejects.toBe("boom");
    expect(runSync(cb.state)).toBe("open");

    await new Promise((r) => setTimeout(r, 35));
    expect(runSync(cb.state)).toBe("half-open");
  });

  test("half-open success closes the breaker", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 20 });
    await expect(runUnchecked(cb.protect(fail("a")))).rejects.toBe("a");
    await new Promise((r) => setTimeout(r, 25));
    expect(runSync(cb.state)).toBe("half-open");

    expect(await run(cb.protect(succeed("ok")).orDie())).toBe("ok");
    expect(runSync(cb.state)).toBe("closed");
    expect(runSync(cb.failures)).toBe(0);
  });

  test("half-open failure re-opens the breaker", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 20 });
    await expect(runUnchecked(cb.protect(fail("a")))).rejects.toBe("a");
    await new Promise((r) => setTimeout(r, 25));
    expect(runSync(cb.state)).toBe("half-open");

    await expect(runUnchecked(cb.protect(fail("b")))).rejects.toBe("b");
    expect(runSync(cb.state)).toBe("open");
  });

  test("isFailure filters which errors count", async () => {
    const cb = CircuitBreaker.make<string>({
      failureThreshold: 2,
      resetTimeoutMs: 100,
      isFailure: (e) => e === "fatal",
    });
    // Non-fatal failures don't count
    await expect(runUnchecked(cb.protect(fail("transient")))).rejects.toBe("transient");
    await expect(runUnchecked(cb.protect(fail("transient")))).rejects.toBe("transient");
    expect(runSync(cb.state)).toBe("closed");
    expect(runSync(cb.failures)).toBe(0);

    // Fatal ones do
    await expect(runUnchecked(cb.protect(fail("fatal")))).rejects.toBe("fatal");
    await expect(runUnchecked(cb.protect(fail("fatal")))).rejects.toBe("fatal");
    expect(runSync(cb.state)).toBe("open");
  });

  test("success resets the failure counter", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 3, resetTimeoutMs: 100 });
    await expect(runUnchecked(cb.protect(fail("a")))).rejects.toBe("a");
    await expect(runUnchecked(cb.protect(fail("b")))).rejects.toBe("b");
    expect(runSync(cb.failures)).toBe(2);

    expect(await run(cb.protect(succeed(42)).orDie())).toBe(42);
    expect(runSync(cb.failures)).toBe(0);
    expect(runSync(cb.state)).toBe("closed");
  });

  test("reset() forces back to closed", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 9999 });
    await expect(runUnchecked(cb.protect(fail("trip")))).rejects.toBe("trip");
    expect(runSync(cb.state)).toBe("open");
    runSync(cb.reset());
    expect(runSync(cb.state)).toBe("closed");
    expect(runSync(cb.failures)).toBe(0);
  });

  // ── Concurrent / slow-path coverage ────────────────────────────

  test("100 concurrent .protect calls (closed, all success) all pass", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 100, resetTimeoutMs: 1000 });
    const results = await run(
      all(Array.from({ length: 100 }, (_, i) => cb.protect(succeed(i)))).orDie(),
    );
    expect(results.length).toBe(100);
    expect(runSync(cb.state)).toBe("closed");
    expect(runSync(cb.failures)).toBe(0);
  });

  test("concurrent failure burst opens the breaker; subsequent calls reject fast", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 5, resetTimeoutMs: 1000 });

    // Concurrent burst — check check + record happen non-atomically per fiber,
    // so all 20 may run their inner eff and record (race-tolerant by design).
    // What matters: after the burst, the breaker IS open.
    await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        run(cb.protect(fail(`f${i}`)).catch((e) => succeed({ caught: e }))),
      ),
    );
    expect(runSync(cb.state)).toBe("open");
    expect(runSync(cb.failures)).toBeGreaterThanOrEqual(5);

    // Now a fresh call should reject with CircuitOpen WITHOUT invoking the inner eff.
    let invoked = false;
    const tracked = sync(() => {
      invoked = true;
      return 1;
    });
    const rejected = await run(cb.protect(tracked).catch((e) => succeed({ caught: e })));
    expect(invoked).toBe(false);
    expect(rejected).toMatchObject({ caught: { _tag: "CircuitOpen" } });
  });

  test("half-open: only one probe at a time, the others see open", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 20 });
    await expect(runUnchecked(cb.protect(fail("trip")))).rejects.toBe("trip");
    expect(runSync(cb.state)).toBe("open");

    await new Promise((r) => setTimeout(r, 25));
    expect(runSync(cb.state)).toBe("half-open");

    // 10 calls arrive together. One is let through to test the dependency;
    // the other 9 are rejected right away instead of piling onto it.
    const probe = sleep(10).flatMap(() => succeed("probe-ok"));
    const results = await run(
      all(
        Array.from({ length: 10 }, () =>
          cb.protect(probe).catch((e) => succeed((e as { _tag: string })._tag)),
        ),
      ),
    );
    expect(results.filter((r) => r === "probe-ok")).toHaveLength(1);
    expect(results.filter((r) => r === "CircuitOpen")).toHaveLength(9);
    expect(runSync(cb.state)).toBe("closed");
  });

  test("half-open: a probe that dies lets the next call probe", async () => {
    const cb = CircuitBreaker.make<string>({ failureThreshold: 1, resetTimeoutMs: 0 });
    await expect(runUnchecked(cb.protect(fail("trip")))).rejects.toBe("trip");
    await expect(runUnchecked(cb.protect(die("boom")))).rejects.toBe("boom");
    expect(await run(cb.protect(succeed("ok")).orDie())).toBe("ok");
    expect(runSync(cb.state)).toBe("closed");
  });

  test("validates options at make time", () => {
    expect(() => CircuitBreaker.make({ failureThreshold: 0, resetTimeoutMs: 100 })).toThrow(
      /failureThreshold/,
    );
    expect(() => CircuitBreaker.make({ failureThreshold: 1, resetTimeoutMs: -1 })).toThrow(
      /resetTimeoutMs/,
    );
  });
});
