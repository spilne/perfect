import { describe, test, expect } from "bun:test";
import {
  CircuitBreaker, succeed, fail, sync, run, runSync,
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

  test("validates options at make time", () => {
    expect(() => CircuitBreaker.make({ failureThreshold: 0, resetTimeoutMs: 100 }))
      .toThrow(/failureThreshold/);
    expect(() => CircuitBreaker.make({ failureThreshold: 1, resetTimeoutMs: -1 }))
      .toThrow(/resetTimeoutMs/);
  });
});
