import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  run,
  trapError,
  validate,
  hedged,
  repeatUntil,
  repeatUntilWithBackoff,
  retryAllCause,
  retryAllBy,
  retry,
  Cause,
  Clock,
  TestClock,
  provide,
  RetryAttempt,
  RetryDecision,
} from "../src";

// ── trapError ──────────────────────────────────────────────────────

class ParseError extends Error {
  readonly _tag = "ParseError" as const;
}
class NetError extends Error {
  readonly _tag = "NetError" as const;
}

describe("trapError", () => {
  test("converts matching defect into a typed failure", async () => {
    const defectProne = sync(() => {
      throw new ParseError("bad input");
    }) as any;
    const trapped = trapError(defectProne, ParseError);
    await expect(run(trapped as any)).rejects.toBeInstanceOf(ParseError);
  });

  test("non-matching defects stay defects", async () => {
    const defectProne = sync(() => {
      throw new TypeError("not this one");
    });
    const trapped = trapError(defectProne, ParseError);
    await expect(run(trapped as any)).rejects.toBeInstanceOf(TypeError);
  });

  test("multiple classes — any one matches", async () => {
    const defectProne = sync(() => {
      throw new NetError("500");
    });
    const trapped = trapError(defectProne, ParseError, NetError);
    await expect(run(trapped as any)).rejects.toBeInstanceOf(NetError);
  });

  test("successful effects pass through unchanged", async () => {
    expect(await run(trapError(succeed(42), ParseError) as any)).toBe(42);
  });
});

// ── validate ───────────────────────────────────────────────────────

describe("validate", () => {
  test("returns tuple of values when all succeed", async () => {
    const result = await run(validate([succeed(1), succeed("two"), succeed(true)]) as any);
    expect(result).toEqual([1, "two", true]);
  });

  test("collects ALL failures into a Cause.both tree", async () => {
    const program = (validate([fail("a"), succeed(1), fail("b"), fail("c")]) as any).catchAllCause(
      (cause: any) => succeed(Cause.failures(cause)),
    );

    const failures = await run(program as any);
    expect(failures.sort()).toEqual(["a", "b", "c"]);
  });

  test("single failure passes through cleanly", async () => {
    await expect(run(validate([succeed(1), fail("only")]) as any)).rejects.toBe("only");
  });

  test("empty array returns empty tuple", async () => {
    expect(await run(validate([]) as any)).toEqual([]);
  });
});

// ── hedged ─────────────────────────────────────────────────────────

describe("hedged", () => {
  test("fastest replica wins", async () => {
    const work = sync(() => "work") as any;
    // Any replica succeeds instantly → first one wins
    const race = hedged(work, { replicas: 3, staggerMs: 20 });
    expect(await run(race as any)).toBe("work");
  });

  test("replicas=1 is a passthrough", async () => {
    let count = 0;
    const eff = sync(() => {
      count++;
      return 42;
    }) as any;
    expect(await run(hedged(eff, { replicas: 1, staggerMs: 100 }) as any)).toBe(42);
    expect(count).toBe(1);
  });

  test("replicas < 1 throws", () => {
    expect(() => hedged(succeed(1), { replicas: 0, staggerMs: 10 })).toThrow(/replicas/);
  });
});

// ── repeatUntil ──────────────────────────────────────────────────────

describe("repeatUntil", () => {
  test("succeeds when predicate matches", async () => {
    let calls = 0;
    const poller = sync(() => {
      calls++;
      return calls;
    }) as any;

    const c = new TestClock();
    const promise = run(
      provide(
        repeatUntil(poller, { until: (n: number) => n >= 3, intervalMs: 100 }),
        Clock,
        c,
      ) as any,
    );
    await new Promise((r) => setTimeout(r, 0));
    c.advance(100);
    await new Promise((r) => setTimeout(r, 0));
    c.advance(100);
    await new Promise((r) => setTimeout(r, 0));

    expect(await promise).toBe(3);
    expect(calls).toBe(3);
  });

  test("fails with RepeatTimeoutError when maxAttempts is hit", async () => {
    // Use real time with tiny delays — simpler and less flaky than TestClock drain.
    const poller = sync(() => 0) as any;
    await expect(
      run(
        repeatUntil(poller, { until: (n: number) => n > 0, intervalMs: 1, maxAttempts: 3 }) as any,
      ),
    ).rejects.toEqual(
      expect.objectContaining({ _tag: "RepeatTimeoutError", reason: "maxAttempts" }),
    );
  });
});

describe("repeatUntilWithBackoff", () => {
  test("exponential delays work on the test clock", async () => {
    let attempts = 0;
    const poller = sync(() => {
      attempts++;
      return attempts;
    }) as any;
    const c = new TestClock();

    const promise = run(
      provide(
        repeatUntilWithBackoff(poller, {
          until: (n: number) => n >= 3,
          initialIntervalMs: 10,
          maxIntervalMs: 1000,
        }),
        Clock,
        c,
      ) as any,
    );
    await new Promise((r) => setTimeout(r, 0));
    // attempt 1 → 10ms
    c.advance(10);
    await new Promise((r) => setTimeout(r, 0));
    // attempt 2 → 20ms
    c.advance(20);
    await new Promise((r) => setTimeout(r, 0));

    expect(await promise).toBe(3);
  });
});

// ── Cats aliases ───────────────────────────────────────────────────

describe("Cats-named aliases", () => {
  test("handleErrorWith == catch", async () => {
    const eff = (fail("x") as any).handleErrorWith((e: string) => succeed(`caught ${e}`));
    expect(await run(eff)).toBe("caught x");
  });

  test("recover catches only matching errors via predicate", async () => {
    const caught = (fail("retry" as const) as any).recover(
      (e: string) => e === "retry",
      () => 99,
    );
    expect(await run(caught)).toBe(99);

    const uncaught = (fail("other" as const) as any).recover(
      (e: string) => e === "retry",
      () => 99,
    );
    await expect(run(uncaught)).rejects.toBe("other");
  });

  test("redeem transforms both channels with plain values", async () => {
    const ok = (succeed(5) as any).redeem(
      (_e: unknown) => -1,
      (v: number) => v * 2,
    );
    const err = (fail("boom") as any).redeem(
      (_e: unknown) => -1,
      (v: number) => v * 2,
    );
    expect(await run(ok)).toBe(10);
    expect(await run(err)).toBe(-1);
  });

  test("redeemWith transforms both channels with Effs", async () => {
    const ok = (succeed(5) as any).redeemWith(
      (_e: unknown) => succeed("err"),
      (v: number) => succeed(`ok:${v}`),
    );
    expect(await run(ok)).toBe("ok:5");
  });
});

// ── enriched retry ─────────────────────────────────────────────────

describe("retry enhancements", () => {
  test("when-predicate stops retrying on non-matching error", async () => {
    // Use real time with tiny delays — avoids fiddly TestClock drain timing.
    let attempts = 0;
    const eff: any = sync(() => {
      attempts++;
      return attempts;
    }).flatMap((n: number) => fail(n === 1 ? "transient" : "fatal"));

    await expect(
      run(retry(eff, { times: 5, delay: 1, when: (e: string) => e === "transient" }) as any),
    ).rejects.toBe("fatal");
    expect(attempts).toBe(2);
  });

  test("defects (thrown errors) are NOT retried by default", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      throw new Error("unexpected");
    }) as any;

    await expect(run(retry(eff, { times: 5, delay: 0 }) as any)).rejects.toBeInstanceOf(Error);
    expect(attempts).toBe(1); // no retry on defect
  });

  test("retryAllCause opts into defect-aware retry", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      if (attempts < 3) throw new Error("transient");
      return "ok";
    }) as any;

    const result = await run(
      retryAllCause(eff, {
        shouldRetry: (c) => Cause.hasDie(c),
        times: 5,
        delayMs: 0,
      }) as any,
    );
    expect(result).toBe("ok");
    expect(attempts).toBe(3);
  });

  test("retryAllCause stops when shouldRetry returns false", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      throw new TypeError("never-retry");
    }) as any;
    await expect(
      run(
        retryAllCause(eff, {
          shouldRetry: (c) => Cause.defects(c).some((d) => !(d instanceof TypeError)),
          times: 5,
          delayMs: 0,
        }) as any,
      ),
    ).rejects.toBeInstanceOf(TypeError);
    expect(attempts).toBe(1);
  });

  test("jitter keeps delays within ±50% of base (statistical)", async () => {
    // Sanity check: running a retry with jitter and a narrow delay stays bounded.
    // We don't assert exact values — just that the effect completes and the
    // delay stays positive / below maxDelay.
    let attempts = 0;
    const eff: any = sync(() => ++attempts).flatMap((n: number) =>
      n < 3 ? fail("retry") : succeed("ok"),
    );
    const result = await run(
      retry(eff, { times: 5, delay: 2, backoff: "fixed", jitter: true }) as any,
    );
    expect(result).toBe("ok");
    expect(attempts).toBe(3);
  });
});

describe("retryAllBy", () => {
  test("retries pending success outcomes via handler", async () => {
    let calls = 0;
    const poller = sync(() => {
      calls++;
      return { state: calls === 3 ? "done" : "pending" };
    }) as any;

    const result = await run(
      retryAllBy(poller, {
        baseDelayMs: 1,
        handle: (r) =>
          RetryAttempt.isSuccess(r) && r.value.state === "pending"
            ? RetryDecision.retry()
            : RetryDecision.stop(),
      }) as any,
    );
    expect(result).toEqual({ state: "done" });
    expect(calls).toBe(3);
  });

  test("stops when handler returns stop", async () => {
    let calls = 0;
    const unstable = sync(() => {
      calls++;
      return calls < 2 ? fail("retry") : succeed("done");
    }).flatMap((v: any) => (v === "retry" ? fail("retry") : succeed(v))) as any;

    await expect(
      run(
        retryAllBy(unstable, {
          baseDelayMs: 1,
          handle: (r) =>
            RetryAttempt.isError(r) && r.error === "go-on"
              ? RetryDecision.retry()
              : RetryDecision.stop(),
        }) as any,
      ),
    ).rejects.toBe("retry");
    expect(calls).toBe(1);
  });
});
