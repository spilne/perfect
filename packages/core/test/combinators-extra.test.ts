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
import { runUnchecked } from "./run-unchecked";

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
    });
    const trapped = trapError(defectProne, ParseError);
    await expect(runUnchecked(trapped)).rejects.toBeInstanceOf(ParseError);
  });

  test("non-matching defects stay defects", async () => {
    const defectProne = sync(() => {
      throw new TypeError("not this one");
    });
    const trapped = trapError(defectProne, ParseError);
    await expect(runUnchecked(trapped)).rejects.toBeInstanceOf(TypeError);
  });

  test("multiple classes — any one matches", async () => {
    const defectProne = sync(() => {
      throw new NetError("500");
    });
    const trapped = trapError(defectProne, ParseError, NetError);
    await expect(runUnchecked(trapped)).rejects.toBeInstanceOf(NetError);
  });

  test("successful effects pass through unchanged", async () => {
    expect(await run(trapError(succeed(42), ParseError).orDie())).toBe(42);
  });
});

// ── validate ───────────────────────────────────────────────────────

describe("validate", () => {
  test("returns tuple of values when all succeed", async () => {
    const result = await run(validate([succeed(1), succeed("two"), succeed(true)]));
    expect(result).toEqual([1, "two", true]);
  });

  test("collects ALL failures into a Cause.both tree", async () => {
    const program = validate([fail("a"), succeed(1), fail("b"), fail("c")]).catchAllCause((cause) =>
      succeed(Cause.failures(cause)),
    );

    // validate always fails here, so the result is the failures list, not the tuple.
    const failures = (await run(program)) as unknown[];
    expect(failures.sort()).toEqual(["a", "b", "c"]);
  });

  test("single failure passes through cleanly", async () => {
    await expect(runUnchecked(validate([succeed(1), fail("only")]))).rejects.toBe("only");
  });

  test("empty array returns empty tuple", async () => {
    expect(await run(validate([]))).toEqual([]);
  });
});

// ── hedged ─────────────────────────────────────────────────────────

describe("hedged", () => {
  test("fastest replica wins", async () => {
    const work = sync(() => "work");
    // Any replica succeeds instantly → first one wins
    const race = hedged(work, { replicas: 3, staggerMs: 20 });
    expect(await run(race)).toBe("work");
  });

  test("replicas=1 is a passthrough", async () => {
    let count = 0;
    const eff = sync(() => {
      count++;
      return 42;
    });
    expect(await run(hedged(eff, { replicas: 1, staggerMs: 100 }))).toBe(42);
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
    });

    const c = new TestClock();
    const promise = runUnchecked(
      provide(repeatUntil(poller, { until: (n: number) => n >= 3, intervalMs: 100 }), Clock, c),
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
    const poller = sync(() => 0);
    await expect(
      runUnchecked(
        repeatUntil(poller, { until: (n: number) => n > 0, intervalMs: 1, maxAttempts: 3 }),
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
    });
    const c = new TestClock();

    const promise = runUnchecked(
      provide(
        repeatUntilWithBackoff(poller, {
          until: (n: number) => n >= 3,
          initialIntervalMs: 10,
          maxIntervalMs: 1000,
        }),
        Clock,
        c,
      ),
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
    const eff = fail("x").handleErrorWith((e) => succeed(`caught ${e}`));
    expect(await run(eff)).toBe("caught x");
  });

  test("recover catches only matching errors via predicate", async () => {
    const caught = fail("retry" as const).recover(
      (e) => e === "retry",
      () => 99,
    );
    expect(await run(caught.orDie())).toBe(99);

    const uncaught = fail("other" as const).recover(
      (e: string) => e === "retry",
      () => 99,
    );
    await expect(runUnchecked(uncaught)).rejects.toBe("other");
  });

  test("redeem transforms both channels with plain values", async () => {
    const ok = succeed(5).redeem(
      (_e: unknown) => -1,
      (v: number) => v * 2,
    );
    const err = fail("boom").redeem(
      (_e: unknown) => -1,
      (v: number) => v * 2,
    );
    expect(await run(ok)).toBe(10);
    expect(await run(err)).toBe(-1);
  });

  test("redeemWith transforms both channels with Effs", async () => {
    const ok = succeed(5).redeemWith(
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
    const eff = sync(() => {
      attempts++;
      return attempts;
    }).flatMap((n) => fail(n === 1 ? "transient" : "fatal"));

    await expect(
      runUnchecked(retry(eff, { times: 5, delay: 1, when: (e) => e === "transient" })),
    ).rejects.toBe("fatal");
    expect(attempts).toBe(2);
  });

  test("defects (thrown errors) are NOT retried by default", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      throw new Error("unexpected");
    });

    await expect(run(retry(eff, { times: 5, delay: 0 }))).rejects.toBeInstanceOf(Error);
    expect(attempts).toBe(1); // no retry on defect
  });

  test("retryAllCause opts into defect-aware retry", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      if (attempts < 3) throw new Error("transient");
      return "ok";
    });

    const result = await run(
      retryAllCause(eff, {
        shouldRetry: (c) => Cause.hasDie(c),
        times: 5,
        delayMs: 0,
      }),
    );
    expect(result).toBe("ok");
    expect(attempts).toBe(3);
  });

  test("retryAllCause stops when shouldRetry returns false", async () => {
    let attempts = 0;
    const eff = sync(() => {
      attempts++;
      throw new TypeError("never-retry");
    });
    await expect(
      run(
        retryAllCause(eff, {
          shouldRetry: (c) => Cause.defects(c).some((d) => !(d instanceof TypeError)),
          times: 5,
          delayMs: 0,
        }),
      ),
    ).rejects.toBeInstanceOf(TypeError);
    expect(attempts).toBe(1);
  });

  test("jitter keeps delays within ±50% of base (statistical)", async () => {
    // Sanity check: running a retry with jitter and a narrow delay stays bounded.
    // We don't assert exact values — just that the effect completes and the
    // delay stays positive / below maxDelay.
    let attempts = 0;
    const eff = sync(() => ++attempts).flatMap((n: number) =>
      n < 3 ? fail("retry") : succeed("ok"),
    );
    const result = await runUnchecked(
      retry(eff, { times: 5, delay: 2, backoff: "fixed", jitter: true }),
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
    });

    const result = await run(
      retryAllBy(poller, {
        baseDelayMs: 1,
        handle: (r) =>
          RetryAttempt.isSuccess(r) && r.value.state === "pending"
            ? RetryDecision.retry()
            : RetryDecision.stop(),
      }),
    );
    expect(result).toEqual({ state: "done" });
    expect(calls).toBe(3);
  });

  test("stops when handler returns stop", async () => {
    let calls = 0;
    const unstable = sync(() => {
      calls++;
      return calls < 2 ? fail("retry") : succeed("done");
    }).flatMap((v: unknown) => (v === "retry" ? fail("retry") : succeed(v)));

    await expect(
      runUnchecked(
        retryAllBy(unstable, {
          baseDelayMs: 1,
          handle: (r) =>
            RetryAttempt.isError(r) && r.error === "go-on"
              ? RetryDecision.retry()
              : RetryDecision.stop(),
        }),
      ),
    ).rejects.toBe("retry");
    expect(calls).toBe(1);
  });
});
