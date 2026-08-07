import { describe, test, expect } from "bun:test";
import { eff, succeed, sync, run, type RepeatTimeoutError, type Eff, type Throws } from "../src";

describe(".repeatUntil — fluent", () => {
  test("succeeds when condition met", async () => {
    let calls = 0;
    const request = eff(function* () {
      calls++;
      return yield* succeed({ status: calls === 3 ? "done" : "pending" });
    });
    const result = await run(
      (request as any).repeatUntil({
        until: (r: any) => r.status === "done",
        intervalMs: 1,
      }),
    );
    expect(result).toEqual({ status: "done" });
    expect(calls).toBe(3);
  });

  test("fails with RepeatTimeoutError carrying lastResult when maxAttempts hit", async () => {
    let calls = 0;
    const request = sync(() => ({ n: ++calls }));
    const program = (request as any).repeatUntil({
      until: () => false,
      intervalMs: 1,
      maxAttempts: 4,
    });
    await expect(run(program)).rejects.toMatchObject({
      _tag: "RepeatTimeoutError",
      reason: "maxAttempts",
      attempts: 4,
      lastResult: { n: 4 },
    });
  });

  test("fails with lastResult when maxDuration hit", async () => {
    const request = sync(() => ({ marker: "observed" }));
    const program = (request as any).repeatUntil({
      until: () => false,
      intervalMs: 30,
      maxDurationMs: 50,
    });
    await expect(run(program)).rejects.toMatchObject({
      _tag: "RepeatTimeoutError",
      reason: "maxDuration",
      lastResult: { marker: "observed" },
    });
  });
});

describe(".catchTag narrows RepeatTimeoutError<A>", () => {
  test("handler sees lastResult typed as A", async () => {
    interface Job {
      id: string;
      status: "pending" | "done";
    }
    const request: Eff<Job, Throws<never>> = succeed({
      id: "j1",
      status: "pending",
    } as Job) as any;

    // Whole pipeline: fluent poll → catchTag with typed narrowing.
    const program = (request as any)
      .repeatUntil({
        until: (j: Job) => j.status === "done",
        intervalMs: 1,
        maxAttempts: 2,
      })
      .catchTag("RepeatTimeoutError", (e: RepeatTimeoutError<Job>) => {
        // TS-level guarantees: e.lastResult is a Job; compiler rejects
        // e.lastResult.bogus. Runtime assertion confirms the data shape.
        return succeed({
          fellBack: true,
          lastId: e.lastResult.id,
          lastStatus: e.lastResult.status,
          attempts: e.attempts,
        });
      });

    const result = await run(program as any);
    expect(result).toEqual({
      fellBack: true,
      lastId: "j1",
      lastStatus: "pending",
      attempts: 2,
    });
  });
});

describe(".repeatUntilWithBackoff — fluent", () => {
  test("succeeds, includes lastResult on timeout", async () => {
    let calls = 0;
    const request = sync(() => ({ n: ++calls }));
    const success = await run(
      (request as any).repeatUntilWithBackoff({
        until: (r: any) => r.n === 3,
        initialIntervalMs: 1,
        maxIntervalMs: 5,
      }),
    );
    expect(success).toEqual({ n: 3 });

    // Exhaustion case
    calls = 0;
    await expect(
      run(
        (sync(() => ({ n: ++calls })) as any).repeatUntilWithBackoff({
          until: () => false,
          initialIntervalMs: 1,
          maxIntervalMs: 5,
          maxAttempts: 3,
        }),
      ),
    ).rejects.toMatchObject({
      _tag: "RepeatTimeoutError",
      attempts: 3,
      lastResult: { n: 3 },
    });
  });
});
