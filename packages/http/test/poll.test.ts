// poll + pollWithBackoff + PollTimeoutError.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, sync, run } from "@perfect/core";
import { poll, pollWithBackoff, PollTimeoutError } from "../src";

describe("poll — fixed interval", () => {
  test("succeeds when until() returns true", async () => {
    let status: "pending" | "pending" | "done" = "pending";
    let polls = 0;
    const request: Eff<{ status: string }, Throws<never>> = sync(() => {
      polls++;
      if (polls === 3) status = "done";
      return { status };
    }) as any;

    const result = await run(
      poll({
        request,
        until: (r) => r.status === "done",
        intervalMs: 5,
        maxAttempts: 10,
      }),
    );
    expect(result.status).toBe("done");
    expect(polls).toBe(3);
  });

  test("fails with PollTimeoutError when maxAttempts exhausted", async () => {
    let polls = 0;
    const request: Eff<{ n: number }, Throws<never>> = sync(() => {
      polls++;
      return { n: polls };
    }) as any;

    await expect(
      run(
        poll({
          request,
          until: () => false,
          intervalMs: 1,
          maxAttempts: 4,
        }) as any,
      ),
    ).rejects.toMatchObject({
      _tag: "PollTimeoutError",
      attempts: 4,
      lastResult: { n: 4 },
    });
    expect(polls).toBe(4);
  });

  test("maxDurationMs caps wall-clock time", async () => {
    const request: Eff<{ k: number }, Throws<never>> = succeed({ k: 1 }) as any;
    const start = Date.now();
    await expect(
      run(
        poll({
          request,
          until: () => false,
          intervalMs: 30,
          maxAttempts: 1000,
          maxDurationMs: 50,
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "PollTimeoutError" });
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(200); // should bail around 50ms
  });

  test("succeeds on first poll if condition already met", async () => {
    const request: Eff<{ done: boolean }, Throws<never>> = succeed({ done: true }) as any;
    const result = await run(
      poll({
        request,
        until: (r) => r.done,
        intervalMs: 100,
      }),
    );
    expect(result).toEqual({ done: true });
  });
});

describe("pollWithBackoff — exponential interval", () => {
  test("succeeds when condition met", async () => {
    let polls = 0;
    const request: Eff<{ n: number }, Throws<never>> = sync(() => {
      polls++;
      return { n: polls };
    }) as any;

    const result = await run(
      pollWithBackoff({
        request,
        until: (r) => r.n === 3,
        initialIntervalMs: 1,
        maxIntervalMs: 10,
        maxAttempts: 10,
      }),
    );
    expect(result.n).toBe(3);
  });

  test("interval is bounded by maxIntervalMs", async () => {
    // Record timestamps of each attempt to verify backoff cap
    const timestamps: number[] = [];
    const request: Eff<number, Throws<never>> = sync(() => {
      timestamps.push(Date.now());
      return timestamps.length;
    }) as any;

    try {
      await run(
        pollWithBackoff({
          request,
          until: () => false,
          initialIntervalMs: 5,
          maxIntervalMs: 15, // cap
          maxAttempts: 5,
        }) as any,
      );
    } catch {
      // expected
    }
    // Intervals between attempts should eventually be ≤ 15ms + scheduling slop
    const intervals = timestamps.slice(1).map((t, i) => t - timestamps[i]!);
    // The first few intervals are small, later ones capped at ~15ms
    expect(intervals.every((i) => i <= 50)).toBe(true);
  });
});
