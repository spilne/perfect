import { describe, expect, test } from "bun:test";
import { Clock, Stream, TestClock, provide, run } from "../src";
import { OffsetTracker, Partition } from "../src/connect";

describe("throttle", () => {
  test("a second run doesn't inherit the first run's schedule", async () => {
    const clock = new TestClock();
    // One stream value, run twice.
    const paced = Stream.of(1, 2).throttle(1_000);
    const throttled = provide(paced.toArray(), Clock, clock);

    // First run: 1 at once, 2 a second later.
    const first = run(throttled);
    await new Promise((r) => setTimeout(r, 5));
    clock.advance(1_000);
    expect(await first).toEqual([1, 2]);

    // Second run of the same stream: 1 must come out at once again. The old
    // code kept the first run's "next allowed time", so the second run's
    // first value waited for the clock.
    let firstValueAt: number | undefined;
    const second = run(
      provide(
        paced
          .tap(() => {
            firstValueAt ??= clock.now();
          })
          .toArray(),
        Clock,
        clock,
      ),
    );
    await new Promise((r) => setTimeout(r, 5));
    expect(firstValueAt).toBe(1_000);
    clock.advance(1_000);
    expect(await second).toEqual([1, 2]);
  });
});

describe("OffsetTracker", () => {
  test("an offset finished again after its commit doesn't pile up as pending", () => {
    const tracker = new OffsetTracker();
    const p = Partition(0);
    tracker.observe(p, 0);
    tracker.complete(p, 0);
    expect(tracker.committable().get(p)).toBe(1);
    // The same message is redelivered and finishes again.
    tracker.complete(p, 0);
    expect(tracker.pendingCount()).toBe(0);
  });
});
