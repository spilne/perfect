import { describe, expect, test } from "bun:test";
import { WorkerPool, all, run, timeoutOption } from "../src";

const busyFor = (ms: number) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {}
  return ms;
};

describe("WorkerPool queue", () => {
  test("fast tasks don't wait behind a slow one", async () => {
    const pool = await run(WorkerPool.make(2, { tasksPerWorker: 1 }));
    try {
      const started = Date.now();
      const finished: number[] = [];
      const task = (ms: number) =>
        pool.execute(busyFor, ms).map(() => finished.push(Date.now() - started));
      // One slow task, then fast ones. With round-robin, every other fast
      // task would land behind the slow one.
      await run(all([task(300), task(5), task(5), task(5), task(5)]));
      // A fast task stuck behind the slow one would finish after it (300 ms
      // or more). Finishing earlier is the proof, however busy the machine.
      const fast = finished.slice(0, 4);
      expect(Math.max(...fast)).toBeLessThan(300);
    } finally {
      await run(pool.shutdown());
    }
  });

  test("a task that gives up while queued is never run", async () => {
    const pool = await run(WorkerPool.make(1, { tasksPerWorker: 1 }));
    try {
      const slow = run(pool.execute(busyFor, 100));
      // Queued behind the slow task, and abandoned right away.
      const abandoned = await run(timeoutOption(pool.execute(busyFor, 1000), 1));
      expect(abandoned).toBeUndefined();
      await slow;
      const started = Date.now();
      expect(await run(pool.execute(busyFor, 1))).toBe(1);
      // If the abandoned 1000 ms task had run, this would have waited for it.
      expect(Date.now() - started).toBeLessThan(1_000);
    } finally {
      await run(pool.shutdown());
    }
  });

  test("tasksPerWorker must be a positive integer", () => {
    expect(() => WorkerPool.make(1, { tasksPerWorker: 0 })).toThrow(RangeError);
  });
});
