import { describe, test, expect } from "bun:test";
import { sync, ensuring, run, WorkerPool } from "../src";
import { runUnchecked } from "./run-unchecked";

describe("WorkerPool", () => {
  test("execute a pure function on a worker", async () => {
    const program = WorkerPool.make(2).flatMap((pool) =>
      ensuring(
        pool.execute((x: number) => x * 2, 21),
        pool.shutdown(),
      ),
    );
    expect(await run(program.orDie())).toBe(42);
  });

  test("execute an async function on a worker", async () => {
    const program = WorkerPool.make(2).flatMap((pool) =>
      ensuring(
        pool.execute(async (x: number) => {
          await new Promise((r) => setTimeout(r, 10));
          return x + 1;
        }, 99),
        pool.shutdown(),
      ),
    );
    expect(await run(program.orDie())).toBe(100);
  });

  test("parMap distributes across workers", async () => {
    const program = WorkerPool.make(4).flatMap((pool) =>
      ensuring(
        pool.parMap([1, 2, 3, 4, 5, 6, 7, 8], (x: number) => x * x),
        pool.shutdown(),
      ),
    );
    expect(await run(program.orDie())).toEqual([1, 4, 9, 16, 25, 36, 49, 64]);
  });

  test("handles errors in worker", async () => {
    const program = WorkerPool.make(2).flatMap((pool) =>
      ensuring(
        pool.execute(() => {
          throw new Error("worker boom");
        }, null),
        pool.shutdown(),
      ),
    );
    await expect(runUnchecked(program)).rejects.toHaveProperty("_tag", "WorkerError");
  });

  test("a thread that dies fails the task instead of hanging", async () => {
    // A worker whose entry cannot be loaded — the shape of every packaging bug
    // here — reports it on the error channel and never answers a message. The
    // pool has to turn that into a WorkerError; otherwise the task sits pending
    // forever and the caller just stops.
    class BrokenWorker {
      onmessage: ((event: { data: unknown }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      postMessage() {
        queueMicrotask(() => this.onerror?.({ message: "ModuleNotFound resolving executor" }));
      }
      terminate() {}
    }

    const realWorker = (globalThis as { Worker?: unknown }).Worker;
    (globalThis as { Worker?: unknown }).Worker = BrokenWorker;

    try {
      const pool = await run(WorkerPool.make(1));
      await expect(runUnchecked(pool.execute((x: number) => x * 2, 21))).rejects.toHaveProperty(
        "_tag",
        "WorkerError",
      );
      // And the pool stays failed rather than routing the next task to a dead thread.
      await expect(runUnchecked(pool.execute((x: number) => x, 1))).rejects.toHaveProperty(
        "_tag",
        "WorkerError",
      );
      await run(pool.shutdown());
    } finally {
      (globalThis as { Worker?: unknown }).Worker = realWorker;
    }
  });

  test("CPU-bound work runs in parallel", async () => {
    const program = WorkerPool.make(4).flatMap((pool) =>
      ensuring(
        sync(() => Date.now()).flatMap((start) =>
          pool
            .parMap(
              Array.from({ length: 4 }, (_, i) => i),
              (i: number) => {
                // CPU-bound: busy loop ~50ms
                const end = Date.now() + 50;
                while (Date.now() < end) {}
                return i;
              },
            )
            .flatMap((results) =>
              sync(() => ({
                results,
                elapsed: Date.now() - start,
              })),
            ),
        ),
        pool.shutdown(),
      ),
    );

    const { results, elapsed } = await run(program.orDie());
    expect(results).toEqual([0, 1, 2, 3]);
    // Each task busy-waits 50 ms of wall-clock time, so running them one after
    // another takes at least 200 ms however loaded the machine is. Less than
    // that means they overlapped. (A tighter limit flaked on busy machines.)
    expect(elapsed).toBeLessThan(200);
  });
});
