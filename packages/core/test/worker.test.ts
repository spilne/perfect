import { describe, test, expect } from "bun:test";
import { succeed, sync, ensuring, run, WorkerPool } from "../src";

describe("WorkerPool", () => {
  test("execute a pure function on a worker", async () => {
    const program = WorkerPool.make(2).flatMap((pool) =>
      ensuring(
        pool.execute((x: number) => x * 2, 21),
        pool.shutdown(),
      ),
    );
    expect(await run(program)).toBe(42);
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
    expect(await run(program)).toBe(100);
  });

  test("parMap distributes across workers", async () => {
    const program = WorkerPool.make(4).flatMap((pool) =>
      ensuring(
        pool.parMap([1, 2, 3, 4, 5, 6, 7, 8], (x: number) => x * x),
        pool.shutdown(),
      ),
    );
    expect(await run(program)).toEqual([1, 4, 9, 16, 25, 36, 49, 64]);
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
    await expect(run(program)).rejects.toHaveProperty("_tag", "WorkerError");
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

    const { results, elapsed } = await run(program);
    expect(results).toEqual([0, 1, 2, 3]);
    // 4 × 50ms tasks on 4 workers should take ~50-80ms, not ~200ms
    expect(elapsed).toBeLessThan(150);
  });
});
