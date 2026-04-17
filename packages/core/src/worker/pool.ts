import { type Eff, type Throws } from "../eff";
import { succeed, fail, sync, async } from "../constructors";
import { all } from "../combinators";

interface PendingTask {
  id: number;
  resolve: (value: any) => void;
  reject: (error: any) => void;
}

class WorkerError {
  readonly _tag = "WorkerError" as const;
  constructor(public message: string) {}
}

// Detect the number of CPU cores available for real parallelism.
// Tries navigator (Bun/modern Node/browser), then Node's os module, then
// falls back to 4 — matches what cats-effect / ZIO do on the JVM side.
function detectCoreCount(): number {
  const nav: any = typeof navigator !== "undefined" ? navigator : undefined;
  if (nav?.hardwareConcurrency) return nav.hardwareConcurrency;
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const os = (globalThis as any).require?.("node:os") ?? require("node:os");
    if (typeof os?.availableParallelism === "function") return os.availableParallelism();
    if (typeof os?.cpus === "function") return os.cpus().length;
  } catch {
    /* not in a Node-like env */
  }
  return 4;
}

/**
 * Pool of Bun/Node Worker threads for CPU-bound parallelism.
 *
 * This is the ONLY place Perfect touches real OS threads. The fiber runtime
 * itself is single-threaded (JS has no shared-memory threading available to
 * effect interpreters — compare to cats-effect/ZIO which distribute fibers
 * across an N-thread work-stealing pool on the JVM).
 *
 * Use WorkerPool when you have CPU-heavy pure functions to parallelise
 * across cores. IO-bound work doesn't need this — the event loop handles
 * that concurrently on one core.
 *
 * @example
 *   const pool = await run(WorkerPool.make())  // auto-sizes to cpu count
 *   const result = await run(pool.parMap(items, expensiveFn))
 */
export class WorkerPool {
  private workers: Worker[] = [];
  private pending = new Map<number, PendingTask>();
  private nextId = 0;
  private roundRobin = 0;
  private _shutdown = false;

  private constructor(private readonly size: number) {}

  /**
   * Create a pool. If `size` is omitted, defaults to the detected CPU count
   * (navigator.hardwareConcurrency, or os.availableParallelism(), or 4).
   */
  static make(size?: number): Eff<WorkerPool, never> {
    return sync(() => {
      const poolSize = size ?? detectCoreCount();
      const pool = new WorkerPool(poolSize);
      const executorPath = new URL("./executor.ts", import.meta.url).href;

      for (let i = 0; i < poolSize; i++) {
        const worker = new Worker(executorPath);
        worker.onmessage = (event: MessageEvent) => {
          const { id, ok, value, error } = event.data;
          const task = pool.pending.get(id);
          if (!task) return;
          pool.pending.delete(id);
          if (ok) task.resolve(value);
          else task.reject(error);
        };
        pool.workers.push(worker);
      }

      return pool;
    });
  }

  execute<A, B>(fn: (arg: A) => B | Promise<B>, arg: A): Eff<B, Throws<WorkerError>> {
    if (this._shutdown) return fail(new WorkerError("Pool is shut down")) as any;

    return async<B, WorkerError>((resume) => {
      const id = this.nextId++;
      const worker = this.workers[this.roundRobin % this.size]!;
      this.roundRobin++;

      this.pending.set(id, {
        id,
        resolve: (value) => resume(succeed(value) as any),
        reject: (error) => resume(fail(new WorkerError(error)) as any),
      });

      worker.postMessage({
        id,
        fnSource: fn.toString(),
        arg,
      });
    }) as any;
  }

  parMap<A, B>(items: A[], fn: (arg: A) => B | Promise<B>): Eff<B[], Throws<WorkerError>> {
    return all(items.map((item) => this.execute(fn, item))) as any;
  }

  get poolSize(): number {
    return this.size;
  }

  shutdown(): Eff<void, never> {
    return sync(() => {
      this._shutdown = true;
      for (const worker of this.workers) worker.terminate();
      this.workers.length = 0;
      for (const [, task] of this.pending) {
        task.reject("Pool shut down");
      }
      this.pending.clear();
    });
  }
}
