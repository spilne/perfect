import { type Eff, type Throws } from "../eff.js";
import { succeed, fail, sync, async } from "../constructors.js";
import { all } from "../combinators.js";
import { Deque } from "../internal/deque.js";

interface PendingTask {
  id: number;
  /** Index of the worker running it (-1 while it waits in the pool's queue). */
  worker: number;
  readonly fnSource: string;
  readonly arg: unknown;
  /** The caller gave up (interrupted); its answer is ignored when it comes. */
  canceled: boolean;
  resolve: (value: any) => void;
  reject: (error: any) => void;
}

// How many tasks a worker is given at once (see WorkerPoolOptions).
const DEFAULT_TASKS_PER_WORKER = 2;

export interface WorkerPoolOptions {
  /**
   * How many tasks one worker holds at a time; the rest wait in the pool
   * until a worker has room. Default 2: while a worker runs one task, the
   * next is already there, and a slow task only holds up one other task.
   *
   * Raise it (to 16, say) for many tiny tasks, where the message round trip
   * costs more than the task itself. With 5000 tasks of a few microseconds,
   * 2 per worker took about 23 ms and 16 about 9 ms; with a mix of 50 ms and
   * 1 ms tasks, 2 per worker took 158 ms and 16 took 500 ms.
   */
  readonly tasksPerWorker?: number;
}

export class WorkerError {
  readonly _tag = "WorkerError" as const;
  constructor(public message: string) {}
}

/** What `executor.js` posts back for every task it is handed. */
interface WorkerResponse {
  id: number;
  ok: boolean;
  value?: unknown;
  error?: unknown;
}

/**
 * The slice of a worker the pool actually touches. Two implementations satisfy
 * it and they disagree on how messages arrive: the web `Worker` global (Bun,
 * Deno, browsers) sets `onmessage` and wraps the payload in a `MessageEvent`,
 * while `node:worker_threads` is an `EventEmitter` that hands over the raw
 * payload. `attach` below normalises both onto one callback.
 */
interface WorkerHandle {
  postMessage(message: unknown): void;
  terminate(): unknown;
  on?(event: string, handler: (payload: any) => void): void;
  onmessage?: ((event: { data: WorkerResponse }) => void) | null;
  onerror?: ((event: { message?: string }) => void) | null;
}

type SpawnWorker = (url: URL) => WorkerHandle;

/**
 * Reach a Node builtin without a static `import`, which would drag `node:` into
 * the module graph of every browser bundle of the root barrel (`WorkerPool` is
 * re-exported from it). `filesystem.ts` uses a dynamic `import()` for the same
 * reason; here the lookup has to stay synchronous so `make()` remains a `sync`
 * effect and `runSync(WorkerPool.make())` keeps working. Browsers have no
 * `process` at all — they hit the `Worker` global path instead.
 */
function nodeBuiltin<T>(id: string): T | undefined {
  const proc = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  try {
    return proc?.getBuiltinModule?.(id) as T | undefined;
  } catch {
    return undefined;
  }
}

// Detect the number of CPU cores available for real parallelism.
// Tries navigator (Bun/modern Node/browser), then Node's os module, then
// falls back to 4 — matches what cats-effect / ZIO do on the JVM side.
function detectCoreCount(): number {
  const nav = (globalThis as { navigator?: { hardwareConcurrency?: number } }).navigator;
  if (nav?.hardwareConcurrency) return nav.hardwareConcurrency;

  const os = nodeBuiltin<{
    availableParallelism?: () => number;
    cpus?: () => unknown[];
  }>("node:os");
  if (typeof os?.availableParallelism === "function") return os.availableParallelism();
  if (typeof os?.cpus === "function") return os.cpus().length;

  return 4;
}

/**
 * Pick the worker implementation this runtime offers. Node has no global
 * `Worker`, only `node:worker_threads` — and `executor.js` speaks both
 * protocols, so either one drives the same pool.
 */
function resolveWorkerSpawner(): SpawnWorker {
  const WebWorker = (
    globalThis as {
      Worker?: new (url: URL, options?: { type?: "module" }) => WorkerHandle;
    }
  ).Worker;
  if (WebWorker) return (url) => new WebWorker(url, { type: "module" });

  const NodeWorker = nodeBuiltin<{
    Worker?: new (url: URL) => WorkerHandle;
  }>("node:worker_threads")?.Worker;
  if (NodeWorker) return (url) => new NodeWorker(url);

  throw new Error(
    "WorkerPool requires either a global Worker (Bun, Deno, browsers) or node:worker_threads",
  );
}

/**
 * Locate the worker entry that runs inside each thread.
 *
 * The published tarball ships `dist/worker/executor.js` right beside
 * `dist/worker/pool.js`, so `./executor.js` is the only specifier that has to
 * resolve on its own — the same `.js`-for-`.ts` convention every relative import
 * in `src/` already follows. Running the sources directly is what needs help,
 * because a `new URL(…)` is a plain runtime path that no build step rewrites:
 * Bun maps it back onto the neighbouring `executor.ts` by itself, and under Node
 * the test loader does the same.
 */
function executorUrl(): URL {
  return new URL("./executor.js", import.meta.url);
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
  private workers: WorkerHandle[] = [];
  private pending = new Map<number, PendingTask>();
  private nextId = 0;
  // Tasks each worker has right now, by worker index.
  private load: number[] = [];
  // Tasks waiting for a worker with room. A worker gets at most
  // `tasksPerWorker` at a time, so a slow task only delays the tasks the
  // worker already has, and the others go to whichever worker frees up.
  private readonly queue = new Deque<PendingTask>();
  // Function source text, worked out once per function.
  private readonly sources = new WeakMap<Function, string>();
  private _shutdown = false;
  private failure: string | undefined;

  private constructor(
    private readonly size: number,
    private readonly tasksPerWorker: number,
  ) {}

  /**
   * Create a pool. If `size` is omitted, defaults to the detected CPU count
   * (navigator.hardwareConcurrency, or os.availableParallelism(), or 4).
   */
  static make(size?: number, options: WorkerPoolOptions = {}): Eff<WorkerPool, never> {
    const tasksPerWorker = options.tasksPerWorker ?? DEFAULT_TASKS_PER_WORKER;
    if (!Number.isInteger(tasksPerWorker) || tasksPerWorker < 1) {
      throw new RangeError("WorkerPool: tasksPerWorker must be a positive integer");
    }
    return sync(() => {
      const poolSize = size ?? detectCoreCount();
      const pool = new WorkerPool(poolSize, tasksPerWorker);
      const spawn = resolveWorkerSpawner();
      const url = executorUrl();

      for (let i = 0; i < poolSize; i++) {
        pool.workers.push(pool.attach(spawn(url)));
        pool.load.push(0);
      }

      return pool;
    });
  }

  private attach(worker: WorkerHandle): WorkerHandle {
    const onMessage = (response: WorkerResponse) => {
      const task = this.pending.get(response.id);
      if (!task) return;
      this.pending.delete(response.id);
      this.load[task.worker]!--;
      this.sendQueued();
      if (task.canceled) return;
      if (response.ok) task.resolve(response.value);
      else task.reject(response.error);
    };

    // A thread that dies — most often because it could not load its executor at
    // all — reports it here and nowhere else. Left unhandled, every in-flight
    // task and every task scheduled onto that thread afterwards simply never
    // settles, so the caller sees a hang instead of a `WorkerError`.
    const onError = (message: string) => {
      this.failure ??= message;
      this.queue.clear();
      for (const [id, task] of this.pending) {
        this.pending.delete(id);
        task.reject(message);
      }
    };

    if (typeof worker.on === "function") {
      worker.on("message", onMessage);
      worker.on("error", (error: { message?: string }) => onError(error?.message ?? String(error)));
    } else {
      worker.onmessage = (event) => onMessage(event.data);
      worker.onerror = (event) => onError(event?.message ?? "worker failed");
    }

    return worker;
  }

  execute<A, B>(fn: (arg: A) => B | Promise<B>, arg: A): Eff<B, Throws<WorkerError>> {
    return async<B, WorkerError>((resume) => {
      // Checked when the task runs, not when execute() is called, so a
      // retried or reused task sees a pool that was shut down in between.
      if (this._shutdown) return resume(fail(new WorkerError("Pool is shut down")) as any);
      // Once a thread has failed the remaining ones are not trustworthy either,
      // and round-robin would keep handing tasks to a dead worker.
      if (this.failure !== undefined) return resume(fail(new WorkerError(this.failure)) as any);

      const task: PendingTask = {
        id: this.nextId++,
        worker: -1,
        fnSource: this.sourceOf(fn),
        arg,
        canceled: false,
        resolve: (value) => resume(succeed(value) as any),
        reject: (error) => resume(fail(new WorkerError(error)) as any),
      };
      this.pending.set(task.id, task);
      this.queue.push(task);
      this.sendQueued();

      // A worker can't be stopped mid-task, so an interrupted caller just
      // stops listening: a queued task is never sent, and a running one's
      // answer is ignored (it still counts as running until it arrives).
      return () => {
        task.canceled = true;
      };
    }) as any;
  }

  // Hand queued tasks to workers that have room, least busy first. (Tasks
  // used to go out round-robin as soon as they were created, so every task
  // that landed behind a slow one on the same worker had to wait for it.)
  private sendQueued(): void {
    while (this.queue.length > 0) {
      const index = this.leastBusyWorker();
      if (this.load[index]! >= this.tasksPerWorker) return;
      const task = this.queue.shift()!;
      if (task.canceled) {
        this.pending.delete(task.id);
        continue;
      }
      task.worker = index;
      this.load[index]!++;
      this.workers[index]!.postMessage({ id: task.id, fnSource: task.fnSource, arg: task.arg });
    }
  }

  private leastBusyWorker(): number {
    let best = 0;
    for (let i = 1; i < this.load.length; i++) {
      if (this.load[i]! < this.load[best]!) best = i;
    }
    return best;
  }

  private sourceOf(fn: Function): string {
    let source = this.sources.get(fn);
    if (source === undefined) {
      source = fn.toString();
      this.sources.set(fn, source);
    }
    return source;
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
      this.queue.clear();
      for (const [, task] of this.pending) {
        task.reject("Pool shut down");
      }
      this.pending.clear();
    });
  }
}
