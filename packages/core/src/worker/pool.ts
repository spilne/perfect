import { type Eff, type Throws } from "../eff"
import { succeed, fail, sync, async } from "../constructors"
import { all } from "../combinators"

interface PendingTask {
  id: number
  resolve: (value: any) => void
  reject: (error: any) => void
}

class WorkerError {
  readonly _tag = "WorkerError" as const
  constructor(public message: string) {}
}

export class WorkerPool {
  private workers: Worker[] = []
  private pending = new Map<number, PendingTask>()
  private nextId = 0
  private roundRobin = 0
  private _shutdown = false

  private constructor(private readonly size: number) {}

  static make(size?: number): Eff<WorkerPool, never> {
    return sync(() => {
      const poolSize = size ?? navigator?.hardwareConcurrency ?? 4
      const pool = new WorkerPool(poolSize)
      const executorPath = new URL("./executor.ts", import.meta.url).href

      for (let i = 0; i < poolSize; i++) {
        const worker = new Worker(executorPath)
        worker.onmessage = (event: MessageEvent) => {
          const { id, ok, value, error } = event.data
          const task = pool.pending.get(id)
          if (!task) return
          pool.pending.delete(id)
          if (ok) task.resolve(value)
          else task.reject(error)
        }
        pool.workers.push(worker)
      }

      return pool
    })
  }

  execute<A, B>(
    fn: (arg: A) => B | Promise<B>,
    arg: A,
  ): Eff<B, Throws<WorkerError>> {
    if (this._shutdown) return fail(new WorkerError("Pool is shut down")) as any

    return async<B, WorkerError>((resume) => {
      const id = this.nextId++
      const worker = this.workers[this.roundRobin % this.size]!
      this.roundRobin++

      this.pending.set(id, {
        id,
        resolve: (value) => resume(succeed(value) as any),
        reject: (error) => resume(fail(new WorkerError(error)) as any),
      })

      worker.postMessage({
        id,
        fnSource: fn.toString(),
        arg,
      })
    }) as any
  }

  parMap<A, B>(
    items: A[],
    fn: (arg: A) => B | Promise<B>,
  ): Eff<B[], Throws<WorkerError>> {
    return all(items.map(item => this.execute(fn, item))) as any
  }

  get poolSize(): number {
    return this.size
  }

  shutdown(): Eff<void, never> {
    return sync(() => {
      this._shutdown = true
      for (const worker of this.workers) worker.terminate()
      this.workers.length = 0
      for (const [, task] of this.pending) {
        task.reject("Pool shut down")
      }
      this.pending.clear()
    })
  }
}
