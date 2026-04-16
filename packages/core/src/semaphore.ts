import { type Eff, Suspend, Op } from "./eff"
import { succeed, sync, async } from "./constructors"
import { ensuring } from "./constructors"

export class Semaphore {
  private permits: number
  private waiters: Array<() => void> = []

  private constructor(permits: number) {
    this.permits = permits
  }

  static make(permits: number): Eff<Semaphore, never> {
    return sync(() => new Semaphore(permits))
  }

  acquire(): Eff<void, never> {
    return async<void>((resume) => {
      if (this.permits > 0) {
        this.permits--
        resume(succeed(undefined) as any)
        return
      }
      this.waiters.push(() => {
        this.permits--
        resume(succeed(undefined) as any)
      })
    }) as any
  }

  release(): Eff<void, never> {
    return sync(() => {
      this.permits++
      const waiter = this.waiters.shift()
      if (waiter) waiter()
    })
  }

  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S> {
    return this.acquire().flatMap(() =>
      ensuring(eff, this.release())
    ) as any
  }

  withPermits<A, S>(n: number, eff: Eff<A, S>): Eff<A, S> {
    const acquireN = Array.from({ length: n }, () => this.acquire())
      .reduce<Eff<void, never>>(
        (acc, a) => (acc as any).flatMap(() => a),
        succeed(undefined),
      )
    const releaseN = Array.from({ length: n }, () => this.release())
      .reduce<Eff<void, never>>(
        (acc, r) => (acc as any).flatMap(() => r),
        succeed(undefined),
      )
    return acquireN.flatMap(() => ensuring(eff, releaseN)) as any
  }

  get available(): Eff<number, never> {
    return sync(() => this.permits)
  }
}
