// Semaphore — counting semaphore with fair FIFO ordering.
// Eff-typed contract; in-process implementation by default.

import { type Eff } from "./eff";
import { succeed, sync, async, ensuring } from "./constructors";

export interface Semaphore<S = never> {
  /** Take one permit, blocking until available. */
  acquire(): Eff<void, S>;
  /** Return one permit, waking the next waiter (if any). */
  release(): Eff<void, S>;
  /** acquire → run → release. Release fires even on failure. */
  withPermit<A, S2>(eff: Eff<A, S2>): Eff<A, S | S2>;
  /** acquire N → run → release N. Useful for weighted operations. */
  withPermits<A, S2>(n: number, eff: Eff<A, S2>): Eff<A, S | S2>;
  /** Current available permits (for metrics/inspection). */
  readonly available: Eff<number, S>;
}

class InProcessSemaphore implements Semaphore {
  private permits: number;
  // FIFO queue; each waiter wants `n` permits, granted atomically. New
  // acquirers queue behind existing waiters even when permits are free, so
  // a large request can't be starved by a stream of small ones.
  private waiters: Array<{ n: number; done: boolean; resume: () => void }> = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  private acquireMany(n: number): Eff<void, never> {
    return async<void>((resume) => {
      if (this.waiters.length === 0 && this.permits >= n) {
        this.permits -= n;
        resume(succeed(undefined) as any);
        return;
      }
      const waiter = {
        n,
        done: false,
        resume: () => {
          if (waiter.done) return;
          waiter.done = true;
          resume(succeed(undefined) as any);
        },
      };
      this.waiters.push(waiter);
      return () => {
        waiter.done = true;
        this.releaseMany(0);
      };
    }) as any;
  }

  private releaseMany(n: number): void {
    this.permits += n;
    while (this.waiters.length > 0) {
      const head = this.waiters[0]!;
      if (head.done) {
        this.waiters.shift();
        continue;
      }
      if (this.permits < head.n) break;
      this.waiters.shift();
      this.permits -= head.n;
      head.resume();
    }
  }

  acquire(): Eff<void, never> {
    return this.acquireMany(1);
  }

  release(): Eff<void, never> {
    return sync(() => this.releaseMany(1));
  }

  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S> {
    return this.acquireMany(1).flatMap(() => ensuring(eff, this.release())) as any;
  }

  withPermits<A, S>(n: number, eff: Eff<A, S>): Eff<A, S> {
    if (n <= 0) return eff;
    return this.acquireMany(n).flatMap(() =>
      ensuring(
        eff,
        sync(() => this.releaseMany(n)),
      ),
    ) as any;
  }

  get available(): Eff<number, never> {
    return sync(() => this.permits);
  }
}

export const Semaphore = {
  make(permits: number): Eff<Semaphore, never> {
    return sync(() => new InProcessSemaphore(permits));
  },
} as const;
