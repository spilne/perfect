// Semaphore — counting semaphore with fair FIFO ordering.
// Eff-typed contract; in-process implementation by default.

import { type Eff } from "./eff";
import { succeed, sync, async, ensuring } from "./constructors";

export interface Semaphore {
  /** Take one permit, blocking until available. */
  acquire(): Eff<void, never>;
  /** Return one permit, waking the next waiter (if any). */
  release(): Eff<void, never>;
  /** acquire → run → release. Release fires even on failure. */
  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S>;
  /** acquire N → run → release N. Useful for weighted operations. */
  withPermits<A, S>(n: number, eff: Eff<A, S>): Eff<A, S>;
  /** Current available permits (for metrics/inspection). */
  readonly available: Eff<number, never>;
}

class InProcessSemaphore implements Semaphore {
  private permits: number;
  private waiters: Array<{ canceled: boolean; resume: () => void }> = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  acquire(): Eff<void, never> {
    return async<void>((resume) => {
      if (this.permits > 0) {
        this.permits--;
        resume(succeed(undefined) as any);
        return;
      }
      const waiter = {
        canceled: false,
        resume: () => {
          if (waiter.canceled) return;
          waiter.canceled = true;
          resume(succeed(undefined) as any);
        },
      };
      this.waiters.push(waiter);
      return () => {
        waiter.canceled = true;
      };
    }) as any;
  }

  release(): Eff<void, never> {
    return sync(() => {
      this.permits++;
      while (this.waiters.length > 0) {
        const waiter = this.waiters.shift()!;
        if (waiter.canceled) continue;
        this.permits--;
        waiter.resume();
        break;
      }
    });
  }

  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S> {
    return this.acquire().flatMap(() => ensuring(eff, this.release())) as any;
  }

  withPermits<A, S>(n: number, eff: Eff<A, S>): Eff<A, S> {
    const acquireN = Array.from({ length: n }, () => this.acquire()).reduce<Eff<void, never>>(
      (acc, a) => (acc as any).flatMap(() => a),
      succeed(undefined),
    );
    const releaseN = Array.from({ length: n }, () => this.release()).reduce<Eff<void, never>>(
      (acc, r) => (acc as any).flatMap(() => r),
      succeed(undefined),
    );
    return acquireN.flatMap(() => ensuring(eff, releaseN)) as any;
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
