// Semaphore — counting semaphore with fair FIFO ordering.
// Eff-typed contract; in-process implementation by default.

import { type Eff } from "./eff.js";
import { succeed, sync, async, ensuring } from "./constructors.js";
import { Waiter, WaiterList } from "./internal/waiter-list.js";

class PermitWaiter extends Waiter {
  constructor(
    readonly n: number,
    readonly grant: () => void,
  ) {
    super();
  }
}

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

// Exported for use inside core (Stream.buffer). Not part of the public API.
export class InProcessSemaphore implements Semaphore {
  private permits: number;
  // FIFO queue; each waiter wants `n` permits, granted atomically. New
  // acquirers queue behind existing waiters even when permits are free, so
  // a large request can't be starved by a stream of small ones.
  private waiters = new WaiterList<PermitWaiter>();

  constructor(permits: number) {
    this.permits = permits;
  }

  // Permits are granted through resume even when they are free right away, so
  // a fiber interrupted before it runs gives them back (see `async`), and
  // withPermits registers its release in the run that receives them.
  acquireMany(n: number): Eff<void, never> {
    return async<void>((resume) => {
      const giveBack = () => this.releaseMany(n);
      if (this.waiters.length === 0 && this.permits >= n) {
        this.permits -= n;
        resume(succeed(undefined) as any, giveBack);
        return;
      }
      const node = this.waiters.push(
        new PermitWaiter(n, () => resume(succeed(undefined) as any, giveBack)),
      );
      // If this waiter was first in line and wanted many permits, the ones
      // behind it might fit now, so check again.
      return () => {
        this.waiters.remove(node);
        this.releaseMany(0);
      };
    }) as any;
  }

  releaseMany(n: number): void {
    this.permits += n;
    let head = this.waiters.peek();
    while (head !== undefined && this.permits >= head.n) {
      this.waiters.shift();
      this.permits -= head.n;
      head.grant();
      head = this.waiters.peek();
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
