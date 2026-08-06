// Pool<R> — generic resource pool with bounded capacity, reuse, and
// blocking acquires.
//
// Acquires reuse a previously-released resource if available; otherwise
// create a new one (up to `size` total). When all `size` are in use, new
// acquires block until a release happens. On shutdown, idle resources are
// released and waiters are rejected.
//
// Optional `validate` runs before handing a reused resource back out — if
// it returns false, the resource is discarded (released) and a fresh one
// is created.
//
// Eff-typed contract; in-process by default. Distributed pools (shared
// across processes via Redis-coordinated leases, etc.) implement the same
// interface.

import { type Eff, type Throws } from "./eff";
import { fail, sync, async, ensuring } from "./constructors";

export class PoolClosed {
  readonly _tag = "PoolClosed" as const;
}

export interface PoolOptions<R> {
  /** Build a fresh resource. Called up to `size` times. */
  readonly acquire: Eff<R, never>;
  /** Tear down a resource (called on `shutdown` and on validate-fail). */
  readonly release: (resource: R) => Eff<void, never>;
  /** Max simultaneous resources held by the pool. */
  readonly size: number;
  /**
   * Optional check before handing a reused resource to a caller. If false,
   * the resource is released and a fresh one is acquired.
   */
  readonly validate?: (resource: R) => Eff<boolean, never>;
}

export interface Pool<R> {
  /**
   * Acquire a resource, run `fn`, auto-release. The resource is returned
   * to the pool on success, failure, OR interrupt.
   */
  use<A, S>(fn: (resource: R) => Eff<A, S>): Eff<A, S | Throws<PoolClosed>>;
  /** Resources currently checked out by users. */
  readonly inUse: Eff<number, never>;
  /** Resources sitting idle in the pool, ready for reuse. */
  readonly idle: Eff<number, never>;
  /** Total resources allocated (in-use + idle). */
  readonly size: Eff<number, never>;
  /** Drain all idle resources and reject pending waiters. */
  shutdown(): Eff<void, never>;
}

class InProcessPool<R> implements Pool<R> {
  private readonly idleList: R[] = [];
  private inUseCount = 0;
  private waiters: Array<{ canceled: boolean; resume: (r: R | PoolClosed) => void }> = [];
  private closed = false;

  constructor(private readonly opts: PoolOptions<R>) {}

  use<A, S>(fn: (resource: R) => Eff<A, S>): Eff<A, S | Throws<PoolClosed>> {
    return (this.acquireOne() as any).flatMap((r: R) =>
      ensuring(fn(r), this.releaseOne(r)),
    ) as any;
  }

  get inUse(): Eff<number, never> {
    return sync(() => this.inUseCount);
  }

  get idle(): Eff<number, never> {
    return sync(() => this.idleList.length);
  }

  get size(): Eff<number, never> {
    return sync(() => this.inUseCount + this.idleList.length);
  }

  shutdown(): Eff<void, never> {
    return sync(() => {
      if (this.closed) return [];
      this.closed = true;
      // Reject waiters
      const waiters = this.waiters.splice(0);
      const closedToken = new PoolClosed();
      for (const w of waiters) if (!w.canceled) w.resume(closedToken);
      // Snapshot idle for release
      const toRelease = this.idleList.splice(0);
      return toRelease;
    }).flatMap((toRelease: R[]) => {
      if (toRelease.length === 0) return sync(() => undefined) as any;
      return toRelease.reduce<Eff<void, never>>(
        (acc, r) => (acc as any).flatMap(() => this.opts.release(r)),
        sync(() => undefined) as any,
      );
    }) as Eff<void, never>;
  }

  // ── internals ──────────────────────────────────────────────────────

  private acquireOne(): Eff<R, Throws<PoolClosed>> {
    return sync(() => {
      if (this.closed) return { kind: "closed" as const };
      // Try to reuse an idle resource
      if (this.idleList.length > 0) {
        const r = this.idleList.shift()!;
        this.inUseCount++;
        return { kind: "ready" as const, resource: r, fresh: false };
      }
      // Capacity available: create a new one
      if (this.inUseCount < this.opts.size) {
        this.inUseCount++;
        return { kind: "create" as const };
      }
      // At capacity: must wait
      return { kind: "wait" as const };
    }).flatMap((decision: any): Eff<R, Throws<PoolClosed>> => {
      if (decision.kind === "closed") return fail(new PoolClosed()) as any;
      if (decision.kind === "ready") {
        const r = decision.resource as R;
        // Validate reused resources
        if (!this.opts.validate) return sync(() => r);
        return (this.opts.validate(r) as any).flatMap((ok: boolean) => {
          if (ok) return sync(() => r);
          // Bad resource — release and re-acquire fresh
          this.inUseCount--; // about to re-enter acquireOne, which increments
          return (this.opts.release(r) as any).flatMap(() => this.acquireOne());
        });
      }
      if (decision.kind === "create") return this.opts.acquire as any;
      // Wait for release
      return async<R, PoolClosed>((resume) => {
        const waiter = {
          canceled: false,
          resume: (r: R | PoolClosed) => {
            if (waiter.canceled) return;
            waiter.canceled = true;
            if (r instanceof PoolClosed) {
              resume(fail(r) as any);
            } else {
              this.inUseCount++;
              resume(sync(() => r) as any);
            }
          },
        };
        this.waiters.push(waiter);
        return () => {
          waiter.canceled = true;
        };
      }) as any;
    }) as Eff<R, Throws<PoolClosed>>;
  }

  private releaseOne(r: R): Eff<void, never> {
    return sync(() => {
      this.inUseCount--;
      if (this.closed) {
        // Pool shutting down — release immediately
        return r;
      }
      // Hand off to a waiter if any
      const waiter = this.nextWaiter();
      if (waiter) {
        waiter.resume(r);
        return null;
      }
      // No waiter: return to idle pool
      this.idleList.push(r);
      return null;
    }).flatMap((toRelease: R | null) => {
      if (toRelease === null) return sync(() => undefined) as any;
      return this.opts.release(toRelease);
    }) as Eff<void, never>;
  }

  private nextWaiter(): { canceled: boolean; resume: (r: R | PoolClosed) => void } | undefined {
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      if (!waiter.canceled) return waiter;
    }
    return undefined;
  }
}

export const Pool = {
  make<R>(opts: PoolOptions<R>): Eff<Pool<R>, never> {
    if (opts.size < 1) throw new Error("Pool.make: size must be >= 1");
    return sync(() => new InProcessPool<R>(opts) as Pool<R>);
  },
} as const;
