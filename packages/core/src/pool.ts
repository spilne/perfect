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

import { type Eff, type Throws } from "./eff.js";
import { fail, succeed, sync, async, ensuring, suspend, uninterruptible } from "./constructors.js";

export class PoolClosed {
  readonly _tag = "PoolClosed" as const;
}

export interface PoolOptions<R, S = never> {
  /**
   * Build a fresh resource. Called up to `size` times. It runs interruptibly
   * so an interrupted use can cancel a slow connect; a resource it produces
   * just as its use is interrupted is not seen by the pool and is not
   * released.
   */
  readonly acquire: Eff<R, S>;
  /** Tear down a resource (called on `shutdown` and on validate-fail). */
  readonly release: (resource: R) => Eff<void, S>;
  /** Max simultaneous resources held by the pool. */
  readonly size: number;
  /**
   * Optional check before handing a reused resource to a caller. If false,
   * the resource is released and a fresh one is acquired.
   */
  readonly validate?: (resource: R) => Eff<boolean, S>;
}

export interface Pool<R, S = never> {
  /**
   * Acquire a resource, run `fn`, auto-release. The resource is returned
   * to the pool on success, failure, OR interrupt.
   */
  use<A, S2>(fn: (resource: R) => Eff<A, S2>): Eff<A, S | S2 | Throws<PoolClosed>>;
  /** Resources currently checked out by users. */
  readonly inUse: Eff<number, S>;
  /** Resources sitting idle in the pool, ready for reuse. */
  readonly idle: Eff<number, S>;
  /** Total resources allocated (in-use + idle). */
  readonly size: Eff<number, S>;
  /** Drain all idle resources and reject pending waiters. */
  shutdown(): Eff<void, S>;
}

// What one `use` holds. `slot` counts toward inUse; `resource` is set once the
// slot has a resource. The use's finalizer returns whatever the lease holds,
// so every step that changes it happens in a single synchronous step.
interface Lease<R> {
  slot: boolean;
  held: boolean;
  resource: R | undefined;
}

interface Waiter<R> {
  canceled: boolean;
  // A resource released by another use; inUse already counts it.
  readonly grant: (resource: R) => void;
  // Capacity freed by a failed create: acquire again.
  readonly retry: () => void;
  readonly close: (error: PoolClosed) => void;
}

class InProcessPool<R, S> implements Pool<R, S> {
  private readonly idleList: R[] = [];
  private inUseCount = 0;
  private waiters: Array<Waiter<R>> = [];
  private closed = false;

  constructor(private readonly opts: PoolOptions<R, S>) {}

  use<A, S2>(fn: (resource: R) => Eff<A, S2>): Eff<A, S | S2 | Throws<PoolClosed>> {
    return suspend(() => {
      const lease: Lease<R> = { slot: false, held: false, resource: undefined };
      return ensuring(
        this.acquireInto(lease).flatMap((r) => suspend(() => fn(r))),
        suspend(() => this.endLease(lease)),
      );
    }) as any;
  }

  get inUse(): Eff<number, S> {
    return sync(() => this.inUseCount);
  }

  get idle(): Eff<number, S> {
    return sync(() => this.idleList.length);
  }

  get size(): Eff<number, S> {
    return sync(() => this.inUseCount + this.idleList.length);
  }

  shutdown(): Eff<void, S> {
    return sync(() => {
      if (this.closed) return [];
      this.closed = true;
      const waiters = this.waiters.splice(0);
      const closedToken = new PoolClosed();
      for (const w of waiters) {
        if (w.canceled) continue;
        w.canceled = true;
        w.close(closedToken);
      }
      return this.idleList.splice(0);
    }).flatMap((toRelease: R[]) => {
      if (toRelease.length === 0) return sync(() => undefined) as any;
      return toRelease.reduce<Eff<void, never>>(
        (acc, r) => (acc as any).flatMap(() => this.opts.release(r)),
        sync(() => undefined) as any,
      );
    }) as Eff<void, never>;
  }

  // ── internals ──────────────────────────────────────────────────────

  // Resources reach the caller through async resumes, so a fiber interrupted
  // before it runs gives them back instead of losing them.
  private acquireInto(lease: Lease<R>): Eff<R, S | Throws<PoolClosed>> {
    return async<R, PoolClosed>((resume) => {
      if (this.closed) {
        resume(fail(new PoolClosed()) as any);
        return;
      }
      if (this.idleList.length > 0) {
        const r = this.idleList.shift()!;
        this.inUseCount++;
        this.grant(lease, r);
        resume(this.validated(lease, r) as any, () => this.ungrant(lease, r));
        return;
      }
      if (this.inUseCount < this.opts.size) {
        this.inUseCount++;
        lease.slot = true;
        resume(this.create(lease) as any, () => this.endSlot(lease));
        return;
      }
      const waiter: Waiter<R> = {
        canceled: false,
        grant: (r) => {
          this.grant(lease, r);
          resume(succeed(r) as any, () => this.ungrant(lease, r));
        },
        retry: () => resume(this.acquireInto(lease) as any, () => this.wakeRetry()),
        close: (error) => resume(fail(error) as any),
      };
      this.waiters.push(waiter);
      return () => {
        waiter.canceled = true;
      };
    }) as any;
  }

  // Creates a resource in the lease's slot. A create that fails or is
  // interrupted leaves the slot without a resource, and the use's finalizer
  // frees it and wakes a waiter.
  private create(lease: Lease<R>): Eff<R, S> {
    return this.opts.acquire.map((resource) => {
      lease.held = true;
      lease.resource = resource;
      return resource;
    });
  }

  private validated(lease: Lease<R>, r: R): Eff<R, S | Throws<PoolClosed>> {
    if (!this.opts.validate) return succeed(r);
    return (this.opts.validate(r) as any).flatMap((ok: boolean) => {
      if (ok) return succeed(r);
      // Bad resource: release it and create a fresh one in the same slot.
      // Dropping it from the lease and releasing it is one uninterruptible
      // step, so it is released once. The slot stays counted in inUse until
      // the replacement exists; if the release fails or the use is
      // interrupted first, the use's finalizer frees the slot and wakes a
      // waiter.
      return uninterruptible(
        suspend(() => {
          lease.held = false;
          lease.resource = undefined;
          return this.opts.release(r);
        }),
      ).flatMap((): Eff<R, S | Throws<PoolClosed>> =>
        this.closed ? fail(new PoolClosed()) : this.create(lease),
      );
    });
  }

  private grant(lease: Lease<R>, r: R): void {
    lease.slot = true;
    lease.held = true;
    lease.resource = r;
  }

  // A granted resource whose fiber was interrupted before it ran. Once the
  // pool is closed nobody waits for it and releasing it takes an effect, so
  // the lease keeps it and the interrupted use's finalizer releases it.
  private ungrant(lease: Lease<R>, r: R): void {
    if (this.closed) return;
    lease.slot = false;
    lease.held = false;
    lease.resource = undefined;
    this.inUseCount--;
    this.giveBack(r);
  }

  private giveBack(r: R): void {
    const waiter = this.nextWaiter();
    if (waiter) {
      this.inUseCount++;
      waiter.grant(r);
    } else {
      this.idleList.push(r);
    }
  }

  private endSlot(lease: Lease<R>): void {
    if (!lease.slot || lease.held) return;
    lease.slot = false;
    this.inUseCount--;
    this.wakeRetry();
  }

  private wakeRetry(): void {
    this.nextWaiter()?.retry();
  }

  // The use's finalizer: return the resource, or the capacity of a create
  // that never produced one.
  private endLease(lease: Lease<R>): Eff<void, S> {
    if (!lease.held) {
      this.endSlot(lease);
      return succeed(undefined);
    }
    const r = lease.resource as R;
    lease.slot = false;
    lease.held = false;
    lease.resource = undefined;
    this.inUseCount--;
    if (this.closed) return this.opts.release(r);
    this.giveBack(r);
    return succeed(undefined);
  }

  private nextWaiter(): Waiter<R> | undefined {
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      if (!waiter.canceled) {
        waiter.canceled = true;
        return waiter;
      }
    }
    return undefined;
  }
}

export const Pool = {
  make<R, S = never>(opts: PoolOptions<R, S>): Eff<Pool<R, S>, never> {
    if (opts.size < 1) throw new Error("Pool.make: size must be >= 1");
    return sync(() => new InProcessPool<R, S>(opts) as Pool<R, S>);
  },
} as const;
