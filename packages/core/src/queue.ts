// Queue<A> — multi-producer, multi-consumer FIFO with backpressure.
//
// Eff-typed contract; in-process implementation by default.
// `close()` (alias `shutdown()`) signals "no more values" — pending takers
// receive `QueueClosed`; new offers fail; already-buffered values drain.

import { type Eff, type Throws } from "./eff";
import { succeed, fail, sync, async, suspend } from "./constructors";

export class QueueClosed {
  readonly _tag = "QueueClosed" as const;
}

/** Backwards-compat alias. Prefer `QueueClosed`. */
export const QueueShutdown = QueueClosed;
export type QueueShutdown = QueueClosed;

type TakeResume<A> = (eff: Eff<A, Throws<QueueClosed>>) => void;
type OfferResume = (eff: Eff<boolean, Throws<QueueClosed>>) => void;
type TakeWaiter<A> = { canceled: boolean; resume: TakeResume<A> };
type OfferWaiter<A> = { canceled: boolean; value: A; resume: OfferResume };
type CloseWaiter = { canceled: boolean; resume: () => void };

export interface Queue<A, S = never> {
  /** Push a value. Blocks if bounded and full. Fails with QueueClosed if closed. */
  offer(value: A): Eff<boolean, S | Throws<QueueClosed>>;
  /** Pop a value. Blocks if empty. Fails with QueueClosed if closed AND empty. */
  take(): Eff<A, S | Throws<QueueClosed>>;
  /** Drain everything immediately, including queued offerers' values. */
  takeAll(): Eff<A[], S>;
  /** Push many — sequentially, respecting backpressure. */
  offerAll(values: A[]): Eff<void, S | Throws<QueueClosed>>;
  /** Number of buffered items (not including pending offerers). */
  readonly size: Eff<number, S>;
  /** Has close() been called? */
  readonly isClosed: Eff<boolean, S>;
  /** Backwards-compat alias for `isClosed`. */
  readonly isShutdown: Eff<boolean, S>;
  /** Signal "no more values" — wakes pending takers/offerers with QueueClosed. */
  close(): Eff<void, S>;
  /** Backwards-compat alias for `close`. */
  shutdown(): Eff<void, S>;
  /** Block until close() is called. */
  readonly awaitClose: Eff<void, S>;
  /** Backwards-compat alias for `awaitClose`. */
  readonly awaitShutdown: Eff<void, S>;
}

class InProcessQueue<A> implements Queue<A> {
  private buffer: A[] = [];
  private takers: TakeWaiter<A>[] = [];
  private offerers: Array<OfferWaiter<A>> = [];
  private _closed = false;
  private closeWaiters: Array<CloseWaiter> = [];

  constructor(private readonly capacity: number) {}

  offer(value: A): Eff<boolean, Throws<QueueClosed>> {
    // Fast path: closed → fail; taker waiting → hand off; buffer has room → push.
    // All sync. Only fall to async when blocking on capacity.
    return suspend(() => {
      if (this._closed) return fail(new QueueClosed()) as any;
      const taker = this.nextTaker();
      if (taker) {
        taker.resume(succeed(value) as any);
        return succeed(true) as any;
      }
      if (this.buffer.length < this.capacity) {
        this.buffer.push(value);
        return succeed(true) as any;
      }
      // Slow path: bounded queue is full — block until a taker arrives.
      return async<boolean, QueueClosed>((resume) => {
        const offerer: OfferWaiter<A> = { canceled: false, value, resume: resume as any };
        this.offerers.push(offerer);
        return () => {
          offerer.canceled = true;
        };
      }) as any;
    }) as any;
  }

  take(): Eff<A, Throws<QueueClosed>> {
    // Fast path: buffer has item → take; offerer waiting → take; closed → fail.
    return suspend(() => {
      if (this.buffer.length > 0) {
        const item = this.buffer.shift()!;
        const offerer = this.nextOfferer();
        if (offerer) {
          this.buffer.push(offerer.value);
          offerer.resume(succeed(true) as any);
        }
        return succeed(item) as any;
      }
      const offerer = this.nextOfferer();
      if (offerer) {
        offerer.resume(succeed(true) as any);
        return succeed(offerer.value) as any;
      }
      if (this._closed) return fail(new QueueClosed()) as any;
      return async<A, QueueClosed>((resume) => {
        const taker: TakeWaiter<A> = { canceled: false, resume: resume as any };
        this.takers.push(taker);
        return () => {
          taker.canceled = true;
        };
      }) as any;
    }) as any;
  }

  takeAll(): Eff<A[], never> {
    return sync(() => {
      const items = this.buffer.splice(0);
      let offerer: OfferWaiter<A> | undefined;
      while ((offerer = this.nextOfferer())) {
        items.push(offerer.value);
        offerer.resume(succeed(true) as any);
      }
      return items;
    });
  }

  offerAll(values: A[]): Eff<void, Throws<QueueClosed>> {
    if (this._closed) return fail(new QueueClosed()) as any;
    return values.reduce<Eff<void, Throws<QueueClosed>>>(
      (acc, v) => (acc as any).flatMap(() => this.offer(v).map(() => undefined)),
      succeed(undefined) as any,
    );
  }

  get size(): Eff<number, never> {
    return sync(() => this.buffer.length);
  }

  get isClosed(): Eff<boolean, never> {
    return sync(() => this._closed);
  }

  get isShutdown(): Eff<boolean, never> {
    return this.isClosed;
  }

  close(): Eff<void, never> {
    return sync(() => {
      if (this._closed) return;
      this._closed = true;
      const takers = this.takers.splice(0);
      const offerers = this.offerers.splice(0);
      for (const t of takers) if (!t.canceled) t.resume(fail(new QueueClosed()) as any);
      for (const o of offerers) if (!o.canceled) o.resume(fail(new QueueClosed()) as any);
      for (const w of this.closeWaiters) if (!w.canceled) w.resume();
      this.closeWaiters.length = 0;
    });
  }

  shutdown(): Eff<void, never> {
    return this.close();
  }

  get awaitClose(): Eff<void, never> {
    if (this._closed) return succeed(undefined);
    return async<void>((resume) => {
      if (this._closed) {
        resume(succeed(undefined) as any);
        return;
      }
      const waiter: CloseWaiter = {
        canceled: false,
        resume: () => resume(succeed(undefined) as any),
      };
      this.closeWaiters.push(waiter);
      return () => {
        waiter.canceled = true;
      };
    }) as any;
  }

  get awaitShutdown(): Eff<void, never> {
    return this.awaitClose;
  }

  private nextTaker(): TakeWaiter<A> | undefined {
    while (this.takers.length > 0) {
      const taker = this.takers.shift()!;
      if (!taker.canceled) {
        taker.canceled = true;
        return taker;
      }
    }
    return undefined;
  }

  private nextOfferer(): OfferWaiter<A> | undefined {
    while (this.offerers.length > 0) {
      const offerer = this.offerers.shift()!;
      if (!offerer.canceled) {
        offerer.canceled = true;
        return offerer;
      }
    }
    return undefined;
  }
}

export const Queue = {
  bounded<A>(capacity: number): Eff<Queue<A>, never> {
    return sync(() => new InProcessQueue<A>(capacity));
  },
  unbounded<A>(): Eff<Queue<A>, never> {
    return sync(() => new InProcessQueue<A>(Infinity));
  },
} as const;
