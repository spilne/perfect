// Queue<A> — multi-producer, multi-consumer FIFO with backpressure.
//
// Eff-typed contract; in-process implementation by default.
// `close()` (alias `shutdown()`) signals "no more values" — pending takers
// receive `QueueClosed`; new offers fail; already-buffered values drain.

import { type Eff, type Throws } from "./eff";
import { succeed, fail, sync, async } from "./constructors";

export class QueueClosed {
  readonly _tag = "QueueClosed" as const;
}

/** Backwards-compat alias. Prefer `QueueClosed`. */
export const QueueShutdown = QueueClosed;
export type QueueShutdown = QueueClosed;

type TakeResume<A> = (eff: Eff<A, Throws<QueueClosed>>) => void;
type OfferResume = (eff: Eff<boolean, Throws<QueueClosed>>) => void;

export interface Queue<A> {
  /** Push a value. Blocks if bounded and full. Fails with QueueClosed if closed. */
  offer(value: A): Eff<boolean, Throws<QueueClosed>>;
  /** Pop a value. Blocks if empty. Fails with QueueClosed if closed AND empty. */
  take(): Eff<A, Throws<QueueClosed>>;
  /** Drain everything immediately, including queued offerers' values. */
  takeAll(): Eff<A[], never>;
  /** Push many — sequentially, respecting backpressure. */
  offerAll(values: A[]): Eff<void, Throws<QueueClosed>>;
  /** Number of buffered items (not including pending offerers). */
  readonly size: Eff<number, never>;
  /** Has close() been called? */
  readonly isClosed: Eff<boolean, never>;
  /** Backwards-compat alias for `isClosed`. */
  readonly isShutdown: Eff<boolean, never>;
  /** Signal "no more values" — wakes pending takers/offerers with QueueClosed. */
  close(): Eff<void, never>;
  /** Backwards-compat alias for `close`. */
  shutdown(): Eff<void, never>;
  /** Block until close() is called. */
  readonly awaitClose: Eff<void, never>;
  /** Backwards-compat alias for `awaitClose`. */
  readonly awaitShutdown: Eff<void, never>;
}

class InProcessQueue<A> implements Queue<A> {
  private buffer: A[] = [];
  private takers: TakeResume<A>[] = [];
  private offerers: Array<{ value: A; resume: OfferResume }> = [];
  private _closed = false;
  private closeWaiters: Array<() => void> = [];

  constructor(private readonly capacity: number) {}

  offer(value: A): Eff<boolean, Throws<QueueClosed>> {
    return async<boolean, QueueClosed>((resume) => {
      if (this._closed) {
        resume(fail(new QueueClosed()) as any);
        return;
      }
      const taker = this.takers.shift();
      if (taker) {
        taker(succeed(value) as any);
        resume(succeed(true) as any);
        return;
      }
      if (this.buffer.length < this.capacity) {
        this.buffer.push(value);
        resume(succeed(true) as any);
        return;
      }
      this.offerers.push({ value, resume: resume as any });
    }) as any;
  }

  take(): Eff<A, Throws<QueueClosed>> {
    return async<A, QueueClosed>((resume) => {
      if (this.buffer.length > 0) {
        const item = this.buffer.shift()!;
        const offerer = this.offerers.shift();
        if (offerer) {
          this.buffer.push(offerer.value);
          offerer.resume(succeed(true) as any);
        }
        resume(succeed(item) as any);
        return;
      }
      if (this._closed) {
        resume(fail(new QueueClosed()) as any);
        return;
      }
      const offerer = this.offerers.shift();
      if (offerer) {
        offerer.resume(succeed(true) as any);
        resume(succeed(offerer.value) as any);
        return;
      }
      this.takers.push(resume as any);
    }) as any;
  }

  takeAll(): Eff<A[], never> {
    return sync(() => {
      const items = this.buffer.splice(0);
      while (this.offerers.length > 0) {
        const offerer = this.offerers.shift()!;
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
      for (const t of takers) t(fail(new QueueClosed()) as any);
      for (const o of offerers) o.resume(fail(new QueueClosed()) as any);
      for (const w of this.closeWaiters) w();
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
      this.closeWaiters.push(() => resume(succeed(undefined) as any));
    }) as any;
  }

  get awaitShutdown(): Eff<void, never> {
    return this.awaitClose;
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
