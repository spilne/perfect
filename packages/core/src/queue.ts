// Queue<A> — multi-producer, multi-consumer FIFO with backpressure.
//
// Eff-typed contract; in-process implementation by default.
// `close()` (alias `shutdown()`) signals "no more values" — pending takers
// receive `QueueClosed`; new offers fail; already-buffered values drain.

import { type Eff, type Throws } from "./eff.js";
import { succeed, fail, sync, async, suspend } from "./constructors.js";
import { Deque } from "./internal/deque.js";
import { Waiter, WaiterList } from "./internal/waiter-list.js";

export class QueueClosed {
  readonly _tag = "QueueClosed" as const;
}

/** Backwards-compat alias. Prefer `QueueClosed`. */
export const QueueShutdown = QueueClosed;
export type QueueShutdown = QueueClosed;

type TakeResume<A> = (eff: Eff<A, Throws<QueueClosed>>, onDiscard?: () => void) => void;
type OfferResume = (eff: Eff<boolean, Throws<QueueClosed>>) => void;
class ResumeWaiter<R> extends Waiter {
  constructor(readonly resume: R) {
    super();
  }
}

class OfferWaiter<A> extends Waiter {
  constructor(
    readonly value: A,
    readonly resume: OfferResume,
  ) {
    super();
  }
}

export interface Queue<A, S = never> {
  /**
   * Push a value. Blocks if bounded and full. Fails with QueueClosed if closed.
   * A blocked offer interrupted after a take made room for it has still
   * enqueued its value.
   */
  offer(value: A): Eff<boolean, S | Throws<QueueClosed>>;
  /**
   * Pop a value. Blocks if empty. Fails with QueueClosed if closed AND empty.
   * A value handed to a take whose fiber is interrupted before it runs goes to
   * the next take instead.
   */
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
  private buffer = new Deque<A>();
  // Items given back by takers interrupted before they ran sit at the head of
  // the buffer, ordered by when they were first handed out; this holds those
  // items' handoff numbers, in buffer order.
  private givenBack = new Deque<number>();
  private nextHandoff = 0;
  // A fiber that stops waiting (timeout, interrupt) removes itself from
  // these lists, so they don't fill up with dead waiters.
  private takers = new WaiterList<ResumeWaiter<TakeResume<A>>>();
  private offerers = new WaiterList<OfferWaiter<A>>();
  private _closed = false;
  private closeWaiters = new WaiterList<ResumeWaiter<() => void>>();

  constructor(private readonly capacity: number) {}

  offer(value: A): Eff<boolean, Throws<QueueClosed>> {
    // Fast path: closed → fail; taker waiting → hand off; buffer has room → push.
    // All sync. Only fall to async when blocking on capacity.
    return suspend(() => {
      if (this._closed) return fail(new QueueClosed()) as any;
      if (this.handOff(value)) return succeed(true) as any;
      if (this.buffer.length < this.capacity) {
        this.buffer.push(value);
        return succeed(true) as any;
      }
      // Slow path: bounded queue is full — block until a taker arrives.
      return async<boolean, QueueClosed>((resume) => {
        const node = this.offerers.push(new OfferWaiter(value, resume as any));
        return () => this.offerers.remove(node);
      }) as any;
    }) as any;
  }

  take(): Eff<A, Throws<QueueClosed>> {
    // Fast path: buffer has item → take; offerer waiting → take; closed → fail.
    return suspend(() => {
      if (this.buffer.length > 0) {
        const item = this.buffer.shift()!;
        if (this.givenBack.length > 0) this.givenBack.shift();
        // Items given back by interrupted takers can leave the buffer over
        // capacity; a blocked offerer gets in only once there is room.
        if (this.buffer.length < this.capacity) {
          const offerer = this.nextOfferer();
          if (offerer) {
            this.buffer.push(offerer.value);
            offerer.resume(succeed(true) as any);
          }
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
        const node = this.takers.push(new ResumeWaiter(resume as any));
        return () => this.takers.remove(node);
      }) as any;
    }) as any;
  }

  takeAll(): Eff<A[], never> {
    return sync(() => {
      const items = this.buffer.drain();
      this.givenBack.clear();
      let offerer: OfferWaiter<A> | undefined;
      while ((offerer = this.nextOfferer())) {
        items.push(offerer.value);
        offerer.resume(succeed(true) as any);
      }
      return items;
    });
  }

  offerAll(values: A[]): Eff<void, Throws<QueueClosed>> {
    const loop = (index: number): Eff<void, Throws<QueueClosed>> =>
      index >= values.length
        ? (succeed(undefined) as any)
        : (this.offer(values[index]!) as any).flatMap(() => loop(index + 1));
    return suspend(() => (this._closed ? (fail(new QueueClosed()) as any) : loop(0))) as any;
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
      for (const t of this.takers.drain()) t.resume(fail(new QueueClosed()) as any);
      for (const o of this.offerers.drain()) o.resume(fail(new QueueClosed()) as any);
      for (const w of this.closeWaiters.drain()) w.resume();
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
      const node = this.closeWaiters.push(
        new ResumeWaiter(() => resume(succeed(undefined) as any)),
      );
      return () => this.closeWaiters.remove(node);
    }) as any;
  }

  get awaitShutdown(): Eff<void, never> {
    return this.awaitClose;
  }

  // Hands value to the oldest waiting taker. A taker interrupted before it
  // runs gives the value back.
  private handOff(value: A, handoff: number = this.nextHandoff++): boolean {
    const taker = this.takers.shift();
    if (taker === undefined) return false;
    taker.resume(succeed(value) as any, () => this.giveBack(value, handoff));
    return true;
  }

  // A value an interrupted taker never received goes to the next waiting
  // taker. Otherwise it goes before every value never handed out, and among
  // other given-back values in the order they were first handed out, so
  // takers that are interrupted in any order still leave the queue FIFO.
  // Values handed out were taken off the buffer, so a bounded queue can hold
  // more than its capacity by at most the number of takers interrupted before
  // they ran.
  private giveBack(value: A, handoff: number): void {
    if (this.handOff(value, handoff)) return;
    const order = this.givenBack;
    let index = order.length;
    while (index > 0 && order.get(index - 1) > handoff) index--;
    order.insert(index, handoff);
    this.buffer.insert(index, value);
  }

  private nextOfferer(): OfferWaiter<A> | undefined {
    return this.offerers.shift();
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
