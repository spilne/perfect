import { type Eff, type Throws } from "./eff";
import { succeed, fail, sync, async } from "./constructors";

class QueueShutdown {
  readonly _tag = "QueueShutdown" as const;
}

type TakeResume<A> = (eff: Eff<A, Throws<QueueShutdown>>) => void;
type OfferResume = (eff: Eff<boolean, Throws<QueueShutdown>>) => void;

export class Queue<A> {
  private buffer: A[] = [];
  private takers: TakeResume<A>[] = [];
  private offerers: Array<{ value: A; resume: OfferResume }> = [];
  private _shutdown = false;
  private shutdownWaiters: Array<() => void> = [];

  private constructor(private readonly capacity: number) {}

  static bounded<A>(capacity: number): Eff<Queue<A>, never> {
    return sync(() => new Queue<A>(capacity));
  }

  static unbounded<A>(): Eff<Queue<A>, never> {
    return sync(() => new Queue<A>(Infinity));
  }

  offer(value: A): Eff<boolean, Throws<QueueShutdown>> {
    return async<boolean, QueueShutdown>((resume) => {
      if (this._shutdown) {
        resume(fail(new QueueShutdown()) as any);
        return;
      }

      // if a taker is waiting, hand off directly
      const taker = this.takers.shift();
      if (taker) {
        taker(succeed(value) as any);
        resume(succeed(true) as any);
        return;
      }

      // if buffer has room, enqueue
      if (this.buffer.length < this.capacity) {
        this.buffer.push(value);
        resume(succeed(true) as any);
        return;
      }

      // buffer full — block
      this.offerers.push({ value, resume: resume as any });
    }) as any;
  }

  take(): Eff<A, Throws<QueueShutdown>> {
    return async<A, QueueShutdown>((resume) => {
      if (this.buffer.length > 0) {
        const item = this.buffer.shift()!;
        // wake a blocked offerer
        const offerer = this.offerers.shift();
        if (offerer) {
          this.buffer.push(offerer.value);
          offerer.resume(succeed(true) as any);
        }
        resume(succeed(item) as any);
        return;
      }

      if (this._shutdown) {
        resume(fail(new QueueShutdown()) as any);
        return;
      }

      // if an offerer is waiting, take directly
      const offerer = this.offerers.shift();
      if (offerer) {
        offerer.resume(succeed(true) as any);
        resume(succeed(offerer.value) as any);
        return;
      }

      // block
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

  offerAll(values: A[]): Eff<void, Throws<QueueShutdown>> {
    if (this._shutdown) return fail(new QueueShutdown()) as any;
    return values.reduce<Eff<void, Throws<QueueShutdown>>>(
      (acc, v) => (acc as any).flatMap(() => this.offer(v).map(() => undefined)),
      succeed(undefined) as any,
    );
  }

  get size(): Eff<number, never> {
    return sync(() => this.buffer.length);
  }

  get isShutdown(): Eff<boolean, never> {
    return sync(() => this._shutdown);
  }

  shutdown(): Eff<void, never> {
    return sync(() => {
      if (this._shutdown) return;
      this._shutdown = true;
      const takers = this.takers.splice(0);
      const offerers = this.offerers.splice(0);
      for (const t of takers) t(fail(new QueueShutdown()) as any);
      for (const o of offerers) o.resume(fail(new QueueShutdown()) as any);
      for (const w of this.shutdownWaiters) w();
      this.shutdownWaiters.length = 0;
    });
  }

  get awaitShutdown(): Eff<void, never> {
    if (this._shutdown) return succeed(undefined);
    return async<void>((resume) => {
      if (this._shutdown) {
        resume(succeed(undefined) as any);
        return;
      }
      this.shutdownWaiters.push(() => resume(succeed(undefined) as any));
    }) as any;
  }
}
