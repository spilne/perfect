// CountDownLatch — block until N events have occurred.
//
// Useful for "wait for N tasks to start/finish/sync up". Single-shot — once
// the latch fires, it stays fired (use Barrier for "all wait at the SAME
// point" or PubSub for repeated signaling).
//
// Lessons from promin:
//   - API: countDown / countDownBy(n) / await / remaining
//   - Effect-typed first-class methods; Promise mirrors only when we need
//     to bridge to Promise-land users
//   - Single Deferred fires when count hits 0, never resets
//
// Eff-typed contract; in-process implementation by default. Distributed
// (Redis/Postgres) backends live in downstream packages.

import { type Eff } from "./eff";
import { succeed } from "./constructors";
import { type Deferred, Deferred as DeferredNS } from "./deferred";
import { Ref } from "./ref";

export interface Latch {
  /** Decrement the counter by 1. Releases all awaiters when it hits 0. */
  readonly countDown: Eff<void, never>;
  /** Decrement by N (clamped at 0). */
  countDownBy(n: number): Eff<void, never>;
  /** Block until the counter reaches 0. Returns immediately if already 0. */
  readonly await: Eff<void, never>;
  /** Current remaining count. */
  readonly remaining: Eff<number, never>;
}

class InProcessLatch implements Latch {
  constructor(
    private readonly count: import("./ref").Ref<number>,
    private readonly deferred: Deferred<void, never>,
  ) {}

  get countDown(): Eff<void, never> {
    return this.countDownBy(1);
  }

  countDownBy(n: number): Eff<void, never> {
    return this.count
      .updateAndGet((c) => Math.max(0, c - n))
      .flatMap((remaining) =>
        remaining === 0
          ? this.deferred.succeed(undefined).map(() => undefined)
          : succeed(undefined),
      ) as Eff<void, never>;
  }

  get await(): Eff<void, never> {
    return this.deferred.await as Eff<void, never>;
  }

  get remaining(): Eff<number, never> {
    return this.count.get;
  }
}

export const Latch = {
  make(opts: { count: number }): Eff<Latch, never> {
    if (opts.count < 0) throw new Error("Latch.make: count must be >= 0");
    return Ref.make(opts.count).flatMap((countRef) =>
      DeferredNS.make<void, never>().flatMap((deferred) => {
        const latch = new InProcessLatch(countRef, deferred);
        // Edge case: starting at 0 = already-released
        return opts.count === 0
          ? deferred.succeed(undefined).map(() => latch as Latch)
          : succeed(latch as Latch);
      }),
    );
  },
} as const;
