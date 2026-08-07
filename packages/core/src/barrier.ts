// Barrier — N parties block until all N have arrived, then all release
// simultaneously. Single-shot (matches promin's design — start with the
// simpler one-shot model; cyclic mode can be added later if needed).
//
// Use cases: coordinated worker startup, multi-phase tests, lock-step
// simulation.
//
// API mirrors promin: `await` does both "arrive" and "block until all
// arrived" — there's no separate arrive step.
//
// Eff-typed contract; in-process by default. Distributed backends fit the
// same interface.

import { type Eff } from "./eff";
import { type Deferred, Deferred as DeferredNS } from "./deferred";
import { Ref } from "./ref";

export interface Barrier {
  /** Arrive at the barrier and block until all `parties` have arrived. */
  readonly await: Eff<void, never>;
  /** Number of parties that have arrived so far. */
  readonly arrived: Eff<number, never>;
}

class InProcessBarrier implements Barrier {
  constructor(
    private readonly parties: number,
    private readonly count: import("./ref").Ref<number>,
    private readonly deferred: Deferred<void, never>,
  ) {}

  get await(): Eff<void, never> {
    return this.count
      .updateAndGet((n) => n + 1)
      .flatMap((n) =>
        n >= this.parties
          ? (this.deferred.succeed(undefined).map(() => undefined) as Eff<void, never>)
          : (this.deferred.await as Eff<void, never>),
      ) as Eff<void, never>;
  }

  get arrived(): Eff<number, never> {
    return this.count.get;
  }
}

export const Barrier = {
  make(opts: { parties: number }): Eff<Barrier, never> {
    if (opts.parties < 1) throw new Error("Barrier.make: parties must be >= 1");
    return Ref.make(0).flatMap((count) =>
      DeferredNS.make<void, never>().map(
        (deferred) => new InProcessBarrier(opts.parties, count, deferred) as Barrier,
      ),
    );
  },
} as const;
