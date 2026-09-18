// Singleflight — request deduplication.
//
// When multiple callers request the same key concurrently, only one
// executes; the rest join and receive the same result. No caching: the key
// is cleared once the call settles. (For caching, use `cached` / `cachedBy`.)
//
// Lessons from promin:
//   - Single `do(key, eff)` method — keyed by string
//   - Settle = remove key (whether success or failure)
//   - Concurrent followers share the SAME Deferred
//
// Eff-typed contract; in-process by default. Distributed (Redis-backed,
// shared-key dedup across processes) implementations live downstream.

import { type Eff, type Throws, Suspend, Op } from "./eff.js";
import { failCause, suspend, sync } from "./constructors.js";
import { Cause } from "./cause.js";
import { type Deferred, InProcessDeferred } from "./deferred.js";
import type { Exit } from "./exit.js";

// Followers see a typed failure; a defect or interrupt is squashed to a value.
function errorValue<E>(cause: Cause): E {
  const typedFail = Cause.firstFail(cause);
  return (typedFail !== null ? typedFail.value : Cause.squash(cause)) as E;
}

export interface Singleflight<SF = never> {
  /**
   * Deduplicate by key. First caller runs the effect; concurrent callers
   * with the same key wait for that execution to settle and receive its
   * result (success or failure). Key is cleared on settle.
   */
  do<A, E>(key: string, eff: Eff<A, Throws<E>>): Eff<A, SF | Throws<E>>;
}

class InProcessSingleflight implements Singleflight {
  private readonly flights = new Map<string, Deferred<unknown, unknown>>();

  do<A, E>(key: string, eff: Eff<A, Throws<E>>): Eff<A, Throws<E>> {
    return suspend(() => {
      let leader: Deferred<A, E> | null = null;
      // The finalizer is in place before the key is registered, so no
      // interrupt can land between registering and the guarantee that the
      // key is cleared and followers are released.
      const flight = new Suspend(
        Op.Ensuring,
        suspend(() => {
          const existing = this.flights.get(key) as Deferred<A, E> | undefined;
          if (existing) return existing.await;
          leader = new InProcessDeferred<A, E>();
          this.flights.set(key, leader as Deferred<unknown, unknown>);
          return eff;
        }),
        (exit: Exit<unknown, A>) => (leader === null ? null : this.settle(key, leader, exit)),
      ) as unknown as Eff<A, Throws<E>>;
      // A leader fails the way its followers do.
      return flight.catchAllCause((cause) =>
        leader === null ? failCause(cause) : leader.await,
      ) as Eff<A, Throws<E>>;
    }) as Eff<A, Throws<E>>;
  }

  private settle<A, E>(
    key: string,
    deferred: Deferred<A, E>,
    exit: Exit<unknown, A>,
  ): Eff<void, never> {
    return (
      exit._tag === "Success"
        ? deferred.succeed(exit.value)
        : deferred.fail(errorValue<E>(exit.cause))
    ).flatMap(() =>
      sync(() => {
        if (this.flights.get(key) === deferred) this.flights.delete(key);
      }),
    );
  }
}

export const Singleflight = {
  make(): Singleflight {
    return new InProcessSingleflight();
  },
} as const;
