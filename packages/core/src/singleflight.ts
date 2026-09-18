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

import { type Eff, type Throws } from "./eff";
import { onExit, sync } from "./constructors";
import { Cause } from "./cause";
import { type Deferred, InProcessDeferred } from "./deferred";
import type { Exit } from "./exit";

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

type LeaderOrFollower<A, E> =
  | { readonly kind: "leader"; readonly deferred: Deferred<A, E> }
  | { readonly kind: "follower"; readonly deferred: Deferred<A, E> };

class InProcessSingleflight implements Singleflight {
  private readonly flights = new Map<string, Deferred<unknown, unknown>>();

  do<A, E>(key: string, eff: Eff<A, Throws<E>>): Eff<A, Throws<E>> {
    // Atomic check + register in a single sync block. (Previously needed a
    // yieldNow prefix to defeat Op.All's side-effect-pre-running fast path,
    // but the runtime now restricts that fast path to literal Succeed only.)
    return sync<LeaderOrFollower<A, E>>(() => {
      const existing = this.flights.get(key) as Deferred<A, E> | undefined;
      if (existing) return { kind: "follower", deferred: existing };
      // Construct InProcessDeferred directly — no nested runSync.
      const deferred = new InProcessDeferred<A, E>();
      this.flights.set(key, deferred as Deferred<unknown, unknown>);
      return { kind: "leader", deferred };
    }).flatMap((state): Eff<A, Throws<E>> => {
      if (state.kind === "follower") return state.deferred.await;
      // Leader: settle the deferred and clear the key in a finalizer, so an
      // interrupted or dying leader still releases its followers, then fail
      // the way followers do.
      const { deferred } = state;
      const settle = (exit: Exit<unknown, A>): Eff<void, never> =>
        (exit._tag === "Success"
          ? deferred.succeed(exit.value)
          : deferred.fail(errorValue<E>(exit.cause))
        ).flatMap(() =>
          sync(() => {
            if (this.flights.get(key) === deferred) this.flights.delete(key);
          }),
        );
      return (onExit(eff, settle) as any).catchAllCause(() => deferred.await) as Eff<A, Throws<E>>;
    }) as Eff<A, Throws<E>>;
  }
}

export const Singleflight = {
  make(): Singleflight {
    return new InProcessSingleflight();
  },
} as const;
