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

import { type Eff, type Throws, Suspend, Op } from "./eff";
import { sync } from "./constructors";
import { Cause } from "./cause";
import { type Deferred, InProcessDeferred } from "./deferred";

export interface Singleflight {
  /**
   * Deduplicate by key. First caller runs the effect; concurrent callers
   * with the same key wait for that execution to settle and receive its
   * result (success or failure). Key is cleared on settle.
   */
  do<A, E>(key: string, eff: Eff<A, Throws<E>>): Eff<A, Throws<E>>;
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
        // Leader: run eff, fan out via deferred, cleanup, return original outcome.
        // Use catchAllCause so defects (uncaught throws → Cause.Die) and
        // interrupts also settle the deferred — otherwise followers deadlock.
        const cleanup = sync(() => {
          this.flights.delete(key);
        });
        const onSuccess = (value: A) =>
          state.deferred.succeed(value).flatMap(() => cleanup.map(() => value));
        const onCause = (cause: any) => {
          // Surface as a typed failure on the deferred so followers can re-throw.
          // For defects, squash to the underlying value.
          const typedFail = Cause.firstFail ? Cause.firstFail(cause) : null;
          const errVal = typedFail !== null ? typedFail.value : Cause.squash(cause);
          return state.deferred
            .fail(errVal as E)
            .flatMap(() => cleanup)
            .flatMap(() => new Suspend(Op.Fail, cause, null) as any);
        };
        return new Suspend(
          Op.CatchAll,
          new Suspend(Op.FlatMap, eff as any, (v: A) => onSuccess(v)),
          (cause: any) => onCause(cause),
        ) as any;
      }) as Eff<A, Throws<E>>;
  }
}

export const Singleflight = {
  make(): Singleflight {
    return new InProcessSingleflight();
  },
} as const;
