// RequestResolver — batch lookups made at the same time into one load.
//
//   const UserById = RequestResolver.make({
//     load: (ids: readonly string[]) =>
//       db.query("select * from users where id = any($1)", [ids])
//         .map((rows) => new Map(rows.map((u) => [u.id, u]))),
//   });
//
//   all(orders.map((o) => UserById.get(o.userId)));  // one query, not one per order
//
// Calls to `get` that arrive together (the fibers of one `all`, or within
// `windowMs`) are collected; the first of them starts a load for all their
// keys (each key once), and every caller gets its own value back. A key the
// load didn't return gives `undefined`. If the load fails, every caller in
// that batch fails the same way.

import { type Eff, type ErrorsOf } from "./eff.js";
import { failCause, forkDaemon, sleep, succeed, suspend, yieldNow } from "./constructors.js";
import { InProcessDeferred } from "./deferred.js";
import type { Exit } from "./exit.js";

export interface RequestResolverOptions<K, A, S> {
  /** Load many keys at once. Keys missing from the result give `undefined`. */
  readonly load: (keys: readonly K[]) => Eff<ReadonlyMap<K, A>, S>;
  /**
   * Also wait this long for more calls before loading. Default 0: only the
   * calls made together (fibers started side by side) share a load.
   */
  readonly windowMs?: number;
  /** Load at most this many keys at once; more start another load. Default: no limit. */
  readonly maxBatchSize?: number;
}

export interface RequestResolver<K, A, S> {
  /** The value for `key`, loaded together with other calls made at the same time. */
  get(key: K): Eff<A | undefined, S>;
}

type Waiter<A> = InProcessDeferred<Exit<unknown, A | undefined>>;

/** Calls waiting for the same load: each key once, with everyone who asked for it. */
class Batch<K, A> {
  readonly waiters = new Map<K, Waiter<A>[]>();
}

export const RequestResolver = {
  make<K, A, S>(options: RequestResolverOptions<K, A, S>): RequestResolver<K, A, S> {
    const windowMs = options.windowMs ?? 0;
    const maxBatchSize = options.maxBatchSize ?? Infinity;
    let open: Batch<K, A> | undefined;

    // Load a batch and hand every caller its result, success or not.
    const run = (batch: Batch<K, A>): Eff<void, never> =>
      suspend(() => {
        // Calls from now on start a new batch.
        if (open === batch) open = undefined;
        const keys = [...batch.waiters.keys()];
        return (
          options.load(keys).exit() as Eff<Exit<ErrorsOf<S>, ReadonlyMap<K, A>>, never>
        ).flatMap((exit) => {
          let handOut: Eff<unknown, never> = succeed(undefined);
          for (const [key, waiters] of batch.waiters) {
            const result: Exit<unknown, A | undefined> =
              exit._tag === "Success" ? { _tag: "Success", value: exit.value.get(key) } : exit;
            for (const waiter of waiters) handOut = handOut.flatMap(() => waiter.succeed(result));
          }
          return handOut.map(() => undefined);
        });
      });

    return {
      get(key: K): Eff<A | undefined, S> {
        return suspend(() => {
          const waiter: Waiter<A> = new InProcessDeferred();
          let batch = open;
          const startsBatch = batch === undefined;
          if (batch === undefined) open = batch = new Batch<K, A>();
          const waiters = batch.waiters.get(key);
          if (waiters) waiters.push(waiter);
          else batch.waiters.set(key, [waiter]);
          if (batch.waiters.size >= maxBatchSize) open = undefined;

          // The first caller starts the load on a background fiber, so that
          // being interrupted doesn't leave the other callers waiting. That
          // fiber runs with this caller's services, which the load may need.
          // It first lets the fibers started alongside get their keys in.
          const start: Eff<unknown, never> = startsBatch
            ? (forkDaemon(
                yieldNow
                  .flatMap(() => (windowMs > 0 ? sleep(windowMs) : succeed(undefined)))
                  .flatMap(() => run(batch!)),
              ) as Eff<unknown, never>)
            : succeed(undefined);

          return start
            .flatMap(() => waiter.await)
            .flatMap((exit) =>
              exit._tag === "Success" ? succeed(exit.value) : failCause(exit.cause),
            ) as Eff<A | undefined, S>;
        });
      },
    };
  },
} as const;
