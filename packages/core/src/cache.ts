// Effect-level memoization.
//
// `cached(eff, opts?)` — single-value cache with optional TTL. The returned
// effect runs the source the first time and replays the stored result on
// subsequent runs. Comes with `.invalidate()`, `.current`, `.isFresh`.
//
// `cachedBy(build, opts?)` — keyed cache with optional TTL (flat or per-value),
// optional LRU eviction via `maxSize`, and introspection methods.

import { type Eff } from "./eff.js";
import { failCause, onExit, succeed, suspend, sync } from "./constructors.js";
import { Clock } from "./clock.js";
import { Cause } from "./cause.js";
import { InProcessDeferred } from "./deferred.js";
import type { Exit } from "./exit.js";

// ── In-flight sharing ──────────────────────────────────────────────
//
// When the cache is empty and several fibers ask at the same time, only
// the first one (the "leader") runs the source. The others wait for the
// leader and get the same result. Before, every caller ran the source,
// which is exactly what a cache is supposed to prevent.
//
// If the leader was interrupted, the waiting callers don't fail with that
// interrupt (it wasn't theirs). They try again, and one of them becomes
// the new leader.
class InFlight<K> {
  private readonly flights = new Map<K, InProcessDeferred<Exit<unknown, unknown>>>();

  run<A, S>(key: K, compute: Eff<A, S>, tryAgain: () => Eff<A, S>): Eff<A, S> {
    return suspend(() => {
      const existing = this.flights.get(key);
      if (existing !== undefined) {
        return (existing.await as Eff<Exit<unknown, A>, never>).flatMap((exit) =>
          replay(exit, tryAgain),
        );
      }
      const flight = new InProcessDeferred<Exit<unknown, unknown>>();
      this.flights.set(key, flight);
      return onExit(compute, (exit) =>
        suspend(() => {
          if (this.flights.get(key) === flight) this.flights.delete(key);
          return flight.succeed(exit).map(() => undefined);
        }),
      );
    }) as Eff<A, S>;
  }
}

function replay<A, S>(exit: Exit<unknown, A>, tryAgain: () => Eff<A, S>): Eff<A, S> {
  if (exit._tag === "Success") return succeed(exit.value);
  if (Cause.isInterruptedOnly(exit.cause)) return tryAgain();
  return failCause(exit.cause) as unknown as Eff<A, S>;
}

// ── cached: single-entry, optional TTL ─────────────────────────────

interface SingleEntry<A> {
  readonly value: A;
  readonly expiresAt: number; // Infinity if no TTL
}

// An intersection, not `interface ... extends Eff`: an interface re-reads
// Suspend's iterator method with its own `this` type, which then no longer
// matches Eff, so `.run()`, `all([...])` and `const e: Eff<A, S> = c` all
// failed to type-check.
export type CachedEff<A, S> = Eff<A, S> & {
  /** Invalidate the cache — next run will re-execute the source. */
  readonly invalidate: Eff<void, never>;
  /** Peek at the current cached value without running anything. Returns
   *  undefined if empty or expired. */
  readonly current: Eff<A | undefined, never>;
  /** Is there a fresh cached value right now? */
  readonly isFresh: Eff<boolean, never>;
};

/**
 * Memoize the first successful result of `eff`. Each call to `cached()`
 * creates an independent cache. Failures are NOT cached.
 *
 * @param opts.ttlMs how long a value stays fresh (default: Infinity)
 */
export function cached<A, S>(eff: Eff<A, S>, opts: { ttlMs?: number } = {}): CachedEff<A, S> {
  const { ttlMs = Infinity } = opts;
  let entry: SingleEntry<A> | null = null;

  const nowEff: Eff<number, never> = (Clock.get as any).flatMap((c: Clock) =>
    sync(() => c.now()),
  ) as Eff<number, never>;

  const inFlight = new InFlight<"value">();
  const getOrCompute: Eff<A, S> = (nowEff as any).flatMap((t: number) => {
    if (entry !== null && entry.expiresAt > t) return succeed(entry.value);
    if (entry !== null) entry = null; // expired
    const compute = (eff as any).flatMap((value: A) => {
      const expiresAt = ttlMs === Infinity ? Infinity : t + ttlMs;
      entry = { value, expiresAt };
      return succeed(value);
    });
    return inFlight.run("value", compute, () => getOrCompute);
  }) as Eff<A, S>;

  const invalidate: Eff<void, never> = sync(() => {
    entry = null;
  });

  const current: Eff<A | undefined, never> = (nowEff as any).map((t: number) =>
    entry !== null && entry.expiresAt > t ? entry.value : undefined,
  ) as Eff<A | undefined, never>;

  const isFresh: Eff<boolean, never> = (nowEff as any).map(
    (t: number) => entry !== null && entry.expiresAt > t,
  ) as Eff<boolean, never>;

  // Attach helpers to the Suspend AST node so users see `.invalidate` etc.
  (getOrCompute as any).invalidate = invalidate;
  (getOrCompute as any).current = current;
  (getOrCompute as any).isFresh = isFresh;
  return getOrCompute as CachedEff<A, S>;
}

// ── cachedBy: keyed cache with optional TTL + LRU ──────────────────

interface Entry<A> {
  value: A;
  expiresAt: number;
}

export interface KeyedCache<K, A, S> {
  readonly get: (key: K) => Eff<A, S>;
  readonly invalidate: (key: K) => Eff<void, never>;
  readonly invalidateAll: Eff<void, never>;
  /** Check whether a fresh value exists for a key without running build. */
  readonly has: (key: K) => Eff<boolean, never>;
  readonly size: Eff<number, never>;
}

/**
 * Build a keyed cache. Each call to `cachedBy()` creates an independent store.
 *
 * @param build how to produce an effect for a key
 * @param opts.ttlMs TTL per entry. Either a number (ms) or a function
 *   `(value) => ms` that computes the TTL from the computed value — useful
 *   for things like OAuth tokens that know their own expiry. Default: Infinity.
 * @param opts.maxSize upper bound on live entries. Oldest-inserted is evicted
 *   when full (FIFO; Map iteration order is insertion order). Default: Infinity.
 * @param opts.keyFn turns a key into a string. Default: String(key), which
 *   only works for primitive keys. Object keys need a keyFn, otherwise every
 *   object would become "[object Object]" and share one entry, so we fail
 *   instead.
 */
export function cachedBy<K, A, S>(
  build: (key: K) => Eff<A, S>,
  opts: {
    ttlMs?: number | ((value: A) => number);
    maxSize?: number;
    keyFn?: (key: K) => string;
  } = {},
): KeyedCache<K, A, S> {
  const { ttlMs, maxSize = Infinity, keyFn = defaultKeyFn } = opts;
  const resolveTtl =
    typeof ttlMs === "function" ? ttlMs : () => (ttlMs === undefined ? Infinity : ttlMs);

  // Map preserves insertion order — we use that for LRU-ish FIFO eviction.
  // On hit, we re-insert to move-to-end (true LRU).
  const store = new Map<string, Entry<A>>();

  const nowEff: Eff<number, never> = (Clock.get as any).flatMap((c: Clock) =>
    sync(() => c.now()),
  ) as Eff<number, never>;

  const inFlight = new InFlight<string>();

  // Expired entries are only removed when read, so with no maxSize a cache
  // of short-lived keys would grow forever. Every time the store doubles in
  // size we walk it once and drop what has expired. That keeps the cost
  // small on average.
  let sweepAt = 64;
  const sweep = (now: number): void => {
    if (store.size < sweepAt) return;
    for (const [hash, entry] of store) if (entry.expiresAt <= now) store.delete(hash);
    sweepAt = Math.max(64, store.size * 2);
  };

  const get = (key: K): Eff<A, S> =>
    (nowEff as any).flatMap((t: number) => {
      const hash = keyFn(key);
      const entry = store.get(hash);
      if (entry !== undefined && entry.expiresAt > t) {
        // move-to-end for LRU
        store.delete(hash);
        store.set(hash, entry);
        return succeed(entry.value);
      }
      if (entry !== undefined) store.delete(hash); // expired
      const compute = (build(key) as any).flatMap((value: A) => {
        const entryTtl = resolveTtl(value);
        const expiresAt = entryTtl === Infinity ? Infinity : t + entryTtl;
        sweep(t);
        // evict oldest if full
        if (store.size >= maxSize) {
          const firstKey = store.keys().next().value;
          if (firstKey !== undefined) store.delete(firstKey);
        }
        store.set(hash, { value, expiresAt });
        return succeed(value);
      });
      return inFlight.run(hash, compute, () => get(key));
    }) as Eff<A, S>;

  const invalidate = (key: K): Eff<void, never> =>
    sync(() => {
      store.delete(keyFn(key));
    });

  const invalidateAll: Eff<void, never> = sync(() => {
    store.clear();
  });

  const has = (key: K): Eff<boolean, never> =>
    (nowEff as any).map((t: number) => {
      const entry = store.get(keyFn(key));
      return entry !== undefined && entry.expiresAt > t;
    }) as Eff<boolean, never>;

  const size: Eff<number, never> = sync(() => store.size);

  return { get, invalidate, invalidateAll, has, size };
}

function defaultKeyFn(key: unknown): string {
  if (typeof key === "object" && key !== null) {
    throw new TypeError("cachedBy: object keys need a keyFn, e.g. keyFn: (k) => k.id");
  }
  return String(key);
}
