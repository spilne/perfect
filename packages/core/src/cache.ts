// Effect-level memoization.
//
// `cached(eff, opts?)` — single-value cache with optional TTL. The returned
// effect runs the source the first time and replays the stored result on
// subsequent runs. Comes with `.invalidate()`, `.current`, `.isFresh`.
//
// `cachedBy(build, opts?)` — keyed cache with optional TTL (flat or per-value),
// optional LRU eviction via `maxSize`, and introspection methods.

import { type Eff, Suspend, Op } from "./eff"
import { succeed, sync } from "./constructors"
import { Clock } from "./clock"

// ── cached: single-entry, optional TTL ─────────────────────────────

interface SingleEntry<A> {
  readonly value: A
  readonly expiresAt: number // Infinity if no TTL
}

export interface CachedEff<A, S> extends Eff<A, S> {
  /** Invalidate the cache — next run will re-execute the source. */
  readonly invalidate: Eff<void, never>
  /** Peek at the current cached value without running anything. Returns
   *  undefined if empty or expired. */
  readonly current: Eff<A | undefined, never>
  /** Is there a fresh cached value right now? */
  readonly isFresh: Eff<boolean, never>
}

/**
 * Memoize the first successful result of `eff`. Each call to `cached()`
 * creates an independent cache. Failures are NOT cached.
 *
 * @param opts.ttlMs how long a value stays fresh (default: Infinity)
 */
export function cached<A, S>(
  eff: Eff<A, S>,
  opts: { ttlMs?: number } = {},
): CachedEff<A, S> {
  const { ttlMs = Infinity } = opts
  let entry: SingleEntry<A> | null = null

  const nowEff: Eff<number, never> =
    (Clock.get as any).flatMap((c: Clock) => sync(() => c.now())) as Eff<number, never>

  const getOrCompute: Eff<A, S> = (nowEff as any).flatMap((t: number) => {
    if (entry !== null && entry.expiresAt > t) return succeed(entry.value)
    if (entry !== null) entry = null // expired
    return (eff as any).flatMap((value: A) => {
      const expiresAt = ttlMs === Infinity ? Infinity : t + ttlMs
      entry = { value, expiresAt }
      return succeed(value)
    })
  }) as Eff<A, S>

  const invalidate: Eff<void, never> = sync(() => { entry = null })

  const current: Eff<A | undefined, never> = (nowEff as any).map((t: number) =>
    entry !== null && entry.expiresAt > t ? entry.value : undefined,
  ) as Eff<A | undefined, never>

  const isFresh: Eff<boolean, never> = (nowEff as any).map((t: number) =>
    entry !== null && entry.expiresAt > t,
  ) as Eff<boolean, never>

  // Attach helpers to the Suspend AST node so users see `.invalidate` etc.
  ;(getOrCompute as any).invalidate = invalidate
  ;(getOrCompute as any).current = current
  ;(getOrCompute as any).isFresh = isFresh
  return getOrCompute as CachedEff<A, S>
}

// ── cachedBy: keyed cache with optional TTL + LRU ──────────────────

interface Entry<A> {
  value: A
  expiresAt: number
}

export interface KeyedCache<K, A, S> {
  readonly get: (key: K) => Eff<A, S>
  readonly invalidate: (key: K) => Eff<void, never>
  readonly invalidateAll: Eff<void, never>
  /** Check whether a fresh value exists for a key without running build. */
  readonly has: (key: K) => Eff<boolean, never>
  readonly size: Eff<number, never>
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
 * @param opts.keyFn how to hash compound keys (default: String(key))
 */
export function cachedBy<K, A, S>(
  build: (key: K) => Eff<A, S>,
  opts: {
    ttlMs?: number | ((value: A) => number)
    maxSize?: number
    keyFn?: (key: K) => string
  } = {},
): KeyedCache<K, A, S> {
  const { ttlMs, maxSize = Infinity, keyFn = (k: K) => String(k) } = opts
  const resolveTtl = typeof ttlMs === "function"
    ? ttlMs
    : () => (ttlMs === undefined ? Infinity : ttlMs)

  // Map preserves insertion order — we use that for LRU-ish FIFO eviction.
  // On hit, we re-insert to move-to-end (true LRU).
  const store = new Map<string, Entry<A>>()

  const nowEff: Eff<number, never> =
    (Clock.get as any).flatMap((c: Clock) => sync(() => c.now())) as Eff<number, never>

  const get = (key: K): Eff<A, S> => {
    const hash = keyFn(key)
    return (nowEff as any).flatMap((t: number) => {
      const entry = store.get(hash)
      if (entry !== undefined && entry.expiresAt > t) {
        // move-to-end for LRU
        store.delete(hash)
        store.set(hash, entry)
        return succeed(entry.value)
      }
      if (entry !== undefined) store.delete(hash) // expired
      return (build(key) as any).flatMap((value: A) => {
        const entryTtl = resolveTtl(value)
        const expiresAt = entryTtl === Infinity ? Infinity : t + entryTtl
        // evict oldest if full
        if (store.size >= maxSize) {
          const firstKey = store.keys().next().value
          if (firstKey !== undefined) store.delete(firstKey)
        }
        store.set(hash, { value, expiresAt })
        return succeed(value)
      })
    }) as Eff<A, S>
  }

  const invalidate = (key: K): Eff<void, never> =>
    sync(() => { store.delete(keyFn(key)) })

  const invalidateAll: Eff<void, never> = sync(() => { store.clear() })

  const has = (key: K): Eff<boolean, never> =>
    (nowEff as any).map((t: number) => {
      const entry = store.get(keyFn(key))
      return entry !== undefined && entry.expiresAt > t
    }) as Eff<boolean, never>

  const size: Eff<number, never> = sync(() => store.size)

  return { get, invalidate, invalidateAll, has, size }
}
