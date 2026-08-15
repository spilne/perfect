// CacheStore<K, V> — pluggable key-value cache with TTL + eviction.
//
// `cached` / `cachedBy` (in cache.ts) provide closed-over memoization built
// on a private Map. CacheStore is the OPEN interface — pluggable backends
// (Redis, memcached, distributed L2) implement the same shape.
//
// Eff-typed contract; in-process LRU+TTL by default. Distributed
// implementations live in downstream packages — they implement this
// interface and slot in via dependency injection.

import { type Eff } from "./eff";
import { sync } from "./constructors";
import { clockNow } from "./clock";

export interface CacheStore<K, V> {
  /** Read a value. Returns `undefined` if missing or expired. */
  get(key: K): Eff<V | undefined, never>;
  /** Write a value with optional per-entry TTL (overrides store default). */
  set(key: K, value: V, ttlMs?: number): Eff<void, never>;
  /** Remove a single entry. */
  delete(key: K): Eff<void, never>;
  /** Cheaper than `get` when you only care about presence. */
  has(key: K): Eff<boolean, never>;
  /** Drop everything. */
  clear(): Eff<void, never>;
  /** Current entry count. */
  readonly size: Eff<number, never>;
}

export interface MemoryCacheStoreOptions {
  /** Default TTL for entries when `set` is called without an explicit ttlMs. */
  readonly ttlMs?: number;
  /** Max entries before LRU eviction. Default: Infinity. */
  readonly maxSize?: number;
}

interface Entry<V> {
  readonly value: V;
  readonly expiresAt: number;
}

class InMemoryCacheStore<K, V> implements CacheStore<K, V> {
  private readonly entries = new Map<K, Entry<V>>();
  private readonly defaultTtl: number;
  private readonly maxSize: number;

  constructor(opts: MemoryCacheStoreOptions = {}) {
    this.defaultTtl = opts.ttlMs ?? Infinity;
    this.maxSize = opts.maxSize ?? Infinity;
  }

  get(key: K): Eff<V | undefined, never> {
    return (clockNow as any).map((now: number) => {
      const entry = this.entries.get(key);
      if (!entry) return undefined;
      if (now > entry.expiresAt) {
        this.entries.delete(key);
        return undefined;
      }
      // Touch as most-recently-used
      this.entries.delete(key);
      this.entries.set(key, entry);
      return entry.value;
    });
  }

  set(key: K, value: V, ttlMs?: number): Eff<void, never> {
    return (clockNow as any).map((now: number) => {
      const ttl = ttlMs ?? this.defaultTtl;
      const expiresAt = ttl === Infinity ? Infinity : now + ttl;
      this.entries.delete(key); // ensure insertion order = recency
      if (this.entries.size >= this.maxSize) {
        const oldest = this.entries.keys().next().value;
        if (oldest !== undefined) this.entries.delete(oldest as K);
      }
      this.entries.set(key, { value, expiresAt });
    });
  }

  delete(key: K): Eff<void, never> {
    return sync(() => {
      this.entries.delete(key);
    });
  }

  has(key: K): Eff<boolean, never> {
    return (clockNow as any).map((now: number) => {
      const entry = this.entries.get(key);
      if (!entry) return false;
      if (now > entry.expiresAt) {
        this.entries.delete(key);
        return false;
      }
      return true;
    });
  }

  clear(): Eff<void, never> {
    return sync(() => {
      this.entries.clear();
    });
  }

  get size(): Eff<number, never> {
    return sync(() => this.entries.size);
  }
}

export const CacheStore = {
  /** In-process LRU + TTL cache. */
  memory<K, V>(opts: MemoryCacheStoreOptions = {}): CacheStore<K, V> {
    return new InMemoryCacheStore<K, V>(opts);
  },
} as const;
