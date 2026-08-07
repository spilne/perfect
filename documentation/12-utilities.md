# Utilities

## Duration

Type-safe time arithmetic. Eliminates magic millisecond numbers in your
code. Functions that take time can accept `DurationInput`
(`number | string | Duration`) and resolve via `resolveMs`.

<!-- @embed packages/core/examples/15-duration-cache.ts#duration-basics -->
```ts
import { Duration, resolveMs } from "@perfect/core";

// Type-safe time arithmetic — no more raw `5000` magic numbers.
const fiveMins = Duration.minutes(5);
console.log(fiveMins.toMillis()); // → 300_000

// Arithmetic + comparison
const total = Duration.hours(1).plus(Duration.minutes(30));
console.log(total.toMillis()); // → 5_400_000
console.log(total.gt(Duration.hours(1))); // → true

// Parsing
console.log(Duration.parse("2h").toMillis()); // → 7_200_000
console.log(Duration.parse("500ms").toMillis()); // → 500

// Coercion: APIs that accept `DurationInput` (number | string | Duration)
console.log(resolveMs(100)); // → 100
console.log(resolveMs("5s")); // → 5000
console.log(resolveMs(Duration.hours(1))); // → 3_600_000
```
<!-- @end -->

| | |
|---|---|
| `Duration.millis(n)` / `seconds` / `minutes` / `hours` / `days` / `weeks` | factories |
| `Duration.parse("5m")` | parse `ms`, `s`, `m`, `h`, `d`, `w` |
| `Duration.from(input)` | coerce `number | string | Duration` |
| `.toMillis()` / `.toSeconds()` / `.toMinutes()` / `.toHours()` / `.toDays()` | convert |
| `.plus(other)` / `.minus(other)` / `.times(n)` | arithmetic |
| `.gt(o)` / `.gte(o)` / `.lt(o)` / `.lte(o)` / `.eq(o)` | comparison |
| `.toString()` | picks largest natural unit (`"5m"`, `"3h"`) |
| `resolveMs(input)` | shortcut to `Duration.from(input).toMillis()` |

**Tip**: in your own APIs, accept `DurationInput` and use `resolveMs`:

```ts
function delay(eff: Eff<A, S>, time: DurationInput): Eff<A, S> {
  return sleep(resolveMs(time)).flatMap(() => eff);
}
delay(myEff, 500);              // ms number
delay(myEff, "5s");             // string
delay(myEff, Duration.minutes(2)); // Duration
```

## CacheStore

Pluggable key-value cache with TTL + LRU eviction. The `CacheStore<K, V>`
interface is `Eff`-typed; in-process implementation ships with
`@perfect/core`. Distributed backends (Redis, memcached) implement the
same interface.

This is the **storage layer** — different from the
`cached` / `cachedBy` *combinators* in `cache.ts`, which provide
closed-over memoization built on a private Map. Use `cached(eff)` for
"memoize this one effect"; use `CacheStore` when you need a pluggable
key-value backend.

### In-memory store

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-memory -->
```ts
import { eff, CacheStore } from "@perfect/core";

// In-process LRU + TTL cache. Pluggable behind the CacheStore interface —
// distributed backends (Redis, memcached) implement the same shape.
const store = CacheStore.memory<string, number>({
  ttlMs: 60_000,
  maxSize: 100,
});

await (
  eff(function* () {
    yield* store.set("hits", 0);
    yield* store.set("hits", 1);
    const v = yield* store.get("hits");
    console.log(v); // → 1

    const present = yield* store.has("hits");
    console.log(present); // → true

    yield* store.delete("hits");
    const after = yield* store.get("hits");
    console.log(after); // → undefined
  }) as any
).run();
```
<!-- @end -->

### TTL — default + per-entry override

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-ttl -->
```ts
import { CacheStore } from "@perfect/core";

// Per-entry TTL overrides the store default.
const ttlStore = CacheStore.memory<string, string>({ ttlMs: 60_000 });

ttlStore.set("short", "expires-fast", 30).runSync(); // overrides default
ttlStore.set("long", "stays-around").runSync(); // uses default 60s

console.log(ttlStore.get("short").runSync()); // → "expires-fast"
await new Promise((r) => setTimeout(r, 40));
// expired
console.log(ttlStore.get("short").runSync()); // → undefined
console.log(ttlStore.get("long").runSync()); // → "stays-around"
```
<!-- @end -->

### LRU eviction

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-lru -->
```ts
import { CacheStore } from "@perfect/core";

// LRU eviction at maxSize.
const lru = CacheStore.memory<string, number>({ maxSize: 3 });
lru.set("a", 1).runSync();
lru.set("b", 2).runSync();
lru.set("c", 3).runSync();
lru.get("a").runSync(); // touches "a" → most recent
lru.set("d", 4).runSync(); // evicts "b" (now LRU), not "a"
console.log(lru.has("a").runSync()); // → true
console.log(lru.has("b").runSync()); // → false
```
<!-- @end -->

### API

| | |
|---|---|
| `CacheStore.memory<K, V>({ ttlMs?, maxSize? })` | in-process LRU + TTL |
| `store.get(k)` | returns `V | undefined` (`undefined` if missing or expired) |
| `store.set(k, v, ttlMs?)` | per-entry TTL overrides default |
| `store.delete(k)` / `store.clear()` | removal |
| `store.has(k)` | presence check |
| `store.size` | current entry count |

### Distributed backends

Implement the `CacheStore<K, V>` interface — methods return `Eff`. A
Redis-backed store would internally use `tryPromise` to bridge the
Redis client:

```ts
class RedisCacheStore<V> implements CacheStore<string, V> {
  constructor(private redis: RedisClient) {}
  get(k) { return tryPromise(() => this.redis.get(k), ...); }
  set(k, v, ttlMs?) { /* ... */ }
  // ... etc
}
```

Then it slots in via Layer wherever a `CacheStore` is needed.

## Next

- [Resilience + Coordination Primitives](./11-resilience-and-coordination.md)
- [Comparison vs other libs](./comparison.md)
