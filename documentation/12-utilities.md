# Utilities

## Duration

Type-safe time arithmetic. Eliminates magic millisecond numbers in your
code. APIs explicitly typed with `DurationInput` accept
`number | string | Duration`. For APIs that accept milliseconds as a number,
pass `.toMillis()` or `resolveMs(...)`.

<!-- @embed packages/core/examples/15-duration-cache.ts#duration-basics -->

```ts
import { Duration, resolveMs } from "@spilne/perfect-core";

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

| API / concept | Behavior |
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
function delayBy<A, S>(eff: Eff<A, S>, time: DurationInput): Eff<A, S> {
  return sleep(resolveMs(time)).flatMap(() => eff);
}
delayBy(myEff, 500);                // ms number
delayBy(myEff, "5s");               // string
delayBy(myEff, Duration.minutes(2)); // Duration
```

## cached and cachedBy

`cached(eff)` runs an effect once and remembers its result. It is the
simplest way to load something expensive (a config, a token) only once.

<!-- @embed packages/core/examples/16-basic-primitives.ts#cached -->

```ts
import { sync, all, cached } from "@spilne/perfect-core";

// cached(eff) runs eff once and remembers the result. Failures are not
// remembered, so a failed run is tried again next time. Callers that ask
// at the same time share one run.
let loads = 0;
const config = cached(
  sync(() => {
    loads++;
    return { region: "eu" };
  }),
  { ttlMs: 60_000 }, // optional: forget the value after a minute
);

await all([config, config, config]).run();
console.log(loads); // → 1

await config.invalidate.run(); // forget it now
await config.run();
console.log(loads); // → 2
```

<!-- @end -->

- Only successes are remembered. If the effect fails, the next run tries
  again.
- `ttlMs` makes the value expire after that many milliseconds.
- `.invalidate` forgets the value now; `.current` reads it without running
  anything (`undefined` when there is none); `.isFresh` says whether a value
  is there and not expired.

`cachedBy(build)` does the same per key:

<!-- @embed packages/core/examples/16-basic-primitives.ts#cached-by -->

```ts
import { sync, cachedBy } from "@spilne/perfect-core";

// cachedBy keeps one cached value per key. maxSize drops the least recently
// used key when the cache is full.
const fetched: string[] = [];
const users = cachedBy(
  (id: string) =>
    sync(() => {
      fetched.push(id);
      return { id, name: `user ${id}` };
    }),
  { ttlMs: 30_000, maxSize: 1_000 },
);

await users.get("a").run();
await users.get("a").run();
await users.get("b").run();
console.log(fetched); // → ["a", "b"]
```

<!-- @end -->

| option | what it does |
| --- | --- |
| `ttlMs` | expiry in ms, or a function `(value) => ms` to pick it per value |
| `maxSize` | keep at most this many keys; the least recently used one is dropped |
| `keyFn` | turn a key into the string used to store it (for object keys) |

The keyed cache has `get(key)`, `invalidate(key)`, `invalidateAll`,
`has(key)` and `size`.

## CacheStore

Pluggable key-value cache with TTL + LRU eviction. The `CacheStore<K, V>`
interface is `Eff`-typed; in-process implementation ships with
`@spilne/perfect-core`. Distributed backends (Redis, memcached) implement the
same interface.

This is the **storage layer** — different from the
`cached` / `cachedBy` *combinators*, which provide
closed-over memoization built on a private Map. Use `cached(eff)` for
"memoize this one effect"; use `CacheStore` when you need a pluggable
key-value backend.

### In-memory store

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-memory -->

```ts
import { eff, CacheStore } from "@spilne/perfect-core";

// In-process LRU + TTL cache. Pluggable behind the CacheStore interface —
// distributed backends (Redis, memcached) implement the same shape.
const store = CacheStore.memory<string, number>({
  ttlMs: 60_000,
  maxSize: 100,
});

await eff(function* () {
  yield* store.set("hits", 0);
  yield* store.set("hits", 1);
  const v = yield* store.get("hits");
  console.log(v); // → 1

  const present = yield* store.has("hits");
  console.log(present); // → true

  yield* store.delete("hits");
  const after = yield* store.get("hits");
  console.log(after); // → undefined
}).run();
```

<!-- @end -->

### TTL — default + per-entry override

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-ttl -->

```ts
import { CacheStore } from "@spilne/perfect-core";

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
import { CacheStore } from "@spilne/perfect-core";

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

| API / concept | Behavior |
|---|---|
| `CacheStore.memory<K, V>({ ttlMs?, maxSize? })` | in-process LRU + TTL |
| `store.get(k)` | returns `V | undefined` (`undefined` if missing or expired) |
| `store.set(k, v, ttlMs?)` | per-entry TTL overrides default |
| `store.delete(k)` / `store.clear()` | removal |
| `store.has(k)` | presence check |
| `store.size` | current entry count |

### Distributed backends

`@spilne/perfect-redis` ships the distributed implementation. It preserves the same
interface while adding `Throws<RedisError>` to each effect:

```ts
import { RedisCacheStore } from "@spilne/perfect-redis";

const store = RedisCacheStore.make<string, User>({
  redis,
  prefix: "users:",
  ttlMs: 60_000,
});
```

The implementation uses prefix-scoped scanning for `clear()` and supports a
custom value codec and key encoder. See
[Distributed backends](./17-distributed-backends.md#redis).

## Next

- [Resilience + Coordination Primitives](./11-resilience-and-coordination.md)
- [Comparison vs other libs](./comparison.md)
