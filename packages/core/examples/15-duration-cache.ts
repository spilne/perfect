// Duration + CacheStore.
//
// Run: bun packages/core/examples/15-duration-cache.ts

import { eff, Duration, resolveMs, CacheStore } from "../src";
import { assertEq } from "./_assert";

// >>> example: duration-basics
// Type-safe time arithmetic — no more raw `5000` magic numbers.
const fiveMins = Duration.minutes(5);
assertEq(fiveMins.toMillis(), 300_000);

// Arithmetic + comparison
const total = Duration.hours(1).plus(Duration.minutes(30));
assertEq(total.toMillis(), 5_400_000);
assertEq(total.gt(Duration.hours(1)), true);

// Parsing
assertEq(Duration.parse("2h").toMillis(), 7_200_000);
assertEq(Duration.parse("500ms").toMillis(), 500);

// Coercion: APIs that accept `DurationInput` (number | string | Duration)
assertEq(resolveMs(100), 100);
assertEq(resolveMs("5s"), 5000);
assertEq(resolveMs(Duration.hours(1)), 3_600_000);
// <<< example

// >>> example: cache-store-memory
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
    assertEq(v, 1);

    const present = yield* store.has("hits");
    assertEq(present, true);

    yield* store.delete("hits");
    const after = yield* store.get("hits");
    assertEq(after, undefined);
  }) as any
).run();
// <<< example

// >>> example: cache-store-ttl
// Per-entry TTL overrides the store default.
const ttlStore = CacheStore.memory<string, string>({ ttlMs: 60_000 });

ttlStore.set("short", "expires-fast", 30).runSync(); // overrides default
ttlStore.set("long", "stays-around").runSync(); // uses default 60s

assertEq(ttlStore.get("short").runSync(), "expires-fast");
await new Promise((r) => setTimeout(r, 40));
assertEq(ttlStore.get("short").runSync(), undefined); // expired
assertEq(ttlStore.get("long").runSync(), "stays-around");
// <<< example

// >>> example: cache-store-lru
// LRU eviction at maxSize.
const lru = CacheStore.memory<string, number>({ maxSize: 3 });
lru.set("a", 1).runSync();
lru.set("b", 2).runSync();
lru.set("c", 3).runSync();
lru.get("a").runSync(); // touches "a" → most recent
lru.set("d", 4).runSync(); // evicts "b" (now LRU), not "a"
assertEq(lru.has("a").runSync(), true);
assertEq(lru.has("b").runSync(), false);
// <<< example
