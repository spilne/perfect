import { describe, test, expect } from "bun:test";
import { eff, run, runSync, CacheStore } from "../src";

describe("CacheStore — memory", () => {
  test("set + get", () => {
    const store = CacheStore.memory<string, number>();
    runSync(store.set("a", 1));
    runSync(store.set("b", 2));
    expect(runSync(store.get("a"))).toBe(1);
    expect(runSync(store.get("b"))).toBe(2);
    expect(runSync(store.get("missing"))).toBeUndefined();
  });

  test("has + delete + clear", () => {
    const store = CacheStore.memory<string, string>();
    runSync(store.set("k", "v"));
    expect(runSync(store.has("k"))).toBe(true);
    expect(runSync(store.has("missing"))).toBe(false);
    runSync(store.delete("k"));
    expect(runSync(store.has("k"))).toBe(false);
    runSync(store.set("a", "1"));
    runSync(store.set("b", "2"));
    expect(runSync(store.size)).toBe(2);
    runSync(store.clear());
    expect(runSync(store.size)).toBe(0);
  });

  test("default ttlMs expires entries", async () => {
    const store = CacheStore.memory<string, number>({ ttlMs: 30 });
    runSync(store.set("k", 99));
    expect(runSync(store.get("k"))).toBe(99);
    await new Promise((r) => setTimeout(r, 40));
    expect(runSync(store.get("k"))).toBeUndefined();
    expect(runSync(store.size)).toBe(0); // expired entry purged on get
  });

  test("per-entry ttlMs overrides default", async () => {
    const store = CacheStore.memory<string, number>({ ttlMs: 10_000 });
    runSync(store.set("short", 1, 30)); // expires fast
    runSync(store.set("long", 2));      // uses default 10s
    expect(runSync(store.get("short"))).toBe(1);
    expect(runSync(store.get("long"))).toBe(2);
    await new Promise((r) => setTimeout(r, 40));
    expect(runSync(store.get("short"))).toBeUndefined();
    expect(runSync(store.get("long"))).toBe(2);
  });

  test("LRU eviction at maxSize", () => {
    const store = CacheStore.memory<string, number>({ maxSize: 3 });
    runSync(store.set("a", 1));
    runSync(store.set("b", 2));
    runSync(store.set("c", 3));
    expect(runSync(store.size)).toBe(3);
    runSync(store.set("d", 4)); // evicts "a"
    expect(runSync(store.has("a"))).toBe(false);
    expect(runSync(store.has("b"))).toBe(true);
    expect(runSync(store.has("c"))).toBe(true);
    expect(runSync(store.has("d"))).toBe(true);
  });

  test("get touches recency for LRU", () => {
    const store = CacheStore.memory<string, number>({ maxSize: 3 });
    runSync(store.set("a", 1));
    runSync(store.set("b", 2));
    runSync(store.set("c", 3));
    runSync(store.get("a")); // moves "a" to most recent
    runSync(store.set("d", 4)); // evicts "b" (now LRU), not "a"
    expect(runSync(store.has("a"))).toBe(true);
    expect(runSync(store.has("b"))).toBe(false);
  });

  test("composes inside eff(function*)", async () => {
    const store = CacheStore.memory<string, string>();
    const program = eff(function* () {
      yield* store.set("greeting", "hi");
      const v = yield* store.get("greeting");
      return v ?? "(missing)";
    });
    expect(await run(program as any)).toBe("hi");
  });
});
