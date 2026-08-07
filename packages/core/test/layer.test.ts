import { describe, test, expect } from "bun:test";
import { eff, succeed, sync, acquireRelease, service, run, runSync, Layer, type Eff } from "../src";

interface Db {
  query(sql: string): Eff<string, never>;
}
interface Cache {
  get(k: string): string | undefined;
  set(k: string, v: string): void;
}
interface Logger {
  log(msg: string): void;
}

const Db = service<Db>("Db");
const Cache = service<Cache>("Cache");
const Logger = service<Logger>("Logger");

describe("Layer", () => {
  test("simplest: succeed with a record installs services", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`result:${s}`) } as Db });

    const program = eff(function* () {
      const db = yield* Db.get;
      return yield* db.query("SELECT 1");
    });

    expect(runSync(program.with(DbLive))).toBe("result:SELECT 1");
  });

  test("merge combines multiple layers", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });
    const CacheLive = succeed({
      Cache: {
        get: (k: string) => (k === "x" ? "hit" : undefined),
        set: () => {},
      } as Cache,
    });
    const LoggerLive = succeed({
      Logger: { log: () => {} } as Logger,
    });

    const AppLive = Layer.merge(DbLive, CacheLive, LoggerLive);

    const program = eff(function* () {
      const db = yield* Db.get;
      const cache = yield* Cache.get;
      const log = yield* Logger.get;
      log.log("running");
      const cached = cache.get("x");
      if (cached) return cached;
      return yield* db.query("SELECT");
    });

    expect(runSync(program.with(AppLive))).toBe("hit");
  });

  test("layer can depend on another layer via yield*", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });

    // CacheLive depends on DbLive
    const CacheLive = eff(function* () {
      const { Db } = yield* DbLive;
      const result = yield* Db.query("preload");
      return {
        Cache: {
          get: () => result,
          set: () => {},
        } as Cache,
      };
    });

    const program = eff(function* () {
      const cache = yield* Cache.get;
      return cache.get("any");
    });

    expect(runSync(program.with(CacheLive))).toBe("db:preload");
  });

  test("effectful layer (yield* sync) works", () => {
    const DbLive = eff(function* () {
      const seed = yield* sync(() => 42);
      return { Db: { query: (_: string) => succeed(`row:${seed}`) } as Db };
    });

    const program = eff(function* () {
      const db = yield* Db.get;
      return yield* db.query("x");
    });

    expect(runSync(program.with(DbLive))).toBe("row:42");
  });

  test("scoped layer releases on exit", async () => {
    const events: string[] = [];
    const LoggerLive = eff(function* () {
      const logger = yield* acquireRelease(
        sync(() => {
          events.push("acquire");
          return { log: (m: string) => events.push(`log:${m}`) } as Logger;
        }),
        () =>
          sync(() => {
            events.push("release");
          }),
      );
      return { Logger: logger };
    });

    const program = eff(function* () {
      const log = yield* Logger.get;
      log.log("hello");
      return "done";
    });

    const result = await run(program.with(LoggerLive));
    expect(result).toBe("done");
    expect(events).toEqual(["acquire", "log:hello", "release"]);
  });

  test("tests swap layers cleanly", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`real:${s}`) } as Db });
    const DbTest = succeed({ Db: { query: (_: string) => succeed("fake") } as Db });

    const program = eff(function* () {
      const db = yield* Db.get;
      return yield* db.query("x");
    });

    expect(runSync(program.with(DbLive))).toBe("real:x");
    expect(runSync(program.with(DbTest))).toBe("fake");
  });

  test("merged scoped layers release in LIFO order", async () => {
    const events: string[] = [];
    const mkLayer = <K extends string>(name: K) =>
      eff(function* () {
        const v = yield* acquireRelease(
          sync(() => {
            events.push(`acq:${name}`);
            return name;
          }),
          () =>
            sync(() => {
              events.push(`rel:${name}`);
            }),
        );
        return { [name]: v } as Record<K, string>;
      });

    const App = Layer.merge(mkLayer("A"), mkLayer("B"), mkLayer("C"));
    await run(succeed(0).with(App as any));
    expect(events).toEqual(["acq:A", "acq:B", "acq:C", "rel:C", "rel:B", "rel:A"]);
  });

  test("empty merge is identity", () => {
    const program = eff(function* () {
      return yield* succeed(123);
    });
    expect(runSync(program.with(Layer.merge()))).toBe(123);
  });

  // ── Chaining API ─────────────────────────────────────────────────

  test(".and chains layers horizontally", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });
    const CacheLive = succeed({
      Cache: { get: () => "c", set: () => {} } as Cache,
    });
    const LoggerLive = succeed({ Logger: { log: () => {} } as Logger });

    const AppLive = DbLive.and(CacheLive).and(LoggerLive);

    const program = eff(function* () {
      const db = yield* Db.get;
      const cache = yield* Cache.get;
      yield* Logger.get;
      return `${cache.get("x")}+${yield* db.query("q")}`;
    });

    expect(runSync(program.with(AppLive))).toBe("c+db:q");
  });

  test(".with chains — program.with(A).with(B).with(C)", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });
    const CacheLive = succeed({
      Cache: { get: () => "hit", set: () => {} } as Cache,
    });
    const LoggerLive = succeed({ Logger: { log: () => {} } as Logger });

    const program = eff(function* () {
      const db = yield* Db.get;
      const cache = yield* Cache.get;
      yield* Logger.get;
      return cache.get("x") ?? (yield* db.query("x"));
    });

    expect(runSync(program.with(DbLive).with(CacheLive).with(LoggerLive))).toBe("hit");
  });

  test(".provideTo — vertical composition: inner built using outer's services", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });

    // CacheNeedsDb: a layer that depends on Db
    const CacheNeedsDb = eff(function* () {
      const db = yield* Db.get;
      const seed = yield* db.query("preload");
      return { Cache: { get: () => seed, set: () => {} } as Cache };
    });

    // Feed DbLive into CacheNeedsDb — result is a layer producing just Cache
    const CacheWired = DbLive.provideTo(CacheNeedsDb);

    const program = eff(function* () {
      const cache = yield* Cache.get;
      return cache.get("any");
    });

    expect(runSync(program.with(CacheWired))).toBe("db:preload");
  });

  test("chain of all three styles produces the same result", () => {
    const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });
    const CacheLive = succeed({
      Cache: { get: () => undefined, set: () => {} } as Cache,
    });
    const LoggerLive = succeed({ Logger: { log: () => {} } as Logger });

    const prog = eff(function* () {
      const db = yield* Db.get;
      yield* Cache.get;
      yield* Logger.get;
      return yield* db.query("ok");
    });

    const viaMerge = runSync(prog.with(Layer.merge(DbLive, CacheLive, LoggerLive)));
    const viaAnd = runSync(prog.with(DbLive.and(CacheLive).and(LoggerLive)));
    const viaWithChain = runSync(prog.with(DbLive).with(CacheLive).with(LoggerLive));

    expect(viaMerge).toBe("db:ok");
    expect(viaAnd).toBe("db:ok");
    expect(viaWithChain).toBe("db:ok");
  });
});
