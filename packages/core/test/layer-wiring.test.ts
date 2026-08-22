import { describe, expect, test } from "bun:test";
import {
  Layer,
  LayerCycleError,
  LayerMissingDependencyError,
  eff,
  run,
  service,
  succeed,
  sync,
  type Eff,
} from "../src";

interface Db {
  name: string;
}
interface Cache {
  backedBy: string;
}
interface Search {
  backedBy: string;
}
interface Api {
  summary: string;
}

const Db = service<Db>("Db");
const Cache = service<Cache>("Cache");
const Search = service<Search>("Search");
const Api = service<Api>("Api");

describe("Layer.build — automatic wiring", () => {
  test("sorts layers regardless of the order they are passed in", async () => {
    const built: string[] = [];

    const DbLive = Layer.describe(
      { provides: ["Db"] },
      sync(() => {
        built.push("Db");
        return { Db: { name: "pg" } as Db };
      }) as any,
    );

    const CacheLive = Layer.describe(
      { provides: ["Cache"], requires: ["Db"] },
      eff(function* () {
        const db = yield* Db.get;
        built.push("Cache");
        return { Cache: { backedBy: db.name } as Cache };
      }) as any,
    );

    // Deliberately reversed: Cache needs Db but is listed first.
    const AppLive = Layer.build(CacheLive, DbLive);

    const program = eff(function* () {
      const cache = yield* Cache.get;
      return cache.backedBy;
    });

    expect(await run(program.with(AppLive) as any)).toBe("pg");
    expect(built).toEqual(["Db", "Cache"]);
  });

  test("diamond: a shared dependency is built once and both branches see it", async () => {
    let dbBuilds = 0;

    const DbLive = Layer.describe(
      { provides: ["Db"] },
      sync(() => {
        dbBuilds++;
        return { Db: { name: `pg-${dbBuilds}` } as Db };
      }) as any,
    );

    const CacheLive = Layer.describe(
      { provides: ["Cache"], requires: ["Db"] },
      eff(function* () {
        const db = yield* Db.get;
        return { Cache: { backedBy: db.name } as Cache };
      }) as any,
    );

    const SearchLive = Layer.describe(
      { provides: ["Search"], requires: ["Db"] },
      eff(function* () {
        const db = yield* Db.get;
        return { Search: { backedBy: db.name } as Search };
      }) as any,
    );

    const ApiLive = Layer.describe(
      { provides: ["Api"], requires: ["Cache", "Search"] },
      eff(function* () {
        const cache = yield* Cache.get;
        const search = yield* Search.get;
        return { Api: { summary: `${cache.backedBy}+${search.backedBy}` } as Api };
      }) as any,
    );

    const AppLive = Layer.build(ApiLive, SearchLive, CacheLive, DbLive);

    const program = eff(function* () {
      const api = yield* Api.get;
      return api.summary;
    });

    expect(await run(program.with(AppLive) as any)).toBe("pg-1+pg-1");
    // The shared Db node is visited once, so both branches observe one instance.
    expect(dbBuilds).toBe(1);
  });

  test("detects a cycle and names the path", () => {
    const A = Layer.describe({ provides: ["Db"], requires: ["Cache"] }, succeed({}) as any);
    const B = Layer.describe({ provides: ["Cache"], requires: ["Search"] }, succeed({}) as any);
    const C = Layer.describe({ provides: ["Search"], requires: ["Db"] }, succeed({}) as any);

    expect(() => Layer.build(A, B, C)).toThrow(LayerCycleError);
    try {
      Layer.build(A, B, C);
    } catch (e) {
      expect(e).toBeInstanceOf(LayerCycleError);
      const cycle = (e as LayerCycleError).cycle;
      // Path returns to where it started.
      expect(cycle[0]).toBe(cycle[cycle.length - 1]);
      expect(cycle).toContain("Db");
      expect(cycle).toContain("Cache");
      expect(cycle).toContain("Search");
    }
  });

  test("detects a two-node cycle", () => {
    const A = Layer.describe({ provides: ["Db"], requires: ["Cache"] }, succeed({}) as any);
    const B = Layer.describe({ provides: ["Cache"], requires: ["Db"] }, succeed({}) as any);
    expect(() => Layer.build(A, B)).toThrow(LayerCycleError);
  });

  test("a layer requiring what it also provides is not a cycle", async () => {
    const SelfLive = Layer.describe(
      { provides: ["Db"], requires: ["Db"] },
      succeed({ Db: { name: "self" } as Db }) as any,
    );
    const program = eff(function* () {
      return (yield* Db.get).name;
    });
    expect(await run(program.with(Layer.build(SelfLive)) as any)).toBe("self");
  });

  test("reports a missing dependency with the service and who wanted it", () => {
    const CacheLive = Layer.describe(
      { provides: ["Cache"], requires: ["Db"] },
      succeed({ Cache: {} as Cache }) as any,
    );

    expect(() => Layer.build(CacheLive)).toThrow(LayerMissingDependencyError);
    try {
      Layer.build(CacheLive);
    } catch (e) {
      const err = e as LayerMissingDependencyError;
      expect(err.service).toBe("Db");
      expect(err.requiredBy).toContain("Cache");
      expect(err.message).toContain("Db");
    }
  });

  test("undescribed layers still work — they build first and provide nothing to sort", async () => {
    const Plain: Eff<{ Db: Db }, never> = succeed({ Db: { name: "plain" } as Db });
    const program = eff(function* () {
      return (yield* Db.get).name;
    });
    expect(await run(program.with(Layer.build(Plain)) as any)).toBe("plain");
  });

  test("build with no layers is an empty layer", async () => {
    expect(await run(succeed(1).with(Layer.build()) as any)).toBe(1);
  });
});
