import { describe, expect, test } from "bun:test";
import {
  RequestResolver,
  all,
  eff,
  fail,
  fork,
  interrupt,
  join,
  provide,
  service,
  sleep,
  succeed,
  sync,
} from "../src";

interface User {
  readonly id: string;
  readonly name: string;
}

const users = new Map<string, User>([
  ["a", { id: "a", name: "Ann" }],
  ["b", { id: "b", name: "Bob" }],
  ["c", { id: "c", name: "Cid" }],
]);

// A resolver that records each load's keys.
function userResolver(options: { windowMs?: number; maxBatchSize?: number } = {}) {
  const loads: string[][] = [];
  const resolver = RequestResolver.make({
    ...options,
    load: (ids: readonly string[]) =>
      sync(() => {
        loads.push([...ids]);
        return new Map(ids.flatMap((id) => (users.has(id) ? [[id, users.get(id)!] as const] : [])));
      }),
  });
  return { resolver, loads };
}

describe("RequestResolver", () => {
  test("calls made together share one load, each key once", async () => {
    const { resolver, loads } = userResolver();
    const found = await all([resolver.get("a"), resolver.get("b"), resolver.get("a")]).run();
    expect(found.map((user) => user?.name)).toEqual(["Ann", "Bob", "Ann"]);
    expect(loads).toEqual([["a", "b"]]);
  });

  test("calls made one after another load separately", async () => {
    const { resolver, loads } = userResolver();
    await resolver
      .get("a")
      .flatMap(() => resolver.get("b"))
      .run();
    expect(loads).toEqual([["a"], ["b"]]);
  });

  test("a key the load didn't return gives undefined", async () => {
    const { resolver } = userResolver();
    expect(await all([resolver.get("a"), resolver.get("nobody")]).run()).toEqual([
      users.get("a"),
      undefined,
    ]);
  });

  test("a failed load fails every caller in the batch, with its typed error", async () => {
    const resolver = RequestResolver.make({
      load: (_ids: readonly string[]) => fail("db down" as const),
    });
    const results = await all([resolver.get("a").either(), resolver.get("b").either()]).run();
    expect(results.map((r) => r._tag)).toEqual(["Left", "Left"]);
    expect(results[0]!._tag === "Left" && results[0]!.left).toBe("db down");
  });

  test("maxBatchSize splits a big batch", async () => {
    const { resolver, loads } = userResolver({ maxBatchSize: 2 });
    await all(["a", "b", "c"].map((id) => resolver.get(id))).run();
    expect(loads).toEqual([["a", "b"], ["c"]]);
  });

  test("windowMs also batches calls that arrive a little apart", async () => {
    const { resolver, loads } = userResolver({ windowMs: 30 });
    await eff(function* () {
      const first = yield* fork(resolver.get("a"));
      yield* sleep(5);
      const second = yield* fork(resolver.get("b"));
      return [yield* join(first), yield* join(second)];
    }).run();
    expect(loads).toEqual([["a", "b"]]);
  });

  test("the load can use the services of the caller that started it", async () => {
    const Db = service<{ readonly table: Map<string, User> }>()("Db");
    const resolver = RequestResolver.make({
      load: (ids: readonly string[]) =>
        Db.get.map((db) => new Map(ids.map((id) => [id, db.table.get(id)!] as const))),
    });
    const found = await provide(all([resolver.get("a"), resolver.get("b")]), Db, {
      table: users,
    }).run();
    expect(found.map((user) => user?.name)).toEqual(["Ann", "Bob"]);
  });

  test("interrupting the caller that started the load doesn't strand the others", async () => {
    const { resolver } = userResolver({ windowMs: 20 });
    const found = await eff(function* () {
      const first = yield* fork(resolver.get("a"));
      const second = yield* fork(resolver.get("b"));
      yield* interrupt(first);
      return yield* join(second);
    }).run();
    expect(found?.name).toBe("Bob");
  });

  test("works with keys that are numbers", async () => {
    const resolver = RequestResolver.make({
      load: (ids: readonly number[]) => succeed(new Map(ids.map((id) => [id, id * 2] as const))),
    });
    expect(await all([resolver.get(1), resolver.get(2)]).run()).toEqual([2, 4]);
  });
});
