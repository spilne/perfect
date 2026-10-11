import { describe, expect, test } from "bun:test";
import {
  Actor,
  Entities,
  TestRuntime,
  all,
  eff,
  fail,
  scoped,
  sleep,
  succeed,
  sync,
  type Eff,
  type Reply,
  type Throws,
} from "../src";

type CounterMessage =
  | { readonly type: "add"; readonly n: number }
  | { readonly type: "get"; readonly reply: Reply<number> }
  | { readonly type: "boom" }
  | { readonly type: "bug" };

const counter = {
  initial: 0,
  receive: (count: number, message: CounterMessage): Eff<number, Throws<string>> => {
    switch (message.type) {
      case "add":
        return succeed(count + message.n);
      case "get":
        return message.reply(count).map(() => count);
      case "boom":
        return fail("boom");
      case "bug":
        return sync(() => {
          throw new Error("a bug");
        });
    }
  },
};

const get = (reply: Reply<number>): CounterMessage => ({ type: "get", reply });

describe("Actor", () => {
  test("handles messages one at a time, in order, and answers asks", async () => {
    const result = await eff(function* () {
      const actor = yield* Actor.make(counter);
      yield* all([1, 2, 3, 4].map((n) => actor.tell({ type: "add", n })));
      return yield* actor.ask(get);
    })
      .orDie()
      .run();
    expect(result).toBe(10);
  });

  test("many concurrent tells never lose an update", async () => {
    const result = await eff(function* () {
      const actor = yield* Actor.make(counter);
      yield* all(Array.from({ length: 1_000 }, () => actor.tell({ type: "add", n: 1 })));
      return yield* actor.ask(get);
    })
      .orDie()
      .run();
    expect(result).toBe(1_000);
  });

  test("a typed failure keeps the state, fails the ask, and the actor goes on", async () => {
    const result = await eff(function* () {
      const actor = yield* Actor.make(counter);
      yield* actor.tell({ type: "add", n: 5 });
      const failed = yield* actor.ask<number>(() => ({ type: "boom" })).either();
      yield* actor.tell({ type: "add", n: 1 });
      return { failed, count: yield* actor.ask(get) };
    })
      .orDie()
      .run();
    expect(result).toEqual({ failed: { _tag: "Left", left: "boom" }, count: 6 });
  });

  test("a defect stops the actor; its ask and later messages fail with ActorStopped", async () => {
    const result = await eff(function* () {
      const actor = yield* Actor.make(counter);
      const crashed = yield* actor.ask<number>(() => ({ type: "bug" })).either();
      const later = yield* actor.tell({ type: "add", n: 1 }).either();
      return { crashed, later };
    })
      .orDie()
      .run();
    const { crashed, later } = result;
    expect(crashed._tag === "Left" && typeof crashed.left !== "string" && crashed.left._tag).toBe(
      "ActorStopped",
    );
    expect(
      crashed._tag === "Left" && typeof crashed.left !== "string" && crashed.left.message,
    ).toContain("a bug");
    expect(later._tag === "Left" && typeof later.left !== "string" && later.left._tag).toBe(
      "ActorStopped",
    );
  });

  test("it stops when its scope ends, failing asks still waiting", async () => {
    const result = await eff(function* () {
      const actor = yield* scoped(Actor.make(counter));
      return yield* actor.ask(get).either();
    })
      .orDie()
      .run();
    expect(result._tag === "Left" && typeof result.left !== "string" && result.left._tag).toBe(
      "ActorStopped",
    );
  });

  test("a bounded mailbox makes tell wait instead of growing", async () => {
    const t = TestRuntime.make();
    const slow = {
      initial: [] as number[],
      receive: (seen: number[], n: number) => sleep(100).map(() => [...seen, n]),
    };
    const result = await t.runAdvancing(
      eff(function* () {
        const actor = yield* Actor.make(slow, { mailboxSize: 2 });
        yield* all([1, 2, 3, 4, 5].map((n) => actor.tell(n)));
        return t.clock.now();
      }).orDie(),
    );
    // One message is being handled and two wait; the last two tells had to
    // wait for room, until the first two messages were done.
    expect(result).toBe(200);
  });
});

describe("Entities", () => {
  test("one actor per key, made on its first message", async () => {
    const result = await eff(function* () {
      const accounts = yield* Entities.make({ behavior: () => counter });
      yield* accounts.tell("ann", { type: "add", n: 10 });
      yield* accounts.tell("bob", { type: "add", n: 1 });
      yield* accounts.tell("ann", { type: "add", n: 5 });
      return {
        ann: yield* accounts.ask("ann", get),
        bob: yield* accounts.ask("bob", get),
        count: yield* accounts.count,
      };
    })
      .orDie()
      .run();
    expect(result).toEqual({ ann: 15, bob: 1, count: 2 });
  });

  test("an idle entity is stopped, and starts fresh on its next message", async () => {
    const t = TestRuntime.make();
    const result = await t.runAdvancing(
      eff(function* () {
        const accounts = yield* Entities.make({ behavior: () => counter, idleMs: 1_000 });
        yield* accounts.tell("ann", { type: "add", n: 10 });
        const before = yield* accounts.count;
        yield* sleep(5_000);
        const after = yield* accounts.count;
        const fresh = yield* accounts.ask("ann", get);
        return { before, after, fresh };
      }).orDie(),
      { maxMs: 60_000 },
    );
    expect(result).toEqual({ before: 1, after: 0, fresh: 0 });
  });

  test("the entities stop with their scope", async () => {
    const result = await eff(function* () {
      const accounts = yield* scoped(
        Entities.make({ behavior: () => counter }).flatMap((e) =>
          e.tell("ann", { type: "add", n: 1 }).map(() => e),
        ),
      );
      return yield* accounts.count;
    })
      .orDie()
      .run();
    expect(result).toBe(0);
  });
});
