import { describe, expect, test } from "bun:test";
import {
  FiberLocal,
  Log,
  Logger,
  TestLogger,
  all,
  eff,
  fork,
  forkDaemon,
  join,
  provide,
  sleep,
  succeed,
} from "../src";

describe("FiberLocal", () => {
  test("is the initial value outside any region", async () => {
    const RequestId = FiberLocal.make<string | undefined>(undefined);
    expect(await RequestId.get.run()).toBeUndefined();
  });

  test("locally sets it for the region only", async () => {
    const RequestId = FiberLocal.make("none");
    const seen = await eff(function* () {
      const inside = yield* RequestId.locally("r-1", RequestId.get);
      const after = yield* RequestId.get;
      return [inside, after];
    }).run();
    expect(seen).toEqual(["r-1", "none"]);
  });

  test("inner regions win, and locallyWith builds on the outer value", async () => {
    const Path = FiberLocal.make<string[]>([]);
    const read = Path.locally(
      ["api"],
      Path.locallyWith((path) => [...path, "users"], Path.get),
    );
    expect(await read.run()).toEqual(["api", "users"]);
  });

  test("forked fibers inherit it, even after the region ends", async () => {
    const RequestId = FiberLocal.make("none");
    const seen = await eff(function* () {
      const child = yield* RequestId.locally("r-1", fork(RequestId.get));
      const late = yield* RequestId.locally(
        "r-2",
        forkDaemon(sleep(5).flatMap(() => RequestId.get)),
      );
      return [yield* join(child), yield* join(late)];
    }).run();
    expect(seen).toEqual(["r-1", "r-2"]);
  });

  test("requests running at the same time each see their own value", async () => {
    const RequestId = FiberLocal.make("none");
    const handle = (id: string) =>
      RequestId.locally(
        id,
        sleep(id === "a" ? 10 : 1).flatMap(() => RequestId.get),
      );
    expect(await all([handle("a"), handle("b")]).run()).toEqual(["a", "b"]);
  });

  test("two locals don't affect each other", async () => {
    const A = FiberLocal.make(0);
    const B = FiberLocal.make(0);
    expect(await A.locally(1, all([A.get, B.get])).run()).toEqual([1, 0]);
  });

  test("logAs adds the value to every log line in the region", async () => {
    const logger = new TestLogger();
    const RequestId = FiberLocal.make<string | undefined>(undefined, { logAs: "requestId" });
    const program = RequestId.locally("r-1", Log.info("inside")).flatMap(() => Log.info("outside"));
    await provide(program, Logger, logger).run();
    expect(logger.entries.map((e) => [e.message, e.annotations.requestId])).toEqual([
      ["inside", "r-1"],
      ["outside", undefined],
    ]);
  });

  test("works with succeed values of any kind, including undefined", async () => {
    const Flag = FiberLocal.make<boolean | undefined>(true);
    expect(await Flag.locally(undefined, Flag.get).run()).toBeUndefined();
    expect(
      await succeed(1)
        .flatMap(() => Flag.get)
        .run(),
    ).toBe(true);
  });
});
