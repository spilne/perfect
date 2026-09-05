import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  async,
  tryPromise,
  all,
  service,
  provide,
  run,
  runSync,
  type Eff,
  type Throws,
} from "../src";

describe("constructors", () => {
  test("succeed", () => {
    const result = runSync(succeed(42));
    expect(result).toBe(42);
  });

  test("fail", async () => {
    const eff = fail("boom");
    await expect(run(eff)).rejects.toBe("boom");
  });

  test("sync", () => {
    const result = runSync(sync(() => 1 + 2));
    expect(result).toBe(3);
  });

  test("sync catches thrown errors", async () => {
    const eff = sync(() => {
      throw new Error("oops");
    });
    await expect(run(eff)).rejects.toBeInstanceOf(Error);
  });

  test("async", async () => {
    const eff = async<number>((resume) => {
      setTimeout(() => resume(succeed(99)), 1);
    });
    const result = await run(eff);
    expect(result).toBe(99);
  });

  test("tryPromise success", async () => {
    const eff = tryPromise(
      () => Promise.resolve("ok"),
      (e) => `failed: ${e}`,
    );
    expect(await run(eff)).toBe("ok");
  });

  test("tryPromise failure", async () => {
    const eff = tryPromise(
      () => Promise.reject("nope"),
      (e) => `caught: ${e}`,
    );
    await expect(run(eff)).rejects.toBe("caught: nope");
  });
});

describe("combinators", () => {
  test("map", () => {
    const result = runSync(succeed(10).map((x) => x * 2));
    expect(result).toBe(20);
  });

  test("flatMap", () => {
    const result = runSync(succeed(5).flatMap((x) => succeed(x + 1)));
    expect(result).toBe(6);
  });

  test("flatMap chain", () => {
    const result = runSync(
      succeed(1)
        .flatMap((x) => succeed(x + 1))
        .flatMap((x) => succeed(x + 1))
        .flatMap((x) => succeed(x + 1)),
    );
    expect(result).toBe(4);
  });

  test("tap", () => {
    let sideEffect = 0;
    const result = runSync(
      succeed(42).tap((x) =>
        sync(() => {
          sideEffect = x;
        }),
      ),
    );
    expect(result).toBe(42);
    expect(sideEffect).toBe(42);
  });

  test("as", () => {
    expect(runSync(succeed(1).as("hello"))).toBe("hello");
  });
});

describe("error handling", () => {
  test("catch recovers from fail", () => {
    const eff = fail("err").catch((e) => succeed(`recovered: ${e}`));
    expect(runSync(eff)).toBe("recovered: err");
  });

  test("catch does not intercept Die", async () => {
    const eff = sync(() => {
      throw new Error("die");
    }).catch(() => succeed("should not reach"));
    await expect(run(eff)).rejects.toBeInstanceOf(Error);
  });

  test("catchTag narrows error type", () => {
    class NotFound {
      readonly _tag = "NotFound" as const;
    }
    class Forbidden {
      readonly _tag = "Forbidden" as const;
    }

    const eff: Eff<string, Throws<NotFound | Forbidden>> = fail(new NotFound()) as any;

    const handled = eff.catchTag("NotFound", () => succeed("default"));
    // After catching NotFound, only Forbidden remains in the type
    const _check: Eff<string, Throws<Forbidden>> = handled as any;

    expect(runSync(handled)).toBe("default");
  });

  test("catchTag passes through unmatched errors", async () => {
    class NotFound {
      readonly _tag = "NotFound" as const;
    }
    class Forbidden {
      readonly _tag = "Forbidden" as const;
    }

    const eff = (fail(new Forbidden()) as Eff<string, Throws<NotFound | Forbidden>>).catchTag(
      "NotFound",
      () => succeed("nope"),
    );

    await expect(run(eff)).rejects.toEqual(new Forbidden());
  });
});

describe("services", () => {
  test("provide and get", () => {
    interface Logger {
      log(msg: string): void;
    }
    const Logger = service<Logger>()("Logger");

    let logged = "";
    const program = Logger.get.flatMap((logger) =>
      sync(() => {
        logger.log("hello");
        return "done";
      }),
    );

    const result = runSync(
      provide(program, Logger, {
        log: (msg) => {
          logged = msg;
        },
      }),
    );

    expect(result).toBe("done");
    expect(logged).toBe("hello");
  });

  test("missing service fails with Die", async () => {
    interface Db {
      query(): string;
    }
    const Db = service<Db>()("Db");

    const program = Db.get.map((db) => db.query());
    await expect(run(program)).rejects.toBeInstanceOf(Error);
  });
});

describe("all", () => {
  test("empty", () => {
    expect(runSync(all([]))).toEqual([]);
  });

  test("all sync", () => {
    const result = runSync(all([succeed(1), succeed(2), succeed(3)]));
    expect(result).toEqual([1, 2, 3]);
  });

  test("all async", async () => {
    const delay = (ms: number, val: number) =>
      async<number>((resume) => {
        setTimeout(() => resume(succeed(val)), ms);
      });

    const result = await run(all([delay(10, 1), delay(5, 2), delay(1, 3)]));
    expect(result).toEqual([1, 2, 3]);
  });

  test("all fails fast", async () => {
    const eff = all([succeed(1), fail("oops"), succeed(3)]);
    await expect(run(eff)).rejects.toBe("oops");
  });
});

describe("deep chain (stack safety)", () => {
  test("10k flatMaps", () => {
    let eff: Eff<number, never> = succeed(0);
    for (let i = 0; i < 10_000; i++) {
      eff = eff.flatMap((x) => succeed(x + 1));
    }
    expect(runSync(eff)).toBe(10_000);
  });
});
