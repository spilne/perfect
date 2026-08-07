import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  run,
  runSync,
  Cause,
  TaggedError,
  type Eff,
  type Throws,
} from "../src";

type AppErr =
  | { _tag: "NotFound"; id: number }
  | { _tag: "Forbidden"; reason: string }
  | { _tag: "Network"; status: number };

describe(".catchTags — bulk handler", () => {
  test("dispatches by _tag", () => {
    const program = (e: AppErr): Eff<string, never> =>
      (fail(e) as Eff<never, Throws<AppErr>>).catchTags({
        NotFound: (e) => succeed(`missing ${e.id}`),
        Forbidden: (e) => succeed(`forbidden: ${e.reason}`),
        Network: (e) => succeed(`net ${e.status}`),
      }) as Eff<string, never>;

    expect(runSync(program({ _tag: "NotFound", id: 7 }))).toBe("missing 7");
    expect(runSync(program({ _tag: "Forbidden", reason: "no" }))).toBe("forbidden: no");
    expect(runSync(program({ _tag: "Network", status: 500 }))).toBe("net 500");
  });

  test("unhandled tags propagate", async () => {
    const partial = (
      fail({ _tag: "Forbidden", reason: "x" } as AppErr) as Eff<never, Throws<AppErr>>
    ).catchTags({
      NotFound: (e) => succeed(`m${e.id}`),
    });
    await expect(run(partial as any)).rejects.toMatchObject({ _tag: "Forbidden" });
  });
});

describe(".matchTag — switch-style", () => {
  test("calls onMatch for matching tag, onElse otherwise", () => {
    const handle = (e: AppErr): Eff<string, never> =>
      (fail(e) as Eff<never, Throws<AppErr>>).matchTag(
        "NotFound",
        (e) => succeed(`hit:${e.id}`),
        (e) => succeed(`other:${e._tag}`),
      ) as any;

    expect(runSync(handle({ _tag: "NotFound", id: 1 }))).toBe("hit:1");
    expect(runSync(handle({ _tag: "Forbidden", reason: "no" }))).toBe("other:Forbidden");
  });
});

describe(".exit", () => {
  test("Success on success", async () => {
    const exit = await run(succeed(42).exit());
    expect(exit).toEqual({ _tag: "Success", value: 42 });
  });

  test("Failure with Cause on typed failure", async () => {
    const exit = await run((fail("boom") as any).exit());
    expect((exit as any)._tag).toBe("Failure");
    expect((exit as any).cause._tag).toBe("Fail");
    expect((exit as any).cause.error).toBe("boom");
  });

  test("Failure on defect", async () => {
    const blowUp = sync(() => {
      throw new Error("oops");
    }) as any;
    const exit = await run(blowUp.exit());
    expect((exit as any)._tag).toBe("Failure");
    expect((exit as any).cause._tag).toBe("Die");
  });
});

describe(".tapDefect", () => {
  test("observes defects only", async () => {
    let observed: unknown = null;
    const program = (
      sync(() => {
        throw new Error("bug");
      }) as any
    )
      .tapDefect((d: unknown) =>
        sync(() => {
          observed = d;
        }),
      )
      .catchAllCause(() => succeed("caught"));
    expect(await run(program as any)).toBe("caught");
    expect((observed as Error).message).toBe("bug");
  });

  test("doesn't fire for typed failures", async () => {
    let observed: unknown = null;
    const program = (fail("typed") as any)
      .tapDefect((d: unknown) =>
        sync(() => {
          observed = d;
        }),
      )
      .catch(() => succeed("ok"));
    expect(await run(program as any)).toBe("ok");
    expect(observed).toBeNull();
  });
});

describe(".mapErrorCause", () => {
  test("transforms the entire Cause tree", async () => {
    let mapped = false;
    const program = (fail("orig") as any).mapErrorCause((c: any) => {
      mapped = true;
      // wrap in a Then with a die
      return Cause.then(c, Cause.die("wrapped"));
    });
    const exit = await run(program.exit());
    expect(mapped).toBe(true);
    expect((exit as any)._tag).toBe("Failure");
    expect((exit as any).cause._tag).toBe("Then");
  });
});

describe("Cause.pretty / prettyMultiline", () => {
  test("pretty: compact one-liner", () => {
    expect(Cause.pretty(Cause.fail("boom"))).toBe("Fail(boom)");
    expect(Cause.pretty(Cause.die("oops"))).toBe("Die(oops)");
    expect(Cause.pretty(Cause.interrupt())).toBe("Interrupt");
    expect(Cause.pretty(Cause.both(Cause.fail("a"), Cause.fail("b")))).toBe("(Fail(a) & Fail(b))");
    expect(Cause.pretty(Cause.then(Cause.fail("x"), Cause.die("y")))).toBe("(Fail(x) ; Die(y))");
  });

  test("pretty: formats objects/Errors readably", () => {
    expect(Cause.pretty(Cause.fail({ _tag: "NotFound", id: 7 }))).toBe(
      `Fail(${JSON.stringify({ _tag: "NotFound", id: 7 })})`,
    );
    expect(Cause.pretty(Cause.die(new TypeError("nope")))).toBe("Die(TypeError: nope)");
  });

  test("prettyMultiline: structured indentation", () => {
    const c = Cause.both(Cause.fail("a"), Cause.then(Cause.fail("b"), Cause.fail("c")));
    const out = Cause.prettyMultiline(c);
    expect(out).toContain("Both:");
    expect(out).toContain("├──");
    expect(out).toContain("└──");
    expect(out).toContain("Fail: a");
    expect(out).toContain("Fail: b");
    expect(out).toContain("Fail: c");
  });

  test("prettyMultiline: includes Error stack for Die", () => {
    const e = new Error("boom");
    const out = Cause.prettyMultiline(Cause.die(e));
    expect(out).toContain("Die: Error: boom");
    expect(out).toContain("at "); // some stack frame
  });
});

describe("TaggedError class helper", () => {
  class NotFound extends TaggedError("NotFound")<{ id: number }>() {}
  class Forbidden extends TaggedError("Forbidden")<{ reason: string }>() {}

  test("instances have _tag, props, and Error semantics", () => {
    const e = new NotFound({ id: 42 });
    expect(e._tag).toBe("NotFound");
    expect(e.id).toBe(42);
    expect(e instanceof Error).toBe(true);
    expect(e instanceof NotFound).toBe(true);
    expect(e.name).toBe("NotFound");
    expect(e.message).toContain("NotFound");
    expect(e.stack).toBeDefined();
  });

  test("works with .catchTag narrowing", () => {
    const result = runSync(
      (fail(new NotFound({ id: 7 })) as any).catchTag("NotFound", (e: NotFound) =>
        succeed(`missing ${e.id}`),
      ),
    );
    expect(result).toBe("missing 7");
  });

  test("works with .catchTags bulk handler", () => {
    type Err = NotFound | Forbidden;
    const program = (e: Err): Eff<string, never> =>
      (fail(e) as Eff<never, Throws<Err>>).catchTags({
        NotFound: (e) => succeed(`m${e.id}`),
        Forbidden: (e) => succeed(`f:${e.reason}`),
      }) as any;

    expect(runSync(program(new NotFound({ id: 1 })))).toBe("m1");
    expect(runSync(program(new Forbidden({ reason: "no" })))).toBe("f:no");
  });

  test("static _tag is accessible", () => {
    expect(NotFound._tag).toBe("NotFound");
  });
});
