import { describe, test, expect } from "bun:test";
import { Cause, fail, runExit, validate } from "../src";

describe("Cause semantics", () => {
  test("Then represents sequential failure order", () => {
    const cause = Cause.then(Cause.fail("body"), Cause.die("release"));

    expect(Cause.pretty(cause)).toBe("(Fail(body) ; Die(release))");
    expect(Cause.failures(cause)).toEqual(["body"]);
    expect(Cause.defects(cause)).toEqual(["release"]);
  });

  test("Both represents parallel/accumulated failures", async () => {
    const exit = await runExit(validate([fail("a"), fail("b")]));

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.pretty(exit.cause)).toBe("(Fail(a) & Fail(b))");
      expect(Cause.failures(exit.cause)).toEqual(["a", "b"]);
    }
  });

  test("squash precedence is Fail, then Die, then Interrupt", () => {
    expect(Cause.squash(Cause.then(Cause.interrupt(), Cause.fail("typed")))).toBe("typed");
    expect(Cause.squash(Cause.then(Cause.interrupt(), Cause.die("defect")))).toBe("defect");
    expect(Cause.squash(Cause.interrupt())).toBeInstanceOf(Error);
  });
});
