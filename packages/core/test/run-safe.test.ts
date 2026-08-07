import { describe, test, expect } from "bun:test";
import { succeed, fail, sync, runSafe, runExit, Exit, Cause } from "../src";

describe("runExit", () => {
  test("success → Exit.Success", async () => {
    const exit = await runExit(succeed(42));
    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) expect(exit.value).toBe(42);
  });

  test("typed failure → Exit.Failure carrying Fail cause", async () => {
    const exit = await runExit(fail("boom"));
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const failures = Cause.failures(exit.cause) as string[];
      expect(failures).toEqual(["boom"]);
    }
  });

  test("defect (thrown) → Exit.Failure carrying Die cause", async () => {
    const exit = await runExit(
      sync(() => {
        throw new Error("oops");
      }) as any,
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(Cause.hasDie(exit.cause)).toBe(true);
      expect(Cause.hasFail(exit.cause)).toBe(false);
    }
  });

  test("never throws — multiple failures composed", async () => {
    const exit = await runExit(fail("a") as any);
    expect(
      Exit.match(
        exit,
        () => "impossible",
        (cause) => Cause.squash(cause),
      ),
    ).toBe("a");
  });
});

describe("runSafe", () => {
  test("success → { data, error: null }", async () => {
    const { data, error } = await runSafe(succeed(42));
    expect(data).toBe(42);
    expect(error).toBeNull();
  });

  test("typed failure → { data: null, error: <fail value> }", async () => {
    const { data, error } = await runSafe(fail("boom" as const));
    expect(data).toBeNull();
    expect(error).toBe("boom");
  });

  test("defect throws by default", async () => {
    const eff = sync(() => {
      throw new Error("oops");
    }) as any;
    await expect(runSafe(eff)).rejects.toBeInstanceOf(Error);
  });

  test("catchDefects: true routes defects to error field", async () => {
    const eff = sync(() => {
      throw new Error("oops");
    }) as any;
    const { data, error } = await runSafe(eff, { catchDefects: true });
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(Error);
  });

  test("catchDefects: true — typed errors still go to error (not thrown)", async () => {
    const { data, error } = await runSafe(fail("x" as const), { catchDefects: true });
    expect(data).toBeNull();
    expect(error).toBe("x");
  });
});
