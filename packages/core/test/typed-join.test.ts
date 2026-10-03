import { describe, expect, test } from "bun:test";
import { Cause, die, fail, fork, forkDaemon, join, run, runExit, sleep, succeed } from "../src";

describe("join re-raises a child's failure as it was", () => {
  test("a typed error stays a typed error", async () => {
    const program = fork(fail("db down" as const)).flatMap(join);
    const caught = await run(program.catch((e) => succeed(`caught ${e}`)));
    expect(caught).toBe("caught db down");
  });

  test("a defect stays a defect, so .catch doesn't see it", async () => {
    const program = fork(die(new Error("bug"))).flatMap(join);
    const exit = await runExit(program.catch(() => succeed("caught")));
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstDie(exit.cause)?.value).toEqual(new Error("bug"));
      expect(Cause.firstFail(exit.cause)).toBeNull();
    }
  });

  test("an interrupted child interrupts the joiner", async () => {
    const program = forkDaemon(sleep(1_000)).flatMap((fiber) => {
      fiber.interrupt();
      return join(fiber);
    });
    const exit = await runExit(program);
    expect(exit._tag === "Failure" && Cause.isInterruptedOnly(exit.cause)).toBe(true);
  });

  test("a successful child's value comes through", async () => {
    expect(await run(fork(succeed(7)).flatMap(join))).toBe(7);
  });
});
