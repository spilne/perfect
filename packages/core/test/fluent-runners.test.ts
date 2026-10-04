import { describe, test, expect } from "bun:test";
import { succeed, fail, sleep, join, type Eff, type Fiber, type Throws } from "../src";

describe("fluent runners", () => {
  test(".runSync executes a synchronous program", () => {
    const program = succeed(21).flatMap((a) => succeed(a * 2));
    expect(program.runSync()).toBe(42);
  });

  test(".run returns a Promise", async () => {
    const program = sleep(1).flatMap(() => succeed("done"));
    expect(await program.run()).toBe("done");
  });

  test(".run rejects with squashed cause on failure", async () => {
    const program = fail("nope") as Eff<never, Throws<string>>;
    // .run() refuses unhandled errors at the type level; this test leaves the
    // error unhandled on purpose to check the rejection.
    await expect((program as Eff<never>).run()).rejects.toBe("nope");
  });

  test(".runExit returns Success on success", async () => {
    const result = await succeed(7).runExit();
    expect(result).toEqual({ _tag: "Success", value: 7 });
  });

  test(".runExit returns Failure on typed failure", async () => {
    const program = fail("boom") as Eff<never, Throws<string>>;
    const result = await program.runExit();
    expect(result._tag).toBe("Failure");
  });

  test(".runFiber returns a Fiber that can be joined", async () => {
    const program = sleep(1).flatMap(() => succeed(99));
    // The fluent .runFiber() is typed Fiber<A>, losing the "no typed errors"
    // that runFiber(eff) keeps, so join() would think it can fail.
    const fiber = program.runFiber() as Fiber<number, never>;
    const value = await join(fiber).run();
    expect(value).toBe(99);
  });

  test("composes naturally at the tail of a chain", async () => {
    const value = await succeed(10)
      .map((x) => x + 5)
      .flatMap((x) => succeed(x * 2))
      .run();
    expect(value).toBe(30);
  });
});
