import { describe, test, expect } from "bun:test";
import { succeed, fail, sync, sleep, run, Cause } from "../src";
import { runUnchecked } from "./run-unchecked";

describe("error combinators", () => {
  test("orDie turns a Fail into a defect", async () => {
    const eff = fail("boom").orDie();
    // defect propagates through run as a thrown value (not the Fail error directly — via squash it's the same)
    await expect(runUnchecked(eff)).rejects.toBe("boom");
  });

  test("mapError transforms the error", async () => {
    const eff = fail("low").mapError((e: string) => `HIGH:${e}`);
    await expect(runUnchecked(eff)).rejects.toBe("HIGH:low");
  });

  test("tapError sees error without consuming it", async () => {
    let seen = null as string | null;
    const eff = fail("oops").tapError((e: string) =>
      sync(() => {
        seen = e;
      }),
    );
    await expect(runUnchecked(eff)).rejects.toBe("oops");
    expect(seen).toBe("oops");
  });

  test("option collapses failure to undefined", async () => {
    expect(await run(fail("x").option())).toBe(undefined);
    expect(await run(succeed(7).option())).toBe(7);
  });

  test("catchSome only catches when handler returns a value", async () => {
    const eff = fail("keep").catchSome((e: string) =>
      e === "other" ? succeed("caught") : undefined,
    );
    await expect(runUnchecked(eff)).rejects.toBe("keep");

    const eff2 = fail("other").catchSome((e: string) =>
      e === "other" ? succeed("caught") : undefined,
    );
    expect(await runUnchecked(eff2)).toBe("caught");
  });

  test("catchAllCause sees the full Cause", async () => {
    let seenCause = null as Cause | null;
    const eff = fail("e").catchAllCause((c: Cause) => {
      seenCause = c;
      return succeed("recovered");
    });
    expect(await run(eff)).toBe("recovered");
    expect(seenCause && Cause.firstFail(seenCause)).toEqual({ value: "e" });
  });

  test("tapBoth fires exactly the matching side", async () => {
    let okRan = 0,
      errRan = 0;
    const success = succeed(1).tapBoth(
      () =>
        sync(() => {
          errRan++;
        }),
      () =>
        sync(() => {
          okRan++;
        }),
    );
    await run(success);
    expect(okRan).toBe(1);
    expect(errRan).toBe(0);

    const failing = fail("x").tapBoth(
      () =>
        sync(() => {
          errRan++;
        }),
      () =>
        sync(() => {
          okRan++;
        }),
    );
    await expect(runUnchecked(failing)).rejects.toBe("x");
    expect(okRan).toBe(1);
    expect(errRan).toBe(1);
  });
});

describe("control flow", () => {
  test("when runs only when cond is true", async () => {
    let ran = 0;
    const side = sync(() => {
      ran++;
      return "done";
    });
    expect(await run(side.when(() => true))).toBe("done");
    expect(ran).toBe(1);
    expect(await run(side.when(() => false))).toBe(undefined);
    expect(ran).toBe(1);
  });

  test("unless is the inverse of when", async () => {
    let ran = 0;
    const side = sync(() => {
      ran++;
    });
    await run(side.unless(() => true));
    expect(ran).toBe(0);
    await run(side.unless(() => false));
    expect(ran).toBe(1);
  });
});

describe("fluent fiber combinators", () => {
  test(".race picks the faster effect", async () => {
    const fast = sleep(5).flatMap(() => succeed("fast"));
    const slow = sleep(50).flatMap(() => succeed("slow"));
    expect(await run(fast.race(slow))).toBe("fast");
  });

  test(".timeoutFail is fluent", async () => {
    const eff = sleep(100)
      .flatMap(() => succeed("done"))
      .timeoutFail(10, () => "nope" as const);
    await expect(runUnchecked(eff)).rejects.toBe("nope");
  });

  test(".delay is fluent", async () => {
    const start = Date.now();
    await run(succeed(1).delay(20));
    expect(Date.now() - start).toBeGreaterThanOrEqual(15);
  });

  test(".uninterruptible on a method call", async () => {
    let ran = 0;
    const work = sleep(20)
      .flatMap(() =>
        sync(() => {
          ran++;
          return 1;
        }),
      )
      .uninterruptible();
    const eff = work.fork().flatMap((f) =>
      sleep(2)
        .flatMap(() =>
          sync(() => {
            f.interrupt();
            return null;
          }),
        )
        // f.await() is a Promise, not an effect: it comes out as the value
        // and run() waits for it.
        .map(() => f.await()),
    );
    await run(eff);
    // ran should be 1 because the uninterruptible body completed before the interrupt could take effect
    expect(ran).toBe(1);
  });
});
