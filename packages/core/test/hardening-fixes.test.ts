import { describe, test, expect } from "bun:test";
import { succeed, fail, die, sync, sleep, run, runExit, Cause } from "../src";
import { Semaphore } from "../src/semaphore";
import { Queue } from "../src/queue";
import { Stream } from "../src/stream";
import { eff } from "../src";
import { all } from "../src";

describe("Semaphore.withPermits atomicity", () => {
  test("two fibers each wanting all permits do not deadlock", async () => {
    const order: string[] = [];

    const program = Semaphore.make(2).flatMap((sem) =>
      all([
        sem.withPermits(
          2,
          sync(() => order.push("a")).flatMap(() => sleep(10)),
        ),
        sem.withPermits(
          2,
          sync(() => order.push("b")).flatMap(() => sleep(10)),
        ),
      ]),
    );

    await run(program as any);
    expect(order.sort()).toEqual(["a", "b"]);
  });

  test("withPermits releases all permits after failure", async () => {
    const program = Semaphore.make(3).flatMap((sem) =>
      sem
        .withPermits(3, fail("boom"))
        .catch(() => succeed("recovered"))
        .flatMap(() => sem.available),
    );

    expect(await run(program as any)).toBe(3);
  });

  test("large request is not starved by a stream of small ones", async () => {
    const done: string[] = [];

    const program = Semaphore.make(2).flatMap((sem) =>
      all([
        sem.withPermit(sleep(5).map(() => done.push("small1"))),
        sem.withPermits(
          2,
          sync(() => done.push("big")),
        ),
        sem.withPermit(sleep(5).map(() => done.push("small2"))),
      ]),
    );

    await run(program as any);
    expect(done).toContain("big");
  });
});

describe("Stream.fromQueue error propagation", () => {
  test("queue close ends the stream normally", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q
        .offer(1)
        .flatMap(() => q.offer(2))
        .flatMap(() => q.close())
        .flatMap(() => Stream.fromQueue(q).toArray()),
    );

    expect(await run(program as any)).toEqual([1, 2]);
  });

  test("non-close failures propagate instead of silently ending", async () => {
    const failingQueue = {
      take: () => fail("db exploded"),
    };

    const exit = await runExit(Stream.fromQueue(failingQueue as any).toArray() as any);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBe("db exploded");
    }
  });
});

describe("generator Cause fidelity", () => {
  test("uncaught defect stays a defect through the generator", async () => {
    const program = eff(function* () {
      yield* die("kaboom");
      return "unreachable";
    });

    const exit = await runExit(program as any);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasDie(exit.cause)).toBe(true);
      expect(Cause.hasFail(exit.cause)).toBe(false);
    }
  });

  test("defect caught and rethrown in the body stays a defect", async () => {
    const program = eff(function* () {
      // the rethrow IS the test: a caught defect thrown again must stay a defect
      /* oxlint-disable no-useless-catch */
      try {
        yield* die("kaboom");
      } catch (e) {
        throw e;
      }
      /* oxlint-enable no-useless-catch */
      return "unreachable";
    });

    const exit = await runExit(program as any);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasDie(exit.cause)).toBe(true);
    }
  });

  test("defect caught and swallowed lets the body continue", async () => {
    const program = eff(function* () {
      let caught = "";
      try {
        yield* die("kaboom");
      } catch (e) {
        caught = String(e);
      }
      return `caught:${caught}`;
    });

    expect(await run(program as any)).toBe("caught:kaboom");
  });
});

describe("ensuring cause fidelity", () => {
  test("failing body + succeeding finalizer keeps the cause un-duplicated", async () => {
    const { ensuring, sync: syncFn } = await import("../src");
    const program = ensuring(
      fail("boom"),
      syncFn(() => {}),
    );
    const exit = await runExit(program as any);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
    }
  });

  test("stream failure through onFinalize keeps the cause un-duplicated", async () => {
    const exit = await runExit((Stream.fail("boom") as any).onFinalize(sync(() => {})).toArray());
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
    }
  });
});

describe("stream concurrency ops — structured cleanup", () => {
  test("parEvalMap on an infinite source stops working after early termination", async () => {
    let calls = 0;
    const result = await run(
      Stream.iterate(0, (n: number) => n + 1)
        .parEvalMap(
          2,
          (x) =>
            sync(() => {
              calls++;
              return x;
            }) as any,
        )
        .take(3)
        .toArray() as any,
    );
    expect(result).toEqual([0, 1, 2]);

    const after = calls;
    await new Promise((r) => setTimeout(r, 30));
    // driver + workers were interrupted — no further mapper calls
    expect(calls).toBe(after);
  });

  test("merge on infinite sources stops after early termination", async () => {
    let pulls = 0;
    const infinite = Stream.repeat(
      sync(() => {
        pulls++;
        return 1;
      }) as any,
    );
    const result = await run(
      infinite
        .merge(infinite as any)
        .take(4)
        .toArray() as any,
    );
    expect(result.length).toBe(4);

    const after = pulls;
    await new Promise((r) => setTimeout(r, 30));
    expect(pulls).toBe(after);
  });
});

describe("throwing callbacks become defects", () => {
  test("throw inside .map is a Die exit, not an escaped exception", async () => {
    const exit = await runExit(
      succeed(1).map(() => {
        throw new Error("mapper blew up");
      }) as any,
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasDie(exit.cause)).toBe(true);
    }
  });

  test("throw inside .catch handler is a Die exit", async () => {
    const exit = await runExit(
      fail("boom").catch(() => {
        throw new Error("handler blew up");
      }) as any,
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasDie(exit.cause)).toBe(true);
    }
  });
});
