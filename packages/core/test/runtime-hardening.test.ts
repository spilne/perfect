import { describe, test, expect } from "bun:test";
import { async, ensuring, forkDaemon, interrupt, join, run, succeed, sync, yieldNow } from "../src";

describe("interruption hardening", () => {
  test("async waiter unregisters once on interrupt and finalizer runs once", async () => {
    let cancelled = 0;
    let finalized = 0;
    const never = async<void>((_resume) => () => {
      cancelled++;
    });

    const fiber = await run(
      forkDaemon(
        ensuring(
          never,
          sync(() => {
            finalized++;
          }),
        ),
      ),
    );

    while (fiber.status !== "suspended") {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    await run(interrupt(fiber));
    await fiber.await();

    expect(cancelled).toBe(1);
    expect(finalized).toBe(1);
    expect(fiber.interrupted).toBe(true);
  });

  test("interrupt racing async completion settles once", async () => {
    for (let i = 0; i < 50; i++) {
      let resumes = 0;
      const fiber = await run(
        forkDaemon(
          async<number>((resume) => {
            const id = setTimeout(() => {
              resumes++;
              resume(succeed(1) as any);
            }, 0);
            return () => clearTimeout(id);
          }),
        ),
      );

      await run(interrupt(fiber));
      const exit = await fiber.await();

      expect(exit._tag).toBe("Failure");
      expect(resumes).toBeLessThanOrEqual(1);
      expect(fiber.status).toBe("done");
    }
  });
});

describe("scheduler fairness", () => {
  test("deep flatMaps do not starve async continuations", async () => {
    let deep = succeed(undefined);
    for (let i = 0; i < 20_000; i++) {
      deep = deep.flatMap(() => succeed(undefined));
    }

    const asyncFiber = await run(
      forkDaemon(
        async<string>((resume) => {
          setTimeout(() => resume(succeed("ready") as any), 0);
        }),
      ),
    );

    await run(deep.flatMap(() => yieldNow));

    expect(await run(join(asyncFiber))).toBe("ready");
  });

  test("many yielding fibers all make progress", async () => {
    const results = new Set<number>();
    const fibers = Array.from({ length: 64 }, (_, i) =>
      run(
        yieldNow.flatMap(() =>
          sync(() => {
            results.add(i);
          }),
        ),
      ),
    );

    await Promise.all(fibers);
    expect(results.size).toBe(64);
  });
});
