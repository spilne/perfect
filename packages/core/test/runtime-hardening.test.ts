import { describe, test, expect } from "bun:test";
import {
  type Eff,
  Clock,
  TestClock,
  acquireRelease,
  all,
  async,
  die,
  ensuring,
  fail,
  forkDaemon,
  interrupt,
  join,
  provide,
  run,
  runFiber,
  scoped,
  sleep,
  succeed,
  sync,
  timeoutOption,
  uninterruptible,
  yieldNow,
} from "../src";
import { DEFAULT_BUDGET, type Scheduler } from "../src/scheduler";

// Runs queued loop slices one at a time so a test can act between them.
class StepScheduler implements Scheduler {
  private readonly queue: Array<() => void> = [];

  schedule(task: () => void): void {
    this.queue.push(task);
  }

  step(): void {
    this.queue.shift()?.();
  }

  flush(): void {
    while (this.queue.length > 0) this.queue.shift()!();
  }

  shutdown(): void {
    this.queue.length = 0;
  }
}

function gate(): { wait: Eff<void, never>; open: () => void } {
  let resume: ((value: Eff<void, never>) => void) | undefined;
  return {
    wait: async<void>((r) => {
      resume = r as typeof resume;
    }),
    open: () => resume?.(succeed(undefined)),
  };
}

const waitForever = async<void>(() => () => {});
const interrupted = { ok: false, cause: { _tag: "Interrupt" } };

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

  test("repeated interrupt before delivery still runs async finalizers once", async () => {
    let finalized = 0;
    const never = async<void>(() => () => {});
    const asyncCleanup = async<void>((resume) => {
      queueMicrotask(() => resume(succeed(undefined) as any));
    }).flatMap(() =>
      sync(() => {
        finalized++;
      }),
    );

    const fiber = await run(forkDaemon(ensuring(never, asyncCleanup)));
    while (fiber.status !== "suspended") {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    fiber.interrupt();
    fiber.interrupt();
    const exit = await fiber.await();

    expect(exit).toEqual({ _tag: "Failure", cause: { _tag: "Interrupt" } });
    expect(finalized).toBe(1);
  });
});

describe("pending interrupts at finalizer boundaries", () => {
  test("an interrupt during an async finalizer does not skip the outer finalizer", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const inner = gate();
    const fiber = runFiber(
      ensuring(
        ensuring(
          waitForever,
          inner.wait.flatMap(() => sync(() => void log.push("inner"))),
        ),
        sync(() => void log.push("outer")),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    fiber.interrupt();
    inner.open();
    scheduler.flush();

    expect(log).toEqual(["inner", "outer"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("all() interrupting a sibling mid-release keeps that sibling's outer finalizer", () => {
    const scheduler = new StepScheduler();
    const clock = new TestClock();
    const log: string[] = [];
    const releases = [gate(), gate()];
    const child = (i: number) =>
      ensuring(
        scoped(
          acquireRelease(succeed(`conn${i}`), (conn) =>
            releases[i]!.wait.flatMap(() => sync(() => void log.push(`release ${conn}`))),
          ).flatMap(() => sleep(60_000)),
        ),
        sync(() => void log.push(`outer${i}`)),
      );
    runFiber(provide(timeoutOption(all([child(0), child(1)]), 1_000), Clock, clock), scheduler);
    scheduler.flush();
    clock.advance(1_000);
    scheduler.flush();

    // The first child's exit makes all() interrupt the second one again while
    // it is still releasing.
    releases[0]!.open();
    scheduler.flush();
    releases[1]!.open();
    scheduler.flush();

    expect(log).toEqual(["release conn0", "outer0", "release conn1", "outer1"]);
  });

  test("a failure leaving an uninterruptible region with an interrupt pending is not caught", () => {
    const cases = [
      { failure: fail("boom"), cause: { _tag: "Interrupt" } },
      {
        failure: die("bug"),
        cause: { _tag: "Then", left: { _tag: "Die", defect: "bug" }, right: { _tag: "Interrupt" } },
      },
    ];
    for (const { failure, cause } of cases) {
      const scheduler = new StepScheduler();
      const log: string[] = [];
      const region = gate();
      const fiber = runFiber(
        ensuring(
          uninterruptible(region.wait.flatMap(() => failure)),
          sync(() => void log.push("outer")),
        ).catch(() => succeed("recovered")),
        scheduler,
      );
      scheduler.flush();
      fiber.interrupt();
      region.open();
      scheduler.flush();

      expect(log).toEqual(["outer"]);
      expect(fiber.result).toEqual({ ok: false, cause });
    }
  });

  test("an op-budget pause just before a finalizer starts cannot drop it", () => {
    // Each flatMap costs three loop steps and each extra succeed() wrapper one,
    // so this sweep puts the pause on every step around the finalizer's start.
    const center = Math.floor(DEFAULT_BUDGET / 3);
    for (let length = center - 16; length <= center + 16; length++) {
      for (let pad = 0; pad < 3; pad++) {
        const scheduler = new StepScheduler();
        let finalized = 0;
        let body: any = 0;
        for (let i = 0; i <= pad; i++) body = succeed(body);
        for (let i = 0; i < length; i++) body = body.flatMap((x: number) => succeed(x));
        const fiber = runFiber(
          ensuring(
            body as Eff<number, never>,
            sync(() => void finalized++),
          ),
          scheduler,
        );
        scheduler.step();
        if (fiber.status === "ready") fiber.interrupt();
        scheduler.flush();

        expect({ length, pad, finalized }).toEqual({ length, pad, finalized: 1 });
      }
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
