import { describe, test, expect } from "bun:test";
import {
  type Eff,
  type Fiber,
  Cause,
  Clock,
  Singleflight,
  Stream,
  TestClock,
  TestTracer,
  Tracer,
  acquireRelease,
  addFiberSupervisor,
  all,
  async,
  die,
  eff,
  ensuring,
  fail,
  failCause,
  forEachPar,
  forkDaemon,
  interrupt,
  interruptible,
  join,
  onExit,
  provide,
  race,
  retry,
  run,
  runFiber,
  scoped,
  sleep,
  succeed,
  sync,
  timeoutOption,
  tryPromise,
  uninterruptible,
  uninterruptibleMask,
  withSpan,
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
const macrotask = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

async function until(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 100 && !condition(); i++) await macrotask();
  expect(condition()).toBe(true);
}

const combinators: Record<string, (effects: Eff<void, never>[]) => Eff<unknown, never>> = {
  all: (effects) => all(effects),
  race: (effects) => race(effects),
};

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

describe("an interrupted fiber does not recover", () => {
  test("a typed finalizer failure after an interrupt is not caught", () => {
    const scheduler = new StepScheduler();
    const fiber = runFiber(
      ensuring(waitForever, fail("close failed")).catch(() => succeed("recovered")),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
  });

  test("a second interrupt during a typed-failing async finalizer is not caught", () => {
    const scheduler = new StepScheduler();
    const release = gate();
    const fiber = runFiber(
      ensuring(
        waitForever,
        release.wait.flatMap(() => fail("close failed")),
      ).catch(() => succeed("recovered")),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();
    fiber.interrupt();
    release.open();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
  });

  test("an interrupt pending when a region fails with an interrupt and a typed error is not caught", () => {
    const scheduler = new StepScheduler();
    const region = gate();
    const fiber = runFiber(
      uninterruptible(
        region.wait.flatMap(() => failCause(Cause.both(Cause.fail("E"), Cause.interrupt()))),
      ).catch(() => succeed("recovered")),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    region.open();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
  });

  test("an interrupt pending while a finalizer's all() fails with an interrupt is not caught", () => {
    const scheduler = new StepScheduler();
    const children: Fiber<any>[] = [];
    const stop = addFiberSupervisor({ onFork: (_parent, child) => void children.push(child) });
    const fiber = runFiber(
      ensuring(fail("E"), all([waitForever, waitForever])).catch(() => succeed("recovered")),
      scheduler,
    );
    scheduler.flush();
    stop();
    fiber.interrupt();
    children[0]!.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
  });

  const deliveries: Record<string, () => { body: Eff<unknown, unknown>; open?: () => void }> = {
    "at a suspension": () => ({ body: waitForever }),
    "when a failing uninterruptible region ends": () => {
      const region = gate();
      return {
        body: uninterruptible(region.wait.flatMap(() => fail("E"))),
        open: region.open,
      };
    },
    "when a succeeding uninterruptible region ends": () => {
      const region = gate();
      return { body: uninterruptible(region.wait), open: region.open };
    },
  };
  const recoveries: Record<string, (body: Eff<unknown, unknown>) => Eff<unknown, unknown>> = {
    exit: (body) => body.exit(),
    catchAllCause: (body) => body.catchAllCause(() => succeed("recovered")),
  };
  for (const [recovery, recover] of Object.entries(recoveries)) {
    for (const [delivery, setup] of Object.entries(deliveries)) {
      test(`${recovery} does not resume a fiber interrupted ${delivery}`, () => {
        const scheduler = new StepScheduler();
        let handled = false;
        let resumed = false;
        const { body, open } = setup();
        const fiber = runFiber(
          recover(body.tapErrorCause(() => sync(() => void (handled = true)))).flatMap(() =>
            sync(() => void (resumed = true)),
          ),
          scheduler,
        );
        scheduler.flush();
        fiber.interrupt();
        open?.();
        scheduler.flush();

        expect({ handled, resumed }).toEqual({ handled: false, resumed: false });
        expect(fiber.result).toEqual(interrupted);
      });
    }
  }

  test("handlers inside an uninterruptible region run, and the interrupt resurfaces at its end", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const fiber = runFiber(
      uninterruptible(
        interruptible(waitForever).catchAllCause((cause) =>
          sync(() => void log.push(`handled ${Cause.pretty(cause)}`)),
        ),
      ).flatMap(() => sync(() => void log.push("after region"))),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(log).toEqual(["handled Interrupt"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("an interruptible region a finalizer enters after an interrupt is interrupted at once", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const inner = gate();
    const fiber = runFiber(
      ensuring(
        waitForever,
        interruptible(inner.wait).flatMap(() => sync(() => void log.push("after wait"))),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(fiber.status).toBe("done");
    expect(log).toEqual([]);
    expect(fiber.interrupted).toBe(true);
  });

  test("retry does not run an interrupted effect again", () => {
    const scheduler = new StepScheduler();
    let attempts = 0;
    const fiber = runFiber(
      retry(
        ensuring(
          sync(() => void attempts++).flatMap(() => waitForever),
          fail("close failed"),
        ),
        { times: 3 },
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(attempts).toBe(1);
    expect(fiber.result).toEqual(interrupted);
  });

  test("a typed failure stays in the cause when no handler was bypassed", () => {
    const scheduler = new StepScheduler();
    const release = gate();
    const fiber = runFiber(
      ensuring(
        succeed(1),
        release.wait.flatMap(() => fail("close failed")),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    release.open();
    scheduler.flush();

    const cause = {
      _tag: "Then",
      left: { _tag: "Fail", error: "close failed" },
      right: { _tag: "Interrupt" },
    };
    expect(fiber.result).toEqual({ ok: false, cause });
    expect(Cause.squash(cause as Cause)).toBe("close failed");
  });
});

describe("cleanup of an interrupted fiber", () => {
  test("onExit runs its handler with the interrupted exit", () => {
    const scheduler = new StepScheduler();
    const exits: unknown[] = [];
    const fiber = runFiber(
      onExit(waitForever, (exit) => sync(() => void exits.push(exit))),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(exits).toEqual([{ _tag: "Failure", cause: { _tag: "Interrupt" } }]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("withSpan ends the span", () => {
    const scheduler = new StepScheduler();
    const tracer = new TestTracer();
    const fiber = runFiber(provide(withSpan(waitForever, "work"), Tracer, tracer), scheduler);
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(tracer.find("work")?.status).toMatchObject({ ok: false, interrupted: true });
  });

  test("singleflight releases the followers and the key", () => {
    const scheduler = new StepScheduler();
    const flights = Singleflight.make();
    const leader = runFiber(flights.do("key", waitForever), scheduler);
    scheduler.flush();
    const follower = runFiber(flights.do("key", succeed("unused")), scheduler);
    scheduler.flush();
    leader.interrupt();
    scheduler.flush();
    const next = runFiber(flights.do("key", succeed("fresh")), scheduler);
    scheduler.flush();

    expect(leader.result).toEqual(interrupted);
    expect(follower.status).toBe("done");
    expect(follower.result?.ok).toBe(false);
    expect(next.result).toEqual({ ok: true, value: "fresh" });
  });

  test("a generator's finally blocks run, and its catch blocks cannot swallow the interrupt", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const fiber = runFiber(
      eff(function* () {
        try {
          yield* waitForever;
        } catch {
          log.push("caught");
        } finally {
          yield* sync(() => void log.push("finally effect"));
          log.push("finally done");
        }
        log.push("after try");
      }),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(log).toEqual(["finally effect", "finally done"]);
    expect(fiber.result).toEqual(interrupted);
  });
});

describe("cleanup registered in the same step as the work it guards", () => {
  test("singleflight clears a key even when an interrupt lands right after registering it", () => {
    let windows = 0;
    for (let length = 0; length <= DEFAULT_BUDGET; length++) {
      const scheduler = new StepScheduler();
      const flights = Singleflight.make();
      let body: Eff<void, never> = succeed(undefined);
      for (let i = 0; i < length; i++) body = body.flatMap(() => succeed(undefined));
      const leader = runFiber(
        body.flatMap(() => flights.do("key", waitForever)),
        scheduler,
      );
      scheduler.step();
      if (leader.status !== "ready" || !(flights as any).flights.has("key")) continue;
      windows++;
      leader.interrupt();
      scheduler.flush();
      const next = runFiber(flights.do("key", succeed("fresh")), scheduler);
      scheduler.flush();

      expect({ length, result: next.result }).toEqual({
        length,
        result: { ok: true, value: "fresh" },
      });
    }
    expect(windows).toBeGreaterThan(0);
  });

  test("a generator's finally runs even when an interrupt lands right after it entered try", () => {
    const center = Math.floor(DEFAULT_BUDGET / 3);
    let windows = 0;
    for (let pad = 0; pad < 6; pad++) {
      for (let length = center - 24; length <= center + 24; length++) {
        const scheduler = new StepScheduler();
        let entered = false;
        let finallyRan = false;
        let body: any = succeed(undefined);
        for (let i = 0; i < length; i++) body = body.flatMap(() => succeed(undefined));
        let padded: any = succeed(undefined);
        for (let i = 0; i < pad; i++) padded = succeed(padded);
        const fiber = runFiber(
          body
            .flatMap(() => padded)
            .flatMap(() =>
              eff(function* () {
                try {
                  entered = true;
                  yield* waitForever;
                } finally {
                  finallyRan = true;
                }
              }),
            ),
          scheduler,
        );
        scheduler.step();
        if (!entered || fiber.status !== "ready") continue;
        windows++;
        fiber.interrupt();
        scheduler.flush();

        expect({ length, pad, finallyRan }).toEqual({ length, pad, finallyRan: true });
      }
    }
    expect(windows).toBeGreaterThan(0);
  });
});

describe("uninterruptibleMask", () => {
  test("restore brings back interruptibility for an ordinary caller", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const fiber = runFiber(
      uninterruptibleMask((restore) =>
        ensuring(
          restore(waitForever),
          sync(() => void log.push("registered release")),
        ),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(log).toEqual(["registered release"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("restore stays uninterruptible when the mask runs inside a finalizer", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const wait = gate();
    const fiber = runFiber(
      ensuring(
        waitForever,
        uninterruptibleMask((restore) =>
          restore(wait.wait).flatMap(() => sync(() => void log.push("waited in cleanup"))),
        ),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();
    expect(fiber.status).toBe("suspended");

    wait.open();
    scheduler.flush();
    expect(log).toEqual(["waited in cleanup"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("a fiber-level scope release behaves like a scoped release of an interrupted fiber", () => {
    const outcomes: string[][] = [];
    for (const withScope of [true, false]) {
      const scheduler = new StepScheduler();
      const log: string[] = [];
      const release = () =>
        interruptible(sync(() => void log.push("interruptible part")))
          .exit()
          .flatMap((exit) => sync(() => void log.push(`release saw ${exit._tag}`)));
      const acquired = acquireRelease(succeed(1), release).flatMap(() => waitForever);
      const fiber = runFiber(withScope ? scoped(acquired) : acquired, scheduler);
      scheduler.flush();
      fiber.interrupt();
      scheduler.flush();
      outcomes.push(log);
    }

    expect(outcomes[0]).toEqual(["release saw Failure"]);
    expect(outcomes[1]).toEqual(outcomes[0]!);
  });
});

describe("interrupt() edge cases", () => {
  test("a fiber whose queued run was dropped by scheduler.shutdown() still completes", () => {
    const scheduler = new StepScheduler();
    let finalized = 0;
    const body = gate();
    const fiber = runFiber(
      ensuring(
        body.wait,
        sync(() => void finalized++),
      ),
      scheduler,
    );
    scheduler.flush();
    body.open();
    scheduler.shutdown();
    fiber.interrupt();
    scheduler.flush();

    expect(finalized).toBe(1);
    expect(fiber.result).toEqual(interrupted);
  });

  test("a defect raised right after a self-interrupt stays in the cause", () => {
    const scheduler = new StepScheduler();
    const error = new Error("bug");
    const fiber: Fiber<void> = runFiber(
      sync(() => {
        fiber.interrupt();
        throw error;
      }),
      scheduler,
    );
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: { _tag: "Then", left: { _tag: "Die", defect: error }, right: { _tag: "Interrupt" } },
    });
  });

  test("a canceler that interrupts its fiber again runs once", () => {
    const scheduler = new StepScheduler();
    let cancels = 0;
    const fiber: Fiber<void> = runFiber(
      ensuring(
        async<void>(() => () => {
          cancels++;
          fiber.interrupt();
        }),
        sync(() => {}),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect(cancels).toBe(1);
    expect(fiber.result).toEqual(interrupted);
  });

  // A throwing canceler is treated like a throwing onDiscard: its error joins
  // the interrupt as a defect, and the interrupt goes ahead.
  const canceled = new Error("canceler blew up");
  const throwingCanceler = async<void>(() => () => {
    throw canceled;
  });

  test("a throwing canceler becomes a defect and the fiber still runs its finalizers", () => {
    const scheduler = new StepScheduler();
    let finalized = 0;
    const fiber = runFiber(
      ensuring(
        throwingCanceler,
        sync(() => void finalized++),
      ),
      scheduler,
    );
    scheduler.flush();

    expect(() => fiber.interrupt()).not.toThrow();
    scheduler.flush();

    expect(finalized).toBe(1);
    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.interrupt(), Cause.die(canceled)),
    });
  });

  test("a throwing canceler with nothing to finalize still completes the fiber", () => {
    const scheduler = new StepScheduler();
    const fiber = runFiber(throwingCanceler, scheduler);
    scheduler.flush();

    expect(() => fiber.interrupt()).not.toThrow();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.interrupt(), Cause.die(canceled)),
    });
  });

  test("a throwing canceler returned by a registration that interrupted its fiber is a defect", () => {
    const scheduler = new StepScheduler();
    const fiber: Fiber<void> = runFiber(
      ensuring(
        async<void>(() => {
          fiber.interrupt();
          return () => {
            throw canceled;
          };
        }),
        sync(() => {}),
      ),
      scheduler,
    );
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.interrupt(), Cause.die(canceled)),
    });
  });

  test.each<[string, (children: Eff<void, never>[]) => Eff<unknown, never>]>([
    ["all", (children) => all(children)],
    ["race", (children) => race(children)],
    ["forEachPar", (children) => forEachPar(children, (child) => child, { concurrency: 2 })],
  ])(
    "%s: a child's throwing canceler does not stop the others from being interrupted",
    (_name, combine) => {
      const scheduler = new StepScheduler();
      const log: string[] = [];
      const parent = runFiber(
        ensuring(
          combine([
            ensuring(
              throwingCanceler,
              sync(() => void log.push("first released")),
            ),
            ensuring(
              waitForever,
              sync(() => void log.push("second released")),
            ),
          ]),
          sync(() => void log.push("parent released")),
        ),
        scheduler,
      );
      scheduler.flush();

      expect(() => parent.interrupt()).not.toThrow();
      scheduler.flush();

      expect(log).toEqual(["first released", "second released", "parent released"]);
      expect(parent.result).toEqual({
        ok: false,
        cause: Cause.both(Cause.interrupt(), Cause.die(canceled)),
      });
    },
  );

  test("race: a loser's throwing canceler fails the race its winner settled", () => {
    const scheduler = new StepScheduler();
    const winner = gate();
    const fiber = runFiber(race([winner.wait.map(() => "won"), throwingCanceler]), scheduler);
    scheduler.flush();

    winner.open();
    expect(() => scheduler.flush()).not.toThrow();

    expect(fiber.result).toEqual({ ok: false, cause: Cause.die(canceled) });
  });

  test("an interrupt during the final scope close leaves a successful fiber not interrupted", () => {
    const scheduler = new StepScheduler();
    const release = gate();
    const fiber = runFiber(
      acquireRelease(succeed(1), () => release.wait).flatMap(() => succeed("value")),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    release.open();
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: true, value: "value" });
    expect(fiber.snapshot().interrupted).toBe(false);
  });
});

describe("interrupting a fiber whose loop is queued or running", () => {
  test("an interrupt during an op-budget pause runs the async finalizer once", () => {
    const scheduler = new StepScheduler();
    const clock = new TestClock();
    const log: string[] = [];
    let body: any = succeed(0);
    for (let i = 0; i < 10_000; i++) body = body.flatMap((x: number) => succeed(x));
    const fiber = runFiber(
      provide(
        ensuring(
          body as Eff<number, never>,
          sync(() => void log.push("release started"))
            .flatMap(() => sleep(10))
            .flatMap(() => sync(() => void log.push("release done"))),
        ),
        Clock,
        clock,
      ),
      scheduler,
    );
    scheduler.step();
    expect(fiber.status).toBe("ready");

    fiber.interrupt();
    scheduler.flush();
    clock.advance(10);
    scheduler.flush();

    expect(log).toEqual(["release started", "release done"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("an interrupt right after an async resume was queued runs the async finalizer", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const body = gate();
    const release = gate();
    const fiber = runFiber(
      ensuring(
        body.wait.flatMap(() => waitForever),
        release.wait.flatMap(() => sync(() => void log.push("release done"))),
      ),
      scheduler,
    );
    scheduler.flush();
    body.open();
    expect(fiber.status).toBe("ready");

    fiber.interrupt();
    scheduler.flush();
    release.open();
    scheduler.flush();

    expect(log).toEqual(["release done"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("every interrupt of a queued fiber reaches supervisors and is delivered once", async () => {
    const scheduler = new StepScheduler();
    let settle!: (value: number) => void;
    let ranAfter = false;
    let finalized = 0;
    const fiber = runFiber(
      ensuring(
        tryPromise(
          () => new Promise<number>((resolve) => (settle = resolve)),
          (e) => e,
        ).flatMap(() => sync(() => void (ranAfter = true))),
        sync(() => void finalized++),
      ),
      scheduler,
    );
    let notified = 0;
    const stop = addFiberSupervisor({
      onInterrupt: (target) => {
        if (target === fiber) notified++;
      },
    });
    scheduler.flush();

    fiber.interrupt();
    settle(1);
    await macrotask();
    fiber.interrupt();
    scheduler.flush();
    stop();

    expect({ notified, ranAfter, finalized }).toEqual({
      notified: 2,
      ranAfter: false,
      finalized: 1,
    });
    expect(fiber.result).toEqual(interrupted);
  });

  test("a fiber interrupting itself still runs the finalizer around it", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const fiber: Fiber<void> = runFiber(
      ensuring(
        sync(() => fiber.interrupt()).flatMap(() => sync(() => void log.push("after interrupt"))),
        sync(() => void log.push("finalizer")),
      ),
      scheduler,
    );
    scheduler.flush();

    expect(log).toEqual(["finalizer"]);
    expect(fiber.result).toEqual(interrupted);
  });
});

describe("callbacks from a wait the fiber has left", () => {
  test("a promise settling during the interrupted fiber's async finalizer does not resume it", async () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    let settle!: (value: number) => void;
    const release = gate();
    const fiber = runFiber(
      ensuring(
        tryPromise(
          () => new Promise<number>((resolve) => (settle = resolve)),
          (e) => e,
        ),
        release.wait.flatMap(() => sync(() => void log.push("release done"))),
      ),
      scheduler,
    );
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    settle(1);
    await macrotask();
    scheduler.flush();
    expect(fiber.status).toBe("suspended");
    expect(log).toEqual([]);

    release.open();
    scheduler.flush();
    expect(log).toEqual(["release done"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("a pull settling during async-iterable cleanup does not end the cleanup early", async () => {
    const events: string[] = [];
    let releaseSecond!: () => void;
    let finishCleanup!: () => void;
    const second = new Promise<void>((resolve) => (releaseSecond = resolve));
    const cleanup = new Promise<void>((resolve) => (finishCleanup = resolve));
    async function* source() {
      try {
        yield 1;
        await second;
        yield 2;
      } finally {
        await cleanup;
        events.push("source cleanup done");
      }
    }
    const fiber = runFiber(
      Stream.fromAsyncIterable(source(), (e) => e)
        .tap((n) => void events.push(`pulled ${n}`))
        .drain(),
    );
    await until(() => events.length === 1 && fiber.status === "suspended");
    fiber.interrupt();
    await until(() => fiber.status === "suspended");

    // The pending next() settles; the generator's return() still waits on cleanup.
    releaseSecond();
    for (let i = 0; i < 5; i++) await macrotask();
    expect(fiber.status).toBe("suspended");

    finishCleanup();
    const exit = await fiber.await();
    expect(events).toEqual(["pulled 1", "source cleanup done"]);
    expect(exit).toEqual({ _tag: "Failure", cause: { _tag: "Interrupt" } });
  });

  for (const [name, combine] of Object.entries(combinators)) {
    test(`${name}: a child finishing during the interrupted parent's async finalizer does not resume it`, () => {
      const scheduler = new StepScheduler();
      const log: string[] = [];
      const children = [gate(), gate()];
      const release = gate();
      const fiber = runFiber(
        ensuring(
          combine(children.map((child) => child.wait)),
          release.wait.flatMap(() => sync(() => void log.push("release done"))),
        ),
        scheduler,
      );
      scheduler.flush();
      fiber.interrupt();
      scheduler.flush();

      for (const child of children) child.open();
      scheduler.flush();
      expect(fiber.status).toBe("suspended");
      expect(log).toEqual([]);

      release.open();
      scheduler.flush();
      expect(log).toEqual(["release done"]);
      expect(fiber.result).toEqual(interrupted);
    });

    test(`${name}: interrupting the waiting parent finalizes and completes it once`, () => {
      const scheduler = new StepScheduler();
      let finalized = 0;
      let ended = 0;
      const fiber = runFiber(
        ensuring(
          combine([waitForever, waitForever]),
          sync(() => void finalized++),
        ),
        scheduler,
      );
      const stop = addFiberSupervisor({
        onEnd: (ending) => {
          if (ending === fiber) ended++;
        },
      });
      scheduler.flush();
      fiber.interrupt();
      scheduler.flush();
      stop();

      expect({ finalized, ended }).toEqual({ finalized: 1, ended: 1 });
      expect(fiber.result).toEqual(interrupted);
    });
  }

  test("an interrupt raised during async registration cancels that registration", () => {
    const scheduler = new StepScheduler();
    let cancelled = 0;
    let resumeLate: (() => void) | undefined;
    let ranAfter = false;
    const fiber: Fiber<void> = runFiber(
      async<void>((resume) => {
        resumeLate = () => resume(succeed(undefined) as any);
        fiber.interrupt();
        return () => {
          cancelled++;
        };
      }).flatMap(() => sync(() => void (ranAfter = true))),
      scheduler,
    );
    scheduler.flush();
    resumeLate?.();
    scheduler.flush();

    expect(cancelled).toBe(1);
    expect(ranAfter).toBe(false);
    expect(fiber.result).toEqual(interrupted);
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
