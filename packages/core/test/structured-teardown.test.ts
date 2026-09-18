import { describe, expect, test } from "bun:test";
import {
  type Eff,
  type Fiber,
  Cause,
  Clock,
  TestClock,
  acquireRelease,
  all,
  async,
  die,
  ensuring,
  fail,
  provide,
  race,
  raceAll,
  raceEither,
  runFiber,
  scoped,
  succeed,
  sync,
  timeoutFail,
  timeoutOption,
  uninterruptible,
  validate,
} from "../src";
import type { Scheduler } from "../src/scheduler";

// Runs queued loop slices one at a time so a test can act between them.
class StepScheduler implements Scheduler {
  readonly queue: Array<() => void> = [];

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
const interruptCause = { _tag: "Interrupt" } as const;

// A child that waits forever and, once interrupted, runs a release that waits
// on a gate before logging.
function slowRelease(log: string[], name: string) {
  const release = gate();
  const effect = ensuring(
    waitForever,
    sync(() => void log.push(`${name} release started`))
      .flatMap(() => release.wait)
      .flatMap(() => sync(() => void log.push(`${name} release done`))),
  );
  return { effect, open: release.open };
}

describe("all() waits for its children", () => {
  test("an interrupt runs the parent's finalizer only after the children's async releases", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const a = slowRelease(log, "a");
    const b = slowRelease(log, "b");
    const fiber = runFiber(
      ensuring(
        all([a.effect, b.effect]),
        sync(() => void log.push("parent finalizer")),
      ),
      scheduler,
    );
    scheduler.flush();

    fiber.interrupt();
    scheduler.flush();
    expect(log).toEqual(["a release started", "b release started"]);
    expect(fiber.status).toBe("suspended");

    a.open();
    scheduler.flush();
    expect(log).not.toContain("parent finalizer");
    b.open();
    scheduler.flush();

    expect(log).toEqual([
      "a release started",
      "b release started",
      "a release done",
      "b release done",
      "parent finalizer",
    ]);
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });

  test("interrupting the parent interrupts the children at once", () => {
    const scheduler = new StepScheduler();
    const children: Fiber<any>[] = [];
    const fiber = runFiber(
      all([waitForever, waitForever]).map((values) => values.length),
      scheduler,
    );
    scheduler.flush();
    children.push(...fiber.childrenSnapshot());
    expect(children.length).toBe(2);

    fiber.interrupt();

    expect(children.every((child) => child.interrupted)).toBe(true);
    scheduler.flush();
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });

  test("a child failure reaches the parent only after its siblings' releases finish", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const sibling = slowRelease(log, "sibling");
    const failNow = gate();
    const fiber = runFiber(
      ensuring(
        all([failNow.wait.flatMap(() => fail("boom")), sibling.effect]),
        sync(() => void log.push("parent finalizer")),
      ),
      scheduler,
    );
    scheduler.flush();

    failNow.open();
    scheduler.flush();
    expect(log).toEqual(["sibling release started"]);
    expect(fiber.status).toBe("suspended");

    sibling.open();
    scheduler.flush();

    expect(log).toEqual(["sibling release started", "sibling release done", "parent finalizer"]);
    expect(fiber.result).toEqual({ ok: false, cause: Cause.fail("boom") });
  });

  test("a sibling's teardown failure joins the child failure with Cause.both", () => {
    const scheduler = new StepScheduler();
    const failNow = gate();
    const fiber = runFiber(
      all([failNow.wait.flatMap(() => fail("boom")), ensuring(waitForever, die("release"))]),
      scheduler,
    );
    scheduler.flush();

    failNow.open();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.both(Cause.fail("boom"), Cause.die("release")),
    });
  });

  test("an interrupt joins the children's teardown failures with Cause.both", () => {
    const scheduler = new StepScheduler();
    const fiber = runFiber(all([ensuring(waitForever, die("release")), waitForever]), scheduler);
    scheduler.flush();

    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.both(Cause.interrupt(), Cause.die("release")),
    });
  });

  test("an interrupt while a failure waits for siblings keeps that failure", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const sibling = slowRelease(log, "sibling");
    const failNow = gate();
    const fiber = runFiber(
      all([failNow.wait.flatMap(() => fail("boom")), sibling.effect]),
      scheduler,
    );
    scheduler.flush();
    failNow.open();
    scheduler.flush();

    fiber.interrupt();
    scheduler.flush();
    expect(fiber.status).toBe("suspended");
    sibling.open();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.both(Cause.interrupt(), Cause.fail("boom")),
    });
  });

  test("an interrupted fiber does not recover through a handler, but still waits", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const child = slowRelease(log, "child");
    const fiber = runFiber(
      all([child.effect])
        .catchAllCause(() => succeed("recovered"))
        .flatMap(() => sync(() => void log.push("continued"))),
      scheduler,
    );
    scheduler.flush();

    fiber.interrupt();
    scheduler.flush();
    expect(fiber.status).toBe("suspended");
    child.open();
    scheduler.flush();

    expect(log).toEqual(["child release started", "child release done"]);
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });

  test("an uninterruptible parent lets its children finish and raises the interrupt after", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const first = gate();
    const second = gate();
    const fiber = runFiber(
      uninterruptible(
        all([first.wait.map(() => 1), second.wait.map(() => 2)]).map((values) => {
          log.push(`got ${values.join(",")}`);
        }),
      ).flatMap(() => sync(() => void log.push("after region"))),
      scheduler,
    );
    scheduler.flush();

    fiber.interrupt();
    first.open();
    second.open();
    scheduler.flush();

    expect(log).toEqual(["got 1,2"]);
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });

  test("an interrupt that lands after the results were delivered fails with the interrupt", () => {
    const scheduler = new StepScheduler();
    const first = gate();
    let got = false;
    const fiber = runFiber(
      all([first.wait]).map(() => {
        got = true;
      }),
      scheduler,
    );
    scheduler.flush();

    first.open();
    scheduler.step();
    expect(fiber.status).toBe("ready");
    fiber.interrupt();
    scheduler.flush();

    expect(got).toBe(false);
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });

  test("scoped resources of the children are released before the parent's scope", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const childRelease = gate();
    const resource = (name: string, wait: Eff<void, never>) =>
      acquireRelease(
        sync(() => name),
        () =>
          wait.flatMap(() =>
            sync(() => {
              log.push(`${name} released`);
            }),
          ),
      );
    const fiber = runFiber(
      scoped(
        resource("parent", succeed(undefined)).flatMap(() =>
          all([scoped(resource("child", childRelease.wait).flatMap(() => waitForever))]),
        ),
      ),
      scheduler,
    );
    scheduler.flush();

    fiber.interrupt();
    scheduler.flush();
    expect(log).toEqual([]);
    childRelease.open();
    scheduler.flush();

    expect(log).toEqual(["child released", "parent released"]);
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });
});

describe("race() waits for its children", () => {
  test("an interrupt waits for every child's release", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const a = slowRelease(log, "a");
    const b = slowRelease(log, "b");
    const fiber = runFiber(
      ensuring(
        race([a.effect, b.effect]),
        sync(() => void log.push("parent finalizer")),
      ),
      scheduler,
    );
    scheduler.flush();

    fiber.interrupt();
    scheduler.flush();
    b.open();
    a.open();
    scheduler.flush();

    expect(log.slice(2)).toEqual(["b release done", "a release done", "parent finalizer"]);
    expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
  });

  test("the winner's value arrives after the losers' releases finish", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const loser = slowRelease(log, "loser");
    const win = gate();
    const fiber = runFiber(
      race([win.wait.map(() => "winner"), loser.effect]).map((value) => {
        log.push(`got ${value}`);
        return value;
      }),
      scheduler,
    );
    scheduler.flush();

    win.open();
    scheduler.flush();
    expect(log).toEqual(["loser release started"]);
    expect(fiber.status).toBe("suspended");

    loser.open();
    scheduler.flush();

    expect(log).toEqual(["loser release started", "loser release done", "got winner"]);
    expect(fiber.result).toEqual({ ok: true, value: "winner" });
  });

  test("a loser's teardown failure fails a race whose winner succeeded", () => {
    const scheduler = new StepScheduler();
    const win = gate();
    const fiber = runFiber(
      race([win.wait.map(() => "winner"), ensuring(waitForever, die("release"))]),
      scheduler,
    );
    scheduler.flush();

    win.open();
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: false, cause: Cause.die("release") });
  });

  test("a loser's teardown failure joins a failed winner with Cause.both", () => {
    const scheduler = new StepScheduler();
    const lose = gate();
    const fiber = runFiber(
      race([lose.wait.flatMap(() => fail("first")), ensuring(waitForever, die("release"))]),
      scheduler,
    );
    scheduler.flush();

    lose.open();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.both(Cause.fail("first"), Cause.die("release")),
    });
  });

  test("an uninterruptible loser delays the race until it finishes", () => {
    const scheduler = new StepScheduler();
    const fast = gate();
    const slow = gate();
    const fiber = runFiber(
      race([fast.wait.map(() => "fast"), uninterruptible(slow.wait)]),
      scheduler,
    );
    scheduler.flush();
    fast.open();
    scheduler.flush();
    expect(fiber.status).toBe("suspended");

    slow.open();
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: true, value: "fast" });
  });

  test("timeoutOption returns after the timed-out effect's release finishes", () => {
    const scheduler = new StepScheduler();
    const clock = new TestClock();
    const log: string[] = [];
    const body = slowRelease(log, "body");
    const fiber = runFiber(
      provide(
        timeoutOption(body.effect, 10).map((value) => {
          log.push(`timed out: ${value === undefined}`);
        }),
        Clock,
        clock,
      ),
      scheduler,
    );
    scheduler.flush();

    clock.advance(10);
    scheduler.flush();
    expect(log).toEqual(["body release started"]);
    expect(fiber.status).toBe("suspended");

    body.open();
    scheduler.flush();

    expect(log).toEqual(["body release started", "body release done", "timed out: true"]);
    expect(fiber.result).toEqual({ ok: true, value: undefined });
  });

  test("timeoutFail fails only after the timed-out effect's release finishes", () => {
    const scheduler = new StepScheduler();
    const clock = new TestClock();
    const log: string[] = [];
    const body = slowRelease(log, "body");
    const fiber = runFiber(
      provide(
        ensuring(
          timeoutFail(body.effect, 10, () => "timeout"),
          sync(() => void log.push("outer finalizer")),
        ),
        Clock,
        clock,
      ),
      scheduler,
    );
    scheduler.flush();
    clock.advance(10);
    scheduler.flush();
    body.open();
    scheduler.flush();

    expect(log).toEqual(["body release started", "body release done", "outer finalizer"]);
    expect(fiber.result).toEqual({ ok: false, cause: Cause.fail("timeout") });
  });

  test("raceEither waits for the loser", () => {
    const scheduler = new StepScheduler();
    const log: string[] = [];
    const loser = slowRelease(log, "loser");
    const win = gate();
    const fiber = runFiber(
      raceEither(
        win.wait.map(() => 1),
        loser.effect,
      ),
      scheduler,
    );
    scheduler.flush();
    win.open();
    scheduler.flush();
    expect(fiber.status).toBe("suspended");

    loser.open();
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: true, value: { _tag: "Left", left: 1 } });
  });
});

describe("combinators built on all()", () => {
  test("raceAll and validate wait for their children when interrupted", () => {
    for (const combine of [
      (effects: Eff<void, never>[]) => raceAll(effects),
      (effects: Eff<void, never>[]) => validate(effects),
    ]) {
      const scheduler = new StepScheduler();
      const log: string[] = [];
      const child = slowRelease(log, "child");
      const fiber = runFiber(
        ensuring(
          combine([child.effect, waitForever]) as Eff<unknown, never>,
          sync(() => void log.push("parent finalizer")),
        ),
        scheduler,
      );
      scheduler.flush();

      fiber.interrupt();
      scheduler.flush();
      expect(fiber.status).toBe("suspended");
      child.open();
      scheduler.flush();

      expect(log).toEqual(["child release started", "child release done", "parent finalizer"]);
      expect(fiber.result).toEqual({ ok: false, cause: interruptCause });
    }
  });
});
