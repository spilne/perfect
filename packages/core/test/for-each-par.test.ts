import { describe, test, expect } from "bun:test";
import {
  type Eff,
  type Fiber,
  type Throws,
  Cause,
  Clock,
  Exit,
  TestClock,
  addFiberSupervisor,
  all,
  async,
  die,
  ensuring,
  fail,
  forEachPar,
  provide,
  run,
  runExit,
  runFiber,
  runSync,
  service,
  sleep,
  succeed,
  sync,
  timeoutOption,
  uninterruptible,
  yieldNow,
} from "../src";

const tick = () => new Promise((r) => setTimeout(r, 0));

const range = (n: number): number[] => Array.from({ length: n }, (_, i) => i);

// Clock-free latency: an item that yields more finishes later.
const yields = (n: number): Eff<void, never> =>
  n <= 0 ? succeed(undefined) : yieldNow.flatMap(() => yields(n - 1));

// An async resume with no timer behind it — finalizers that suspend like real
// cleanup (closing a socket) without depending on the clock.
const microtask: Eff<void, never> = async<void>((resume) => {
  queueMicrotask(() => resume(succeed(undefined)));
});

const never: Eff<void, never> = async<void>(() => () => {});

function inFlightTracker() {
  let current = 0;
  let max = 0;
  return {
    get current() {
      return current;
    },
    get max() {
      return max;
    },
    track<A, S>(eff: Eff<A, S>): Eff<A, S> {
      return sync(() => {
        current++;
        max = Math.max(max, current);
      }).flatMap(() =>
        ensuring(
          eff,
          sync(() => {
            current--;
          }),
        ),
      );
    },
  };
}

async function untilSleeping(clock: TestClock, count: number): Promise<void> {
  for (let i = 0; i < 100 && clock.pendingCount < count; i++) await tick();
  expect(clock.pendingCount).toBe(count);
}

function deferredCount(target: number) {
  let count = 0;
  let resolve!: () => void;
  const reached = new Promise<void>((r) => (resolve = r));
  return {
    reached,
    hit: () => {
      if (++count === target) resolve();
    },
  };
}

describe("forEachPar — results", () => {
  test("collects results in input order when later items finish first", async () => {
    const latencies = [5, 0, 3, 1, 4, 2];
    const result = await run(
      forEachPar(latencies, (n, i) => yields(n * 3).map(() => `${i}:${n}`), { concurrency: 3 }),
    );
    expect(result).toEqual(latencies.map((n, i) => `${i}:${n}`));
  });

  test("empty input succeeds with an empty array without calling f", () => {
    let calls = 0;
    const program = forEachPar(
      [],
      () => {
        calls++;
        return succeed(1);
      },
      { concurrency: 2 },
    );
    expect(runSync(program)).toEqual([]);
    expect(calls).toBe(0);
  });

  test("accepts any iterable and passes the index", () => {
    const result = runSync(
      forEachPar(new Set(["a", "b", "c"]), (s, i) => succeed(`${i}${s}`), { concurrency: 2 }),
    );
    expect(result).toEqual(["0a", "1b", "2c"]);
  });

  test("is a reusable description — every run calls f again", async () => {
    let calls = 0;
    const program = forEachPar(
      [1, 2, 3],
      (n) =>
        sync(() => {
          calls++;
          return n * 2;
        }),
      { concurrency: 2 },
    );
    expect(await run(program)).toEqual([2, 4, 6]);
    expect(await run(program)).toEqual([2, 4, 6]);
    expect(calls).toBe(6);
  });

  test("children see services provided around the traversal", () => {
    const Multiplier = service<{ factor: number }>()("ForEachParMultiplier");
    const program = forEachPar([1, 2, 3], (n) => Multiplier.get.map((m) => n * m.factor), {
      concurrency: 2,
    });
    expect(runSync(provide(program, Multiplier, { factor: 10 }))).toEqual([10, 20, 30]);
  });

  test("rejects a non-positive or fractional concurrency", () => {
    for (const concurrency of [0, -1, 1.5, Number.NaN]) {
      expect(() => forEachPar([1], succeed, { concurrency })).toThrow(RangeError);
    }
  });
});

describe("forEachPar — concurrency bound", () => {
  test("never runs more than `concurrency` effects at once", async () => {
    const tracker = inFlightTracker();
    const items = range(40);
    const result = await run(
      forEachPar(items, (i) => tracker.track(yields(i % 7).map(() => i * 2)), { concurrency: 4 }),
    );
    expect(result).toEqual(items.map((i) => i * 2));
    expect(tracker.max).toBe(4);
    expect(tracker.current).toBe(0);
  });

  test("refills a slot as soon as one frees up (TestClock)", async () => {
    const clock = new TestClock();
    const durations = [100, 300, 100, 100];
    const promise = run(
      provide(
        forEachPar(durations, (ms, i) => sleep(ms).map(() => i), { concurrency: 2 }),
        Clock,
        clock,
      ),
    );

    await untilSleeping(clock, 2);
    expect(clock.pendingDeadlines()).toEqual([100, 300]);
    clock.advance(100);
    await untilSleeping(clock, 2);
    expect(clock.pendingDeadlines()).toEqual([200, 300]);
    clock.advance(100);
    await untilSleeping(clock, 2);
    expect(clock.pendingDeadlines()).toEqual([300, 300]);
    clock.advance(100);

    expect(await promise).toEqual([0, 1, 2, 3]);
    expect(clock.now()).toBe(300);
  });

  test("concurrency 1 runs items strictly one after another", async () => {
    const log: string[] = [];
    await run(
      forEachPar(
        [0, 1, 2],
        (i) =>
          sync(() => log.push(`start ${i}`))
            .flatMap(() => yields(3))
            .flatMap(() => sync(() => log.push(`end ${i}`))),
        { concurrency: 1 },
      ),
    );
    expect(log).toEqual(["start 0", "end 0", "start 1", "end 1", "start 2", "end 2"]);
  });

  test("omitted or unbounded concurrency starts every item at once, like all", async () => {
    for (const options of [undefined, { concurrency: "unbounded" as const }]) {
      const tracker = inFlightTracker();
      const result = await run(
        forEachPar(range(20), (i) => tracker.track(yields(3).map(() => i)), options),
      );
      expect(result).toEqual(range(20));
      expect(tracker.max).toBe(20);
    }
  });

  test("handles a large input while holding only `concurrency` fibers", async () => {
    const n = 100_000;
    const tracker = inFlightTracker();
    const children = new Set<Fiber<any>>();
    let maxLiveChildren = 0;
    let parent: Fiber<any> | null = null;
    const stop = addFiberSupervisor({
      onFork: (p, child) => {
        parent ??= p;
        if (p !== parent) return;
        children.add(child);
        maxLiveChildren = Math.max(maxLiveChildren, children.size);
      },
      onEnd: (fiber) => {
        children.delete(fiber);
      },
    });
    try {
      const result = await run(
        forEachPar(range(n), (i) => tracker.track(yieldNow.map(() => i)), { concurrency: 8 }),
      );
      expect(result.length).toBe(n);
      expect(result.every((v, i) => v === i)).toBe(true);
    } finally {
      stop();
    }
    expect(tracker.max).toBe(8);
    expect(maxLiveChildren).toBe(8);
  });
});

describe("forEachPar — scheduling", () => {
  test("already-successful effects are collected without forking fibers", () => {
    let forks = 0;
    const stop = addFiberSupervisor({
      onFork: () => {
        forks++;
      },
    });
    try {
      const result = runSync(forEachPar(range(1_000), (i) => succeed(i * 2), { concurrency: 4 }));
      expect(result).toEqual(range(1_000).map((i) => i * 2));
    } finally {
      stop();
    }
    expect(forks).toBe(0);
  });

  test("a long synchronous traversal still lets other fibers run", async () => {
    let otherRan = false;
    let sawOtherBeforeEnd = false;
    const other = yieldNow.flatMap(() =>
      sync(() => {
        otherRan = true;
      }),
    );
    const traversal = forEachPar(
      range(50_000),
      (i) =>
        sync(() => {
          if (otherRan) sawOtherBeforeEnd = true;
          return i;
        }),
      { concurrency: 4 },
    );

    await run(all([traversal, other]));

    expect(sawOtherBeforeEnd).toBe(true);
  });

  test("synchronous children stay stack-safe at scale", () => {
    for (const concurrency of [1, 8, "unbounded" as const]) {
      const result = runSync(forEachPar(range(100_000), (i) => sync(() => i), { concurrency }));
      expect(result.length).toBe(100_000);
      expect(result[99_999]).toBe(99_999);
    }
  });
});

describe("forEachPar — failure and interruption", () => {
  test("first typed failure interrupts in-flight siblings and starts no pending items", async () => {
    const clock = new TestClock();
    const called: number[] = [];
    const finalized: number[] = [];
    const program = forEachPar(
      range(10),
      (i) => {
        called.push(i);
        const body: Eff<number, Throws<string>> = i === 1
          ? yields(2).flatMap(() => fail("boom"))
          : sleep(60_000).map(() => i);
        return ensuring(
          body,
          microtask.flatMap(() =>
            sync(() => {
              finalized.push(i);
            }),
          ),
        );
      },
      { concurrency: 3 },
    );

    const exit = await runExit(provide(program, Clock, clock));

    expect(exit).toEqual(Exit.failure(Cause.fail("boom")));
    expect(called).toEqual([0, 1, 2]);
    // Async finalizers already finished: the failure waits for siblings.
    expect(finalized.sort()).toEqual([0, 1, 2]);
    expect(clock.pendingCount).toBe(0);
  });

  test("a defect fails the traversal and interrupts siblings", async () => {
    const clock = new TestClock();
    const kaboom = new Error("kaboom");
    const finalized: number[] = [];
    const program = forEachPar(
      range(4),
      (i) =>
        ensuring(
          i === 2 ? yields(1).flatMap(() => die(kaboom)) : sleep(60_000),
          sync(() => {
            finalized.push(i);
          }),
        ),
      { concurrency: 4 },
    );

    const exit = await runExit(provide(program, Clock, clock));

    expect(exit).toEqual(Exit.failure(Cause.die(kaboom)));
    expect(finalized.sort()).toEqual([0, 1, 2, 3]);
  });

  test("stops calling f as soon as a failure is observed, even mid-batch", async () => {
    const boom = new Error("boom");
    const mappers: Array<[string, (i: number) => Eff<void, Throws<Error>>]> = [
      [
        "f throws",
        (i) => {
          if (i === 1) throw boom;
          return sleep(60_000);
        },
      ],
      ["f returns fail", (i) => (i === 1 ? fail(boom) : sleep(60_000))],
      [
        "f returns an effect that throws synchronously",
        (i) =>
          i === 1
            ? sync(() => {
                throw boom;
              })
            : sleep(60_000),
      ],
    ];
    for (const [label, mapper] of mappers) {
      const clock = new TestClock();
      const called: number[] = [];
      const program = forEachPar(
        range(6),
        (i) => {
          called.push(i);
          return mapper(i);
        },
        { concurrency: 4 },
      );

      const exit = await runExit(provide(program, Clock, clock));

      expect(Exit.isFailure(exit), label).toBe(true);
      expect(called, label).toEqual([0, 1]);
      expect(clock.pendingCount, label).toBe(0);
    }
  });

  test("failures raised while siblings are torn down join the cause", async () => {
    const clock = new TestClock();
    const finalizerDefect = new Error("finalizer blew up");
    const program = forEachPar(
      [0, 1],
      (i) =>
        i === 0
          ? yields(1).flatMap(() => fail("boom"))
          : ensuring(
              sleep(60_000),
              sync(() => {
                throw finalizerDefect;
              }),
            ),
      { concurrency: 2 },
    );

    const exit = await runExit(provide(program, Clock, clock));

    expect(exit).toEqual(Exit.failure(Cause.both(Cause.fail("boom"), Cause.die(finalizerDefect))));
  });

  test("the failure type is catchable like any other typed error", async () => {
    const program = forEachPar([1, 2, 3], (n) => (n === 2 ? fail(`bad ${n}`) : succeed(n)), {
      concurrency: 2,
    }).catch((e) => succeed([e]));
    expect(await run(program)).toEqual(["bad 2"]);
  });

  test("interrupting the traversal interrupts in-flight items and waits for their finalizers", async () => {
    const clock = new TestClock();
    const called: number[] = [];
    const finalized: number[] = [];
    const program = forEachPar(
      range(10),
      (i) => {
        called.push(i);
        return ensuring(
          sleep(60_000),
          microtask.flatMap(() =>
            sync(() => {
              finalized.push(i);
            }),
          ),
        );
      },
      { concurrency: 3 },
    );

    const fiber = runFiber(provide(program, Clock, clock));
    await untilSleeping(clock, 3);
    fiber.interrupt();

    expect(await fiber.await()).toEqual(Exit.interrupt());
    expect(finalized.sort()).toEqual([0, 1, 2]);
    expect(called).toEqual([0, 1, 2]);
    expect(clock.pendingCount).toBe(0);
  });

  test("finalizers around the traversal run after the children's finalizers", async () => {
    const log: string[] = [];
    const program = ensuring(
      forEachPar(
        [0, 1],
        (i) =>
          ensuring(
            never,
            microtask.flatMap(() =>
              sync(() => {
                log.push(`child ${i}`);
              }),
            ),
          ),
        { concurrency: 2 },
      ),
      sync(() => {
        log.push("parent");
      }),
    );

    const fiber = runFiber(program);
    for (let i = 0; i < 100 && fiber.childCount < 2; i++) await tick();
    expect(fiber.childCount).toBe(2);
    fiber.interrupt();

    expect(await fiber.await()).toEqual(Exit.interrupt());
    expect(log.slice(0, 2).sort()).toEqual(["child 0", "child 1"]);
    expect(log[2]).toBe("parent");
  });

  test("an interrupt while siblings are being torn down still waits for them", async () => {
    const log: string[] = [];
    let release: (() => void) | undefined;
    const slowCleanup = async<void>((resume) => {
      release = () => resume(succeed(undefined));
    });
    const program = ensuring(
      forEachPar(
        [0, 1],
        (i) =>
          i === 0
            ? yields(1).flatMap(() => fail("boom"))
            : ensuring(
                never,
                slowCleanup.flatMap(() =>
                  sync(() => {
                    log.push("child cleanup");
                  }),
                ),
              ),
        { concurrency: 2 },
      ),
      sync(() => {
        log.push("parent finalizer");
      }),
    );

    const fiber = runFiber(program);
    for (let i = 0; i < 100 && release === undefined; i++) await tick();
    expect(release).toBeDefined();
    fiber.interrupt();
    for (let i = 0; i < 5; i++) await tick();
    expect(log).toEqual([]);
    expect(fiber.status).not.toBe("done");

    release!();
    const exit = await fiber.await();

    expect(log).toEqual(["child cleanup", "parent finalizer"]);
    expect(exit).toEqual(Exit.failure(Cause.both(Cause.interrupt(), Cause.fail("boom"))));
  });

  test("an interrupt after the failure was delivered has the same cause as one before", () => {
    // Runs queued loop slices one at a time.
    const queue: Array<() => void> = [];
    const scheduler = {
      schedule: (task: () => void) => void queue.push(task),
      flush: () => {
        while (queue.length > 0) queue.shift()!();
      },
      shutdown: () => void (queue.length = 0),
    };
    const results: unknown[] = [];
    for (const deliveredFirst of [false, true]) {
      let failNow!: () => void;
      let releaseSibling!: () => void;
      const fiber = runFiber(
        forEachPar(
          [0, 1],
          (i) =>
            i === 0
              ? async<void>((resume) => {
                  failNow = () => resume(succeed(undefined));
                }).flatMap(() => fail("e1"))
              : uninterruptible(
                  async<void>((resume) => {
                    releaseSibling = () => resume(succeed(undefined));
                  }),
                ),
          { concurrency: 2 },
        ),
        scheduler,
      );
      scheduler.flush();
      failNow();
      scheduler.flush();
      if (deliveredFirst) {
        releaseSibling();
        while (fiber.status !== "ready" && queue.length > 0) queue.shift()!();
        expect(fiber.status).toBe("ready");
        fiber.interrupt();
      } else {
        expect(fiber.status).toBe("suspended");
        fiber.interrupt();
        releaseSibling();
      }
      scheduler.flush();
      results.push(fiber.result);
    }

    const expected = { ok: false, cause: Cause.both(Cause.interrupt(), Cause.fail("e1")) };
    expect(results).toEqual([expected, expected]);
  });

  test("a timeout around the traversal interrupts in-flight items and runs their finalizers", async () => {
    const clock = new TestClock();
    const finalized: number[] = [];
    const allFinalized = deferredCount(2);
    const program = timeoutOption(
      forEachPar(
        range(6),
        (i) =>
          ensuring(
            sleep(60_000),
            microtask.flatMap(() =>
              sync(() => {
                finalized.push(i);
                allFinalized.hit();
              }),
            ),
          ),
        { concurrency: 2 },
      ),
      1_000,
    );

    const promise = run(provide(program, Clock, clock));
    await untilSleeping(clock, 3);
    clock.advance(1_000);

    expect(await promise).toBeUndefined();
    await allFinalized.reached;
    expect(finalized.sort()).toEqual([0, 1]);
  });
});

describe("forEachPar — iterables", () => {
  test("pulls the next item only when a slot frees up, starting at run time", async () => {
    const clock = new TestClock();
    const pulled: number[] = [];
    function* numbers() {
      for (let i = 0; i < 4; i++) {
        pulled.push(i);
        yield i;
      }
    }
    const program = forEachPar(numbers(), (i) => sleep(100).map(() => i * 10), {
      concurrency: 2,
    });
    expect(pulled).toEqual([]);

    const promise = run(provide(program, Clock, clock));
    await untilSleeping(clock, 2);
    expect(pulled).toEqual([0, 1]);
    clock.advance(100);
    await untilSleeping(clock, 2);
    expect(pulled).toEqual([0, 1, 2, 3]);
    clock.advance(100);

    expect(await promise).toEqual([0, 10, 20, 30]);
  });

  test("an infinite iterable works under a timeout and is closed on interrupt", async () => {
    const clock = new TestClock();
    let pulled = 0;
    let closed = false;
    function* naturals() {
      try {
        for (let i = 0; ; i++) {
          pulled++;
          yield i;
        }
      } finally {
        closed = true;
      }
    }
    const program = timeoutOption(
      forEachPar(naturals(), (i) => sleep(300).map(() => i), { concurrency: 4 }),
      1_000,
    );

    const promise = run(provide(program, Clock, clock));
    for (const step of [300, 300, 300]) {
      await untilSleeping(clock, 5);
      clock.advance(step);
    }
    await untilSleeping(clock, 5);
    clock.advance(100);

    expect(await promise).toBeUndefined();
    expect(pulled).toBe(16);
    expect(closed).toBe(true);
    expect(clock.pendingCount).toBe(0);
  });

  test("a throwing iterator is a defect that interrupts in-flight items", async () => {
    const clock = new TestClock();
    const boom = new Error("iterator threw");
    const finalized: number[] = [];
    function* flaky() {
      yield 0;
      yield 1;
      throw boom;
    }
    const program = forEachPar(
      flaky(),
      (i) =>
        ensuring(
          sleep(60_000),
          sync(() => {
            finalized.push(i);
          }),
        ),
      { concurrency: 3 },
    );

    const exit = await runExit(provide(program, Clock, clock));

    expect(exit).toEqual(Exit.failure(Cause.die(boom)));
    expect(finalized.sort()).toEqual([0, 1]);
    expect(clock.pendingCount).toBe(0);
  });

  test("closes the iterator when the traversal stops early", async () => {
    const boom = new Error("mapper threw");
    let closed = false;
    function* items() {
      try {
        yield 0;
        yield 1;
        yield 2;
      } finally {
        closed = true;
      }
    }
    const program = forEachPar(
      items(),
      (i) => {
        if (i === 1) throw boom;
        return succeed(i);
      },
      { concurrency: 2 },
    );

    expect(await runExit(program)).toEqual(Exit.failure(Cause.die(boom)));
    expect(closed).toBe(true);
  });

  test("an iterable whose iterator cannot be created fails at run time, not construction", async () => {
    const boom = new Error("no iterator");
    const hostile: Iterable<number> = {
      [Symbol.iterator]() {
        throw boom;
      },
    };
    const program = forEachPar(hostile, (n) => succeed(n));
    expect(await runExit(program)).toEqual(Exit.failure(Cause.die(boom)));
  });

  test("reads the input on every run: arrays and re-iterables repeat, a generator object is one-shot", () => {
    const xs = [1];
    const fromArray = forEachPar(xs, (n) => succeed(n));
    xs.push(2);
    expect(runSync(fromArray)).toEqual([1, 2]);

    const reusable = {
      *[Symbol.iterator]() {
        yield 1;
        yield 2;
      },
    };
    const fromReusable = forEachPar(reusable, (n) => succeed(n * 10));
    expect(runSync(fromReusable)).toEqual([10, 20]);
    expect(runSync(fromReusable)).toEqual([10, 20]);

    function* once() {
      yield 1;
      yield 2;
    }
    const fromGenerator = forEachPar(once(), (n) => succeed(n * 10));
    expect(runSync(fromGenerator)).toEqual([10, 20]);
    expect(runSync(fromGenerator)).toEqual([]);
  });
});
