import { describe, expect, test } from "bun:test";
import {
  Cause,
  Chunk,
  Clock,
  RetryPolicy,
  Stream,
  SyncScheduler,
  TestClock,
  addFiberSupervisor,
  provide,
  runExit,
  runFiber,
  sleep,
  sync,
  TaggedError,
  ensuring,
  fail,
  succeed,
  uninterruptible,
  type Eff,
  type Fiber,
} from "../src";
import { clockNow } from "../src/clock";
import type { FiberResult } from "../src/fiber";

// Operators that fork background fibers, composed with operators that run each
// pull on a short-lived race fiber (timeout, deadline, interruptAfter,
// interruptOn, takeUntil). The background fibers belong to the stream run, not
// to whichever fiber executed the pull that started them.

interface Probe {
  /** A counted source: acquisitions, finalizations, and emission times. */
  source<A, S>(label: string, stream: Stream<A, S>): Stream<A, S>;
  /** Sleep, recording when it completes. */
  delay(ms: number): Eff<void, never>;
  readonly acquired: Map<string, number>;
  readonly finalized: Map<string, number>;
  /** Virtual times at which a source emitted or a delay completed. */
  readonly arrivals: Set<number>;
}

const makeProbe = (): Probe => {
  const acquired = new Map<string, number>();
  const finalized = new Map<string, number>();
  const arrivals = new Set<number>();
  const bump = (counts: Map<string, number>, label: string) =>
    counts.set(label, (counts.get(label) ?? 0) + 1);
  const arrived = clockNow.map((now) => void arrivals.add(now));
  return {
    acquired,
    finalized,
    arrivals,
    source: (label, stream) =>
      Stream.suspend(() => {
        bump(acquired, label);
        return stream.tapEffect(() => arrived);
      }).onFinalize(sync(() => void bump(finalized, label))),
    delay: (ms) => sleep(ms).flatMap(() => arrived),
  };
};

const ticks = (probe: Probe, label: string, everyMs: number, count: number): Stream<string> =>
  probe.source(
    label,
    Stream.tick(everyMs)
      .take(count)
      .mapAccumulate(0, (index) => [index + 1, `${label}${index + 1}`] as const),
  );

const show = (value: unknown): string =>
  value instanceof Chunk
    ? show(value.toArray())
    : Array.isArray(value)
      ? value.map(show).join("|")
      : String(value);

interface Scenario {
  readonly build: (probe: Probe) => Stream<unknown, never>;
  readonly expected: readonly string[];
  /**
   * `timeout(ms).retry()` setup for operators whose pull runs its own timers:
   * a retried pull restarts them, so the timeout must not cut through them.
   * Other operators get a timeout picked to cut pulls away from arrivals.
   */
  readonly resume?: {
    readonly timeoutMs: number;
    readonly build?: (probe: Probe) => Stream<unknown, never>;
  };
}

const scenarios: Record<string, Scenario> = {
  merge: {
    build: (p) => ticks(p, "a", 10, 3).merge(ticks(p, "b", 17, 2)),
    expected: ["a1", "b1", "a2", "a3", "b2"],
  },
  mergeAll: {
    build: (p) => Stream.mergeAll(ticks(p, "a", 10, 2), ticks(p, "b", 17, 2), ticks(p, "c", 23, 1)),
    expected: ["a1", "b1", "a2", "c1", "b2"],
  },
  parJoin: {
    // the third inner waits for a slot, which frees when "a" ends at 30
    build: (p) =>
      p
        .source("outer", Stream.of(ticks(p, "a", 10, 3), ticks(p, "b", 17, 2), ticks(p, "c", 7, 2)))
        .parJoin(2),
    expected: ["a1", "b1", "a2", "a3", "b2", "c1", "c2"],
  },
  parJoinUnbounded: {
    build: (p) =>
      p
        .source("outer", Stream.of(ticks(p, "a", 10, 3), ticks(p, "b", 17, 2), ticks(p, "c", 7, 2)))
        .parJoinUnbounded(),
    expected: ["c1", "a1", "c2", "b1", "a2", "a3", "b2"],
  },
  switchMap: {
    build: (p) => ticks(p, "o", 55, 2).switchMap((outer) => ticks(p, `${outer}-`, 10, 6)),
    expected: [
      "o1-1",
      "o1-2",
      "o1-3",
      "o1-4",
      "o1-5",
      "o2-1",
      "o2-2",
      "o2-3",
      "o2-4",
      "o2-5",
      "o2-6",
    ],
  },
  exhaustMap: {
    build: (p) => ticks(p, "o", 55, 2).exhaustMap((outer) => ticks(p, `${outer}-`, 10, 6)),
    expected: ["o1-1", "o1-2", "o1-3", "o1-4", "o1-5", "o1-6"],
  },
  parEvalMap: {
    build: (p) =>
      ticks(p, "a", 10, 3).parEvalMap(2, (value) =>
        p.delay(value === "a2" ? 17 : 5).map(() => value),
      ),
    expected: ["a1", "a2", "a3"],
  },
  parEvalMapUnordered: {
    build: (p) =>
      ticks(p, "a", 10, 3).parEvalMapUnordered(2, (value) =>
        p.delay(value === "a1" ? 27 : 5).map(() => value),
      ),
    expected: ["a2", "a3", "a1"],
  },
  buffer: {
    build: (p) => ticks(p, "a", 10, 3).buffer(2),
    expected: ["a1", "a2", "a3"],
  },
  combineLatest: {
    build: (p) => ticks(p, "a", 10, 3).combineLatest(ticks(p, "b", 25, 2)),
    expected: ["a2|b1", "a3|b1", "a3|b2"],
  },
  withLatest: {
    build: (p) => ticks(p, "a", 25, 3).withLatest(ticks(p, "b", 12, 7)),
    expected: ["a1|b2", "a2|b4", "a3|b6"],
  },
  broadcastThrough: {
    build: (p) =>
      ticks(p, "a", 10, 3).broadcastThrough(
        (stream) => stream.map((value) => `${value}L`),
        (stream) => stream.map((value) => `${value}R`),
      ),
    expected: ["a1L", "a1R", "a2L", "a2R", "a3L", "a3R"],
  },
  observe: {
    build: (p) => ticks(p, "a", 10, 3).observe((stream) => stream.map((value) => value.length)),
    expected: ["a1", "a2", "a3"],
  },
  groupWithin: {
    build: (p) => ticks(p, "a", 10, 5).groupWithin(2, 25),
    expected: ["a1|a2", "a3|a4", "a5"],
  },
  debounce: {
    build: (p) => ticks(p, "a", 10, 3).debounce(5),
    expected: ["a1", "a2", "a3"],
    resume: { timeoutMs: 7 },
  },
  sample: {
    build: (p) => ticks(p, "a", 10, 3).sample(12),
    expected: ["a1", "a2"],
    resume: { timeoutMs: 8, build: (p) => ticks(p, "a", 11, 3).sample(5) },
  },
  audit: {
    build: (p) => ticks(p, "a", 10, 3).audit(15),
    expected: ["a2", "a3"],
    resume: { timeoutMs: 8, build: (p) => ticks(p, "a", 10, 3).audit(5) },
  },
  takeUntil: {
    build: (p) => ticks(p, "a", 10, 3).takeUntil(p.source("signal", Stream.fromEffect(sleep(25)))),
    expected: ["a1", "a2"],
    // takeUntil re-pulls its source, and a tick pull restarts its sleep; a
    // buffered source keeps the retried pull resumable.
    resume: {
      timeoutMs: 4,
      build: (p) =>
        ticks(p, "a", 10, 3)
          .buffer(4)
          .takeUntil(p.source("signal", Stream.fromEffect(sleep(25)))),
    },
  },
};

const NEVER_MS = 1_000_000;

const wrappers: Record<string, (stream: Stream<unknown, never>) => Stream<unknown, any>> = {
  none: (stream) => stream,
  timeout: (stream) => stream.timeout(1_000),
  deadline: (stream) => stream.deadline(10_000),
  interruptAfter: (stream) => stream.interruptAfter(10_000),
  interruptOn: (stream) => stream.interruptOn(new AbortController().signal),
  takeUntil: (stream) => stream.takeUntil(Stream.fromEffect(sleep(NEVER_MS))),
  "timeout + interruptAfter": (stream) => stream.timeout(1_000).interruptAfter(10_000),
};

interface VirtualRun {
  readonly result: FiberResult<any> | null;
  readonly now: number;
  /** Fibers started during the run that had not ended once it settled. */
  readonly leaked: readonly Fiber<any>[];
}

const runVirtual = (params: {
  effect: Eff<unknown, any>;
  maxMs?: number;
  onTick?: (now: number, fiber: Fiber<unknown>) => void;
}): VirtualRun => {
  const { effect, maxMs = 2_000, onTick } = params;
  const scheduler = new SyncScheduler();
  const clock = new TestClock();
  const live = new Set<Fiber<any>>();
  const unsubscribe = addFiberSupervisor({
    onStart: (fiber) => live.add(fiber),
    onEnd: (fiber) => live.delete(fiber),
  });
  try {
    const fiber = runFiber(provide(effect, Clock, clock) as Eff<unknown, never>, scheduler);
    scheduler.flush();
    while (fiber.result === null && clock.now() < maxMs) {
      clock.advance(1);
      onTick?.(clock.now(), fiber);
      scheduler.flush();
    }
    return { result: fiber.result, now: clock.now(), leaked: Array.from(live) };
  } finally {
    unsubscribe();
  }
};

const collect = (stream: Stream<unknown, any>): Eff<string[], any> =>
  stream.map(show).toArray() as Eff<string[], any>;

/** Values with the virtual time each was delivered downstream. */
const timeline = (stream: Stream<unknown, any>): Eff<[string, number][], any> =>
  stream
    .evalMap((value) => clockNow.map((now) => [show(value), now] as [string, number]))
    .toArray();

const expectAllFinalized = (probe: Probe) => {
  for (const [label, count] of probe.acquired) {
    expect({ label, finalized: probe.finalized.get(label) ?? 0 }).toEqual({
      label,
      finalized: count,
    });
  }
};

interface Reference {
  readonly values: readonly [string, number][];
  readonly arrivals: ReadonlySet<number>;
  readonly endedAt: number;
}

const reference = (build: (probe: Probe) => Stream<unknown, never>): Reference => {
  const probe = makeProbe();
  const run = runVirtual({ effect: timeline(build(probe)) });
  if (run.result?.ok !== true) throw new Error("reference run failed");
  return { values: run.result.value, arrivals: probe.arrivals, endedAt: run.now };
};

/** Halfway between two delivery times, so no delivery ties with the cut. */
const midStream = ({ values }: Reference): number => {
  const times = [...new Set(values.map(([, at]) => at))];
  const index = Math.max(1, Math.floor(times.length / 2));
  return Math.floor((times[index - 1]! + times[index]!) / 2);
};

const deliveredBefore = ({ values }: Reference, cutAt: number): string[] =>
  values.filter(([, at]) => at < cutAt).map(([value]) => value);

/**
 * A timeout that cuts some pulls but never at an instant when a value reaches
 * the pull: a value handed to a pull as it is interrupted is lost with it, so
 * such a tie would test the handoff rather than resumption.
 */
const tieFreeTimeout = ({ values, arrivals, endedAt }: Reference): number => {
  const boundaries = [...new Set(values.map(([, at]) => at)), endedAt];
  for (let timeoutMs = 3; timeoutMs < 10; timeoutMs++) {
    let start = 0;
    let tie = false;
    for (const boundary of boundaries) {
      for (let cut = start + timeoutMs; cut <= boundary && !tie; cut += timeoutMs) {
        tie = cut === boundary || arrivals.has(cut);
      }
      start = boundary;
    }
    if (!tie) return timeoutMs;
  }
  throw new Error("no tie-free timeout");
};

describe.each(Object.entries(scenarios))("%s", (_name, scenario) => {
  test.each(Object.keys(wrappers))(
    "emits every element, finalizes, and leaks no fibers under %s",
    (wrapperName) => {
      const probe = makeProbe();
      const run = runVirtual({ effect: collect(wrappers[wrapperName]!(scenario.build(probe))) });

      expect(run.result).toEqual({ ok: true, value: [...scenario.expected] });
      expect(run.now).toBeLessThan(500);
      expect(run.leaked).toEqual([]);
      for (const count of probe.acquired.values()) expect(count).toBe(1);
      expectAllFinalized(probe);
    },
  );

  test("interruptAfter firing mid-stream keeps the prefix and stops every fiber", () => {
    const ref = reference(scenario.build);
    const cutAt = midStream(ref);
    const probe = makeProbe();
    const run = runVirtual({ effect: collect(scenario.build(probe).interruptAfter(cutAt)) });

    expect(deliveredBefore(ref, cutAt).length).toBeGreaterThan(0);
    expect(run.result).toEqual({ ok: true, value: deliveredBefore(ref, cutAt) });
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("takeUntil firing mid-stream keeps the prefix and stops every fiber", () => {
    const ref = reference(scenario.build);
    const cutAt = midStream(ref);
    const probe = makeProbe();
    const run = runVirtual({
      effect: collect(scenario.build(probe).takeUntil(Stream.fromEffect(sleep(cutAt)))),
    });

    expect(run.result).toEqual({ ok: true, value: deliveredBefore(ref, cutAt) });
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("interruptOn firing mid-stream keeps the prefix and stops every fiber", () => {
    const ref = reference(scenario.build);
    const cutAt = midStream(ref);
    const probe = makeProbe();
    const controller = new AbortController();
    const run = runVirtual({
      effect: collect(scenario.build(probe).interruptOn(controller.signal)),
      onTick: (now) => {
        if (now === cutAt) controller.abort();
      },
    });

    expect(run.result).toEqual({ ok: true, value: deliveredBefore(ref, cutAt) });
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("interrupting the consumer mid-stream stops every fiber", () => {
    const cutAt = midStream(reference(scenario.build));
    const probe = makeProbe();
    const run = runVirtual({
      effect: collect(scenario.build(probe)),
      onTick: (now, fiber) => {
        if (now === cutAt) fiber.interrupt();
      },
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) expect(Cause.isInterruptedOnly(run.result.cause)).toBe(true);
    expect(run.now).toBe(cutAt);
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("timeout firing fails with StreamTimeoutError and stops every fiber", () => {
    const probe = makeProbe();
    const run = runVirtual({ effect: collect(scenario.build(probe).timeout(3)) });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.firstFail(run.result.cause)?.value).toMatchObject({
        _tag: "StreamTimeoutError",
      });
    }
    expect(run.now).toBe(3);
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("deadline firing fails with StreamDeadlineError and stops every fiber", () => {
    const cutAt = midStream(reference(scenario.build));
    const probe = makeProbe();
    const run = runVirtual({ effect: collect(scenario.build(probe).deadline(cutAt)) });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.firstFail(run.result.cause)?.value).toMatchObject({
        _tag: "StreamDeadlineError",
      });
    }
    expect(run.now).toBe(cutAt);
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("timeout(ms).retry() resumes interrupted pulls without restarting fibers", () => {
    const build = scenario.resume?.build ?? scenario.build;
    const ref = reference(build);
    const timeoutMs = scenario.resume?.timeoutMs ?? tieFreeTimeout(ref);
    const probe = makeProbe();
    let retries = 0;
    const policy = RetryPolicy.recurs(1_000).onRetry(() =>
      sync(() => {
        retries++;
      }),
    );
    const run = runVirtual({ effect: collect(build(probe).timeout(timeoutMs).retry(policy)) });

    expect(run.result).toEqual({ ok: true, value: ref.values.map(([value]) => value) });
    expect(retries).toBeGreaterThan(0);
    expect(run.leaked).toEqual([]);
    for (const count of probe.acquired.values()) expect(count).toBe(1);
    expectAllFinalized(probe);
  });
});

class SourceError extends TaggedError("SourceError")<{}>() {}
class TeardownError extends TaggedError("TeardownError")<{}>() {}

describe("run teardown", () => {
  test("a fiber forked by a pull that outlives the stop is interrupted before it starts", () => {
    const acquired: string[] = [];
    const released: string[] = [];
    // inner1's slow finalizer keeps the losing pull inside switchMap's launch
    // until after interruptAfter has stopped the run
    const outer = Stream.fromEffect(sync(() => 1)).concat(
      Stream.fromEffect(sleep(10).map(() => 2)),
    );
    const stream = outer
      .switchMap((value) =>
        Stream.suspend(() => {
          acquired.push(`inner${value}`);
          return Stream.tick(1);
        }).onFinalize(
          (value === 1 ? sleep(20) : sync(() => undefined)).flatMap(() =>
            sync(() => void released.push(`inner${value}`)),
          ),
        ),
      )
      .interruptAfter(15);
    const run = runVirtual({ effect: stream.drain(), maxMs: 200 });

    expect(run.result?.ok).toBe(true);
    expect(acquired).toEqual(released);
    expect(run.leaked).toEqual([]);
  });

  test("a finalizer failing while its fiber is stopped fails the stream", () => {
    const inner = Stream.of("a")
      .concat(Stream.fromEffect(sleep(100).map(() => "b")))
      .onFinalize(fail(new TeardownError({})));
    const run = runVirtual({
      effect: collect(
        Stream.of(1)
          .switchMap(() => inner)
          .take(1),
      ),
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.failures(run.result.cause)).toEqual([new TeardownError({})]);
    }
    expect(run.leaked).toEqual([]);
  });

  test("a worker failing during teardown fails the stream", () => {
    const run = runVirtual({
      effect: collect(
        Stream.of(1, 2)
          .parEvalMap(2, (n) =>
            n === 1
              ? sleep(1).map(() => n)
              : ensuring(
                  sleep(100).map(() => n),
                  fail(new TeardownError({})),
                ),
          )
          .take(1),
      ),
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.failures(run.result.cause)).toEqual([new TeardownError({})]);
    }
    expect(run.leaked).toEqual([]);
  });

  // Each background fiber is stopped inside a pull whose own cleanup fails.
  // An interrupted fiber skips interruptible error handlers, so the failure
  // only surfaces if the operator's handler still sees the whole cause.
  const failingCleanup = <A>(value: A): Stream<A, never> =>
    Stream.fromEffect(
      ensuring(
        sleep(100).map(() => value),
        fail(new TeardownError({})),
      ),
    ) as unknown as Stream<A, never>;
  const oneThenStuck = (): Stream<number> => Stream.of(1).concat(failingCleanup(2));

  test.each<[string, () => Stream<unknown, unknown>]>([
    ["merge", () => Stream.of(1).merge(failingCleanup(2))],
    ["parJoin inner", () => Stream.of(Stream.of(1), failingCleanup(2)).parJoinUnbounded()],
    [
      "parJoin outer",
      () =>
        oneThenStuck()
          .map((n) => Stream.of(n))
          .parJoin(2),
    ],
    ["combineLatest", () => Stream.of(1).combineLatest(oneThenStuck())],
    [
      "withLatest",
      () =>
        Stream.fromEffect(sleep(1).map(() => 1))
          .concat(Stream.fromEffect(sleep(100).map(() => 2)))
          .withLatest(oneThenStuck()),
    ],
    ["broadcastThrough upstream", () => oneThenStuck().broadcastThrough((s) => s)],
    [
      "broadcastThrough branch",
      () =>
        Stream.of(1)
          .concat(Stream.fromEffect(sleep(100).map(() => 2)))
          .broadcastThrough((s) => s.onFinalize(fail(new TeardownError({})))),
    ],
    ["observe", () => oneThenStuck().observe((s) => s)],
    ["switchMap outer", () => oneThenStuck().switchMap((n) => Stream.of(n))],
    ["exhaustMap outer", () => oneThenStuck().exhaustMap((n) => Stream.of(n))],
    ["parEvalMap source", () => oneThenStuck().parEvalMap(2, (n) => succeed(n))],
    ["parEvalMapUnordered source", () => oneThenStuck().parEvalMapUnordered(2, (n) => succeed(n))],
    [
      "parEvalMapUnordered worker",
      () =>
        Stream.of(1, 2).parEvalMapUnordered(2, (n) =>
          n === 1
            ? sleep(1).map(() => n)
            : ensuring(
                sleep(100).map(() => n),
                fail(new TeardownError({})),
              ),
        ),
    ],
    ["buffer", () => oneThenStuck().buffer(2)],
    ["groupWithin", () => oneThenStuck().groupWithin(1, 10)],
    ["debounce", () => oneThenStuck().debounce(5)],
    ["sample", () => oneThenStuck().sample(5)],
    ["audit", () => oneThenStuck().audit(5)],
    ["takeUntil signal", () => Stream.of(1).takeUntil(failingCleanup(2))],
  ])("%s: a cleanup failing while its fiber is stopped fails the stream", (_name, build) => {
    const run = runVirtual({ effect: collect(build().take(1)) });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.failures(run.result.cause)).toEqual([new TeardownError({})]);
    }
    expect(run.leaked).toEqual([]);
  });

  test("cleanup failures of fibers stopped together are combined in parallel", () => {
    const run = runVirtual({
      effect: collect(
        Stream.fromEffect(sleep(1).map(() => 1))
          .concat(failingCleanup(2))
          .withLatest(oneThenStuck())
          .take(1),
      ),
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      const cause = run.result.cause;
      expect(Cause.failures(cause)).toEqual([new TeardownError({}), new TeardownError({})]);
      const hasBoth = (c: Cause): boolean =>
        c._tag === "Both" || (c._tag === "Then" && (hasBoth(c.left) || hasBoth(c.right)));
      expect(hasBoth(cause)).toBe(true);
    }
    expect(run.leaked).toEqual([]);
  });
});

describe("switchMap: the inner stream a switch tears down", () => {
  // Emits 1 at once and 2 at t=5, so the inner stream for 1 is switched away
  // from at t=5.
  const switchAt5 = (): Stream<number> =>
    Stream.of(1).concat(Stream.fromEffect(sleep(5).map(() => 2)));
  const teardownFailures = (run: VirtualRun): unknown[] =>
    run.result?.ok === false ? Cause.failures(run.result.cause) : [];

  test.each<[string, () => Stream<number, unknown>]>([
    [
      "its effect's cleanup",
      () =>
        Stream.fromEffect(
          ensuring(
            sleep(100).map(() => 0),
            fail(new TeardownError({})),
          ),
        ),
    ],
    [
      "its stream finalizer",
      () => Stream.fromEffect(sleep(100).map(() => 0)).onFinalize(fail(new TeardownError({}))),
    ],
  ])("a failure in %s fails the stream", (_name, first) => {
    let launched = 0;
    const run = runVirtual({
      effect: collect(
        switchAt5().switchMap((n) => {
          if (n === 1) return first();
          launched++;
          return Stream.of(n);
        }),
      ),
    });

    expect(run.result?.ok).toBe(false);
    expect(teardownFailures(run)).toEqual([new TeardownError({})]);
    expect(launched).toBe(0);
    expect(run.now).toBe(5);
    expect(run.leaked).toEqual([]);
  });

  const slowFailingCleanup = (): Stream<number> =>
    Stream.fromEffect(
      ensuring(
        sleep(100).map(() => 0),
        sleep(10).flatMap(() => fail(new TeardownError({}))),
      ),
    ) as unknown as Stream<number>;

  test("the next inner stream waits for the cleanup and fails when it fails", () => {
    const run = runVirtual({
      effect: collect(
        switchAt5().switchMap((n) => (n === 1 ? slowFailingCleanup() : Stream.of(n))),
      ),
    });

    expect(teardownFailures(run)).toEqual([new TeardownError({})]);
    expect(run.now).toBe(15);
    expect(run.leaked).toEqual([]);
  });

  test("a launch cut while the cleanup runs fails once retried", () => {
    let outerAcquired = 0;
    const outer = Stream.suspend(() => {
      outerAcquired++;
      return switchAt5();
    });
    const run = runVirtual({
      effect: collect(
        outer
          .switchMap((n) => (n === 1 ? slowFailingCleanup() : Stream.of(n)))
          .timeout(3)
          .retry({ times: 10 }),
      ),
    });

    expect(teardownFailures(run)).toEqual([new TeardownError({})]);
    expect(outerAcquired).toBe(1);
    expect(run.leaked).toEqual([]);
  });

  test("a cleanup still running when the stream stops fails the stream", () => {
    const run = runVirtual({
      effect: collect(
        switchAt5()
          .switchMap((n) => (n === 1 ? slowFailingCleanup() : Stream.of(n)))
          .interruptAfter(7),
      ),
    });

    expect(teardownFailures(run)).toEqual([new TeardownError({})]);
    expect(run.now).toBe(15);
    expect(run.leaked).toEqual([]);
  });

  // The pull waiting for the cleanup is cut at the instant the cleanup fails,
  // so no launch delivers the failure; the stream's stop raises it instead.
  test.each<[string, (stream: Stream<number, unknown>) => Stream<number, unknown>, string[]]>([
    ["interruptAfter", (stream) => stream.interruptAfter(15), ["TeardownError"]],
    ["timeout", (stream) => stream.timeout(15), ["StreamTimeoutError", "TeardownError"]],
    ["takeUntil", (stream) => stream.takeUntil(Stream.fromEffect(sleep(15))), ["TeardownError"]],
  ])("a cleanup failing as %s cuts the launch still fails the stream", (_name, wrap, tags) => {
    const run = runVirtual({
      effect: collect(
        wrap(switchAt5().switchMap((n) => (n === 1 ? slowFailingCleanup() : Stream.of(n)))),
      ),
    });

    expect(run.result?.ok).toBe(false);
    expect(teardownFailures(run).map((e) => (e as { _tag: string })._tag)).toEqual(tags);
    expect(run.now).toBe(15);
    expect(run.leaked).toEqual([]);
  });

  test("a cleanup that succeeds switches as before", () => {
    const run = runVirtual({
      effect: collect(
        switchAt5().switchMap((n) =>
          n === 1
            ? Stream.fromEffect(
                ensuring(
                  sleep(100).map(() => 0),
                  sleep(10),
                ),
              )
            : Stream.of(n),
        ),
      ),
    });

    expect(run.result).toEqual({ ok: true, value: ["2"] });
    expect(run.now).toBe(15);
    expect(run.leaked).toEqual([]);
  });
});

describe("cleanup of a cut pull", () => {
  // A racing wrapper waits for the pull it cut to finish its cleanup. A linear
  // source cleans up inside that pull; an operator with background fibers only
  // stops waiting on its queue there, and its fibers clean up when the stream
  // is finalized.
  const failingCleanup = () =>
    Stream.fromEffect(
      ensuring(
        sleep(100).map(() => 1),
        fail(new TeardownError({})),
      ),
    );
  const slowCleanup = () =>
    Stream.fromEffect(
      ensuring(
        sleep(100).map(() => 1),
        sleep(20),
      ),
    );
  const sources: Record<string, (build: () => Stream<number, any>) => Stream<number, any>> = {
    linear: (build) => build(),
    merge: (build) => build().merge(Stream.empty<number>()),
  };

  const wrappers: {
    name: string;
    wrap: (stream: Stream<number, any>) => Stream<number, any>;
    error?: string;
  }[] = [
    { name: "timeout", wrap: (s) => s.timeout(5), error: "StreamTimeoutError" },
    { name: "deadline", wrap: (s) => s.deadline(5), error: "StreamDeadlineError" },
    { name: "interruptAfter", wrap: (s) => s.interruptAfter(5) },
    { name: "takeUntil", wrap: (s) => s.takeUntil(Stream.fromEffect(sleep(5))) },
  ];

  describe.each(wrappers)("$name", ({ wrap, error }) => {
    test.each(Object.keys(sources))(
      "%s source: a failing cleanup joins the outcome instead of replacing it",
      (source) => {
        const run = runVirtual({ effect: wrap(sources[source]!(failingCleanup)).toArray() });

        expect(run.result?.ok).toBe(false);
        if (run.result?.ok === false) {
          const tags = Cause.failures(run.result.cause).map((e) => (e as { _tag: string })._tag);
          expect(tags).toEqual(error === undefined ? ["TeardownError"] : [error, "TeardownError"]);
        }
        expect(run.now).toBe(5);
        expect(run.leaked).toEqual([]);
      },
    );

    test.each(Object.keys(sources))(
      "%s source: a slow cleanup finishes before the stream does",
      (source) => {
        const run = runVirtual({ effect: wrap(sources[source]!(slowCleanup)).toArray() });

        if (error === undefined) {
          expect(run.result).toEqual({ ok: true, value: [] });
        } else {
          expect(run.result?.ok).toBe(false);
          if (run.result?.ok === false) {
            expect(Cause.firstFail(run.result.cause)?.value).toMatchObject({ _tag: error });
          }
        }
        expect(run.now).toBe(25);
        expect(run.leaked).toEqual([]);
      },
    );
  });

  // The failing signal cuts the pull like a timer that fails, so its failure
  // stays in the outcome next to the cleanup failure instead of being replaced.
  test.each(Object.keys(sources))(
    "takeUntil: %s source: a failing signal joins a failing cleanup of the cut pull",
    (source) => {
      const signal = Stream.fromEffect(sleep(5).flatMap(() => fail(new SourceError({}))));
      const run = runVirtual({
        effect: sources[source]!(failingCleanup).takeUntil(signal).toArray(),
      });

      expect(run.result?.ok).toBe(false);
      if (run.result?.ok === false) {
        const tags = Cause.failures(run.result.cause).map((e) => (e as { _tag: string })._tag);
        expect(tags).toEqual(["SourceError", "TeardownError"]);
      }
      expect(run.now).toBe(5);
      expect(run.leaked).toEqual([]);
    },
  );

  test("interruptOn: a failing cleanup of the cut pull fails the stream", () => {
    const controller = new AbortController();
    const run = runVirtual({
      effect: failingCleanup().interruptOn(controller.signal).toArray(),
      onTick: (now) => {
        if (now === 5) controller.abort();
      },
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.failures(run.result.cause)).toEqual([new TeardownError({})]);
    }
    expect(run.leaked).toEqual([]);
  });

  test("a merge pull is cut on time; its fibers clean up at finalization", () => {
    let timedOutAt = -1;
    const run = runVirtual({
      effect: collect(
        slowCleanup()
          .merge(Stream.empty<number>())
          .timeout(5)
          .catchTag("StreamTimeoutError", () =>
            Stream.fromEffect(clockNow.map((now) => void (timedOutAt = now))),
          ),
      ),
    });

    expect(timedOutAt).toBe(5);
    expect(run.now).toBe(25);
    expect(run.leaked).toEqual([]);
  });
});

describe("retry and reuse", () => {
  test("a later failed pull fails again under retry instead of hanging", () => {
    const probe = makeProbe();
    const failing = probe.source(
      "failing",
      Stream.fromEffect(sleep(5).map(() => "f1")).concat(
        Stream.fromEffect(sleep(7).flatMap(() => fail(new SourceError({})))),
      ),
    );
    const run = runVirtual({
      effect: collect(failing.merge(ticks(probe, "a", 10, 3)).retry({ times: 2 })),
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.firstFail(run.result.cause)?.value).toBeInstanceOf(SourceError);
    }
    expect(run.now).toBe(12);
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  test("a failed first pull fails again under retry without reacquiring sources", () => {
    const probe = makeProbe();
    let attempts = 0;
    const flaky = Stream.suspend(() =>
      attempts++ === 0
        ? Stream.fromEffect(sleep(3).flatMap(() => fail(new SourceError({}))))
        : Stream.fromEffect(sleep(3).map(() => "x")),
    );
    const run = runVirtual({
      effect: collect(flaky.merge(ticks(probe, "a", 10, 2)).retry({ times: 3 })),
    });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.firstFail(run.result.cause)?.value).toBeInstanceOf(SourceError);
    }
    expect(run.now).toBe(3);
    expect(attempts).toBe(1);
    expect(probe.acquired.get("a")).toBe(1);
    expect(run.leaked).toEqual([]);
    expectAllFinalized(probe);
  });

  // Linear operators run a failed pull again. A concurrent operator's failure
  // happened in a background fiber, so retrying delivers it again rather than
  // skipping the failed element.
  test.each([
    {
      name: "parEvalMap",
      build: () =>
        Stream.of(1, 2, 3)
          .rechunk(1)
          .parEvalMap(2, (n) => (n === 2 ? fail(new SourceError({})) : succeed(n))),
    },
    {
      name: "parEvalMapUnordered",
      build: () =>
        Stream.of(1, 2, 3)
          .rechunk(1)
          .parEvalMapUnordered(1, (n) => (n === 2 ? fail(new SourceError({})) : succeed(n))),
    },
    {
      name: "parJoin",
      build: () =>
        Stream.of<Stream<number, any>>(Stream.of(1), Stream.fail(new SourceError({})), Stream.of(3))
          .rechunk(1)
          .parJoin(1),
    },
    {
      name: "switchMap",
      build: () =>
        Stream.of(1, 2)
          .rechunk(1)
          .switchMap((n) =>
            n === 1 ? Stream.of(10).concat(Stream.fail(new SourceError({}))) : Stream.of(20),
          ),
    },
  ])("$name: retrying a delivered element failure fails again", ({ build }) => {
    const run = runVirtual({ effect: collect(build().retry({ times: 2 })) });

    expect(run.result?.ok).toBe(false);
    if (run.result?.ok === false) {
      expect(Cause.firstFail(run.result.cause)?.value).toBeInstanceOf(SourceError);
    }
    expect(run.leaked).toEqual([]);
  });

  test("a stream run again after an interrupted run starts fresh", () => {
    const build = (probe: Probe) => ticks(probe, "a", 10, 3).merge(ticks(probe, "b", 16, 2));
    const probe = makeProbe();
    const shared = build(probe);
    const run = runVirtual({ effect: collect(shared.interruptAfter(12).concat(shared)) });
    const separate = runVirtual({
      effect: collect(build(makeProbe()).interruptAfter(12).concat(build(makeProbe()))),
    });

    expect(separate.result).toEqual({
      ok: true,
      value: ["a1", "a1", "b1", "a2", "a3", "b2"],
    });
    expect(run.result).toEqual(separate.result);
    expect(probe.acquired.get("a")).toBe(2);
    expect(run.leaked).toEqual([]);
  });

  test("a stream used as its own catch fallback starts fresh", () => {
    let attempts = 0;
    const flaky = Stream.suspend(() =>
      attempts++ === 0
        ? Stream.fromEffect(sleep(3).flatMap(() => fail(new SourceError({}))))
        : Stream.fromEffect(sleep(3).map(() => "x")),
    );
    const merged = flaky.merge(Stream.of("y"));
    const run = runVirtual({ effect: collect(merged.catch(() => merged)) });

    expect(run.result?.ok).toBe(true);
    if (run.result?.ok) expect([...run.result.value].sort()).toEqual(["x", "y", "y"]);
    expect(attempts).toBe(2);
    expect(run.leaked).toEqual([]);
  });

  test("a stop interrupted while starting over keeps the old run's fibers owned", () => {
    const events: string[] = [];
    const log = (event: string) => clockNow.map((now) => void events.push(`${now}:${event}`));
    const slow = Stream.fromEffect(
      uninterruptible(sleep(50).flatMap(() => log("old driver step done"))),
    ).onFinalize(log("source released"));
    const merged = slow.merge(Stream.empty<undefined>());
    // the second run's first pull waits for the cut first run's driver, and
    // interruptAfter(10) cuts that wait
    const run = runVirtual({
      effect: merged.interruptAfter(3).concat(merged).interruptAfter(10).drain(),
    });

    expect(run.result?.ok).toBe(true);
    expect(events[0]).toBe("50:old driver step done");
    expect(events.slice(1).every((event) => event === "50:source released")).toBe(true);
    expect(run.leaked).toEqual([]);
  });

  test("switchMap resumes a launch cut while the previous inner stream finalizes", () => {
    let outerAcquired = 0;
    const outer = Stream.suspend(() => {
      outerAcquired++;
      return Stream.of(1, 2).rechunk(1);
    });
    const run = runVirtual({
      effect: collect(
        outer
          .switchMap((n) =>
            n === 1
              ? Stream.fromEffect(sleep(1_000).map(() => "never"))
                  .filter(() => false)
                  .onFinalize(sleep(22))
              : Stream.of(`inner${n}`),
          )
          .timeout(5)
          .retry({ times: 10 }),
      ),
      maxMs: 200,
    });

    expect(run.result).toEqual({ ok: true, value: ["inner2"] });
    expect(outerAcquired).toBe(1);
    expect(run.leaked).toEqual([]);
  });

  test("a pull cut under nested retries resumes the same run", () => {
    const probe = makeProbe();
    const merged = ticks(probe, "a", 10, 2).merge(ticks(probe, "b", 15, 1));
    const run = runVirtual({
      effect: collect(merged.retry({ times: 1 }).timeout(4).retry({ times: 100 })),
    });

    expect(run.result).toEqual({ ok: true, value: ["a1", "b1", "a2"] });
    for (const count of probe.acquired.values()) expect(count).toBe(1);
    expect(run.leaked).toEqual([]);
  });

  test("pulling step by hand leaves fibers running until the finalizer runs", () => {
    const merged = Stream.tick(1).merge(Stream.tick(1));
    const run = runVirtual({
      effect: merged.step.flatMap((step) =>
        (merged._finalizer ?? succeed(undefined)).map(() => step._tag),
      ),
    });

    expect(run.result).toEqual({ ok: true, value: "Emit" });
    expect(run.leaked).toEqual([]);
  });

  test("one stream instance can be run again after it completes", () => {
    const probe = makeProbe();
    const merged = ticks(probe, "a", 10, 2).merge(ticks(probe, "b", 15, 1));
    const run = runVirtual({ effect: collect(merged.concat(merged)) });

    expect(run.result).toEqual({ ok: true, value: ["a1", "b1", "a2", "a1", "b1", "a2"] });
    expect(run.leaked).toEqual([]);
  });

  test("one stream instance can be pulled by two consumers", () => {
    const build = (probe: Probe) => ticks(probe, "a", 10, 2).merge(ticks(probe, "b", 15, 1));
    const shared = build(makeProbe());
    const run = runVirtual({ effect: collect(shared.zip(shared)) });
    const separate = runVirtual({ effect: collect(build(makeProbe()).zip(build(makeProbe()))) });

    expect(separate.result).toEqual({ ok: true, value: ["a1|a1", "b1|b1", "a2|a2"] });
    expect(run.result).toEqual(separate.result);
    expect(run.leaked).toEqual([]);
  });
});

describe("real clock", () => {
  test.each([
    ["parEvalMap", (s: Stream<number>) => s.parEvalMap(4, (n) => sleep(1).map(() => n))],
    ["buffer", (s: Stream<number>) => s.buffer(4)],
  ] as const)("%s under timeout finishes without a spurious timeout", async (_name, operator) => {
    const source = Stream.range(0, 20)
      .rechunk(1)
      .evalMap((n) => sleep(1).map(() => n));
    const exit = await runExit(operator(source).timeout(1_000).toArray());

    expect(exit).toEqual({
      _tag: "Success",
      value: Array.from({ length: 20 }, (_, i) => i),
    });
  });

  const merged = () =>
    Stream.tick(10)
      .take(3)
      .map(() => "a")
      .merge(
        Stream.tick(15)
          .take(2)
          .map(() => "b"),
      );

  test("merge under timeout emits every element without a spurious timeout", async () => {
    const started = Date.now();
    const exit = await runExit(merged().timeout(1_000).toArray());

    expect(exit._tag).toBe("Success");
    if (exit._tag === "Success") expect([...exit.value].sort()).toEqual(["a", "a", "a", "b", "b"]);
    expect(Date.now() - started).toBeLessThan(500);
  });

  test("merge under interruptAfter is not truncated", async () => {
    const exit = await runExit(merged().interruptAfter(500).toArray());

    expect(exit._tag).toBe("Success");
    if (exit._tag === "Success") expect([...exit.value].sort()).toEqual(["a", "a", "a", "b", "b"]);
  });
});
