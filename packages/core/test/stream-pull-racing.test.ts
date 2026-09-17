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
  /**
   * The consumer pull waits inside `race` (sample's interval, takeUntil's
   * signal). The runtime re-runs the finalizers of a fiber interrupted there
   * once its race children finish, independently of the stream operators.
   */
  readonly consumerWaitsInRace?: boolean;
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
    consumerWaitsInRace: true,
  },
  audit: {
    build: (p) => ticks(p, "a", 10, 3).audit(15),
    expected: ["a2", "a3"],
    resume: { timeoutMs: 8, build: (p) => ticks(p, "a", 10, 3).audit(5) },
  },
  takeUntil: {
    build: (p) => ticks(p, "a", 10, 3).takeUntil(p.source("signal", Stream.fromEffect(sleep(25)))),
    expected: ["a1", "a2"],
    consumerWaitsInRace: true,
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
    if (scenario.consumerWaitsInRace) {
      for (const label of probe.acquired.keys()) {
        expect(probe.finalized.get(label) ?? 0).toBeGreaterThan(0);
      }
    } else {
      expectAllFinalized(probe);
    }
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
            n === 1 ? sleep(1).map(() => n) : ensuring(sleep(100), fail(new TeardownError({}))),
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

  test("a failed first pull stops its fibers before retry starts over", () => {
    const probe = makeProbe();
    let attempts = 0;
    const flaky = Stream.suspend(() =>
      attempts++ === 0
        ? Stream.fromEffect(sleep(3).flatMap(() => fail(new SourceError({}))))
        : Stream.fromEffect(sleep(3).map(() => "x")),
    );
    const run = runVirtual({
      effect: collect(flaky.merge(ticks(probe, "a", 10, 2)).retry({ times: 1 })),
    });

    expect(run.result).toEqual({ ok: true, value: ["x", "a1", "a2"] });
    // the first attempt's driver for "a" would have emitted at 10
    expect([...probe.arrivals]).toEqual([13, 23]);
    expect(probe.acquired.get("a")).toBe(2);
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
