import { describe, expect, test } from "bun:test";
import {
  Clock,
  Stream,
  SyncScheduler,
  TestClock,
  provide,
  run,
  runFiber,
  sleep,
  succeed,
  sync,
  type Eff,
} from "../src";
import type { FiberResult } from "../src/fiber";

// Count and window arguments used to be clamped with Math.max(1, Math.floor(x)):
// NaN stayed NaN (a semaphore that never grants, a window that never closes)
// and 0 or -1 silently became 1. Invalid values now throw when the operator is
// built.

/** Run on a SyncScheduler with a TestClock stepped 1 ms at a time. */
const runVirtual = <A>(effect: Eff<A, unknown>): FiberResult<A> | null => {
  const scheduler = new SyncScheduler();
  const clock = new TestClock();
  const fiber = runFiber(provide(effect, Clock, clock) as Eff<A, never>, scheduler);
  scheduler.flush();
  while (fiber.result === null && clock.now() < 200) {
    clock.advance(1);
    scheduler.flush();
  }
  return fiber.result;
};

const NOT_POSITIVE_INTEGERS = [0, -1, 1.5, Number.NaN, -Infinity];
const NOT_DURATIONS = [-1, Number.NaN, Infinity, -Infinity];

describe.each(["parEvalMap", "parEvalMapUnordered"] as const)("%s concurrency", (method) => {
  test.each(NOT_POSITIVE_INTEGERS)("rejects %p", (concurrency) => {
    expect(() => Stream.of(1)[method](concurrency, (n) => succeed(n))).toThrow(
      new RangeError(
        `${method}: concurrency must be a positive integer or Infinity, got ${String(concurrency)}`,
      ),
    );
  });

  // Virtual time: on the real clock a loaded machine can let a 5 ms worker
  // finish before the last one starts.
  test.each([Infinity, 2 ** 60])("runs every element at once with %p", (concurrency) => {
    let inFlight = 0;
    let maxInFlight = 0;
    const result = runVirtual(
      Stream.range(0, 9)
        [method](concurrency, (n) =>
          sync(() => {
            maxInFlight = Math.max(maxInFlight, ++inFlight);
          })
            .flatMap(() => sleep(n % 3 === 0 ? 20 : 5))
            .map(() => {
              inFlight--;
              return n;
            }),
        )
        .toArray(),
    );

    expect(result?.ok).toBe(true);
    const values = result?.ok ? result.value : [];
    expect([...values].sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8]);
    if (method === "parEvalMap") expect(values).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8]);
    expect(maxInFlight).toBe(9);
  });

  test("still bounds a finite concurrency", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    await run(
      Stream.range(0, 9)
        [method](2, (n) =>
          sync(() => {
            maxInFlight = Math.max(maxInFlight, ++inFlight);
          })
            .flatMap(() => sleep(2))
            .map(() => {
              inFlight--;
              return n;
            }),
        )
        .drain(),
    );

    expect(maxInFlight).toBe(2);
  });
});

describe("buffer capacity", () => {
  test.each(NOT_POSITIVE_INTEGERS)("rejects %p", (capacity) => {
    expect(() => Stream.of(1).buffer(capacity)).toThrow(
      new RangeError(
        `buffer: capacity must be a positive integer or Infinity, got ${String(capacity)}`,
      ),
    );
  });

  test("accepts Infinity", async () => {
    expect(await run(Stream.range(0, 5).buffer(Infinity).toArray())).toEqual([0, 1, 2, 3, 4]);
  });
});

describe("groupWithin maxSize", () => {
  test.each(NOT_POSITIVE_INTEGERS)("rejects %p", (maxSize) => {
    expect(() => Stream.of(1).groupWithin(maxSize, 10)).toThrow(
      new RangeError(
        `groupWithin: maxSize must be a positive integer or Infinity, got ${String(maxSize)}`,
      ),
    );
  });

  test("Infinity groups by time only", () => {
    const sizes = Stream.tick(10)
      .take(5)
      .groupWithin(Infinity, 25)
      .map((group) => group.length);

    expect(runVirtual(sizes.toArray())).toEqual({ ok: true, value: [3, 2] });
  });
});

describe("sliding", () => {
  test.each([...NOT_POSITIVE_INTEGERS, Infinity])("rejects size %p", (size) => {
    expect(() => Stream.of(1).sliding(size)).toThrow(
      new RangeError(`sliding: size must be a positive integer, got ${String(size)}`),
    );
  });

  test.each([...NOT_POSITIVE_INTEGERS, Infinity])("rejects step %p", (step) => {
    expect(() => Stream.of(1).sliding(2, step)).toThrow(
      new RangeError(`sliding: step must be a positive integer, got ${String(step)}`),
    );
  });
});

// One value, then a source that stays open, so a 1 ms window closes before it ends.
const lingering = Stream.of(1).concat(Stream.fromEffect(sleep(1_000).map(() => 2)));

describe.each([
  { operator: "sample", name: "intervalMs", build: (ms: number) => lingering.sample(ms) },
  { operator: "audit", name: "ms", build: (ms: number) => lingering.audit(ms) },
  {
    operator: "pauseWhen",
    name: "pollMs",
    build: (ms: number) => lingering.pauseWhen({ get: succeed(false) }, ms),
  },
])("$operator window", ({ operator, name, build }) => {
  test.each(NOT_DURATIONS)("rejects %p", (ms) => {
    expect(() => build(ms)).toThrow(
      new RangeError(
        `${operator}: ${name} must be a finite, non-negative number of milliseconds, got ${String(ms)}`,
      ),
    );
  });

  test("accepts 0 and runs with a 1 ms window", () => {
    expect(runVirtual(build(0).take(1).toArray())).toEqual({ ok: true, value: [1] });
  });
});

describe.each([
  { operator: "debounce", name: "ms", build: (ms: number) => Stream.of(1).debounce(ms) },
  {
    operator: "groupWithin",
    name: "timeoutMs",
    build: (ms: number) =>
      Stream.of(1)
        .groupWithin(10, ms)
        .map((group) => group.length),
  },
  { operator: "throttle", name: "ms", build: (ms: number) => Stream.of(1).throttle(ms) },
  { operator: "throttle", name: "ms", build: (ms: number) => Stream.of(1).metered(ms) },
  { operator: "spaced", name: "ms", build: (ms: number) => Stream.of(1).spaced(ms) },
  {
    operator: "Stream.tick",
    name: "intervalMs",
    build: (ms: number) =>
      Stream.tick(ms)
        .take(1)
        .map(() => 1),
  },
  { operator: "timeout", name: "ms", build: (ms: number) => Stream.of(1).timeout(ms) },
  { operator: "deadline", name: "ms", build: (ms: number) => Stream.of(1).deadline(ms) },
  { operator: "deadline", name: "ms", build: (ms: number) => Stream.of(1).timeoutTotal(ms) },
  {
    operator: "interruptAfter",
    name: "ms",
    build: (ms: number) => Stream.of(1).interruptAfter(ms),
  },
])("$operator duration", ({ operator, name, build }) => {
  test.each(NOT_DURATIONS)("rejects %p", (ms) => {
    expect(() => build(ms)).toThrow(
      new RangeError(
        `${operator}: ${name} must be a finite, non-negative number of milliseconds, got ${String(ms)}`,
      ),
    );
  });

  test("accepts a finite duration", () => {
    expect(runVirtual(build(5).toArray())).toEqual({ ok: true, value: [1] });
  });
});
