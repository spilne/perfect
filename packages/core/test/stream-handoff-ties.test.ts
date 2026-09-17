import { describe, expect, test } from "bun:test";
import {
  Chunk,
  Clock,
  RetryPolicy,
  Stream,
  SyncScheduler,
  TestClock,
  provide,
  runFiber,
  sleep,
  succeed,
  type Eff,
} from "../src";
import type { FiberResult } from "../src/fiber";

// A value arriving at the same virtual instant as a timer. When a queue hands
// the value to a waiting take whose fiber is interrupted before it runs (the
// take lost its race against the timer), the value is lost with that fiber.

const SKIP_REASON = "value handoff lost when taker interrupted — fixed by handoff-safety PR";

const runVirtual = <A>(effect: Eff<A, unknown>): FiberResult<A> | null => {
  const scheduler = new SyncScheduler();
  const clock = new TestClock();
  const fiber = runFiber(provide(effect, Clock, clock) as Eff<A, never>, scheduler);
  scheduler.flush();
  while (fiber.result === null && clock.now() < 500) {
    clock.advance(1);
    scheduler.flush();
  }
  return fiber.result;
};

const values = <A>(result: FiberResult<A> | null): A | "incomplete" | "failed" =>
  result === null ? "incomplete" : result.ok ? result.value : "failed";

describe(`timer ties inside operators (skipped: ${SKIP_REASON})`, () => {
  test.skip("sample ends when its source ends exactly on a sampling boundary", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(20).map(() => 0)).filter(() => false))
      .sample(5);
    expect(values(runVirtual(stream.toArray()))).toEqual([1]);
  });

  test.skip("debounce keeps a value that arrives as the quiet window closes", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .debounce(5);
    const result = values(runVirtual(stream.toArray()));
    expect(Array.isArray(result) && result.at(-1)).toBe(2);
  });

  test.skip("groupWithin keeps an item that arrives at the window deadline", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .groupWithin(10, 5)
      .map((group: Chunk<number>) => group.toArray());
    const result = values(runVirtual(stream.toArray()));
    expect(Array.isArray(result) && result.flat()).toEqual([1, 2]);
  });

  test.skip("audit ends when its source ends exactly as the window closes", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 0)).filter(() => false))
      .audit(5);
    expect(values(runVirtual(stream.toArray()))).toEqual([1]);
  });
});

describe(`timeout(ms).retry() ties (skipped: ${SKIP_REASON})`, () => {
  test.skip("merge keeps an element arriving exactly at the timeout", () => {
    const stream = Stream.fromEffect(sleep(5).map(() => "a"))
      .merge(Stream.fromEffect(sleep(12).map(() => "b")))
      .timeout(5)
      .retry(RetryPolicy.recurs(100));
    const result = values(runVirtual(stream.toArray()));
    expect(Array.isArray(result) && [...result].sort()).toEqual(["a", "b"]);
  });

  const operators = {
    merge: (source: Stream<number>) => source.merge(Stream.fromEffect(sleep(12).map(() => 100))),
    buffer: (source: Stream<number>) => source.buffer(2),
    parEvalMap: (source: Stream<number>) => source.parEvalMap(2, (n) => succeed(n)),
    combineLatest: (source: Stream<number>) => source.combineLatest(Stream.of("z")).map(([n]) => n),
  };
  const expected: Record<keyof typeof operators, number[]> = {
    merge: [0, 1, 2, 3, 100],
    buffer: [0, 1, 2, 3],
    parEvalMap: [0, 1, 2, 3],
    combineLatest: [0, 1, 2, 3],
  };

  for (const [name, build] of Object.entries(operators)) {
    for (const timeoutMs of [3, 4, 5, 6]) {
      test.skip(`${name} with timeout(${timeoutMs}) and arrivals on multiples of 6`, () => {
        let index = 0;
        const source = Stream.tick(6)
          .take(4)
          .map(() => index++);
        const stream = build(source).timeout(timeoutMs).retry(RetryPolicy.recurs(1_000));
        const result = values(runVirtual(stream.toArray()));
        expect(Array.isArray(result) && [...result].sort((a, b) => a - b)).toEqual(
          expected[name as keyof typeof operators],
        );
      });
    }
  }
});
