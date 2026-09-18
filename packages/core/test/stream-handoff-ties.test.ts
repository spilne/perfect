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

// A value arriving at the same virtual instant as a timer. A queue can hand
// the value to a waiting take whose fiber is interrupted before it runs, when
// the take lost its race against the timer. The value must go back to the
// queue, not be lost with that fiber.

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

describe("timer ties inside operators", () => {
  test("sample ends when its source ends exactly on a sampling boundary", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(20).map(() => 0)).filter(() => false))
      .sample(5);
    expect(values(runVirtual(stream.toArray()))).toEqual([1]);
  });

  test("debounce keeps a value that arrives as the quiet window closes", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .debounce(5);
    const result = values(runVirtual(stream.toArray()));
    expect(Array.isArray(result) && result.at(-1)).toBe(2);
  });

  test("groupWithin keeps an item that arrives at the window deadline", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .groupWithin(10, 5)
      .map((group: Chunk<number>) => group.toArray());
    const result = values(runVirtual(stream.toArray()));
    expect(Array.isArray(result) && result.flat()).toEqual([1, 2]);
  });

  test("audit ends when its source ends exactly as the window closes", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 0)).filter(() => false))
      .audit(5);
    expect(values(runVirtual(stream.toArray()))).toEqual([1]);
  });
});

describe("timeout(ms).retry() ties", () => {
  test("merge keeps an element arriving exactly at the timeout", () => {
    const stream = Stream.fromEffect(sleep(5).map(() => "a"))
      .merge(Stream.fromEffect(sleep(12).map(() => "b")))
      .timeout(5)
      .retry(RetryPolicy.recurs(100));
    const result = values(runVirtual(stream.toArray()));
    expect(Array.isArray(result) && [...result].sort()).toEqual(["a", "b"]);
  });

  const operators = {
    merge: (source: Stream<number>) => source.merge(Stream.fromEffect(sleep(12).map(() => 100))),
    mergeAll: (source: Stream<number>) =>
      Stream.mergeAll(source, Stream.fromEffect(sleep(12).map(() => 100)), Stream.empty<number>()),
    buffer: (source: Stream<number>) => source.buffer(2),
    parEvalMap: (source: Stream<number>) => source.parEvalMap(2, (n) => succeed(n)),
    parEvalMapUnordered: (source: Stream<number>) =>
      source.parEvalMapUnordered(2, (n) => succeed(n)),
    combineLatest: (source: Stream<number>) => source.combineLatest(Stream.of("z")).map(([n]) => n),
    withLatest: (source: Stream<number>) =>
      Stream.fromEffect(sleep(1))
        .flatMap(() => source)
        .withLatest(Stream.of("z"))
        .map(([n]) => n),
    switchMap: (source: Stream<number>) => source.switchMap((n) => Stream.of(n)),
    exhaustMap: (source: Stream<number>) => source.exhaustMap((n) => Stream.of(n)),
    broadcastThrough: (source: Stream<number>) =>
      source.broadcastThrough(
        (s) => s.map((n) => n * 10),
        (s) => s,
      ),
    observe: (source: Stream<number>) => source.observe((s) => s),
  };
  const expected: Record<keyof typeof operators, number[]> = {
    merge: [0, 1, 2, 3, 100],
    mergeAll: [0, 1, 2, 3, 100],
    buffer: [0, 1, 2, 3],
    parEvalMap: [0, 1, 2, 3],
    parEvalMapUnordered: [0, 1, 2, 3],
    combineLatest: [0, 1, 2, 3],
    withLatest: [0, 1, 2, 3],
    switchMap: [0, 1, 2, 3],
    exhaustMap: [0, 1, 2, 3],
    broadcastThrough: [0, 0, 1, 2, 3, 10, 20, 30],
    observe: [0, 1, 2, 3],
  };

  for (const [name, build] of Object.entries(operators)) {
    for (const timeoutMs of [3, 4, 5, 6]) {
      test(`${name} with timeout(${timeoutMs}) and arrivals on multiples of 6`, () => {
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
