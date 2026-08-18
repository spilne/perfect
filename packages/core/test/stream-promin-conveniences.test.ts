import { describe, expect, test } from "bun:test";
import {
  Cause,
  Clock,
  Ref,
  Stream,
  StreamDeadlineError,
  TestClock,
  provide,
  run,
  runExit,
  sleep,
  sync,
} from "../src";

const drainScheduler = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) await new Promise((resolve) => setImmediate(resolve));
};

describe("Promin stream conveniences", () => {
  test("metered emits immediately and then paces values", async () => {
    const clock = new TestClock();
    const emittedAt: number[] = [];
    const done = run(
      provide(
        Stream.of(1, 2, 3)
          .metered(100)
          .tap(() => emittedAt.push(clock.now()))
          .toArray(),
        Clock,
        clock,
      ),
    );

    await drainScheduler();
    expect(emittedAt).toEqual([0]);
    clock.advance(100);
    await drainScheduler();
    expect(emittedAt).toEqual([0, 100]);
    clock.advance(100);
    expect(await done).toEqual([1, 2, 3]);
    expect(emittedAt).toEqual([0, 100, 200]);
  });

  test("spaced delays every value including the first", async () => {
    const clock = new TestClock();
    const emittedAt: number[] = [];
    const done = run(
      provide(
        Stream.of(1, 2)
          .spaced(100)
          .tap(() => emittedAt.push(clock.now()))
          .toArray(),
        Clock,
        clock,
      ),
    );

    await drainScheduler();
    expect(emittedAt).toEqual([]);
    clock.advance(100);
    await drainScheduler();
    expect(emittedAt).toEqual([100]);
    clock.advance(100);
    expect(await done).toEqual([1, 2]);
    expect(emittedAt).toEqual([100, 200]);
  });

  test("collectFirst and collectWhile stop early", async () => {
    expect(await run(Stream.of(1, 2, 3, 4).collectFirst((value) => value > 2))).toBe(3);
    expect(await run(Stream.of(1, 2, 3, 1).collectWhile((value) => value < 3))).toEqual([1, 2]);
  });

  test("repeatN and repeatForever reacquire their factories", async () => {
    let finiteBuilds = 0;
    expect(
      await run(
        Stream.repeatN(() => {
          finiteBuilds++;
          return Stream.of(finiteBuilds);
        }, 3).toArray(),
      ),
    ).toEqual([1, 2, 3]);

    let foreverBuilds = 0;
    expect(
      await run(
        Stream.repeatForever(() => Stream.of(++foreverBuilds))
          .take(3)
          .toArray(),
      ),
    ).toEqual([1, 2, 3]);
  });

  test("mergeAll merges every input stream", async () => {
    const values = await run(
      Stream.mergeAll(Stream.of(1, 2), Stream.of(3), Stream.of(4, 5)).toArray(),
    );
    expect(values.sort()).toEqual([1, 2, 3, 4, 5]);
  });

  test("tapEffectFork starts detached effects without changing values", async () => {
    const observed: number[] = [];
    expect(
      await run(
        Stream.of(1, 2, 3)
          .tapEffectFork((value) => sync(() => observed.push(value)))
          .toArray(),
      ),
    ).toEqual([1, 2, 3]);
    await drainScheduler();
    expect(observed.sort()).toEqual([1, 2, 3]);
  });

  test("pauseWhen resumes through a shared Ref contract", async () => {
    const clock = new TestClock();
    const paused = await run(Ref.make(true));
    const done = run(provide(Stream.of(1, 2).pauseWhen(paused, 50).toArray(), Clock, clock));

    await drainScheduler();
    expect(clock.pendingCount).toBe(1);
    await run(paused.set(false));
    clock.advance(50);
    expect(await done).toEqual([1, 2]);
  });
});

describe("whole-stream deadline", () => {
  test("fails once total elapsed time is exhausted", async () => {
    const clock = new TestClock();
    const source = Stream.fromEffect(sleep(60).map(() => 1)).concat(
      Stream.fromEffect(sleep(60).map(() => 2)),
    );
    const done = runExit(provide(source.deadline(100).toArray(), Clock, clock));

    await drainScheduler();
    clock.advance(60);
    await drainScheduler();
    clock.advance(40);
    const exit = await done;
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const error = Cause.firstFail(exit.cause)?.value;
      expect(error).toBeInstanceOf(StreamDeadlineError);
      expect((error as StreamDeadlineError).ms).toBe(100);
    }
  });

  test("allows a stream that completes within the total deadline", async () => {
    const clock = new TestClock();
    expect(await run(provide(Stream.of(1, 2).timeoutTotal(100).toArray(), Clock, clock))).toEqual([
      1, 2,
    ]);
  });
});

describe("full-Cause conveniences", () => {
  class RetryableDefect extends Error {}

  test("tapAnyError observes typed errors and defects without replacing them", async () => {
    const observed: unknown[] = [];
    const tap = (error: unknown) => sync(() => observed.push(error));

    const typed = await runExit(Stream.fail("typed").tapAnyError(tap).toArray());
    const defect = await runExit(
      Stream.fromEffect(
        sync(() => {
          throw new RetryableDefect("defect");
        }),
      )
        .tapAnyError(tap)
        .toArray(),
    );

    expect(typed._tag).toBe("Failure");
    expect(defect._tag).toBe("Failure");
    expect(observed[0]).toBe("typed");
    expect(observed[1]).toBeInstanceOf(RetryableDefect);
  });

  test("trapError moves matching defects into the typed channel", async () => {
    const exit = await runExit(
      Stream.fromEffect(
        sync(() => {
          throw new RetryableDefect("retry me");
        }),
      )
        .trapError(RetryableDefect)
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBeInstanceOf(RetryableDefect);
      expect(Cause.hasDie(exit.cause)).toBe(false);
    }
  });

  test("trapError leaves unmatched defects as defects", async () => {
    const exit = await runExit(
      Stream.fromEffect(
        sync(() => {
          throw new TypeError("not retryable");
        }),
      )
        .trapError(RetryableDefect)
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasFail(exit.cause)).toBe(false);
      expect(Cause.firstDie(exit.cause)?.value).toBeInstanceOf(TypeError);
    }
  });
});
