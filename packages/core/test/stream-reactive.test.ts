import { describe, expect, test } from "bun:test";
import {
  Cause,
  Clock,
  Stream,
  TaggedError,
  TestClock,
  provide,
  run,
  runExit,
  sleep,
  sync,
} from "../src";

class ReactiveError extends TaggedError("ReactiveError")<{
  readonly message: string;
}>() {}

class AsyncIterableError extends TaggedError("AsyncIterableError")<{
  readonly cause: unknown;
}>() {}

const drainScheduler = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) await new Promise((resolve) => setImmediate(resolve));
};

describe("Stream.switchMap", () => {
  test("drains synchronous inner streams before advancing the outer stream", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3)
          .switchMap((value) => Stream.of(value * 10))
          .toArray(),
      ),
    ).toEqual([10, 20, 30]);
  });

  test("cancels and finalizes the previous inner stream", async () => {
    let firstFinalized = false;
    const outer = Stream.of(1).concat(Stream.fromEffect(sleep(10).map(() => 2)));

    const result = await run(
      outer
        .switchMap((value) =>
          Stream.fromEffect(sleep(value === 1 ? 40 : 5).map(() => value * 10)).onFinalize(
            value === 1
              ? sync(() => {
                  firstFinalized = true;
                })
              : sync(() => undefined),
          ),
        )
        .toArray(),
    );

    expect(result).toEqual([20]);
    expect(firstFinalized).toBe(true);
  });
});

describe("Stream.exhaustMap", () => {
  test("accepts each outer value when synchronous inner streams finish immediately", async () => {
    expect(
      await run(
        Stream.of(1, 2, 3)
          .exhaustMap((value) => Stream.of(value * 10))
          .toArray(),
      ),
    ).toEqual([10, 20, 30]);
  });

  test("ignores outer values while the current inner stream is active", async () => {
    const outer = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .concat(Stream.fromEffect(sleep(30).map(() => 3)));

    const result = await run(
      outer.exhaustMap((value) => Stream.fromEffect(sleep(20).map(() => value * 10))).toArray(),
    );

    expect(result).toEqual([10, 30]);
  });
});

describe("Stream.combineLatest", () => {
  test("emits whenever either initialized side changes", async () => {
    const left = Stream.fromEffect(sleep(10).map(() => 1)).concat(
      Stream.fromEffect(sleep(20).map(() => 2)),
    );
    const right = Stream.fromEffect(sleep(20).map(() => "a")).concat(
      Stream.fromEffect(sleep(20).map(() => "b")),
    );

    expect(await run(left.combineLatest(right).toArray())).toEqual([
      [1, "a"],
      [2, "a"],
      [2, "b"],
    ]);
  });

  test("completes immediately when one side ends before producing a value", async () => {
    let rightFinalized = false;
    const right = Stream.repeat(sleep(100).map(() => 1)).onFinalize(
      sync(() => {
        rightFinalized = true;
      }),
    );

    expect(await run(Stream.empty<number>().combineLatest(right).toArray())).toEqual([]);
    expect(rightFinalized).toBe(true);
  });
});

describe("Stream.withLatest", () => {
  test("emits only on the main stream using the latest side value", async () => {
    const main = Stream.fromEffect(sleep(10).map(() => 1))
      .concat(Stream.fromEffect(sleep(10).map(() => 2)))
      .concat(Stream.fromEffect(sleep(10).map(() => 3)));
    const side = Stream.fromEffect(sleep(5).map(() => "a")).concat(
      Stream.fromEffect(sleep(20).map(() => "b")),
    );

    expect(await run(main.withLatest(side).toArray())).toEqual([
      [1, "a"],
      [2, "a"],
      [3, "b"],
    ]);
  });

  test("cancels the side stream when the main stream completes", async () => {
    let sideFinalized = false;
    const side = Stream.of("ready")
      .concat(Stream.repeat(sleep(100).map(() => "later")))
      .onFinalize(
        sync(() => {
          sideFinalized = true;
        }),
      );

    expect(
      await run(
        Stream.fromEffect(sleep(5).map(() => 1))
          .withLatest(side)
          .toArray(),
      ),
    ).toEqual([[1, "ready"]]);
    expect(sideFinalized).toBe(true);
  });
});

describe("Stream.sample", () => {
  test("uses Clock and emits the latest value at the sampling boundary", async () => {
    const clock = new TestClock();
    let value = 0;
    const program = provide(
      Stream.tick(10)
        .map(() => ++value)
        .sample(25)
        .take(1)
        .toArray(),
      Clock,
      clock,
    );

    const result = run(program);
    await drainScheduler();
    clock.advance(10);
    await drainScheduler();
    clock.advance(10);
    await drainScheduler();
    clock.advance(5);

    expect(await result).toEqual([2]);
  });
});

describe("Stream.audit", () => {
  test("uses Clock and emits the last value without resetting the open window", async () => {
    const clock = new TestClock();
    let value = 0;
    const program = provide(
      Stream.tick(10)
        .map(() => ++value)
        .audit(25)
        .take(1)
        .toArray(),
      Clock,
      clock,
    );

    const result = run(program);
    await drainScheduler();
    clock.advance(10);
    await drainScheduler();
    clock.advance(10);
    await drainScheduler();
    clock.advance(10);
    await drainScheduler();
    clock.advance(5);

    expect(await result).toEqual([3]);
  });

  test("flushes the pending value when the source completes", async () => {
    expect(await run(Stream.of(1, 2, 3).audit(100).toArray())).toEqual([3]);
  });
});

describe("reactive operator failures", () => {
  test("preserve failures from every concurrent source and inner stream", async () => {
    const error = new ReactiveError({ message: "reactive failed" });
    const streams = [
      Stream.of(1).switchMap(() => Stream.fail(error)),
      Stream.of(1).exhaustMap(() => Stream.fail(error)),
      Stream.of(1).combineLatest(Stream.fail(error)),
      Stream.fromEffect(sleep(100).map(() => 1)).withLatest(Stream.fail(error)),
      Stream.fail(error).sample(10),
      Stream.fail(error).audit(10),
    ];

    for (const stream of streams) {
      const exit = await runExit(stream.toArray());
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") expect(Cause.failures(exit.cause)).toContain(error);
    }
  });
});

describe("Stream.fromAsyncIterable", () => {
  test("is pull-based and calls return when downstream stops early", async () => {
    let pulls = 0;
    let finalized = false;
    async function* values(): AsyncGenerator<number> {
      try {
        for (let value = 1; value <= 5; value++) {
          pulls++;
          yield value;
        }
      } finally {
        finalized = true;
      }
    }

    const result = await run(
      Stream.fromAsyncIterable(values(), (cause) => new AsyncIterableError({ cause }))
        .take(2)
        .toArray()
        .orDie(),
    );

    expect(result).toEqual([1, 2]);
    expect(pulls).toBe(2);
    expect(finalized).toBe(true);
  });

  test("maps iterator rejection into the typed error channel", async () => {
    const source: AsyncIterable<number> = {
      [Symbol.asyncIterator]() {
        return {
          next: async () => {
            throw new Error("iterator failed");
          },
        };
      },
    };

    const exit = await runExit(
      Stream.fromAsyncIterable(source, (cause) => new AsyncIterableError({ cause })).toArray(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.failures(exit.cause)[0]).toBeInstanceOf(AsyncIterableError);
    }
  });

  test("maps synchronous iterator acquisition errors into the typed error channel", async () => {
    const source: AsyncIterable<number> = {
      [Symbol.asyncIterator](): AsyncIterator<number> {
        throw new Error("iterator acquisition failed");
      },
    };

    const exit = await runExit(
      Stream.fromAsyncIterable(source, (cause) => new AsyncIterableError({ cause })).toArray(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.failures(exit.cause)[0]).toBeInstanceOf(AsyncIterableError);
    }
  });
});
