import { describe, expect, test } from "bun:test";
import {
  type Eff,
  Stream,
  TaggedError,
  acquireRelease,
  ensuring,
  sleep,
  succeed,
  sync,
} from "../src";

class SourceError extends TaggedError("SourceError")<{
  readonly message: string;
}>() {}

const DONE = { done: true, value: undefined };

const nextTick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

const drainScheduler = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) await nextTick();
};

const collect = async <A>(iterable: AsyncIterable<A>): Promise<A[]> => {
  const values: A[] = [];
  for await (const value of iterable) values.push(value);
  return values;
};

const batches = (pulls: { count: number }, source: readonly number[][]): Stream<number> =>
  Stream.unfoldEffect(0, (index) =>
    sync((): [readonly number[], number] | null => {
      if (index >= source.length) return null;
      pulls.count++;
      return [source[index]!, index + 1];
    }),
  ).flatMap((batch) => Stream.fromArray(batch));

const stalled = <A>(onInterrupt: () => void): Stream<A> =>
  Stream.fromEffect(ensuring(sleep(60_000), sync(onInterrupt))).filterMap(
    (): A | undefined => undefined,
  );

describe("Stream.toAsyncIterable", () => {
  test("iterates every element and finalizes before the loop ends", async () => {
    const events: string[] = [];
    const stream = Stream.of(1, 2, 3)
      .map((value) => value * 10)
      .onFinalize(sync(() => events.push("finalized")));

    const values: number[] = [];
    for await (const value of stream.toAsyncIterable()) {
      values.push(value);
      expect(events).toEqual([]);
    }

    expect(values).toEqual([10, 20, 30]);
    expect(events).toEqual(["finalized"]);
  });

  test("an empty stream completes immediately and still finalizes", async () => {
    let finalized = 0;
    const iterator = Stream.empty<number>()
      .onFinalize(sync(() => finalized++))
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    expect(await iterator.next()).toEqual({ done: true, value: undefined });
    expect(finalized).toBe(1);
    expect(await iterator.next()).toEqual({ done: true, value: undefined });
  });

  test("serves buffered chunk elements without pulling upstream again", async () => {
    const pulls = { count: 0 };
    const iterator = batches(pulls, [
      [1, 2, 3],
      [4, 5],
    ])
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    await drainScheduler();
    expect(pulls.count).toBe(0);

    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    expect(pulls.count).toBe(1);
    expect(await iterator.next()).toEqual({ done: false, value: 2 });
    expect(await iterator.next()).toEqual({ done: false, value: 3 });
    await drainScheduler();
    expect(pulls.count).toBe(1);

    expect(await iterator.next()).toEqual({ done: false, value: 4 });
    expect(pulls.count).toBe(2);
    expect(await iterator.next()).toEqual({ done: false, value: 5 });
    expect(await iterator.next()).toEqual({ done: true, value: undefined });
  });

  test("iterates large chunked streams in order", async () => {
    const values = await collect(Stream.range(0, 10_000).toAsyncIterable());
    expect(values).toHaveLength(10_000);
    expect(values[0]).toBe(0);
    expect(values[9_999]).toBe(9_999);
  });

  test("queues concurrent next calls in order", async () => {
    const iterator = Stream.iterate(0, (n) => n + 1)
      .take(3)
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    const results = await Promise.all([
      iterator.next(),
      iterator.next(),
      iterator.next(),
      iterator.next(),
    ]);

    expect(results).toEqual([
      { done: false, value: 0 },
      { done: false, value: 1 },
      { done: false, value: 2 },
      { done: true, value: undefined },
    ]);
  });

  test("settles next calls in call order, including calls from resolution callbacks", async () => {
    for (const source of [Stream.of(0, 1, 2), Stream.iterate(0, (n) => n + 1).take(3)]) {
      const iterator = source.toAsyncIterable()[Symbol.asyncIterator]();
      let late: Promise<IteratorResult<number>> | undefined;
      const first = iterator.next();
      void first.then(() => {
        late = iterator.next();
      });
      const second = iterator.next();

      expect(await first).toEqual({ done: false, value: 0 });
      expect(await second).toEqual({ done: false, value: 1 });
      expect(await late).toEqual({ done: false, value: 2 });
      expect(await iterator.next()).toEqual(DONE);
    }
  });

  test("concurrent consumers each receive the next element in call order", async () => {
    const iterator = Stream.range(0, 20).rechunk(3).toAsyncIterable()[Symbol.asyncIterator]();
    const seen: number[] = [];
    const worker = async (received: number[]) => {
      for (let result = await iterator.next(); !result.done; result = await iterator.next()) {
        seen.push(result.value);
        received.push(result.value);
      }
    };
    const first: number[] = [];
    const second: number[] = [];

    await Promise.all([worker(first), worker(second)]);

    const all = [...Array(20).keys()];
    expect(seen).toEqual(all);
    expect(first).toEqual(all.filter((value) => value % 2 === 0));
    expect(second).toEqual(all.filter((value) => value % 2 === 1));
  });

  test("the iterator is itself async iterable", async () => {
    const iterator = Stream.of(1, 2, 3)
      .toAsyncIterable()
      [Symbol.asyncIterator]() as AsyncIterableIterator<number>;

    expect(iterator[Symbol.asyncIterator]()).toBe(iterator);
    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    const rest: number[] = [];
    for await (const value of iterator) rest.push(value);
    expect(rest).toEqual([2, 3]);
  });

  test("each iterator runs the stream again", async () => {
    let finalized = 0;
    const iterable = Stream.of("a", "b")
      .onFinalize(sync(() => finalized++))
      .toAsyncIterable();

    expect(await collect(iterable)).toEqual(["a", "b"]);
    expect(await collect(iterable)).toEqual(["a", "b"]);
    expect(finalized).toBe(2);
  });

  test("break stops pulling and runs finalizers, ensuring, and scoped releases", async () => {
    const events: string[] = [];
    const pulls = { count: 0 };
    const stream = Stream.fromEffect(
      acquireRelease(succeed("connection"), () => sync(() => events.push("released"))),
    )
      .flatMap(() =>
        batches(pulls, [[1, 2], [3], [4]]).onFinalize(sync(() => events.push("inner finalized"))),
      )
      .onFinalize(sync(() => events.push("outer finalized")));

    const values: number[] = [];
    for await (const value of stream.toAsyncIterable()) {
      values.push(value);
      if (value === 3) break;
    }

    expect(values).toEqual([1, 2, 3]);
    expect(pulls.count).toBe(2);
    expect(events).toEqual(["inner finalized", "outer finalized", "released"]);
  });

  test("a throw in the loop body finalizes the stream and propagates", async () => {
    let finalized = 0;
    const stream = Stream.range(0, 100).onFinalize(sync(() => finalized++));

    const consume = async () => {
      for await (const value of stream.toAsyncIterable()) {
        if (value === 2) throw new Error("consumer failed");
      }
    };

    await expect(consume()).rejects.toThrow("consumer failed");
    expect(finalized).toBe(1);
  });

  test("return before the first pull never starts the stream", async () => {
    const pulls = { count: 0 };
    const iterator = batches(pulls, [[1]])
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    expect(await iterator.return!("stopped")).toEqual({ done: true, value: "stopped" });
    expect(await iterator.next()).toEqual({ done: true, value: undefined });
    expect(pulls.count).toBe(0);
  });

  test("return right after the first next runs finalizers before resolving", async () => {
    let finalized = 0;
    const iterator = Stream.of(1)
      .onFinalize(sync(() => finalized++))
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    expect(await iterator.return!()).toEqual(DONE);
    expect(finalized).toBe(1);
    expect(await pending).toEqual(DONE);
  });

  test("return during an in-flight pull interrupts it and finalizes", async () => {
    const events: string[] = [];
    const iterator = stalled<number>(() => events.push("pull interrupted"))
      .onFinalize(sync(() => events.push("finalized")))
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await drainScheduler();
    expect(events).toEqual([]);

    expect(await iterator.return!()).toEqual({ done: true, value: undefined });
    expect(await pending).toEqual({ done: true, value: undefined });
    expect(events).toEqual(["pull interrupted", "finalized"]);
  });

  test("early return interrupts merged upstream drivers", async () => {
    const events: string[] = [];
    const merged = Stream.of(1)
      .merge(stalled(() => events.push("slow side interrupted")))
      .onFinalize(sync(() => events.push("finalized")));

    for await (const value of merged.toAsyncIterable()) {
      expect(value).toBe(1);
      break;
    }

    expect(events).toEqual(["slow side interrupted", "finalized"]);
  });

  test("early return interrupts parEvalMap workers", async () => {
    const interrupted: number[] = [];
    const stream = Stream.of(1, 2, 3).parEvalMap(3, (value) =>
      value === 1
        ? succeed(value)
        : ensuring(
            sleep(60_000),
            sync(() => interrupted.push(value)),
          ).map(() => value),
    );

    for await (const value of stream.toAsyncIterable()) {
      expect(value).toBe(1);
      break;
    }

    expect(interrupted.sort()).toEqual([2, 3]);
  });

  test("typed failures reject next with the original error after finalizers", async () => {
    const error = new SourceError({ message: "boom" });
    const events: string[] = [];
    const stream = Stream.of(1)
      .concat(Stream.fail(error))
      .onFinalize(sync(() => events.push("finalized")))
      .orDie();

    const values: number[] = [];
    const iterator = stream.toAsyncIterable()[Symbol.asyncIterator]();
    values.push((await iterator.next()).value);

    const rejection = await iterator.next().then(
      () => null,
      (reason: unknown) => reason,
    );
    expect(rejection).toBe(error);
    expect(values).toEqual([1]);
    expect(events).toEqual(["finalized"]);
    expect(await iterator.next()).toEqual({ done: true, value: undefined });
  });

  test("defects reject the loop with the thrown value", async () => {
    const defect = new Error("defect");
    let finalized = 0;
    const stream = Stream.of(1)
      .concat(
        Stream.fromEffect(
          sync((): number => {
            throw defect;
          }),
        ),
      )
      .onFinalize(sync(() => finalized++));

    const values: number[] = [];
    const consume = async () => {
      for await (const value of stream.toAsyncIterable()) values.push(value);
    };

    await expect(consume()).rejects.toBe(defect);
    expect(values).toEqual([1]);
    expect(finalized).toBe(1);
  });

  test("a finalizer failing on completion rejects next", async () => {
    const failure = new Error("close failed");
    const iterator = Stream.of(1)
      .onFinalize(
        sync(() => {
          throw failure;
        }),
      )
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    await expect(iterator.next()).rejects.toBe(failure);
    expect(await iterator.next()).toEqual(DONE);
  });

  test("a failing finalizer rejects the first return only", async () => {
    const failure = new Error("release failed");
    const iterator = Stream.of(1, 2)
      .onFinalize(
        sync(() => {
          throw failure;
        }),
      )
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    await expect(iterator.return!()).rejects.toBe(failure);
    expect(await iterator.return!("again")).toEqual({ done: true, value: "again" });
    expect(await iterator.next()).toEqual(DONE);
  });

  // Skipped until the runtime ignores a late resume from a canceler-less async
  // op (such as a fromAsyncIterable pull) after the fiber was interrupted.
  test.skip("return during a pending async-iterable pull resolves after source cleanup", async () => {
    const events: string[] = [];
    async function* source() {
      try {
        yield 1;
        await nextTick();
        yield 2;
      } finally {
        await nextTick();
        events.push("cleaned up");
      }
    }
    const iterator = Stream.fromAsyncIterable(source(), (cause) => cause)
      .orDie()
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    const pending = iterator.next();
    expect(await iterator.return!()).toEqual(DONE);
    expect(events).toEqual(["cleaned up"]);
    expect(await pending).toEqual(DONE);
  });

  // Skipped until interrupting a fiber that is already scheduled as Ready no
  // longer schedules a second run loop.
  test.skip("return during a CPU-heavy pull runs the finalizer once before resolving", async () => {
    let finalized = 0;
    let heavy: Eff<number> = succeed(0);
    for (let i = 0; i < 10_000; i++) heavy = heavy.flatMap((n) => succeed(n + 1));
    const iterator = Stream.fromEffect(heavy)
      .onFinalize(sleep(10).flatMap(() => sync(() => finalized++)))
      .toAsyncIterable()
      [Symbol.asyncIterator]();

    void iterator.next();
    await nextTick();
    expect(await iterator.return!()).toEqual(DONE);
    expect(finalized).toBe(1);
    await drainScheduler();
    expect(finalized).toBe(1);
  });
});
