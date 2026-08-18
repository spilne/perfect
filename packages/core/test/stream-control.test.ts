import { describe, expect, test } from "bun:test";
import {
  Cause,
  Deferred,
  Queue,
  Stream,
  TaggedError,
  fail,
  run,
  runExit,
  sleep,
  sync,
} from "../src";

class SignalError extends TaggedError("SignalError")<{
  readonly message: string;
}>() {}

class ObserverError extends TaggedError("ObserverError")<{
  readonly value: number;
}>() {}

describe("Stream.takeUntil", () => {
  test("stops after the signal emits while preserving prior source values", async () => {
    const queue = await run(Queue.unbounded<number>());
    const stop = await run(Deferred.make<void>());
    const seen = await run(Deferred.make<number>());
    const result = run(
      Stream.fromQueue(queue)
        .tapEffect((value) => seen.succeed(value))
        .takeUntil(Stream.fromEffect(stop.await))
        .toArray(),
    );

    await run(queue.offer(1));
    await run(seen.await);
    await run(stop.succeed(undefined));

    expect(await result).toEqual([1]);
  });

  test("an empty signal lets the source finish", async () => {
    expect(await run(Stream.of(1, 2, 3).takeUntil(Stream.empty()).toArray())).toEqual([1, 2, 3]);
  });

  test("a failing signal fails the result", async () => {
    const blocked = await run(Deferred.make<number>());
    const exit = await runExit(
      Stream.fromEffect(blocked.await)
        .takeUntil(Stream.fail(new SignalError({ message: "stop failed" })))
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBeInstanceOf(SignalError);
    }
  });

  test("awaits both signal and source finalizers", async () => {
    const blocked = await run(Deferred.make<number>());
    let sourceFinalized = 0;
    let signalFinalized = 0;
    const source = Stream.fromEffect(blocked.await).onFinalize(sync(() => sourceFinalized++));
    const signal = Stream.succeed(undefined).onFinalize(
      sleep(5).flatMap(() => sync(() => signalFinalized++)),
    );

    expect(await run(source.takeUntil(signal).toArray())).toEqual([]);
    expect(sourceFinalized).toBe(1);
    expect(signalFinalized).toBe(1);
  });
});

describe("Stream.observe", () => {
  test("shares one source and preserves observer batching", async () => {
    let pulls = 0;
    const batches: number[][] = [];
    const source = Stream.unfold(1, (value) => {
      pulls++;
      return value <= 6 ? [value, value + 1] : null;
    });

    const values = await run(
      source
        .observe((stream) =>
          stream.grouped(3).tap((chunk) => {
            batches.push(Array.from(chunk));
          }),
        )
        .toArray(),
    );

    expect(values).toEqual([1, 2, 3, 4, 5, 6]);
    expect(batches).toEqual([
      [1, 2, 3],
      [4, 5, 6],
    ]);
    expect(pulls).toBe(7);
  });

  test("observer failures fail the main stream", async () => {
    const exit = await runExit(
      Stream.of(1, 2, 3)
        .observe((stream) =>
          stream.evalMap((value) =>
            value === 2 ? fail(new ObserverError({ value })) : sync(() => value),
          ),
        )
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toEqual(new ObserverError({ value: 2 }));
    }
  });

  test("downstream completion awaits observer finalization", async () => {
    let finalized = 0;
    const values = await run(
      Stream.of(1, 2, 3)
        .observe((stream) => stream.onFinalize(sleep(5).flatMap(() => sync(() => finalized++))))
        .take(1)
        .toArray(),
    );

    expect(values).toEqual([1]);
    expect(finalized).toBe(1);
  });
});
