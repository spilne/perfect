import { describe, expect, test } from "bun:test";
import {
  Cause,
  Deferred,
  Stream,
  TaggedError,
  awaitFiber,
  fail,
  run,
  runExit,
  runFiber,
  sleep,
  sync,
} from "../src";

class BranchFailure extends TaggedError("BranchFailure")<{}>() {}

describe("Stream.broadcastThrough", () => {
  test("pulls the source once and fans every item out to every branch", async () => {
    let pulls = 0;
    const seenA: number[] = [];
    const seenB: number[] = [];
    const source = Stream.unfoldEffect(1, (next) =>
      sync(() => {
        if (next > 3) return null;
        pulls++;
        return [next, next + 1] as [number, number];
      }),
    );

    const output = await run(
      source
        .broadcastThrough(
          (stream) => stream.tap((value) => seenA.push(value)).map((value) => value * 10),
          (stream) => stream.tap((value) => seenB.push(value)).map((value) => value * 100),
        )
        .toArray(),
    );

    expect(pulls).toBe(3);
    expect(seenA).toEqual([1, 2, 3]);
    expect(seenB).toEqual([1, 2, 3]);
    expect(output.sort((a, b) => a - b)).toEqual([10, 20, 30, 100, 200, 300]);
  });

  test("an early-ending branch unsubscribes without blocking the other branches", async () => {
    const output = await run(
      Stream.unfold(1, (next) => (next > 5 ? null : [next, next + 1]))
        .broadcastThrough(
          (stream) => stream.take(1),
          (stream) => stream,
        )
        .toArray(),
    );

    expect(output.sort((a, b) => a - b)).toEqual([1, 1, 2, 3, 4, 5]);
  });

  test("the slowest branch backpressures the single upstream driver", async () => {
    const gate = await run(Deferred.make<void>());
    let pulls = 0;
    const source = Stream.unfoldEffect(1, (next) =>
      sync(() => {
        if (next > 20) return null;
        pulls++;
        return [next, next + 1] as [number, number];
      }),
    );
    const fiber = runFiber(
      source
        .broadcastThrough(
          (stream) => stream.evalMap((value) => gate.await.map(() => value)),
          (stream) => stream,
        )
        .drain(),
    );

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(pulls).toBeGreaterThan(0);
    expect(pulls).toBeLessThanOrEqual(3);

    await run(gate.succeed(undefined));
    const exit = await run(awaitFiber(fiber));
    expect(exit._tag).toBe("Success");
    expect(pulls).toBe(20);
  });

  test("a branch failure interrupts siblings and finalizes upstream once", async () => {
    let upstreamFinalized = 0;
    let siblingFinalized = 0;
    const source = Stream.iterate(1, (value) => value + 1).onFinalize(
      sync(() => {
        upstreamFinalized++;
      }),
    );

    const exit = await runExit(
      source
        .broadcastThrough(
          (stream) => stream.evalMap(() => fail(new BranchFailure({}))),
          (stream) =>
            stream
              .evalMap((value) => sleep(1_000).map(() => value))
              .onFinalize(
                sync(() => {
                  siblingFinalized++;
                }),
              ),
        )
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(BranchFailure);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(siblingFinalized).toBe(1);
    expect(upstreamFinalized).toBe(1);
  });

  test("downstream short-circuit finalizes every branch and upstream exactly once", async () => {
    let upstreamFinalized = 0;
    const branchFinalized = [0, 0];
    const source = Stream.iterate(1, (value) => value + 1).onFinalize(
      sync(() => {
        upstreamFinalized++;
      }),
    );

    const output = await run(
      source
        .broadcastThrough(
          (stream) =>
            stream.onFinalize(
              sync(() => {
                branchFinalized[0]!++;
              }),
            ),
          (stream) =>
            stream
              .map((value) => value * 10)
              .onFinalize(
                sync(() => {
                  branchFinalized[1]!++;
                }),
              ),
        )
        .take(1)
        .toArray(),
    );

    expect(output).toHaveLength(1);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(branchFinalized).toEqual([1, 1]);
    expect(upstreamFinalized).toBe(1);
  });

  test("zero branches preserves the original stream", async () => {
    expect(await run(Stream.of(1, 2, 3).broadcastThrough().toArray())).toEqual([1, 2, 3]);
  });
});
