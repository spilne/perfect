import { describe, expect, test } from "bun:test";
import { Chunk, Stream, run, runFiber, sleep, sync } from "../src";

describe("push sources with backpressure", () => {
  test("emit returns a promise when the buffer fills, and it resolves after a pull", async () => {
    const results: Array<"room" | "full"> = [];
    let resumed = false;
    const stream = Stream.async<number, never>(
      (emit, close) =>
        sync(() => {
          void (async () => {
            for (let i = 0; i < 6; i++) {
              const wait = emit(i);
              results.push(wait === undefined ? "room" : "full");
              if (wait) await wait;
            }
            resumed = true;
            close();
          })();
        }),
      { bufferSize: 2 },
    );
    expect(await run(stream.toArray())).toEqual([0, 1, 2, 3, 4, 5]);
    expect(resumed).toBe(true);
    expect(results.filter((r) => r === "full").length).toBeGreaterThan(0);
  });

  test("a producer that awaits never gets ahead of the consumer by more than the buffer", async () => {
    let produced = 0;
    let maxAhead = 0;
    let consumed = 0;
    const stream = Stream.async<number, never>(
      (emit, close) =>
        sync(() => {
          void (async () => {
            for (let i = 0; i < 200; i++) {
              produced++;
              maxAhead = Math.max(maxAhead, produced - consumed);
              await emit(i);
            }
            close();
          })();
        }),
      { bufferSize: 8 },
    );
    await run(stream.forEach(() => sync(() => void consumed++)));
    expect(consumed).toBe(200);
    // One chunk can be in the consumer's hands while the buffer refills.
    expect(maxAhead).toBeLessThanOrEqual(8 * 2 + 1);
  });

  test("a waiting producer is released when the consumer stops early", async () => {
    let finished = false;
    const stream = Stream.async<number, never>(
      (emit) =>
        sync(() => {
          void (async () => {
            for (let i = 0; i < 100; i++) await emit(i);
            finished = true;
          })();
        }),
      { bufferSize: 1 },
    );
    await run(stream.take(3).drain());
    await run(sleep(5));
    expect(finished).toBe(true);
  });

  test("asyncChunks keeps chunk boundaries and counts chunks", async () => {
    const steps: number[][] = [];
    const stream = Stream.asyncChunks<number, never>(
      (emit, close) =>
        sync(() => {
          emit(Chunk.fromArray([1, 2]));
          emit(Chunk.empty());
          emit(Chunk.fromArray([3]));
          close();
        }),
      { bufferSize: 1 },
    );
    await run(stream.mapChunks((c) => (steps.push(c.toArray()), c)).drain());
    expect(steps).toEqual([[1, 2], [3]]);
  });

  test("each run registers and cleans up on its own", async () => {
    let registered = 0;
    let cleaned = 0;
    const stream = Stream.async<number, never>((emit, close) =>
      sync(() => {
        registered++;
        emit(registered);
        close();
        return () => void cleaned++;
      }),
    );
    expect(await run(stream.toArray())).toEqual([1]);
    expect(await run(stream.toArray())).toEqual([2]);
    expect(cleaned).toBe(2);
  });

  test("interrupting the consumer runs the cleanup", async () => {
    let cleaned = 0;
    const fiber = runFiber(
      Stream.async<number, never>(() => sync(() => () => void cleaned++)).drain(),
    );
    await run(sleep(1));
    fiber.interrupt();
    await fiber.await();
    expect(cleaned).toBe(1);
  });

  test("bufferSize must be at least 1", () => {
    expect(() => Stream.fromCallback(() => {}, 0)).toThrow(RangeError);
  });
});
