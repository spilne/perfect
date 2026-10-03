import { describe, expect, test } from "bun:test";
import { Stream, run, runExit, sync, fail } from "../src";

function tracked(log: string[], name: string, values: number[]) {
  return Stream.bracket(
    sync(() => {
      log.push(`open ${name}`);
      return values;
    }),
    () => sync(() => void log.push(`close ${name}`)),
  ).flatMap((vs) => Stream.fromArray(vs));
}

describe("concat", () => {
  test("closes each side before pulling the next", async () => {
    const log: string[] = [];
    const out = await run(
      tracked(log, "a", [1])
        .concat(tracked(log, "b", [2]))
        .concat(tracked(log, "c", [3]))
        .toArray(),
    );
    expect(out).toEqual([1, 2, 3]);
    expect(log).toEqual(["open a", "close a", "open b", "close b", "open c", "close c"]);
  });

  test("keeps at most one resource open while repeating", async () => {
    let open = 0;
    let peak = 0;
    const resource = Stream.bracket(
      sync(() => {
        open++;
        peak = Math.max(peak, open);
      }),
      () => sync(() => void open--),
    ).flatMap(() => Stream.of(1));
    await run(
      Stream.repeatForever(() => resource)
        .take(200)
        .drain(),
    );
    expect(peak).toBe(1);
    expect(open).toBe(0);
  });

  test("an early stop closes the open side once", async () => {
    const log: string[] = [];
    await run(
      tracked(log, "a", [1, 2])
        .concat(tracked(log, "b", [3, 4]))
        .take(3)
        .drain(),
    );
    expect(log).toEqual(["open a", "close a", "open b", "close b"]);
  });

  test("a failure in the second side still closes it", async () => {
    const log: string[] = [];
    const exit = await runExit(
      tracked(log, "a", [1])
        .concat(tracked(log, "b", [2]).concat(Stream.fail("boom")))
        .drain(),
    );
    expect(exit._tag).toBe("Failure");
    expect(log).toEqual(["open a", "close a", "open b", "close b"]);
  });

  test("a failing finalizer fails the stream", async () => {
    const exit = await runExit(
      Stream.of(1).onFinalize(fail("close failed")).concat(Stream.of(2)).toArray(),
    );
    expect(exit._tag).toBe("Failure");
  });

  test("a description runs again from the start", async () => {
    const both = Stream.of(1).concat(Stream.of(2)).toArray();
    expect(await run(both)).toEqual([1, 2]);
    expect(await run(both)).toEqual([1, 2]);
  });

  test("a long left-nested chain stays linear", async () => {
    let stream: Stream<number, never> = Stream.empty();
    for (let i = 0; i < 5_000; i++) stream = stream.concat(Stream.of(i));
    const started = performance.now();
    expect(await run(stream.count())).toBe(5_000);
    expect(performance.now() - started).toBeLessThan(1_000);
  });
});
