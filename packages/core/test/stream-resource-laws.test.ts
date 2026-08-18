import { describe, expect, test } from "bun:test";
import { fail, run, runExit, succeed, sync } from "../src";
import { Stream } from "../src/stream";

describe("Stream resource laws", () => {
  test("bracket releases after early termination", async () => {
    let releases = 0;
    const stream = Stream.bracket(succeed("resource"), () =>
      sync(() => {
        releases++;
      }),
    );

    expect(await run(stream.take(1).toArray())).toEqual(["resource"]);
    expect(releases).toBe(1);
  });

  test("flatMap finalizes every inner stream before the outer stream", async () => {
    const events: string[] = [];
    const stream = Stream.of(1, 2)
      .onFinalize(sync(() => events.push("outer")))
      .flatMap((value) => Stream.of(value).onFinalize(sync(() => events.push(`inner:${value}`))));

    expect(await run(stream.toArray())).toEqual([1, 2]);
    expect(events).toEqual(["inner:1", "inner:2", "outer"]);
  });

  test("flatMap finalizes the active inner and outer streams on failure", async () => {
    const events: string[] = [];
    const stream = Stream.of(1)
      .onFinalize(sync(() => events.push("outer")))
      .flatMap(() => Stream.fail("boom").onFinalize(sync(() => events.push("inner"))));

    expect((await runExit(stream.toArray()))._tag).toBe("Failure");
    expect(events).toEqual(["inner", "outer"]);
  });

  test("flatMap finalizes only the active inner on early termination", async () => {
    const events: string[] = [];
    const stream = Stream.of(1, 2)
      .onFinalize(sync(() => events.push("outer")))
      .flatMap((value) =>
        Stream.of(value, value).onFinalize(sync(() => events.push(`inner:${value}`))),
      );

    expect(await run(stream.take(1).toArray())).toEqual([1]);
    expect(events).toEqual(["inner:1", "outer"]);
  });

  test("zip finalizes both inputs on early termination", async () => {
    const events: string[] = [];
    const left = Stream.of(1, 2).onFinalize(sync(() => events.push("left")));
    const right = Stream.of("a", "b").onFinalize(sync(() => events.push("right")));

    expect(await run(left.zip(right).take(1).toArray())).toEqual([[1, "a"]]);
    expect(events).toEqual(["left", "right"]);
  });

  test("fiber-backed operators preserve upstream finalizers", async () => {
    const finalized = { left: 0, right: 0, buffer: 0, parallel: 0 };
    const left = Stream.of(1, 2).onFinalize(
      sync(() => {
        finalized.left++;
      }),
    );
    const right = Stream.of(3, 4).onFinalize(
      sync(() => {
        finalized.right++;
      }),
    );

    await run(left.merge(right).take(1).toArray());
    await run(
      Stream.of(1, 2, 3)
        .onFinalize(
          sync(() => {
            finalized.buffer++;
          }),
        )
        .buffer(1)
        .take(1)
        .toArray(),
    );
    await run(
      Stream.of(1, 2, 3)
        .onFinalize(
          sync(() => {
            finalized.parallel++;
          }),
        )
        .parEvalMap(2, (value) => succeed(value))
        .take(1)
        .toArray(),
    );

    expect(finalized).toEqual({ left: 1, right: 1, buffer: 1, parallel: 1 });
  });

  test("finalizers run exactly once when an upstream pull fails", async () => {
    let finalizations = 0;
    const stream = Stream.fromEffect(fail("boom")).onFinalize(
      sync(() => {
        finalizations++;
      }),
    );

    expect((await runExit(stream.buffer(2).toArray()))._tag).toBe("Failure");
    expect(finalizations).toBe(1);
  });
});
