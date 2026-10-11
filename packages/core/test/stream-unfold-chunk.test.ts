import { describe, expect, test } from "bun:test";
import { fail, succeed, sync } from "../src";
import { Stream } from "../src/stream";

describe("Stream.unfoldChunkEffect", () => {
  test("each batch becomes the values of the stream, in order, until null", async () => {
    const batches = [[1, 2], [3], [4, 5, 6]];
    const stream = Stream.unfoldChunkEffect(0, (i) =>
      succeed(i < batches.length ? ([batches[i]!, i + 1] as const) : null),
    );
    expect(await stream.toArray().run()).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test("an empty batch emits nothing and the stream asks again", async () => {
    const batches: number[][] = [[], [1], [], [], [2]];
    const stream = Stream.unfoldChunkEffect(0, (i) =>
      succeed(i < batches.length ? ([batches[i]!, i + 1] as const) : null),
    );
    expect(await stream.toArray().run()).toEqual([1, 2]);
  });

  test("pulls only as many batches as the reader needs", async () => {
    let pulls = 0;
    const stream = Stream.unfoldChunkEffect(0, (i) =>
      sync(() => {
        pulls++;
        return [[i * 10, i * 10 + 1], i + 1] as const;
      }),
    );
    expect(await stream.take(3).toArray().run()).toEqual([0, 1, 10]);
    expect(pulls).toBe(2);
  });

  test("a failing pull fails the stream", async () => {
    const stream = Stream.unfoldChunkEffect(0, (i) =>
      i === 0 ? succeed([[1], 1] as const) : fail("broken"),
    );
    const result = await stream.toArray().either().run();
    expect(result).toEqual({ _tag: "Left", left: "broken" });
  });
});
