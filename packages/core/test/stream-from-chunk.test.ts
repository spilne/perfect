import { describe, test, expect } from "bun:test";
import { run, Stream, Chunk } from "../src";

describe("Stream.fromChunk", () => {
  test("emits the chunk's elements", async () => {
    expect(await run(Stream.fromChunk(Chunk.of(1, 2, 3)).toArray())).toEqual([1, 2, 3]);
  });

  test("empty chunk yields empty stream", async () => {
    expect(await run(Stream.fromChunk(Chunk.empty<number>()).toArray())).toEqual([]);
  });
});
