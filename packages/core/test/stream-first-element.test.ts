import { describe, expect, test } from "bun:test";
import { Chunk, Sinks, Stream, run, sleep, sync } from "../src";

// Filters, mapChunks and other chunk-level operators can emit empty chunks.
// First/last terminals must look past them rather than treat a chunk boundary
// as the end of the stream.

const emptyThen = <A>(stream: Stream<A>): Stream<A> =>
  Stream.of(0 as unknown as A)
    .mapChunks(() => Chunk.empty<A>())
    .concat(stream);

describe("Stream.head", () => {
  test("skips leading empty chunks", async () => {
    expect(await run(emptyThen(Stream.of(3, 4)).head())).toBe(3);
    expect(await run(emptyThen(emptyThen(Stream.of(5))).head())).toBe(5);
    expect(await run(Stream.fromChunk(Chunk.empty<number>()).concat(Stream.of(6)).head())).toBe(6);
  });

  test("skips chunks a filter emptied", async () => {
    expect(
      await run(
        Stream.of(1, 2)
          .filter((n) => n > 2)
          .concat(Stream.of(3))
          .head(),
      ),
    ).toBe(3);
    expect(
      await run(
        Stream.range(0, 10)
          .rechunk(2)
          .filter((n) => n > 7)
          .head(),
      ),
    ).toBe(8);
  });

  test("skips an empty chunk that arrives asynchronously", async () => {
    const delayed = Stream.fromEffect(sleep(1).map(() => 1))
      .filter(() => false)
      .concat(Stream.fromEffect(sleep(1).map(() => 2)));
    expect(await run(delayed.head())).toBe(2);
  });

  test("returns undefined when every chunk is empty", async () => {
    expect(await run(emptyThen(Stream.empty<number>()).head())).toBeUndefined();
    expect(
      await run(
        Stream.range(0, 10)
          .rechunk(3)
          .filter(() => false)
          .head(),
      ),
    ).toBeUndefined();
  });

  test("stops pulling at the first non-empty chunk and finalizes once", async () => {
    const visited: number[] = [];
    let finalized = 0;
    const head = await run(
      Stream.range(0, 100)
        .rechunk(1)
        .tap((n) => visited.push(n))
        .filter((n) => n >= 2)
        .onFinalize(sync(() => void finalized++))
        .head(),
    );

    expect(head).toBe(2);
    expect(visited).toEqual([0, 1, 2]);
    expect(finalized).toBe(1);
  });

  test("Sinks.head skips empty chunks", async () => {
    expect(await run(Sinks.head<string>().run(emptyThen(Stream.of("a"))))).toBe("a");
  });
});

describe("Stream.collectFirst", () => {
  test("finds a match after a chunk with no matches", async () => {
    expect(
      await run(
        Stream.of(1, 2)
          .concat(Stream.of(3))
          .collectFirst((n) => n === 3),
      ),
    ).toBe(3);
  });

  test("finds a match in a later chunk of a rechunked stream", async () => {
    expect(
      await run(
        Stream.range(0, 20)
          .rechunk(4)
          .collectFirst((n) => n % 7 === 6),
      ),
    ).toBe(6);
  });

  test("returns undefined when nothing matches", async () => {
    expect(
      await run(
        Stream.of(1, 2)
          .concat(Stream.of(3))
          .collectFirst((n) => n > 3),
      ),
    ).toBeUndefined();
  });
});

describe("Stream.last", () => {
  test("keeps the last value across trailing empty chunks", async () => {
    expect(await run(Stream.of(1, 2).concat(emptyThen(Stream.empty<number>())).last())).toBe(2);
    expect(
      await run(
        Stream.of(1, 2)
          .concat(Stream.of(3).filter(() => false))
          .last(),
      ),
    ).toBe(2);
  });

  test("returns a nullish last value instead of an earlier one", async () => {
    expect(await run(Stream.of<number | null>(1).concat(Stream.of(null)).last())).toBeNull();
    expect(
      await run(Stream.of<number | undefined>(1).concat(Stream.of(undefined)).last()),
    ).toBeUndefined();
    expect(
      await run(
        Sinks.last<number | null>().run(Stream.of<number | null>(1).concat(Stream.of(null))),
      ),
    ).toBeNull();
  });
});
