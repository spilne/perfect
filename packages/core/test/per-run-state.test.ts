import { describe, expect, test } from "bun:test";
import {
  Gen,
  Random,
  Sink,
  Stream,
  TestRandom,
  provide,
  run,
  runSync,
  succeed,
  sync,
} from "../src";
import { runUnchecked } from "./run-unchecked";

// A stream or effect is a description: running it twice must give two
// independent runs, never state carried over from the first.
describe("state is created per run", () => {
  test("toArray", async () => {
    const eff = Stream.range(0, 3).toArray();
    expect(await run(eff)).toEqual([0, 1, 2]);
    expect(await run(eff)).toEqual([0, 1, 2]);
  });

  test("zipWithIndex, changes and dedupe", async () => {
    const indexed = Stream.of("a", "b").zipWithIndex().toArray();
    expect(await run(indexed)).toEqual([
      ["a", 0],
      ["b", 1],
    ]);
    expect(await run(indexed)).toEqual([
      ["a", 0],
      ["b", 1],
    ]);

    const changes = Stream.of(1, 1, 2).changes().toArray();
    expect(await run(changes)).toEqual([1, 2]);
    expect(await run(changes)).toEqual([1, 2]);

    const deduped = Stream.of(1, 2, 1).dedupe().toArray();
    expect(await run(deduped)).toEqual([1, 2]);
    expect(await run(deduped)).toEqual([1, 2]);
  });

  test("fromAsyncIterable asks the iterable for a fresh iterator", async () => {
    const iterable = {
      async *[Symbol.asyncIterator]() {
        yield 1;
        yield 2;
      },
    };
    const eff = Stream.fromAsyncIterable(iterable, (e) => e).count();
    expect(await runUnchecked(eff)).toBe(2);
    expect(await runUnchecked(eff)).toBe(2);
  });

  test("Sink.foldEffect", async () => {
    const eff = Stream.of(1, 2, 3).runSink(
      Sink.foldEffect(0, (acc, n: number) => succeed(acc + n)),
    );
    expect(await run(eff)).toBe(6);
    expect(await run(eff)).toBe(6);
  });

  test("Gen.tuple and Gen.object", () => {
    const sample = <A>(gen: { generate: any }): A =>
      runSync(provide(gen.generate, Random, new TestRandom(1)) as any) as A;
    const pair = Gen.tuple(Gen.int(0, 9), Gen.bool);
    const record = Gen.object({ n: Gen.int(0, 9) });
    const first = sample<unknown[]>(pair);
    const second = sample<unknown[]>(pair);
    expect(first).toHaveLength(2);
    expect(second).toHaveLength(2);
    expect(first).not.toBe(second);
    expect(sample<object>(record)).not.toBe(sample<object>(record));
  });
});

describe("forEachWhile", () => {
  test("stops pulling once the callback returns false", async () => {
    const seen: number[] = [];
    await run(
      Stream.repeatValue(1)
        .zipWithIndex()
        .forEachWhile(([, i]) => sync(() => (seen.push(i), i < 3))),
    );
    expect(seen).toEqual([0, 1, 2, 3]);
  });

  test("Sink.forEachWhile stops an infinite stream and runs its finalizer", async () => {
    let finalized = 0;
    await run(
      Stream.repeatValue(1)
        .onFinalize(sync(() => void finalized++))
        .runSink(Sink.forEachWhile(() => succeed(false))),
    );
    expect(finalized).toBe(1);
  });
});

describe("evalMap and forEach over a chunk", () => {
  test("keep order across inline and suspended effects", async () => {
    const out = await run(
      Stream.fromArray([1, 2, 3, 4])
        .evalMap((n) => (n % 2 === 0 ? succeed(n * 10) : sync(() => n * 10)))
        .toArray(),
    );
    expect(out).toEqual([10, 20, 30, 40]);

    const seen: number[] = [];
    await run(
      Stream.fromArray([1, 2, 3]).forEach((n) =>
        n === 2 ? succeed(void seen.push(n)) : sync(() => void seen.push(n)),
      ),
    );
    expect(seen).toEqual([1, 2, 3]);
  });
});
