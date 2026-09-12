import { expect, test } from "bun:test";
import { Chunk, Stream, run, sync } from "../src";

for (const method of ["evalMap", "forEach"] as const) {
  test(`${method} visits sparse entries in a sliced chunk`, async () => {
    const values = new Array<number | undefined>(7);
    values[0] = 99;
    values[3] = 5;
    values[6] = 99;
    const chunk = Chunk.fromArray(values).drop(2).take(3);
    const seen: Array<number | undefined> = [];
    const visit = (value: number | undefined) =>
      sync(() => {
        seen.push(value);
      });
    const stream = Stream.fromChunk(chunk);
    const program = method === "evalMap" ? stream.evalMap(visit).drain() : stream.forEach(visit);

    expect(seen).toEqual([]);
    await run(program);
    expect(seen).toEqual([undefined, 5, undefined]);
  });

  test(`${method} captures chunk values before executing callbacks`, async () => {
    const values = [1, 2, 3];
    const seen: number[] = [];
    const visit = (value: number) =>
      sync(() => {
        values[1] = 99;
        seen.push(value);
      });
    const stream = Stream.fromArray(values);
    const program = method === "evalMap" ? stream.evalMap(visit).drain() : stream.forEach(visit);

    await run(program);
    expect(seen).toEqual([1, 2, 3]);
  });
}
