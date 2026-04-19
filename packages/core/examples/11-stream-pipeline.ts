// End-to-end stream: source → transform → side-effect → collect.
// Demonstrates fusion (adjacent map/filter/tap walk one chunk),
// short-circuiting (take), and stateful scan.
//
// Run: bun packages/core/examples/11-stream-pipeline.ts

import { Stream, succeed, run } from "../src";
import { assertEq } from "./_assert";

// >>> example: pipeline-etl
// A small ETL: parse, filter, enrich, accumulate.
type Row = { city: string; population: number };
const rawCsv = [
  "tokyo,37000000",
  "delhi,32000000",
  "shanghai,28000000",
  "saopaulo,22000000",
  "mexicocity,22000000",
];

const kept: string[] = [];
const top3RunningTotals = await run(
  Stream.fromArray(rawCsv)
    .map((line) => {
      const [city, n] = line.split(",");
      return { city, population: Number(n) } as Row;
    })
    .filter((r) => r.population >= 25_000_000) // pure filter
    .tap((r) => { kept.push(r.city); }) // side effect, fused
    .take(3) // short-circuit
    .scan(0, (acc, r) => acc + r.population) // running total (includes seed)
    .runCollect(),
);

assertEq(kept, ["tokyo", "delhi", "shanghai"]);
assertEq(top3RunningTotals, [0, 37_000_000, 69_000_000, 97_000_000]);
// <<< example

// >>> example: pipeline-foreach
// runForEach for "do something per element, return when done".
let count = 0;
await run(
  Stream.range(1, 11) // 1..10
    .filter((n) => n % 2 === 0)
    .runForEach((n) => succeed(void (count += n))),
);
assertEq(count, 30); // 2 + 4 + 6 + 8 + 10
// <<< example
