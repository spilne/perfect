// Human-readable local report for the HTTP client suite.
//
//   bun run bench            (from packages/http)
//
// The suite definitions live in scripts/perf/suites/http.ts so this and the
// CI gate measure exactly the same cases. For regression detection use
// `bun run perf:compare` at the repo root, which measures a git baseline on
// the same machine instead of eyeballing absolute numbers.

import { measure } from "mitata";
import { httpSuite } from "../../../scripts/perf/suites/http";

const SAMPLES = Number(process.env.BENCH_SAMPLES ?? "50");
const WARMUP = Number(process.env.BENCH_WARMUP ?? "10");
const PRIME = Number(process.env.BENCH_PRIME ?? "40");

await httpSuite.setup?.();
const cases = await httpSuite.cases();

// Prime every case before measuring any — otherwise whichever runs first
// absorbs JIT tiering and connection setup and looks slowest.
for (const c of cases) for (let i = 0; i < PRIME; i++) await c.run();

const rows: { name: string; median: number; p99: number }[] = [];
for (const c of cases) {
  const stats = await measure(c.run, {
    min_samples: SAMPLES,
    max_samples: SAMPLES,
    warmup_samples: WARMUP,
  });
  rows.push({ name: c.name, median: stats.p50 / 1_000, p99: stats.p99 / 1_000 });
}

await httpSuite.teardown?.();

const baseline = rows.find((r) => r.name.includes("fetch (baseline)"))!.median;
const width = Math.max(...rows.map((r) => r.name.length));

console.log(`\nSamples ${SAMPLES}, warmup ${WARMUP}, ${PRIME} priming requests per client.`);
console.log("Local Bun.serve on loopback.\n");
console.log(`${"client".padEnd(width)}  ${"median".padStart(10)}  ${"p99".padStart(10)}  vs fetch`);
console.log("-".repeat(width + 36));
for (const row of rows) {
  console.log(
    `${row.name.padEnd(width)}  ${`${row.median.toFixed(1)} µs`.padStart(10)}  ${`${row.p99.toFixed(1)} µs`.padStart(10)}  ${(row.median / baseline).toFixed(2)}×`,
  );
}
console.log(
  "\nThe number that matters is @perfect/http vs the fetch baseline: that gap is\n" +
    "the cost of the Eff wrapper, typed errors and parsing — not of the network.\n",
);
