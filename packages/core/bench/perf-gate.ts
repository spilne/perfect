import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { do_not_optimize, measure } from "mitata";
import { all, run, runSync, Stream, succeed, sync } from "../src";
import type { Eff } from "../src";

type Benchmark = {
  name: string;
  unit: "ns/op" | "ns/item";
  threshold: number;
  run: () => unknown | Promise<unknown>;
  divisor: number;
};

type Result = {
  name: string;
  unit: Benchmark["unit"];
  median: number;
  p99: number;
  threshold: number;
  /** threshold / median — how much margin is left before the gate trips. */
  headroom: number;
  passed: boolean;
};

const SAMPLES = Number(process.env.PERF_SAMPLES ?? "40");
const WARMUP = Number(process.env.PERF_WARMUP ?? "8");
const OUT = process.env.PERF_OUT ?? "../../.perf/perf-gate.json";
const MARKDOWN_OUT = process.env.PERF_MARKDOWN_OUT ?? "../../.perf/perf-gate.md";

const FLATMAP_N = 10_000;
const ALL_N = 100;
const STREAM_N = 20_000;

/**
 * Thresholds are absolute ns, so they have to absorb the gap between a quiet
 * dev machine and a shared CI runner. They are derived from the worst median
 * observed locally (Apple Silicon, several runs) times a headroom factor:
 *
 *   ×6  for amortized rows (divisor > 1) — per-op cost over 100–20 000 ops,
 *       so runner noise averages out
 *   ×10 for single-op rows — a ~10 ns measurement is dominated by timer
 *       resolution and scheduling jitter, and needs the slack
 *
 * The factors are calibrated against the two rows that already ran at 4.3×
 * and 5.8× headroom and passed on GitHub's runners, so that band is known to
 * be survivable. Every row now sits at 4–10× instead of 4–200×, which catches
 * a ~4× regression rather than only a catastrophic one.
 *
 * Normalizing against an in-run calibration benchmark was tried and rejected:
 * the calibration's own variance (1.3× locally) propagated into every ratio
 * and made them noisier than the raw medians.
 *
 * PERF_THRESHOLD_SCALE multiplies every threshold — the escape hatch if a
 * runner turns out slower than this assumes. Prefer re-deriving the numbers
 * over leaving a scale factor set in CI.
 */
const THRESHOLD_SCALE = Number(process.env.PERF_THRESHOLD_SCALE ?? "1");

function flatMapChain(n: number): Eff<number, never> {
  let eff: Eff<number, never> = succeed(0);
  for (let i = 0; i < n; i++) {
    eff = eff.flatMap((x) => succeed(x + 1));
  }
  return eff;
}

function streamProgram(n: number): Eff<number[], never> {
  return Stream.range(0, n)
    .map((x) => x + 1)
    .filter((x) => x % 3 === 0)
    .take(1_000)
    .toArray();
}

const benchmarks: Benchmark[] = [
  {
    name: "runSync(succeed)",
    unit: "ns/op",
    // worst local median 12.7 ns × 10 (single-op row)
    threshold: 130,
    divisor: 1,
    run: () => do_not_optimize(runSync(succeed(42))),
  },
  {
    name: "run(sync)",
    unit: "ns/op",
    // worst local median 4 333 ns × ~5.8 — already in band, unchanged
    threshold: 25_000,
    divisor: 1,
    run: async () => do_not_optimize(await run(sync(() => 42))),
  },
  {
    name: "flatMap chain x10k runSync",
    unit: "ns/op",
    // worst local median 28.3 ns × ~4.2 — already in band, unchanged
    threshold: 120,
    divisor: FLATMAP_N,
    run: () => do_not_optimize(runSync(flatMapChain(FLATMAP_N))),
  },
  {
    name: "all x100 run",
    unit: "ns/op",
    // worst local median 13.2 ns × 6 (amortized over 100 ops)
    threshold: 80,
    divisor: ALL_N,
    run: async () =>
      do_not_optimize(await run(all(Array.from({ length: ALL_N }, (_, i) => succeed(i))) as any)),
  },
  {
    name: "stream map/filter/take",
    unit: "ns/item",
    // worst local median 5.4 ns × 6 (amortized over 20 000 items)
    threshold: 35,
    divisor: STREAM_N,
    run: async () => do_not_optimize(await run(streamProgram(STREAM_N))),
  },
];

async function measureBenchmark(benchmark: Benchmark): Promise<Result> {
  const stats = await measure(benchmark.run, {
    min_samples: SAMPLES,
    max_samples: SAMPLES,
    warmup_samples: WARMUP,
  });
  const median = stats.p50 / benchmark.divisor;
  const p99 = stats.p99 / benchmark.divisor;
  const threshold = benchmark.threshold * THRESHOLD_SCALE;
  return {
    name: benchmark.name,
    unit: benchmark.unit,
    median,
    p99,
    threshold,
    headroom: threshold / median,
    passed: median <= threshold,
  };
}

function renderMarkdown(results: Result[]): string {
  const lines = [
    "# Performance Gate",
    "",
    `Framework: mitata.measure(), samples: ${SAMPLES}, warmup samples: ${WARMUP}`,
    "",
    "| benchmark | median | p99 | threshold | headroom | status |",
    "|---|---:|---:|---:|---:|---|",
  ];
  for (const result of results) {
    const status = result.passed ? "pass" : "fail";
    lines.push(
      `| ${result.name} | ${result.median.toFixed(2)} ${result.unit} | ${result.p99.toFixed(
        2,
      )} ${result.unit} | ${result.threshold.toFixed(2)} ${result.unit} | ${result.headroom.toFixed(
        1,
      )}× | ${status} |`,
    );
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

await mkdir(dirname(OUT), { recursive: true });
await mkdir(dirname(MARKDOWN_OUT), { recursive: true });

const results = [];
for (const benchmark of benchmarks) {
  results.push(await measureBenchmark(benchmark));
}

const markdown = renderMarkdown(results);
await writeFile(OUT, `${JSON.stringify({ results }, null, 2)}\n`);
await writeFile(MARKDOWN_OUT, markdown);

console.log(markdown);

const summary = process.env.GITHUB_STEP_SUMMARY;
if (summary !== undefined && summary.length > 0) {
  await writeFile(summary, markdown, { flag: "a" });
}

const failed = results.filter((result) => !result.passed);
if (failed.length > 0) {
  console.error(`Performance gate failed: ${failed.map((result) => result.name).join(", ")}`);
  process.exit(1);
}
