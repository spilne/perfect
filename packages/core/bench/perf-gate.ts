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
  passed: boolean;
};

const SAMPLES = Number(process.env.PERF_SAMPLES ?? "40");
const WARMUP = Number(process.env.PERF_WARMUP ?? "8");
const OUT = process.env.PERF_OUT ?? "../../.perf/perf-gate.json";
const MARKDOWN_OUT = process.env.PERF_MARKDOWN_OUT ?? "../../.perf/perf-gate.md";

const FLATMAP_N = 10_000;
const ALL_N = 100;
const STREAM_N = 20_000;

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
    threshold: 2_500,
    divisor: 1,
    run: () => do_not_optimize(runSync(succeed(42))),
  },
  {
    name: "run(sync)",
    unit: "ns/op",
    threshold: 15_000,
    divisor: 1,
    run: async () => do_not_optimize(await run(sync(() => 42))),
  },
  {
    name: "flatMap chain x10k runSync",
    unit: "ns/op",
    threshold: 120,
    divisor: FLATMAP_N,
    run: () => do_not_optimize(runSync(flatMapChain(FLATMAP_N))),
  },
  {
    name: "all x100 run",
    unit: "ns/op",
    threshold: 500,
    divisor: ALL_N,
    run: async () =>
      do_not_optimize(await run(all(Array.from({ length: ALL_N }, (_, i) => succeed(i))) as any)),
  },
  {
    name: "stream map/filter/take",
    unit: "ns/item",
    threshold: 400,
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
  return {
    name: benchmark.name,
    unit: benchmark.unit,
    median,
    p99,
    threshold: benchmark.threshold,
    passed: median <= benchmark.threshold,
  };
}

function renderMarkdown(results: Result[]): string {
  const lines = [
    "# Performance Gate",
    "",
    `Framework: mitata.measure(), samples: ${SAMPLES}, warmup samples: ${WARMUP}`,
    "",
    "| benchmark | median | p99 | threshold | status |",
    "|---|---:|---:|---:|---|",
  ];
  for (const result of results) {
    const status = result.passed ? "pass" : "fail";
    lines.push(
      `| ${result.name} | ${result.median.toFixed(2)} ${result.unit} | ${result.p99.toFixed(
        2,
      )} ${result.unit} | ${result.threshold.toFixed(2)} ${result.unit} | ${status} |`,
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
