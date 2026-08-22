// Absolute-threshold gate: collect (or read) results and fail on a breach.
//
//   bun scripts/perf/gate.ts                       # collect then check
//   bun scripts/perf/gate.ts --results a.json      # check an existing file
//
// This is the catastrophic floor, deliberately generous. The precise check is
// scripts/perf/compare-refs.ts, which measures a baseline on the same runner
// and adapts its tolerance per benchmark. See suites/core.ts for why the two
// mechanisms are separate.

import { appendFile, readFile } from "node:fs/promises";
import type { ResultsFile } from "./suites/types";

function arg(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1] !== undefined) return process.argv[index + 1];
  return fallback;
}

const RESULTS = arg("results");
const OUT = arg("out", ".perf/current.json")!;

let file: ResultsFile;
if (RESULTS !== undefined) {
  file = JSON.parse(await readFile(RESULTS, "utf8")) as ResultsFile;
} else {
  const passthrough: string[] = [];
  for (const name of ["samples", "warmup", "prime", "suite"]) {
    const value = arg(name);
    if (value !== undefined) passthrough.push(`--${name}`, value);
  }
  const proc = Bun.spawn(
    ["bun", "scripts/perf/collect.ts", "--out", OUT, "--label", "gate", ...passthrough],
    { stdout: "inherit", stderr: "inherit" },
  );
  if ((await proc.exited) !== 0) process.exit(1);
  file = JSON.parse(await readFile(OUT, "utf8")) as ResultsFile;
}

const gated = file.results.filter((r) => r.threshold !== undefined);
const breaches = gated.filter((r) => r.unavailable === undefined && r.median > r.threshold!);

const width = Math.max(...gated.map((r) => r.name.length), 10);
const lines: string[] = [];
lines.push("## Performance gate (absolute floor)");
lines.push("");
lines.push("| benchmark | median | threshold | headroom | status |");
lines.push("|---|---:|---:|---:|---|");
for (const r of gated) {
  if (r.unavailable !== undefined) {
    lines.push(`| ${r.name} | — | ${r.threshold} ${r.unit} | — | unavailable |`);
    continue;
  }
  const headroom = r.threshold! / r.median;
  lines.push(
    `| ${r.name} | ${r.median.toFixed(2)} ${r.unit} | ${r.threshold} ${r.unit} | ${headroom.toFixed(1)}× | ${r.median <= r.threshold! ? "pass" : "**FAIL**"} |`,
  );
}
lines.push("");
lines.push("<sub>A generous floor by design — the precise check is the baseline comparison.</sub>");
lines.push("");
const markdown = `${lines.join("\n")}\n`;

console.log(
  `\n${"benchmark".padEnd(width)}  ${"median".padStart(12)}  ${"threshold".padStart(12)}  headroom`,
);
console.log("-".repeat(width + 42));
for (const r of gated) {
  if (r.unavailable !== undefined) {
    console.log(
      `${r.name.padEnd(width)}  ${"—".padStart(12)}  ${String(r.threshold).padStart(12)}  unavailable`,
    );
    continue;
  }
  const status = r.median <= r.threshold! ? "" : "  <-- FAIL";
  console.log(
    `${r.name.padEnd(width)}  ${`${r.median.toFixed(2)}`.padStart(12)}  ${String(r.threshold).padStart(12)}  ${(r.threshold! / r.median).toFixed(1)}×${status}`,
  );
}
console.log();

const summary = process.env.GITHUB_STEP_SUMMARY;
if (summary !== undefined && summary.length > 0) await appendFile(summary, `\n${markdown}`);

if (breaches.length > 0) {
  for (const r of breaches) {
    console.error(`THRESHOLD ${r.name}: ${r.median.toFixed(2)} > ${r.threshold} ${r.unit}`);
  }
  process.exit(1);
}
