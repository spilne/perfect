// Append a run to the perf history and regenerate the trend report.
//
//   bun scripts/perf/history.ts --results .perf/current.json --dir perf-history
//
// A per-run CI artifact expires and shows one point; this is the record that
// answers "has this benchmark been drifting for a month". The comparison gate
// catches a single bad commit, but a 3% regression repeated ten times is
// invisible to it — every step is inside tolerance while the total is not.
//
// Storage is one JSON object per line, appended, never rewritten. CI keeps it
// on a dedicated branch so it does not churn main's history.

import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ResultsFile } from "./suites/types";

function arg(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1] !== undefined) return process.argv[index + 1];
  return fallback;
}

const RESULTS = arg("results", ".perf/current.json")!;
const DIR = arg("dir", "perf-history")!;
/** Runs shown in the rendered trend table. */
const WINDOW = Number(arg("window", "20"));
const TIMESTAMP = arg("timestamp") ?? new Date().toISOString();

const file = JSON.parse(await readFile(RESULTS, "utf8")) as ResultsFile;

interface HistoryEntry {
  readonly timestamp: string;
  readonly commit: string;
  readonly ref: string;
  readonly runtime: ResultsFile["runtime"];
  readonly samples: number;
  readonly medians: Record<string, number>;
}

const entry: HistoryEntry = {
  timestamp: TIMESTAMP,
  commit: file.commit,
  ref: file.ref,
  runtime: file.runtime,
  samples: file.config.samples,
  medians: Object.fromEntries(
    file.results
      .filter((r) => r.unavailable === undefined && Number.isFinite(r.median))
      .map((r) => [`${r.suite}/${r.name}`, Number(r.median.toFixed(4))]),
  ),
};

await mkdir(DIR, { recursive: true });
const jsonlPath = join(DIR, "history.jsonl");
await appendFile(jsonlPath, `${JSON.stringify(entry)}\n`);

// ── Render the trend ──────────────────────────────────────────────

const lines = (await readFile(jsonlPath, "utf8")).split("\n").filter((l) => l.trim().length > 0);
const entries = lines.map((l) => JSON.parse(l) as HistoryEntry);
const recent = entries.slice(-WINDOW);

const benchmarks = [...new Set(recent.flatMap((e) => Object.keys(e.medians)))].sort();

const out: string[] = [];
out.push("# Performance history");
out.push("");
out.push(
  `Last ${recent.length} run(s) of ${entries.length} recorded. Medians in each benchmark's own ` +
    "unit (ns/op or ns/item). Absolute values are only comparable within a runner class — the " +
    "trend is the signal, not the number.",
);
out.push("");

for (const name of benchmarks) {
  const series = recent
    .map((e) => ({ commit: e.commit.slice(0, 8), value: e.medians[name], at: e.timestamp }))
    .filter((p) => p.value !== undefined) as { commit: string; value: number; at: string }[];
  if (series.length === 0) continue;

  const first = series[0]!.value;
  const last = series[series.length - 1]!.value;
  const drift = first > 0 ? ((last - first) / first) * 100 : 0;
  const min = Math.min(...series.map((p) => p.value));
  const max = Math.max(...series.map((p) => p.value));

  out.push(`## ${name}`);
  out.push("");
  out.push(
    `latest **${last.toFixed(2)}** · window min ${min.toFixed(2)} / max ${max.toFixed(2)} · ` +
      `drift across window ${drift >= 0 ? "+" : ""}${drift.toFixed(1)}%`,
  );
  out.push("");
  out.push("| run | commit | median |");
  out.push("|---:|---|---:|");
  for (const point of series.slice(-10)) {
    out.push(`| ${point.at.slice(0, 10)} | \`${point.commit}\` | ${point.value.toFixed(2)} |`);
  }
  out.push("");
}

await writeFile(join(DIR, "README.md"), `${out.join("\n")}\n`);

console.log(
  `Recorded ${Object.keys(entry.medians).length} medians for ${entry.commit.slice(0, 8)}`,
);
console.log(`History now holds ${entries.length} run(s) at ${jsonlPath}`);
