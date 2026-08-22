// Compare baseline results against current results and decide if anything
// regressed.
//
//   bun scripts/perf/compare.ts --baseline a.json --baseline b.json \
//                               --current c.json --current d.json \
//                               --out .perf/compare.md
//
// Why a per-benchmark adaptive tolerance instead of one global percentage:
// these benchmarks have wildly different noise profiles. `runSync(succeed)` is
// a ~10 ns measurement dominated by timer resolution and jitters by ~25% run to
// run; `all x100` is amortized over 100 ops and holds to ~1%. A single 10%
// threshold would cry wolf on the first and sleep through a real 8% regression
// on the second.
//
// So each benchmark's own dispersion sets its bar. The statistic that matters
// is NOT the raw sample spread but the uncertainty of the MEDIAN, which shrinks
// with sample count — using the spread directly produced tolerances of ±50-87%,
// wide enough to wave a catastrophic regression through.
//
// Derivation, all relative to the median:
//   sigma   ~= IQR / 1.349                  robust spread estimate
//   SE(med) ~= 1.253 * sigma / sqrt(n)      standard error of a median
//   => SE_rel = 1.858 * rsd / sqrt(n)       where rsd = IQR / (2 * median)
//
// n is samples x rounds, since interleaved rounds are what make the extra
// samples independent of thermal drift. Both sides combine in quadrature and a
// change must clear K times that, or an absolute floor — whichever is larger.
// The floor exists because sampling error is not the only error: a runner can
// be systematically slower for a whole job in a way no within-run statistic
// sees.
//
// In practice this lands tolerances between the 8% floor (stable benchmarks
// like `all x100`, whose own statistical bar is ~1.5%) and ~20% (a 12 ns
// measurement where timer granularity dominates).

import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import type { BenchResult, ResultsFile } from "./suites/types";

function arg(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1] !== undefined) return process.argv[index + 1];
  return fallback;
}

function argAll(name: string): string[] {
  const out: string[] = [];
  for (let i = 0; i < process.argv.length; i++) {
    if (process.argv[i] === `--${name}` && process.argv[i + 1] !== undefined) {
      out.push(process.argv[i + 1]!);
    }
  }
  return out;
}

/**
 * Minimum relative change before anything is called a regression.
 *
 * 12%, not the 8% first tried: on a quiet machine an unchanged tree compared
 * within -3.2%..+2.0%, but under load the same comparison drifted up to 18%.
 * Sampling error is not the whole story — a machine can be systematically
 * slower for a whole job. Regressions smaller than this are the trend
 * report's job, not the gate's.
 */
const MIN_TOLERANCE = Number(arg("min-tolerance", "0.12"));
/** How many combined-noise multiples a change must clear. */
const NOISE_K = Number(arg("noise-k", "2.5"));
const OUT = arg("out", ".perf/compare.md")!;

const baselineFiles = argAll("baseline");
const currentFiles = argAll("current");

if (baselineFiles.length === 0 || currentFiles.length === 0) {
  console.error("usage: compare.ts --baseline <file...> --current <file...> [--out md]");
  process.exit(2);
}

async function load(paths: string[]): Promise<ResultsFile[]> {
  return Promise.all(paths.map(async (p) => JSON.parse(await readFile(p, "utf8")) as ResultsFile));
}

function median(values: number[]): number {
  if (values.length === 0) return Number.NaN;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!;
}

const key = (r: { suite: string; name: string }): string => `${r.suite}/${r.name}`;

interface Side {
  /** Round medians, in round order — the basis for the paired comparison. */
  perRound: number[];
  median: number;
  gating: boolean;
  /** Relative spread, IQR-based. */
  rsd: number;
  /** Samples per round. */
  samples: number;
  /** Rounds that produced a usable measurement. */
  rounds: number;
  unit: string;
  threshold?: number;
  unavailable?: string;
}

/**
 * Relative standard error of the median. See the derivation at the top: the
 * uncertainty of the estimate, not the spread of the samples, is what a
 * tolerance has to cover.
 */
function relativeStandardError(side: Side): number {
  const n = Math.max(1, side.samples * Math.max(1, side.rounds));
  return (1.858 * side.rsd) / Math.sqrt(n);
}

/**
 * Fold N rounds into one figure per benchmark: the median of the round medians,
 * and the WORST spread seen (pessimistic — a benchmark that was noisy in any
 * round is treated as noisy).
 */
function fold(files: ResultsFile[]): Map<string, Side> {
  const grouped = new Map<string, BenchResult[]>();
  for (const file of files) {
    for (const result of file.results) {
      const list = grouped.get(key(result)) ?? [];
      list.push(result);
      grouped.set(key(result), list);
    }
  }

  const out = new Map<string, Side>();
  for (const [name, list] of grouped) {
    const unavailable = list.find((r) => r.unavailable !== undefined)?.unavailable;
    const usable = list.filter((r) => r.unavailable === undefined && Number.isFinite(r.median));
    if (usable.length === 0) {
      out.set(name, {
        perRound: [],
        median: Number.NaN,
        gating: list[0]!.gating ?? true,
        rsd: Number.NaN,
        samples: 0,
        rounds: 0,
        unit: list[0]!.unit,
        threshold: list[0]!.threshold,
        unavailable: unavailable ?? "no usable samples",
      });
      continue;
    }
    const med = median(usable.map((r) => r.median));
    const spreads = usable.map((r) => {
      const iqr = r.p75 - r.p25;
      return r.median > 0 ? iqr / (2 * r.median) : 0;
    });
    out.set(name, {
      perRound: usable.map((r) => r.median),
      median: med,
      gating: usable[0]!.gating ?? true,
      // Pessimistic: a benchmark noisy in any round is treated as noisy.
      rsd: Math.max(...spreads),
      samples: Math.min(...usable.map((r) => r.samples)),
      rounds: usable.length,
      unit: usable[0]!.unit,
      threshold: usable[0]!.threshold,
    });
  }
  return out;
}

type Verdict = "regressed" | "improved" | "neutral" | "new" | "removed" | "unavailable";

interface Row {
  name: string;
  unit: string;
  gating: boolean;
  baseline: number;
  current: number;
  ratio: number;
  tolerance: number;
  verdict: Verdict;
  note?: string;
  thresholdBreach?: { threshold: number; value: number };
}

const [baselineFilesLoaded, currentFilesLoaded] = await Promise.all([
  load(baselineFiles),
  load(currentFiles),
]);
const baseline = fold(baselineFilesLoaded);
const current = fold(currentFilesLoaded);

const rows: Row[] = [];
const names = [...new Set([...baseline.keys(), ...current.keys()])].sort();

for (const name of names) {
  const b = baseline.get(name);
  const c = current.get(name);

  if (c === undefined) {
    rows.push({
      name,
      unit: b!.unit,
      gating: b!.gating,
      baseline: b!.median,
      current: Number.NaN,
      ratio: Number.NaN,
      tolerance: 0,
      verdict: "removed",
      note: "not present in current",
    });
    continue;
  }

  // A threshold breach is judged on the current tree alone — it is the
  // absolute floor and does not depend on having a baseline.
  const thresholdBreach =
    c.threshold !== undefined && Number.isFinite(c.median) && c.median > c.threshold
      ? { threshold: c.threshold, value: c.median }
      : undefined;

  if (c.unavailable !== undefined) {
    rows.push({
      name,
      unit: c.unit,
      gating: c.gating,
      baseline: b?.median ?? Number.NaN,
      current: Number.NaN,
      ratio: Number.NaN,
      tolerance: 0,
      verdict: "unavailable",
      note: c.unavailable,
    });
    continue;
  }

  if (b === undefined || b.unavailable !== undefined || !Number.isFinite(b.median)) {
    rows.push({
      name,
      unit: c.unit,
      gating: c.gating,
      baseline: Number.NaN,
      current: c.median,
      ratio: Number.NaN,
      tolerance: 0,
      verdict: "new",
      note: b?.unavailable ?? "no baseline",
      thresholdBreach,
    });
    continue;
  }

  // Pair the sides round by round. Each round's two measurements sit next to
  // each other in time, so a per-round ratio cancels whatever the machine was
  // doing at that moment; the spread of those ratios then measures the noise in
  // exactly the quantity being judged. Folding each side to a single median
  // first throws that pairing away — and with it the only estimate that sees
  // BETWEEN-run variance. That mattered: `stream map/filter/take` holds to
  // 1-2% within a run and still moved 30% between runs, so a within-run
  // estimate called an unchanged tree a regression.
  const paired = Math.min(b.perRound.length, c.perRound.length);
  const ratios: number[] = [];
  for (let i = 0; i < paired; i++) {
    const denominator = b.perRound[i]!;
    if (denominator > 0) ratios.push(c.perRound[i]! / denominator);
  }

  const ratio = ratios.length > 0 ? median(ratios) : c.median / b.median;

  let tolerance: number;
  if (ratios.length >= 3) {
    const mean = ratios.reduce((sum, r) => sum + r, 0) / ratios.length;
    const variance =
      ratios.reduce((sum, r) => sum + (r - mean) * (r - mean), 0) / (ratios.length - 1);
    const standardError = Math.sqrt(variance) / Math.sqrt(ratios.length);
    tolerance = Math.max(MIN_TOLERANCE, (NOISE_K * standardError) / Math.max(mean, 1e-9));
  } else {
    // Too few rounds to estimate between-run noise; fall back to the within-run
    // standard error, which is better than nothing.
    const seB = relativeStandardError(b);
    const seC = relativeStandardError(c);
    tolerance = Math.max(MIN_TOLERANCE, NOISE_K * Math.sqrt(seB * seB + seC * seC));
  }

  // A verdict also has to be CONSISTENT. Noise rarely pushes the same benchmark
  // the same way in most rounds; a real regression does. With three or more
  // rounds, require a majority to agree before failing a build.
  const over = ratios.filter((r) => r - 1 > tolerance).length;
  const under = ratios.filter((r) => 1 - r > tolerance).length;
  const majority = (count: number): boolean => ratios.length < 3 || count * 2 > ratios.length;

  let verdict: Verdict = "neutral";
  if (ratio - 1 > tolerance && majority(over)) verdict = "regressed";
  else if (1 - ratio > tolerance && majority(under)) verdict = "improved";

  rows.push({
    name,
    unit: c.unit,
    gating: c.gating,
    baseline: b.median,
    current: c.median,
    ratio,
    tolerance,
    verdict,
    thresholdBreach,
  });
}

// ── Report ────────────────────────────────────────────────────────

const allRegressions = rows.filter((r) => r.verdict === "regressed");
const regressions = allRegressions.filter((r) => r.gating);
const informational = allRegressions.filter((r) => !r.gating);
const breaches = rows.filter((r) => r.thresholdBreach !== undefined);
const improvements = rows.filter((r) => r.verdict === "improved");

const icon: Record<Verdict, string> = {
  regressed: "🔴",
  improved: "🟢",
  neutral: "⚪",
  new: "🆕",
  removed: "⚠️",
  unavailable: "⚠️",
};

const pct = (ratio: number): string =>
  Number.isFinite(ratio) ? `${ratio >= 1 ? "+" : ""}${((ratio - 1) * 100).toFixed(1)}%` : "—";
const num = (value: number): string => (Number.isFinite(value) ? value.toFixed(2) : "—");

const lines: string[] = [];
lines.push("## Performance: current vs baseline");
lines.push("");

const baseCommit = baselineFilesLoaded[0]?.commit.slice(0, 8) ?? "?";
const curCommit = currentFilesLoaded[0]?.commit.slice(0, 8) ?? "?";
lines.push(
  `Baseline \`${baseCommit}\` vs current \`${curCommit}\` — ` +
    `${baselineFilesLoaded.length} × ${currentFilesLoaded.length} interleaved rounds on the same runner.`,
);
lines.push("");

if (regressions.length > 0) {
  lines.push(`**${regressions.length} regression(s)** beyond the noise band.`);
} else if (breaches.length > 0) {
  lines.push(`**${breaches.length} absolute threshold breach(es).**`);
} else {
  lines.push("No regressions beyond the noise band.");
}
if (informational.length > 0) {
  lines.push("");
  lines.push(
    `🟡 ${informational.length} non-gating benchmark(s) moved beyond tolerance ` +
      "(informational suites — reported, never fatal).",
  );
}
lines.push("");
lines.push("| | benchmark | baseline | current | change | tolerance |");
lines.push("|---|---|---:|---:|---:|---:|");
for (const row of rows) {
  const mark = row.verdict === "regressed" && !row.gating ? "🟡" : icon[row.verdict];
  const tol = row.tolerance > 0 ? `±${(row.tolerance * 100).toFixed(1)}%` : "—";
  const note = row.note !== undefined ? ` _(${row.note})_` : "";
  const breach =
    row.thresholdBreach !== undefined ? ` **> ${row.thresholdBreach.threshold} ${row.unit}**` : "";
  lines.push(
    `| ${mark} | ${row.name}${note}${breach} | ${num(row.baseline)} | ${num(row.current)} ${row.unit} | ${pct(row.ratio)} | ${tol} |`,
  );
}
lines.push("");
lines.push(
  `<sub>Tolerance is per-benchmark: max(${(MIN_TOLERANCE * 100).toFixed(0)}%, ` +
    `${NOISE_K} × standard error of the per-round ratios). A change must also move the same ` +
    `way in a majority of rounds. Anything less is indistinguishable from runner noise.</sub>`,
);
lines.push("");

const markdown = `${lines.join("\n")}\n`;
await mkdir(dirname(OUT), { recursive: true });
await writeFile(OUT, markdown);
console.log(markdown);

const summary = process.env.GITHUB_STEP_SUMMARY;
if (summary !== undefined && summary.length > 0) await appendFile(summary, `\n${markdown}`);

if (improvements.length > 0) {
  console.log(`${improvements.length} benchmark(s) improved beyond the noise band.`);
}
for (const row of informational) {
  console.log(`note (non-gating) ${row.name}: ${pct(row.ratio)}`);
}
if (regressions.length > 0 || breaches.length > 0) {
  for (const row of regressions) {
    console.error(
      `REGRESSION ${row.name}: ${pct(row.ratio)} (tolerance ±${(row.tolerance * 100).toFixed(1)}%)`,
    );
  }
  for (const row of breaches) {
    console.error(
      `THRESHOLD ${row.name}: ${num(row.thresholdBreach!.value)} > ${row.thresholdBreach!.threshold} ${row.unit}`,
    );
  }
  process.exit(1);
}
