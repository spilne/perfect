// Measure every suite and emit one results file.
//
//   bun scripts/perf/collect.ts --out .perf/current.json --label current
//   bun scripts/perf/collect.ts --suite core --samples 100
//
// Two habits here exist because getting them wrong produced visibly wrong
// numbers earlier:
//
//   1. EVERY case is primed before ANY case is measured. Measuring in
//      declaration order made whichever case ran first look slowest, because it
//      absorbed JIT tiering and connection setup the later ones then reused.
//   2. A case that throws is recorded as `unavailable`, not dropped. Baselines
//      are older trees; a case exercising an API that did not exist yet must
//      show up as "no baseline", never as a silent pass.

import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { measure } from "mitata";
import { coreSuite } from "./suites/core";
import { httpSuite } from "./suites/http";
import type { BenchResult, ResultsFile, Suite } from "./suites/types";

const ALL_SUITES: readonly Suite[] = [coreSuite, httpSuite];

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

const OUT = arg("out", ".perf/current.json")!;
const LABEL = arg("label", "current")!;
const SAMPLES = Number(arg("samples", process.env.PERF_SAMPLES ?? "50"));
const WARMUP = Number(arg("warmup", process.env.PERF_WARMUP ?? "10"));
const PRIME = Number(arg("prime", process.env.PERF_PRIME ?? "40"));

const selected = argAll("suite");
const suites =
  selected.length > 0 ? ALL_SUITES.filter((s) => selected.includes(s.name)) : ALL_SUITES;
if (suites.length === 0) {
  console.error(
    `No suites matched ${selected.join(", ")}. Known: ${ALL_SUITES.map((s) => s.name).join(", ")}`,
  );
  process.exit(2);
}

async function sh(command: string[]): Promise<string> {
  const proc = Bun.spawn(command, { stdout: "pipe", stderr: "ignore" });
  const out = await new Response(proc.stdout).text();
  await proc.exited;
  return out.trim();
}

const results: BenchResult[] = [];

for (const suite of suites) {
  await suite.setup?.();
  try {
    const cases = await suite.cases();

    // Phase 1 — prime everything, so no case pays for another's warmup.
    const broken = new Map<string, string>();
    for (const c of cases) {
      try {
        for (let i = 0; i < PRIME; i++) await c.run();
      } catch (error) {
        broken.set(c.name, error instanceof Error ? error.message : String(error));
      }
    }

    // Phase 2 — measure.
    for (const c of cases) {
      const failure = broken.get(c.name);
      if (failure !== undefined) {
        console.error(`  ! ${suite.name}/${c.name}: ${failure}`);
        results.push({
          suite: suite.name,
          name: c.name,
          unit: c.unit,
          median: Number.NaN,
          p25: Number.NaN,
          p75: Number.NaN,
          p99: Number.NaN,
          samples: 0,
          threshold: c.threshold,
          gating: suite.gating ?? true,
          unavailable: failure,
        });
        continue;
      }

      const stats = await measure(c.run, {
        min_samples: SAMPLES,
        max_samples: SAMPLES,
        warmup_samples: WARMUP,
      });
      results.push({
        suite: suite.name,
        name: c.name,
        unit: c.unit,
        median: stats.p50 / c.divisor,
        p25: (stats as any).p25 / c.divisor,
        p75: (stats as any).p75 / c.divisor,
        p99: stats.p99 / c.divisor,
        samples: SAMPLES,
        threshold: c.threshold,
        gating: suite.gating ?? true,
      });
      const last = results[results.length - 1]!;
      console.log(
        `  ${suite.name}/${c.name}: ${last.median.toFixed(2)} ${c.unit} (p99 ${last.p99.toFixed(2)})`,
      );
    }
  } finally {
    await suite.teardown?.();
  }
}

const file: ResultsFile = {
  schemaVersion: 1,
  commit: (await sh(["git", "rev-parse", "HEAD"])) || "unknown",
  ref: (await sh(["git", "rev-parse", "--abbrev-ref", "HEAD"])) || "unknown",
  label: LABEL,
  runtime: {
    bun: Bun.version,
    platform: process.platform,
    arch: process.arch,
    cpus: navigator.hardwareConcurrency ?? 0,
  },
  config: { samples: SAMPLES, warmup: WARMUP },
  results,
};

await mkdir(dirname(OUT), { recursive: true });
await writeFile(OUT, `${JSON.stringify(file, null, 2)}\n`);
console.log(`\nWrote ${results.length} results to ${OUT}`);
