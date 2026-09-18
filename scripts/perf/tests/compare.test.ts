// The screen/confirm contract, exercised through the real CLI.
//
// These are the decisions that fail or pass a build, so they are tested against
// synthetic results files rather than by running the benchmarks: the point is
// the verdict logic, and feeding it fabricated numbers is the only way to assert
// on an exact one.

import { describe, expect, it } from "bun:test";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const repoRoot = resolve(import.meta.dir, "../../..");
const compare = join(repoRoot, "scripts/perf/compare.ts");

interface CaseSpec {
  readonly name: string;
  readonly median: number;
  readonly gating?: boolean;
  readonly threshold?: number;
}

async function writeResults(
  dir: string,
  file: string,
  label: string,
  cases: readonly CaseSpec[],
): Promise<string> {
  const path = join(dir, file);
  await writeFile(
    path,
    JSON.stringify({
      schemaVersion: 1,
      commit: "0".repeat(40),
      ref: "HEAD",
      label,
      runtime: { bun: "1.0.0", platform: "linux", arch: "x64", cpus: 4 },
      config: { samples: 50, warmup: 10 },
      results: cases.map((c) => ({
        suite: "core",
        name: c.name,
        unit: "ns/op",
        median: c.median,
        // A tight IQR keeps the fallback statistic out of the way; with three or
        // more rounds the paired ratios decide anyway.
        p25: c.median * 0.99,
        p75: c.median * 1.01,
        p99: c.median * 1.02,
        samples: 50,
        threshold: c.threshold,
        gating: c.gating ?? true,
      })),
    }),
  );
  return path;
}

/** Three rounds a side, with `currents` scaled off `baselines`. */
async function rounds(
  dir: string,
  tag: string,
  perRound: readonly (readonly CaseSpec[])[],
  label: string,
): Promise<string[]> {
  return Promise.all(
    perRound.map(async (cases, i) => writeResults(dir, `${tag}-${i + 1}.json`, label, cases)),
  );
}

const flat = (name: string, values: readonly number[], gating = true): readonly CaseSpec[][] =>
  values.map((median) => [{ name, median, gating }]);

async function run(
  args: readonly string[],
): Promise<{ code: number; stdout: string; stderr: string }> {
  const proc = Bun.spawn(["bun", compare, ...args], {
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, GITHUB_STEP_SUMMARY: "" },
  });
  const [stdout, stderr] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  return { code: await proc.exited, stdout, stderr };
}

async function setup(
  baseline: readonly number[],
  current: readonly number[],
  name = "steady",
): Promise<{ dir: string; args: string[] }> {
  const dir = await mkdtemp(join(tmpdir(), "perf-compare-"));
  const b = await rounds(dir, "baseline", flat(name, baseline), "baseline");
  const c = await rounds(dir, "current", flat(name, current), "current");
  return {
    dir,
    args: [
      ...b.flatMap((f) => ["--baseline", f]),
      ...c.flatMap((f) => ["--current", f]),
      "--out",
      join(dir, "out.md"),
    ],
  };
}

describe("compare --pass screen", () => {
  it("passes and shortlists nothing when the rounds agree", async () => {
    const { dir, args } = await setup([100, 101, 99], [100, 100, 101]);
    const flagged = join(dir, "flagged.json");
    const result = await run([...args, "--pass", "screen", "--flagged-out", flagged]);

    expect(result.code).toBe(0);
    expect(JSON.parse(await readFile(flagged, "utf8")).cases).toEqual([]);
  });

  it("shortlists a clear regression but does NOT fail the build", async () => {
    // +40% in every round: far beyond the 12% floor and unanimous.
    const { dir, args } = await setup([100, 101, 99], [140, 141, 139]);
    const flagged = join(dir, "flagged.json");
    const result = await run([...args, "--pass", "screen", "--flagged-out", flagged]);

    expect(result.code).toBe(0);
    expect(result.stderr).toContain("FLAGGED core/steady");
    const shortlist = JSON.parse(await readFile(flagged, "utf8"));
    expect(shortlist.cases.map((c: { name: string }) => c.name)).toEqual(["core/steady"]);
    expect(shortlist.cases[0].ratio).toBeGreaterThan(1.3);
  });

  it("never shortlists a non-gating benchmark", async () => {
    const dir = await mkdtemp(join(tmpdir(), "perf-compare-"));
    const b = await rounds(dir, "baseline", flat("jittery", [100, 101, 99], false), "baseline");
    const c = await rounds(dir, "current", flat("jittery", [140, 141, 139], false), "current");
    const flagged = join(dir, "flagged.json");
    const result = await run([
      ...b.flatMap((f) => ["--baseline", f]),
      ...c.flatMap((f) => ["--current", f]),
      "--out",
      join(dir, "out.md"),
      "--pass",
      "screen",
      "--flagged-out",
      flagged,
    ]);

    expect(result.code).toBe(0);
    expect(JSON.parse(await readFile(flagged, "utf8")).cases).toEqual([]);
  });

  it("still fails on an absolute threshold breach — a different mechanism", async () => {
    const dir = await mkdtemp(join(tmpdir(), "perf-compare-"));
    const spec = (median: number): CaseSpec[] => [{ name: "steady", median, threshold: 102 }];
    const b = await rounds(dir, "baseline", [spec(100), spec(100), spec(100)], "baseline");
    const c = await rounds(dir, "current", [spec(105), spec(105), spec(105)], "current");
    const result = await run([
      ...b.flatMap((f) => ["--baseline", f]),
      ...c.flatMap((f) => ["--current", f]),
      "--out",
      join(dir, "out.md"),
      "--pass",
      "screen",
      "--flagged-out",
      join(dir, "flagged.json"),
    ]);

    // The relative change is nothing; the absolute ceiling is what fires, and a
    // screening pass does not get to defer it.
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("THRESHOLD");
  });
});

describe("compare --pass confirm", () => {
  const shortlistFile = async (dir: string, names: readonly string[]): Promise<string> => {
    const path = join(dir, "flagged.json");
    await writeFile(
      path,
      JSON.stringify({
        rounds: 3,
        cases: names.map((name) => ({
          name,
          suite: "core",
          unit: "ns/op",
          ratio: 1.4,
          tolerance: 0.12,
        })),
      }),
    );
    return path;
  };

  it("fails when the shortlisted benchmark moves the same way again", async () => {
    const { dir, args } = await setup([100, 101, 99, 100], [140, 141, 139, 140]);
    const result = await run([
      ...args,
      "--pass",
      "confirm",
      "--confirming",
      await shortlistFile(dir, ["core/steady"]),
    ]);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain("REGRESSION core/steady");
  });

  it("passes when the shortlisted benchmark comes back inside tolerance", async () => {
    const { dir, args } = await setup([100, 101, 99, 100], [100, 102, 99, 101]);
    const result = await run([
      ...args,
      "--pass",
      "confirm",
      "--confirming",
      await shortlistFile(dir, ["core/steady"]),
    ]);

    expect(result.code).toBe(0);
    expect(await readFile(join(dir, "out.md"), "utf8")).toContain("Not reproduced");
  });

  it("does not fail on a benchmark the screen never flagged", async () => {
    const { dir, args } = await setup([100, 101, 99, 100], [140, 141, 139, 140]);
    const result = await run([
      ...args,
      "--pass",
      "confirm",
      "--confirming",
      await shortlistFile(dir, ["core/something-else"]),
    ]);

    // `core/something-else` is not in these results at all, so it is also
    // unanswered — which is fatal for its own reason. What matters here is that
    // the +40% on `core/steady` is not what fails it.
    expect(result.stderr).not.toContain("REGRESSION core/steady");
    expect(result.stderr).toContain("UNCONFIRMED core/something-else");
  });

  it("fails rather than passing silently when a shortlisted benchmark was not re-measured", async () => {
    const { dir, args } = await setup([100, 101, 99, 100], [100, 100, 101, 100]);
    const result = await run([
      ...args,
      "--pass",
      "confirm",
      "--confirming",
      await shortlistFile(dir, ["core/steady", "core/vanished"]),
    ]);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain("UNCONFIRMED core/vanished");
  });
});

describe("collect --only", () => {
  it("measures just the named cases, and refuses a name that matches nothing", async () => {
    const dir = await mkdtemp(join(tmpdir(), "perf-collect-"));
    const out = join(dir, "only.json");
    const collect = join(repoRoot, "scripts/perf/collect.ts");
    const args = ["--suite", "core", "--samples", "1", "--warmup", "1", "--prime", "1"];

    const ok = Bun.spawn(
      ["bun", collect, "--out", out, ...args, "--only", "core/all(succeed) x100 fast path"],
      { cwd: repoRoot, stdout: "ignore", stderr: "ignore" },
    );
    expect(await ok.exited).toBe(0);
    const results = JSON.parse(await readFile(out, "utf8")).results;
    expect(results.map((r: { name: string }) => r.name)).toEqual(["all(succeed) x100 fast path"]);

    const typo = Bun.spawn(["bun", collect, "--out", out, ...args, "--only", "core/nope"], {
      cwd: repoRoot,
      stdout: "ignore",
      stderr: "ignore",
    });
    expect(await typo.exited).toBe(2);
  });
});
