// Benchmark this tree against another git ref, on this machine, right now.
//
//   bun scripts/perf/compare-refs.ts --baseline main
//   bun scripts/perf/compare-refs.ts --baseline HEAD~1 --rounds 3
//
// Why not store main's numbers and compare against them later: runner-to-runner
// variance on shared CI is larger than most regressions worth catching, so a
// stored baseline measured on a different machine days ago tells you almost
// nothing. Measuring both trees in the same job, alternating between them,
// cancels the machine out — which is the only way a tight tolerance is honest.
//
// Rounds alternate, AND the order within each round flips: round 1 runs
// current then baseline, round 2 runs baseline then current. Interleaving alone
// is not enough — with a fixed order, any drift over the run (thermal, a
// background process ramping up) biases whichever side always goes second. With
// the order flipped, first-order drift cancels between rounds. This was not
// theoretical: a fixed order made all five core benchmarks look 0.6-12% faster
// on a tree with no source changes at all.
//
// The harness (scripts/perf) is copied from THIS tree into the baseline
// worktree before running. Only the measured source differs; otherwise an edit
// to the benchmark definitions would show up as a performance change, and a
// baseline predating the harness could not be measured at all.

import { cp, mkdir, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

function arg(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1] !== undefined) return process.argv[index + 1];
  return fallback;
}

const BASELINE_REF = arg("baseline", "main")!;
const ROUNDS = Number(arg("rounds", "2"));
const OUT_DIR = resolve(arg("out-dir", ".perf")!);
const WORKTREE = resolve(arg("worktree", ".perf/baseline-tree")!);
const KEEP = process.argv.includes("--keep-worktree");
const passthrough: string[] = [];
for (const name of ["samples", "warmup", "prime", "suite", "min-tolerance", "noise-k"]) {
  const value = arg(name);
  if (value !== undefined) passthrough.push(`--${name}`, value);
}
const collectArgs = passthrough.filter((_, i, a) => {
  // only collect-relevant flags
  const flag = i % 2 === 0 ? a[i]! : a[i - 1]!;
  return ["--samples", "--warmup", "--prime", "--suite"].includes(flag);
});
const compareArgs = passthrough.filter((_, i, a) => {
  const flag = i % 2 === 0 ? a[i]! : a[i - 1]!;
  return ["--min-tolerance", "--noise-k"].includes(flag);
});

const repoRoot = resolve(import.meta.dir, "../..");

async function sh(command: string[], cwd = repoRoot, quiet = false): Promise<number> {
  const proc = Bun.spawn(command, {
    cwd,
    stdout: quiet ? "ignore" : "inherit",
    stderr: quiet ? "ignore" : "inherit",
    env: process.env,
  });
  return proc.exited;
}

async function shOut(command: string[], cwd = repoRoot): Promise<string> {
  const proc = Bun.spawn(command, { cwd, stdout: "pipe", stderr: "ignore" });
  const text = await new Response(proc.stdout).text();
  await proc.exited;
  return text.trim();
}

// ── Resolve the baseline commit ───────────────────────────────────

const baselineSha = await shOut(["git", "rev-parse", BASELINE_REF]);
if (baselineSha === "") {
  console.error(`Cannot resolve baseline ref "${BASELINE_REF}"`);
  process.exit(2);
}
const headSha = await shOut(["git", "rev-parse", "HEAD"]);
// Comparing against HEAD is legitimate when the working tree is dirty — that is
// "did my uncommitted change cost anything". Only refuse when there is
// genuinely nothing between the two.
const dirty = (await shOut(["git", "status", "--porcelain"])) !== "";
if (baselineSha === headSha && !dirty) {
  console.error(
    `Baseline ${BASELINE_REF} (${baselineSha.slice(0, 8)}) is the current commit and the tree is clean — nothing to compare.`,
  );
  process.exit(2);
}

console.log(`Baseline : ${BASELINE_REF} ${baselineSha.slice(0, 8)}`);
console.log(`Current  : HEAD ${headSha.slice(0, 8)}`);
console.log(`Rounds   : ${ROUNDS} (interleaved)\n`);

// ── Prepare the baseline worktree ─────────────────────────────────

await rm(WORKTREE, { recursive: true, force: true });
await sh(["git", "worktree", "prune"], repoRoot, true);
if (
  (await sh(["git", "worktree", "add", "--detach", WORKTREE, baselineSha], repoRoot, true)) !== 0
) {
  console.error("git worktree add failed");
  process.exit(1);
}

async function cleanup(): Promise<void> {
  if (KEEP) {
    console.log(`\nBaseline worktree kept at ${WORKTREE}`);
    return;
  }
  await rm(WORKTREE, { recursive: true, force: true });
  await sh(["git", "worktree", "prune"], repoRoot, true);
}

try {
  // Same harness on both sides — only the measured source differs.
  await rm(join(WORKTREE, "scripts/perf"), { recursive: true, force: true });
  await mkdir(join(WORKTREE, "scripts"), { recursive: true });
  await cp(join(repoRoot, "scripts/perf"), join(WORKTREE, "scripts/perf"), { recursive: true });

  console.log("Installing baseline dependencies…");
  if ((await sh(["bun", "install", "--frozen-lockfile"], WORKTREE, true)) !== 0) {
    // A baseline whose lockfile predates a dependency the harness needs still
    // has to install something, so fall back to a non-frozen install.
    console.log("  frozen install failed, retrying unfrozen");
    if ((await sh(["bun", "install"], WORKTREE, true)) !== 0) {
      console.error("bun install failed in the baseline worktree");
      process.exit(1);
    }
  }

  await mkdir(OUT_DIR, { recursive: true });

  const currentFiles: string[] = [];
  const baselineFiles: string[] = [];

  for (let round = 1; round <= ROUNDS; round++) {
    const currentOut = join(OUT_DIR, `current-${round}.json`);
    const baselineOut = join(OUT_DIR, `baseline-${round}.json`);

    const runCurrent = async (): Promise<void> => {
      console.log(`\n── Round ${round}/${ROUNDS}: current ──`);
      if (
        (await sh(
          [
            "bun",
            "scripts/perf/collect.ts",
            "--out",
            currentOut,
            "--label",
            "current",
            ...collectArgs,
          ],
          repoRoot,
        )) !== 0
      ) {
        console.error("collect failed on the current tree");
        process.exit(1);
      }
      currentFiles.push(currentOut);
    };

    const runBaseline = async (): Promise<void> => {
      console.log(`\n── Round ${round}/${ROUNDS}: baseline ──`);
      // Absolute out path so the worktree writes into the main .perf directory.
      if (
        (await sh(
          [
            "bun",
            "scripts/perf/collect.ts",
            "--out",
            baselineOut,
            "--label",
            "baseline",
            ...collectArgs,
          ],
          WORKTREE,
        )) !== 0
      ) {
        console.error("collect failed on the baseline tree");
        process.exit(1);
      }
      baselineFiles.push(baselineOut);
    };

    // Flip the order every round so drift over the run cancels instead of
    // always penalising whichever side goes second.
    if (round % 2 === 1) {
      await runCurrent();
      await runBaseline();
    } else {
      await runBaseline();
      await runCurrent();
    }
  }

  console.log("\n── Comparison ──\n");
  const compare = [
    "bun",
    "scripts/perf/compare.ts",
    ...baselineFiles.flatMap((f) => ["--baseline", f]),
    ...currentFiles.flatMap((f) => ["--current", f]),
    "--out",
    join(OUT_DIR, "compare.md"),
    ...compareArgs,
  ];
  const code = await sh(compare, repoRoot);
  await cleanup();
  process.exit(code);
} catch (error) {
  console.error(error);
  await cleanup();
  process.exit(1);
}
