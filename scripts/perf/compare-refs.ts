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
//
// TWO PASSES. Those rounds are a SCREEN: they decide which benchmarks deserve a
// second look, and they never fail the build on their own. Whatever they flag is
// then RE-MEASURED over more rounds — only the flagged benchmarks, so a clean
// run costs nothing extra — and the build fails only if the same benchmark moves
// the same way again.
//
// Three rounds is a sample of three, and a sample of three is wrong often enough
// to matter. Fifteen recorded rounds of an identical-code comparison contain 455
// distinct three-round windows; 7.0% of them flag at least one gating benchmark,
// which is the false-failure rate the gate had. Following each of those windows
// with five confirmation rounds brought it to 0. See scripts/perf/README.md.
//
// The confirmation run uses an EVEN number of rounds. Order-flipping only
// cancels drift when each side leads equally often, and with the three rounds CI
// screens on, the current tree leads twice and the baseline once. That residual
// was measured at ~0.4% on a quiet machine — small, but it is a bias with a
// sign, and the pass that decides the build should not carry one.

import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

function arg(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1] !== undefined) return process.argv[index + 1];
  return fallback;
}

const BASELINE_REF = arg("baseline", "main")!;
const ROUNDS = Number(arg("rounds", "2"));
/**
 * Rounds the confirmation run measures the flagged benchmarks over. Rounded up
 * to an even number so each side leads exactly half of them.
 */
const CONFIRM_ROUNDS = (() => {
  const requested = Number(arg("confirm-rounds", "6"));
  return requested % 2 === 0 ? requested : requested + 1;
})();
/** Skip the confirmation run and let the first pass fail the build, as before. */
const NO_CONFIRM = process.argv.includes("--no-confirm");
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
console.log(
  `Rounds   : ${ROUNDS} (interleaved)` +
    (NO_CONFIRM
      ? ", confirmation disabled\n"
      : `, + ${CONFIRM_ROUNDS} to confirm anything they flag\n`),
);

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

  interface PassResult {
    readonly baselineFiles: readonly string[];
    readonly currentFiles: readonly string[];
  }

  /**
   * Measure both trees over `rounds` interleaved rounds.
   *
   * `currentLeadsFirst` picks which side goes first in round 1; the order flips
   * every round after that. `extra` carries the `--only` filter a confirmation
   * run uses to narrow what gets MEASURED — priming stays whole, so a
   * re-measured benchmark meets the same machine state it did the first time.
   */
  async function runRounds(options: {
    rounds: number;
    prefix: string;
    currentLeadsFirst: boolean;
    extra?: readonly string[];
  }): Promise<PassResult> {
    const { rounds, prefix, currentLeadsFirst, extra = [] } = options;
    const currentFiles: string[] = [];
    const baselineFiles: string[] = [];

    for (let round = 1; round <= rounds; round++) {
      const currentOut = join(OUT_DIR, `${prefix}current-${round}.json`);
      const baselineOut = join(OUT_DIR, `${prefix}baseline-${round}.json`);

      const runCurrent = async (): Promise<void> => {
        console.log(`\n── Round ${round}/${rounds}: current ──`);
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
              ...extra,
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
        console.log(`\n── Round ${round}/${rounds}: baseline ──`);
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
              ...extra,
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
      if ((round % 2 === 1) === currentLeadsFirst) {
        await runCurrent();
        await runBaseline();
      } else {
        await runBaseline();
        await runCurrent();
      }
    }

    return { baselineFiles, currentFiles };
  }

  const compareCommand = (pass: PassResult, out: string, extra: readonly string[]): string[] => [
    "bun",
    "scripts/perf/compare.ts",
    ...pass.baselineFiles.flatMap((f) => ["--baseline", f]),
    ...pass.currentFiles.flatMap((f) => ["--current", f]),
    "--out",
    out,
    ...compareArgs,
    ...extra,
  ];

  const combinedOut = join(OUT_DIR, "compare.md");
  const screenOut = join(OUT_DIR, "compare-screen.md");
  const confirmOut = join(OUT_DIR, "compare-confirm.md");
  const flaggedOut = join(OUT_DIR, "flagged.json");

  // ── Pass 1: screen ──────────────────────────────────────────────

  const screen = await runRounds({ rounds: ROUNDS, prefix: "", currentLeadsFirst: true });

  console.log("\n── Comparison ──\n");
  const screenCode = await sh(
    compareCommand(
      screen,
      NO_CONFIRM ? combinedOut : screenOut,
      NO_CONFIRM ? [] : ["--pass", "screen", "--flagged-out", flaggedOut],
    ),
    repoRoot,
  );
  if (NO_CONFIRM) {
    await cleanup();
    process.exit(screenCode);
  }
  // The report is the reason the job exists, so never let a missing one turn a
  // clear verdict into a stack trace.
  const publishScreen = async (): Promise<void> => {
    if (await Bun.file(screenOut).exists()) await cp(screenOut, combinedOut);
  };

  if (screenCode !== 0) {
    // A screening pass only exits non-zero on an absolute threshold breach,
    // which no amount of re-measurement argues with.
    await publishScreen();
    await cleanup();
    process.exit(screenCode);
  }

  const flagged = JSON.parse(await readFile(flaggedOut, "utf8")) as {
    cases: { name: string; suite: string }[];
  };
  if (flagged.cases.length === 0) {
    await publishScreen();
    await cleanup();
    process.exit(0);
  }

  // ── Pass 2: confirm ─────────────────────────────────────────────

  console.log(
    `\n── Confirming ${flagged.cases.length} flagged benchmark(s) over ${CONFIRM_ROUNDS} rounds ──`,
  );
  for (const c of flagged.cases) console.log(`   ${c.name}`);

  // Only the suites that actually flagged something. Suites are set up, primed,
  // measured and torn down one at a time, so dropping a suite that runs LATER
  // changes nothing about the ones kept — and dropping the HTTP suite saves its
  // server setup. `--only` then narrows the measure phase to the flagged cases
  // while every case in the surviving suites is still primed.
  const suites = [...new Set(flagged.cases.map((c) => c.suite))];
  const confirmArgs = [
    ...(collectArgs.includes("--suite") ? [] : suites.flatMap((s) => ["--suite", s])),
    ...flagged.cases.flatMap((c) => ["--only", c.name]),
  ];

  const confirm = await runRounds({
    rounds: CONFIRM_ROUNDS,
    prefix: "confirm-",
    // Opposite lead to the screen's first round, and an even round count, so
    // this pass is order-balanced on its own.
    currentLeadsFirst: false,
    extra: confirmArgs,
  });

  console.log("\n── Confirmation ──\n");
  const confirmCode = await sh(
    compareCommand(confirm, confirmOut, ["--pass", "confirm", "--confirming", flaggedOut]),
    repoRoot,
  );

  await writeFile(
    combinedOut,
    `${await readFile(screenOut, "utf8")}\n${await readFile(confirmOut, "utf8")}`,
  );
  await cleanup();
  process.exit(confirmCode);
} catch (error) {
  console.error(error);
  await cleanup();
  process.exit(1);
}
