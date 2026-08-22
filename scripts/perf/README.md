# Performance harness

Two mechanisms with different jobs.

**The baseline comparison is the real gate.** It measures the current tree and a
git baseline _on the same machine, in the same job_, alternating between them,
and flags any change that clears a per-benchmark noise band **and** moves the
same way in a majority of rounds. On a quiet runner it resolves regressions from
about 12%; on a loaded one the noisier benchmarks widen their own band and stay
quiet rather than guessing.

**The absolute thresholds are a catastrophic floor.** Deliberately generous
(~20× the median measured on a dev machine), they exist to catch "something went
badly wrong" without ever false-positiving on a slow runner.

```bash
bun run perf:collect      # measure, write .perf/current.json
bun run perf:gate         # measure + enforce absolute thresholds
bun run perf:compare --baseline main          # the real check
bun run perf:compare --baseline HEAD --rounds 4   # is my uncommitted change slow?
```

## Why not store main's numbers and compare later

Runner-to-runner variance on shared CI is larger than most regressions worth
catching, so a baseline recorded on a different machine days ago tells you
almost nothing. Measuring both trees in one job cancels the machine out, which
is the only way a tight tolerance is honest. It costs 2× the benchmark time.

## Design decisions, and the evidence for them

Each of these was a measured failure before it was a rule.

**Prime every case before measuring any.** Measuring in declaration order made
whichever case ran first look _slowest_, because it absorbed JIT tiering and
connection setup that later cases then reused. The first HTTP run had
`@perfect/http` apparently beating raw `fetch` — the baseline had simply gone
first. Priming also changed `run(sync)` by ~20×, which is why thresholds
calibrated before 2026-08-21 are not comparable to later numbers.

**Alternate the order of the two sides between rounds.** Interleaving alone is
not enough: with a fixed current→baseline order, drift over the run biases
whichever side always goes second. On a tree with _no source changes_, that
produced five benchmarks all reading 0.6–12% "faster". Flipping the order every
round cancels first-order drift; the same test then read −3.2% to +2.0%, all
neutral.

**Compare round for round, not median to median.** This went through two wrong
answers before the right one.

The first attempt used the raw interquartile spread, which produced tolerances
of ±50–87% — wide enough to wave a catastrophic regression through. The second
used the standard error of the median, which is the correct statistic for
_within-run_ uncertainty:

```
sigma   ~= IQR / 1.349                 robust spread estimate
SE(med) ~= 1.253 * sigma / sqrt(n)     standard error of a median
=> SE_rel = 1.858 * rsd / sqrt(n)      rsd = IQR / (2 * median)
```

That is still the fallback when there are fewer than three rounds. But it has a
structural blind spot: a benchmark can be perfectly steady _within_ a run and
still shift between runs, as JIT decisions, memory layout and CPU frequency
settle differently. `stream map/filter/take` holds to 1–2% within a run and was
observed moving 30% between runs — so a within-run estimate called an unchanged
tree a regression.

With three or more rounds the comparison is therefore **paired**: round _i_ of
current is divided by round _i_ of baseline. The two measurements sit next to
each other in time, so the ratio cancels whatever the machine was doing, and the
spread of those ratios measures noise in exactly the quantity being judged.
Tolerance is `max(12%, K × standard error of the ratios)`, which widens itself
precisely on the benchmarks that need it — in practice 12% on steady ones and
20–24% on jittery ones.

**A verdict must also be consistent.** Noise rarely pushes the same benchmark the
same way in most rounds; a real regression does. With three or more rounds a
majority must agree before the build fails.

**Some suites cannot gate at all.** `Suite.gating: false` marks a suite whose
run-to-run variance swamps what it measures — its rows are reported with 🟡 and
never fail the build. The HTTP suite is the case in point: a full comparison
between two commits touching no HTTP code still reported axios +30% and raw
fetch +23%.

**One set of suite definitions.** `suites/` is the single source of truth, used
by the gate, the comparison, and the local HTTP report. Two copies would drift,
and a perf tool nobody trusts is worse than none.

**The harness is copied from the current tree into the baseline worktree.** Only
the measured source differs. Otherwise an edit to the benchmark definitions
would show up as a performance change, and a baseline predating the harness
could not be measured at all.

**A case that throws is recorded as `unavailable`, never dropped.** Baselines are
older trees; a case exercising an API that did not exist yet must show up as "no
baseline", not as a silent pass.

## Validation

Both directions were tested against a real tree, and should be re-tested if the
statistics change:

- _False positives_ — comparing two commits with no source change between them
  must report every benchmark neutral. It does: −8.9% to +3.8% over four rounds,
  with tolerances self-widening to ±24% on the jittery benchmarks.
- _True positives_ — a large slowdown injected into `succeed()` was caught on
  three benchmarks at +228%, +130% and +157%. A deliberately modest one was
  caught on `all x100 run` at +20.8%. Both exited 1.

Every intermediate version of the statistics passed one of these tests and
failed the other; that is what the two together are for.

## CI

The `performance` job runs the absolute gate, then compares against the PR's
merge-base (or `HEAD~1` on a push), and posts the table as a sticky PR comment.
`performance-history` appends each main run to the `perf-history` branch, which
holds `history.jsonl` plus a rendered trend — the comparison gate catches one bad
commit, but cannot see a 3% regression repeated ten times where every step is
inside tolerance.

## Files

| file              | role                                                   |
| ----------------- | ------------------------------------------------------ |
| `suites/types.ts` | `BenchCase` / `Suite` / results schema                 |
| `suites/core.ts`  | runtime benchmarks + absolute thresholds               |
| `suites/http.ts`  | HTTP client comparison (no thresholds — relative only) |
| `collect.ts`      | measure suites → `results.json`                        |
| `compare.ts`      | baseline vs current → verdict + markdown               |
| `compare-refs.ts` | worktree + interleaved rounds, calls `compare`         |
| `gate.ts`         | absolute-threshold check                               |
| `history.ts`      | append a run, render the trend                         |
