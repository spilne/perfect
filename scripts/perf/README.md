# Performance harness

Two mechanisms with different jobs.

**The baseline comparison is the real gate.** It measures the current tree and a
git baseline _on the same machine, in the same job_, alternating between them,
and flags any change that clears a per-benchmark noise band **and** moves the
same way in a majority of rounds. On a quiet runner it resolves regressions from
about 12%; on a loaded one the noisier benchmarks widen their own band and stay
quiet rather than guessing. Whatever it flags is then **re-measured** before the
build fails — see [Screen, then confirm](#screen-then-confirm).

**The absolute thresholds are a catastrophic floor.** Deliberately generous
(the original cases used ~20× local medians; scaling cases use broader ceilings),
they aim to catch large regressions while leaving room for runner noise.

```bash
bun run perf:collect      # measure, write .perf/current.json
bun run perf:gate         # measure + enforce absolute thresholds
bun run test:perf         # the verdict logic, against fixed numbers
bun run perf:compare --baseline main          # the real check
bun run perf:compare --baseline HEAD --rounds 4   # is my uncommitted change slow?
bun run perf:compare --baseline main --no-confirm # fail on the first pass
```

## Why not store main's numbers and compare later

Runner-to-runner variance on shared CI is larger than most regressions worth
catching, so a baseline recorded on a different machine days ago tells you
almost nothing. Measuring both trees in one job cancels the machine out, which
is the only way a tight tolerance is honest. It costs 2× the benchmark time.

## Screen, then confirm

A flagged benchmark is re-measured before it can fail a build.

The first pass is exactly the comparison described above — three interleaved
rounds on CI — but it is now a **screen**: it reports, it writes a shortlist, and
it exits 0. Anything on that shortlist is then measured again over six more
interleaved rounds, and the build fails only if the same benchmark moves the same
way past its own tolerance a second time. Both passes appear in the job summary
and in the PR comment.

**Why.** Three rounds is a sample of three. The statistics above are honest about
_within-round_ and _between-round_ noise, but they are estimated from three
numbers, and three numbers are wrong often enough to matter:

> Fifteen recorded rounds of an **identical-code** comparison (09eb654 against
> itself, this machine, interleaved and order-flipped) contain 455 distinct
> three-round windows. **7.0% of those windows flag at least one gating
> benchmark.** Following each window with five more rounds and requiring the
> same verdict again: **0 of 455.**

That 7% is not a hypothetical. Four `performance` failures in a row, each on a
different benchmark, each in a situation where a regression was impossible or was
later disproved:

| flagged                                     | comparison                       | why it was not real                                 |
| ------------------------------------------- | -------------------------------- | --------------------------------------------------- |
| `all(sync) x100 fibers` +23.6% (±18.0%)     | PR #26 head                      | a re-run of the same head flagged a different case  |
| `stream map/filter full traversal` +49.9%   | PR #26 head, re-run              | same tree as the run above                          |
| `stream map/filter full traversal` +57.3%   | 09eb654 vs f890d6d               | the case was measuring its own warmup; fixed in #27 |
| `deferred waiter cancellation x1000` +13.2% | d9cf54e vs 09eb654 (#27's merge) | that diff touches `scripts/perf` only               |

The last one is the clearest: the two trees have byte-identical `packages/`, and
the harness is copied from the current tree into the baseline worktree, so both
sides ran the same benchmark definitions over the same runtime. There was nothing
there to regress.

**Why this still catches regressions.** The screen is the old gate, unchanged —
nothing it used to catch stops being caught there. The confirmation run then sees
the same effect with _twice_ the rounds, so its standard error is smaller and its
tolerance tighter: a change the screen can resolve, the confirmation run can
resolve too. What the second pass removes is the 1-in-14 window where three
rounds happened to line up.

**Cost.** Nothing on a clean run, which is the common case: the shortlist is
empty and the job ends after the screen. When something does flag, the
confirmation run measures **only the shortlisted benchmarks** — `collect.ts
--only` narrows the measure phase — so six rounds a side cost about as much as
one full round instead of six.

`--only` deliberately does **not** narrow priming. Priming the whole suite is
what puts the runtime in the state the benchmarks are measured in; priming only
the shortlist measures something else. Measured: priming just
`stream map/filter full traversal` reads ~4.8 ns/item where priming the whole
suite first reads ~7.8 ns/item in the same process. A confirmation run has to
re-ask the same question, not a cheaper one.

**The confirmation run uses an even number of rounds.** Flipping the order every
round only cancels drift when each side leads equally often. With three rounds
the current tree leads twice and the baseline once, leaving a residual; measured
on the identical-code control, the side that leads a round reads ~1.1% slower,
so three rounds carry ~0.4% of systematic bias. Small — but it has a sign, and
the pass that decides a build should not carry one.

**No absolute-effect floor.** A minimum change in nanoseconds was considered and
rejected on the data. The false flags in that control were not sub-nanosecond
artifacts: `range take(1)` moved a median of 640-920 ns/op across them and
`group singleton chunks` 17-30 ns/item. A floor low enough to be safe for a real
regression (≤1 ns/item) blocks none of them; one high enough to block them would
also hide real regressions on exactly the benchmarks whose per-operation cost is
small. The noise here is proportional, so the bar stays proportional.

## Design decisions, and the evidence for them

Each of these was a measured failure before it was a rule.

**Prime every case before measuring any.** Measuring in declaration order made
whichever case ran first look _slowest_, because it absorbed JIT tiering and
connection setup that later cases then reused. The first HTTP run had
`@spilne/perfect-http` apparently beating raw `fetch` — the baseline had simply gone
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

**Some benchmarks cannot gate at all.** `gating: false` — on a whole suite, or
on an individual case — marks something whose run-to-run variance swamps what it
measures. Those rows are reported with 🟡, recorded in the trend, and never fail
the build.

Three are currently marked, each on evidence rather than suspicion:

- the whole **HTTP suite** — a comparison between two commits touching no HTTP
  code reported axios +30% and raw fetch +23%;
- **`runSync(succeed)`** — at ~10–30 ns it is dominated by timer resolution.
  Two consecutive CI runs against trees with no measured code change read −8.6%
  and then +73.6%, the second clearing its own widened ±56.1% band and failing
  the build;
- **`stream map/filter/take`** — ~2–5 ns per item, seen swinging +32% on CI and
  +30% locally between identical trees.

What still gates: `all x100 run`, `flatMap chain x10k runSync` and `run(sync)` —
the rows amortized over enough operations to hold steady. Absolute thresholds
still apply to every row, so a catastrophic change is caught regardless.

**A bimodal row is measuring its own settle, not the runtime.** Before reaching
for `gating: false`, check whether the case is still speeding up when the window
opens. `warmup` on a case overrides the run-wide `--warmup` for exactly that.

`stream map/filter full traversal` is the case in point. One traversal costs
~160 µs, so mitata takes a single iteration per sample and ten warmup samples
are ten iterations. At that warmup almost every process reported 7.6–9.3
ns/item and a few caught the same build already settled and reported 5.1–5.4 —
with nothing in between. Which side of that split each tree landed on then
decided the comparison: over 15 interleaved rounds of two _identical_ trees one
round read −34%, and against the real baseline another read +55%. Both are the
split, not a code change.

Priming harder does not reach it — unchanged at 800 prime iterations — because
priming never enters mitata's measurement loop, which is where the settle
happens. A 200-sample warmup does: across fresh processes the row reads
5.13–5.34 ns/item, a 4% spread with no second mode, and the within-window IQR
stays at 2–5%. Widening the window instead (500 samples) also moves the median,
but it buries the transient rather than excluding it — the IQR and p99 carry it
for the rest of the run. The row keeps its gate and now reports the steady state
it was always meant to.

A long per-case warmup is a fair thing to be suspicious of, because `collect.ts`
measures every case in one process: 200 warmup samples of a 160 µs traversal is
~32 ms of extra work that the twelve cases measured after it inherit the JIT and
GC state from. It was checked. Twelve interleaved, order-flipped rounds of two
trees whose `packages/` are byte-identical and whose harnesses differ only by
this `warmup: 200`: the row itself moves −33.1% (which is the point of it), and
**every other case lands within ±2.4%, none flagged**, including
`deferred waiter cancellation` at +1.1% (x1000) and −1.5% (x8000). Round-to-round
spread does not grow either — mean CV across the suite is 3.05% with the long
warmup against 3.70% without.

It could not have explained a between-tree difference in any case: `compare-refs`
copies `scripts/perf` from the current tree into the baseline worktree, so both
sides of that comparison ran the same 200-sample warmup over the same runtime.

The general rule: if a benchmark's per-operation cost is near the runner's
timing floor, measure it and watch the trend, but do not let it fail a build.

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

The screen/confirm split was validated the same way, at CI settings
(`--rounds 3`) on a machine deliberately left loaded:

- _False positives_ — three identical-code comparisons, all exited 0. The
  interesting one is the second: its screen flagged
  `fiber reverse completion x1000` at **+12.9%** against a ±12.0% band, which is
  a build failure under the old gate. The confirmation run re-measured that one
  benchmark over 6 × 6 rounds and read **+0.1%**. Passed.
- _True positives_ — a deliberately modest slowdown injected into `succeed()`
  (one multiply-modulo per call, ≈ +15% on `all(succeed) x100 fast path`
  measured in isolation). The screen read **+23.1%**, the confirmation run
  **+14.7%** against ±12.0%, and the job exited 1. A regression barely above the
  tolerance floor still fails, which is the property the second pass had to keep.
- _Cost_ — a full `core` round takes ~31 s per side; a round measuring one
  shortlisted case takes ~1 s, because all that remains is priming. Six
  confirmation rounds a side therefore add ~12 s to a ~3 min screen, and nothing
  at all when the shortlist is empty.

## CI

The `performance` job runs the harness's own unit tests and the absolute gate,
then compares against the PR's merge-base (or `HEAD~1` on a push), and posts both
passes as a sticky PR comment.
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
| `compare-refs.ts` | worktree + interleaved rounds, screen then confirm     |
| `gate.ts`         | absolute-threshold check                               |
| `history.ts`      | append a run, render the trend                         |
| `tests/`          | the verdict logic, against fixed numbers               |
