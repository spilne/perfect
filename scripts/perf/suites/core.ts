// Core runtime suite — the same cases the absolute gate enforces.
//
// Definitions live here rather than in packages/core/bench so the gate and the
// baseline comparison measure exactly the same thing. Two copies would drift,
// and a perf tool nobody trusts is worse than none.
//
// On thresholds: these are a CATASTROPHIC floor, not the real gate.
//
// The precise check is the baseline-vs-current comparison (scripts/perf/
// compare.ts), which measures both trees on the same runner in the same job and
// adapts its tolerance to each benchmark's own noise. That reliably catches an
// 8% regression. Absolute thresholds cannot: a shared CI runner is 2-4x slower
// than a dev machine and varies run to run, so any threshold tight enough to
// catch a modest regression also fires on a noisy neighbour.
//
// So the two mechanisms have different jobs. The comparison catches drift; the
// thresholds catch "something went catastrophically wrong" and must never
// false-positive. The original cases were set at 20x the worst median observed locally
// (Apple Silicon, three runs, primed), which leaves ~7x margin even on a
// runner 3x slower.
//
// Measured locally at the time of writing, for reference:
//   runSync(succeed)            12.2-16.6 ns   (IQR spread 12-18%)
//   run(sync)                    181-205 ns    (IQR spread 8-14%)
//   flatMap chain x10k runSync  16.5-16.9 ns   (IQR spread 6-29%)
//   all x100 run                13.0-13.1 ns   (IQR spread 1.5-2.8%)
//   stream map/filter/take      2.15-2.19 ns   (IQR spread 1-2.3%)
//
// Note these differ sharply from pre-2026-08-21 numbers because the harness now
// primes every case before measuring any. run(sync) in particular reads ~20x
// faster once the async path is warm; the old un-primed figures were measuring
// JIT tiering as much as the runtime.
// Scaling cases use broad safety ceilings; paired sizes participate in relative
// comparisons. Historical stream timings above used an inaccurate denominator;
// the renamed early-termination case now reports time per entire run.

import { do_not_optimize } from "mitata";
import { all, run, runSync, Stream, succeed, sync, yieldNow } from "../../../packages/core/src";
import {
  cancelDeferredWaiters,
  completeChildren,
  fillSlidingWindow,
  groupedSingletons,
} from "./core-workloads";
import type { Eff } from "../../../packages/core/src";
import type { BenchCase, Suite } from "./types";

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

export const coreSuite: Suite = {
  name: "core",
  cases(): readonly BenchCase[] {
    return [
      {
        name: "runSync(succeed)",
        unit: "ns/op",
        divisor: 1,
        threshold: 350,
        // Reported, never fatal. At ~10-30ns this is dominated by timer
        // resolution and scheduling jitter on a shared runner. Two consecutive
        // CI runs against trees with NO measured code change read -8.6% and
        // then +73.6%; the second cleared its own widened +-56.1% band and
        // failed the build. The absolute threshold above still catches a
        // catastrophic change.
        gating: false,
        run: () => do_not_optimize(runSync(succeed(42))),
      },
      {
        name: "run(sync)",
        unit: "ns/op",
        divisor: 1,
        threshold: 4_200,
        run: async () => do_not_optimize(await run(sync(() => 42))),
      },
      {
        name: "flatMap chain x10k runSync",
        unit: "ns/op",
        divisor: FLATMAP_N,
        threshold: 350,
        run: () => do_not_optimize(runSync(flatMapChain(FLATMAP_N))),
      },
      {
        name: "all(succeed) x100 fast path",
        unit: "ns/op",
        divisor: ALL_N,
        threshold: 270,
        run: async () =>
          do_not_optimize(await run(all(Array.from({ length: ALL_N }, (_, i) => succeed(i))))),
      },
      {
        name: "stream map/filter/take end-to-end",
        unit: "ns/op",
        divisor: 1,
        threshold: 900_000,
        // Early termination and chunking make input-item normalization misleading.
        gating: false,
        run: async () => do_not_optimize(await run(streamProgram(STREAM_N))),
      },
      ...[false, true].map((suspended): BenchCase => ({
        name: suspended ? "all(yieldNow) x100 fibers" : "all(sync) x100 fibers",
        unit: "ns/item",
        divisor: ALL_N,
        threshold: 50_000,
        run: async () =>
          do_not_optimize(
            await run(
              all(
                Array.from({ length: ALL_N }, (_, i) =>
                  suspended ? yieldNow.map(() => i) : sync(() => i),
                ),
              ),
            ),
          ),
      })),
      {
        name: "stream map/filter full traversal",
        unit: "ns/item",
        divisor: STREAM_N,
        threshold: 1_000,
        run: () =>
          do_not_optimize(
            runSync(
              Stream.range(0, STREAM_N)
                .map((x) => x + 1)
                .filter((x) => x % 3 === 0)
                .drain(),
            ),
          ),
      },
      ...[10_000, 1_000_000].flatMap((n): BenchCase[] => [
        {
          name: `range construction x${n}`,
          unit: "ns/op",
          divisor: 1,
          threshold: 100_000,
          gating: false,
          run: () => do_not_optimize(Stream.range(0, n)),
        },
        {
          name: `range take(1) x${n}`,
          unit: "ns/op",
          divisor: 1,
          threshold: 1_000_000,
          run: () => do_not_optimize(runSync(Stream.range(0, n).take(1).toArray())),
        },
      ]),
      ...[
        { name: "group singleton chunks", workload: groupedSingletons },
        { name: "sliding window fill", workload: fillSlidingWindow },
        { name: "fiber reverse completion", workload: completeChildren },
        { name: "deferred waiter cancellation", workload: cancelDeferredWaiters },
      ].flatMap(({ name, workload }) =>
        [1_000, 8_000].map((n): BenchCase => ({
          name: `${name} x${n}`,
          unit: "ns/item",
          divisor: n,
          threshold: 20_000,
          run: () => do_not_optimize(workload(n)),
        })),
      ),
    ];
  },
};
