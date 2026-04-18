// Bench ensuring + scoped to validate the fiber-vs-frame implementation switch.
//
// Runs N times in a tight loop. Today (fiber per call): each ensuring/scoped
// allocates a Fiber, registers onComplete, schedules via setImmediate.
// After (frame per call): one Cont push, no allocations besides the frame.

import { group, bench, run as mitataRun } from "mitata";
import { ensuring, scoped, succeed, sync, run, runSync, Stream } from "../src";

// ── single-call cost ───────────────────────────────────────────────

group("ensuring single call", () => {
  bench("baseline: succeed only", async () => run(succeed(42)));
  bench("ensuring(succeed, sync) + run", async () =>
    run(
      ensuring(
        succeed(42),
        sync(() => undefined),
      ),
    ));
});

group("scoped single call", () => {
  bench("baseline: succeed only", async () => run(succeed(42)));
  bench("scoped(succeed) + run", async () => run(scoped(succeed(42))));
});

// ── nested ensurings — N levels ────────────────────────────────────

for (const N of [1, 5, 50]) {
  group(`ensuring × ${N} nested`, () => {
    const build = () => {
      let inner = succeed(0) as any;
      for (let i = 0; i < N; i++) {
        inner = ensuring(
          inner,
          sync(() => undefined),
        );
      }
      return inner;
    };
    bench("nested + run", async () => run(build()));
  });
}

// ── ensuring inside Stream — the realistic hot-path use ────────────

group("Stream with finalizer per emit", () => {
  const items = Array.from({ length: 100 }, (_, i) => i);

  bench("Stream.fromArray + map (no ensuring)", async () =>
    run(
      Stream.fromArray(items)
        .map((x) => x + 1)
        .runDrain(),
    ));
  bench("Stream + ensuring per element", async () =>
    run(
      Stream.fromArray(items)
        .map((x) => x + 1)
        .runForEach((x) =>
          ensuring(
            succeed(x),
            sync(() => undefined),
          ),
        ),
    ));
});

await mitataRun();
