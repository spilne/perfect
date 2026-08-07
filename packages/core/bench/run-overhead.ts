// Isolated run() overhead bench.
//
// Most primitive benches are dominated by the body of the effect (flatMap
// chains, mutations, etc.). To measure run() ITSELF, we use minimal effs
// where wrapping cost dominates.
//
// Run: bun packages/core/bench/run-overhead.ts

import { group, bench, run as mitataRun } from "mitata";
import { succeed, sync, run, runSync, tryPromise } from "../src";
import { Effect } from "effect";

const N = 1000;

group(`run() — N small calls`, () => {
  // Literal Op.Succeed — should hit fast path
  bench("perfect run(succeed) × N", async () => {
    for (let i = 0; i < N; i++) await run(succeed(i));
  });

  bench("effect runPromise(succeed) × N", async () => {
    for (let i = 0; i < N; i++) await Effect.runPromise(Effect.succeed(i));
  });

  // Op.Sync — slow path (no fast-path eligibility — would leak side effects)
  bench("perfect run(sync) × N", async () => {
    for (let i = 0; i < N; i++) await run(sync(() => i));
  });

  bench("effect runPromise(sync) × N", async () => {
    for (let i = 0; i < N; i++) await Effect.runPromise(Effect.sync(() => i));
  });

  // Bare async (Promise bridge) — uses PROMISE_THUNK shortcut via thenable
  bench("perfect await tryPromise × N (shortcut)", async () => {
    for (let i = 0; i < N; i++)
      await tryPromise(
        () => Promise.resolve(i),
        () => "err",
      );
  });

  bench("perfect run(tryPromise) × N (no shortcut)", async () => {
    for (let i = 0; i < N; i++)
      await run(
        tryPromise(
          () => Promise.resolve(i),
          () => "err",
        ),
      );
  });

  bench("baseline: await Promise.resolve × N", async () => {
    for (let i = 0; i < N; i++) await Promise.resolve(i);
  });
});

group(`runSync() — pure sync overhead`, () => {
  bench("perfect runSync(succeed)", () => {
    for (let i = 0; i < N; i++) runSync(succeed(i));
  });

  bench("effect runSync(succeed)", () => {
    for (let i = 0; i < N; i++) Effect.runSync(Effect.succeed(i));
  });

  bench("perfect runSync(sync)", () => {
    for (let i = 0; i < N; i++) runSync(sync(() => i));
  });

  bench("effect runSync(sync)", () => {
    for (let i = 0; i < N; i++) Effect.runSync(Effect.sync(() => i));
  });
});

await mitataRun();
