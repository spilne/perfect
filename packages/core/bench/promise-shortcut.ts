// Bench: bare `await tryPromise(p)` shortcut vs fiber-spawn path.
//
// Run: bun packages/core/bench/promise-shortcut.ts

import { group, bench, run as mitataRun } from "mitata";
import { tryPromise, run } from "../src";

const N = 1000;

group(`Promise bridging × ${N}`, () => {
  // Direct: just await fetch() / Promise.resolve(). The "what we're competing with".
  bench("await Promise.resolve directly (baseline)", async () => {
    for (let i = 0; i < N; i++) await Promise.resolve(i);
  });

  // With shortcut: thenable detects PROMISE_THUNK marker and skips fiber spawn.
  bench("await tryPromise(...) — bare (uses shortcut)", async () => {
    for (let i = 0; i < N; i++)
      await tryPromise(
        () => Promise.resolve(i),
        () => "err",
      );
  });

  // Forced through run(): explicit fiber spawn (no shortcut benefit).
  bench("await run(tryPromise(...)) — forced fiber path", async () => {
    for (let i = 0; i < N; i++)
      await run(
        tryPromise(
          () => Promise.resolve(i),
          () => "err",
        ),
      );
  });
});

await mitataRun();
