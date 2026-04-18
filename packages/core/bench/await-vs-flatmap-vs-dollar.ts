// Compare three ways to write an effect pipeline:
//
//   A. Composed `.flatMap` chain — single fiber, all steps share state
//   B. `await eff` per step — one thenable fast-path per await + microtask
//   C. `eff(($) => { ... $(e) ... })` source syntax — compiles to (A)
//      via @perfect/transform, so runtime cost is identical to A
//
// Scenarios capture realistic mixes: pure compute, single async, all-async,
// service injection, and error recovery.
//
// Headline results (Bun, M-series, N=100):
//
//   pure × 100         composed flatMap   1.5 µs   ◀ baseline
//                      eff($) compiled    3.5 µs   ~2× (async IIFE wrap)
//                      Promise.then       3.0 µs
//                      await Promise/step 2.3 µs
//                      await eff/step    13.5 µs   ~9× — avoid in hot loops
//
//   service × 100      composed (lookup once) 3.0 µs
//                      await (lookup per step) 44 µs   ~15× — avoid
//
//   all-async sleep(0) all approaches collapse — overhead dominated by timer
//
// Takeaways:
//   1. Hot loops → composed `.flatMap`. Don't `await` per step.
//   2. Real I/O → any approach is fine; pipeline cost is in the noise.
//   3. `eff($)` syntax costs ~2× raw composed but reads best — use where
//      readability beats microseconds.
//   4. Composed Eff is faster than raw Promise chains in JS once built.
//
// Full table + per-scenario interpretation: ./await-vs-flatmap-vs-dollar.md
//
// Run: bun packages/core/bench/await-vs-flatmap-vs-dollar.ts

import { group, bench, run as mitataRun } from "mitata";
import {
  succeed, fail, sync, sleep, service, provide, run, runSync,
  type Eff, type Throws, type Needs,
} from "../src";
import { rewriteEffBlocks } from "../../transform/src/rewrite";

const N = 100;

// ── 1. Pure compute: N sequential transformations ─────────────────────

group(`pure compute × ${N}`, () => {
  // (A) composed flatMap chain — built once, run many times
  const composedPure = (() => {
    let e: Eff<number, never> = succeed(0);
    for (let i = 0; i < N; i++) e = e.flatMap((x) => succeed(x + 1));
    return e;
  })();

  bench("composed flatMap + runSync (one fiber)", () => runSync(composedPure));
  bench("composed flatMap + await (single thenable fast-path)", async () => await composedPure);

  bench("await eff per step (N fibers via thenable)", async () => {
    let x = 0;
    for (let i = 0; i < N; i++) x = await succeed(x + 1);
    return x;
  });

  // Promise baselines — what the JS event loop costs without any Eff.
  bench("Promise.then chain × N (composed)", async () => {
    let p: Promise<number> = Promise.resolve(0);
    for (let i = 0; i < N; i++) p = p.then((x) => x + 1);
    return p;
  });

  bench("await Promise.resolve per step", async () => {
    let x = 0;
    for (let i = 0; i < N; i++) x = await Promise.resolve(x + 1);
    return x;
  });

  bench("for loop (no Promise at all)", () => {
    let x = 0;
    for (let i = 0; i < N; i++) x = x + 1;
    return x;
  });

  // (C) $ syntax — compiles to flatMap chain via @perfect/transform.
  // The execution cost is identical to (A); we include it to PROVE the
  // syntactic sugar has zero runtime tax. The rewriter only recognises
  // `const x = $(e)` (declarations), so we generate fresh names per step.
  const dollarBinds = ["const x0 = $(succeed(0));"]
    .concat(Array.from({ length: N }, (_, i) => `const x${i + 1} = $(succeed(x${i} + 1));`))
    .join("\n      ");
  const dollarSrc = `
    const program = eff(($) => {
      ${dollarBinds}
      return x${N};
    });
    return await run(program);
  `;
  const dollarRewritten = rewriteEffBlocks(dollarSrc);
  const dollarFn = new Function(
    "succeed", "sync", "run",
    `return (async () => { ${dollarRewritten} })()`,
  );
  bench("eff($) syntax (compiled at load) → run", async () => dollarFn(succeed, sync, run));
});

// ── 2. Single async — sleep(0) once, then N-1 pure steps ──────────────

group(`single async + pure × ${N - 1}`, () => {
  const composedMixed = (() => {
    let e: Eff<number, never> = sleep(0).flatMap(() => succeed(0));
    for (let i = 0; i < N - 1; i++) e = e.flatMap((x) => succeed(x + 1));
    return e;
  })();

  bench("composed flatMap + run (one fiber)", async () => run(composedMixed));
  bench("await composed (single thenable, async path)", async () => await composedMixed);
  bench("await eff per step (sleep then loop)", async () => {
    await sleep(0);
    let x = 0;
    for (let i = 0; i < N - 1; i++) x = await succeed(x + 1);
    return x;
  });

  // Promise: setTimeout(0) once + N-1 sync steps via .then chain
  bench("Promise: setTimeout once + .then chain", async () => {
    let p: Promise<number> = new Promise((r) => setTimeout(() => r(0), 0));
    for (let i = 0; i < N - 1; i++) p = p.then((x) => x + 1);
    return p;
  });

  bench("Promise: await setTimeout + await per step", async () => {
    await new Promise((r) => setTimeout(r, 0));
    let x = 0;
    for (let i = 0; i < N - 1; i++) x = await Promise.resolve(x + 1);
    return x;
  });
});

// ── 3. All async — N sleeps (worst case for await-per-step) ───────────

const N_SMALL = 20;
group(`all-async sleep(0) × ${N_SMALL}`, () => {
  const composedAsync = (() => {
    let e: Eff<number, never> = succeed(0);
    for (let i = 0; i < N_SMALL; i++) {
      e = e.flatMap((x) => sleep(0).flatMap(() => succeed(x + 1)));
    }
    return e;
  })();

  bench("composed flatMap + run", async () => run(composedAsync));
  bench("await eff per step (each await spawns a fiber)", async () => {
    let x = 0;
    for (let i = 0; i < N_SMALL; i++) {
      await sleep(0);
      x = await succeed(x + 1);
    }
    return x;
  });

  // Pure Promise baseline — N setTimeout(0) calls with await
  bench("await setTimeout(0) per step (Promise)", async () => {
    let x = 0;
    for (let i = 0; i < N_SMALL; i++) {
      await new Promise((r) => setTimeout(r, 0));
      x = x + 1;
    }
    return x;
  });
});

// ── 4. Service injection: lookup once vs per-step ─────────────────────

interface Counter { add(n: number): Eff<number, never> }
const Counter = service<Counter>("Counter");
const counterImpl: Counter = { add: (n: number) => succeed(n + 1) };

group(`service lookup × ${N}`, () => {
  // (A) Composed: lookup once, reuse
  const composedSvc = provide(
    Counter.get.flatMap((c: Counter) => {
      let e: Eff<number, never> = succeed(0);
      for (let i = 0; i < N; i++) e = e.flatMap((x) => c.add(x));
      return e;
    }),
    Counter,
    counterImpl,
  );

  bench("composed: lookup once + chain + run", async () => run(composedSvc as any));

  // (B) await: lookup PER step (worst-case naive translation)
  bench("await: lookup + add per step (× N awaits)", async () => {
    let x = 0;
    for (let i = 0; i < N; i++) {
      const c: any = await provide(Counter.get, Counter, counterImpl);
      x = await c.add(x);
    }
    return x;
  });

  // Promise: equivalent — closure capture + .then chain
  const counterPromise = (n: number) => Promise.resolve(n + 1);
  bench("Promise: closure + .then chain", async () => {
    let p: Promise<number> = Promise.resolve(0);
    for (let i = 0; i < N; i++) p = p.then(counterPromise);
    return p;
  });
});

// ── 5. Error recovery: fail mid-chain, catch + continue ───────────────

group("fail mid-chain + recover", () => {
  // (A) composed
  const composedRecover = (() => {
    let e: Eff<number, never> = succeed(0);
    for (let i = 0; i < N / 2; i++) e = e.flatMap((x) => succeed(x + 1));
    e = (e as any).flatMap(() => fail("midway")).catch(() => succeed(999));
    for (let i = 0; i < N / 2; i++) e = e.flatMap((x) => succeed(x + 1));
    return e;
  })();

  bench("composed: flatMap + catch + flatMap (one fiber)", async () => run(composedRecover));
  bench("await: try/catch around fail per step", async () => {
    let x = 0;
    for (let i = 0; i < N / 2; i++) x = await succeed(x + 1);
    try { await fail("midway"); } catch { x = 999; }
    for (let i = 0; i < N / 2; i++) x = await succeed(x + 1);
    return x;
  });

  // Promise: .then chain with mid-failure + .catch
  bench("Promise: .then chain + .catch + .then chain", async () => {
    let p: Promise<number> = Promise.resolve(0);
    for (let i = 0; i < N / 2; i++) p = p.then((x) => x + 1);
    p = p.then(() => Promise.reject("midway")).catch(() => 999);
    for (let i = 0; i < N / 2; i++) p = p.then((x) => x + 1);
    return p;
  });
});

await mitataRun();
