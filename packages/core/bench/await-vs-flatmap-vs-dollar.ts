// Compare four ways to write an effect pipeline:
//
//   A. Composed `.flatMap` chain — single fiber, all steps share state
//   B. `await eff` per step — one thenable fast-path per await + microtask
//   C. `eff(($) => { ... $(e) ... })` — build-time rewriter, compiles to (A)
//   D. `eff(function* () { yield* e })` — runtime generator driver, no build
//
// Scenarios: pure compute, single async, all-async, service injection,
// error recovery.
//
// Headline results (Bun, M-series, N=100, per step):
//
//   pure × 100          for loop (no Promise)     0.3 ns/step
//                       composed .flatMap        14.5 ns/step  ◀ baseline
//                       await Promise/step       22.4 ns
//                       Promise.then chain       28.5 ns
//                       eff($) compiled          34   ns
//                       eff(function*)           64   ns  ~4× baseline
//                       await eff/step          128   ns  ~9× baseline
//
//   service × 100       composed                 3.0 µs  ◀ lookup once
//                       eff(function*)           9.3 µs
//                       await per step          42   µs  ~14× — avoid
//
//   all-async sleep(0)  all approaches ≈ identical (timer dominates)
//
// Takeaways:
//   1. Hot loops → composed `.flatMap`. Don't `await` per step.
//   2. Real I/O → any syntax; pipeline cost is invisible.
//   3. `eff(function*)` is the Pareto sweet spot — no build step, ~4×
//      composed, but 2× faster than `await` per step.
//   4. `eff($)` needs SWC/TS plugin; costs the build pipeline for perf wins.
//
// Full table + per-scenario interpretation: ./await-vs-flatmap-vs-dollar.md
//
// Run: bun packages/core/bench/await-vs-flatmap-vs-dollar.ts

import { group, bench, run as mitataRun } from "mitata";
import {
  eff,
  succeed,
  fail,
  sync,
  sleep,
  tryPromise,
  service,
  provide,
  run,
  runSync,
  type Eff,
  type Throws,
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

  // (C) $ syntax — compiles to flatMap chain via @spilne/perfect-transform.
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
    "succeed",
    "sync",
    "run",
    `return (async () => { ${dollarRewritten} })()`,
  );
  bench("eff($) syntax (compiled at load) → run", async () => dollarFn(succeed, sync, run));

  // (D) eff(function*) — runtime generator syntax, no build step.
  // Driver builds a lazy FlatMap tree equivalent to (A).
  const genPure = eff(function* () {
    let x: number = yield* succeed(0);
    for (let i = 0; i < N; i++) x = yield* succeed(x + 1);
    return x;
  });
  bench("eff(function*) + runSync (generator, no build step)", () => runSync(genPure));
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

interface Counter {
  add(n: number): Eff<number, never>;
}
const Counter = service<Counter>()("Counter");
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

  // (D) generator: lookup once via yield*, reuse inside the loop
  const genSvc = provide(
    eff(function* () {
      const c = yield* Counter.get;
      let x: number = 0;
      for (let i = 0; i < N; i++) x = yield* c.add(x);
      return x;
    }),
    Counter,
    counterImpl,
  );
  bench("eff(function*): lookup once + loop of yield* c.add()", async () => run(genSvc as any));
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
    try {
      await fail("midway");
    } catch {
      x = 999;
    }
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

  // (D) generator: try/catch inside the generator body
  const genRecover = eff(function* () {
    let x = 0;
    for (let i = 0; i < N / 2; i++) x = yield* succeed(x + 1);
    try {
      yield* fail("midway") as Eff<never, Throws<string>>;
    } catch {
      x = 999;
    }
    for (let i = 0; i < N / 2; i++) x = yield* succeed(x + 1);
    return x;
  });
  bench("eff(function*): try/catch around yield* fail", async () => run(genRecover as any));
});

// ── 6. Eff wrapping promises vs plain promises ────────────────────────
//
// Question: if every step of a flatMap chain awaits a real Promise (via
// tryPromise), does Eff still beat plain Promise.then? Hypothesis: NO —
// the microtask cost of the Promise resolution dominates and Eff adds
// interpreter overhead on top.

group(`Eff wrapping promises vs plain promises × ${N}`, () => {
  // Baseline: pure Eff with succeed (no Promise wrapping at all)
  const composedSucceed = (() => {
    let e: Eff<number, never> = succeed(0);
    for (let i = 0; i < N; i++) e = e.flatMap((x) => succeed(x + 1));
    return e;
  })();
  bench("Eff: composed flatMap + succeed (NO promises)", async () => run(composedSucceed));

  // Eff wrapping a Promise per step — every step pays a microtask
  const composedTryPromise = (() => {
    let e: Eff<number, any> = succeed(0);
    for (let i = 0; i < N; i++) {
      e = e.flatMap((x) => tryPromise(() => Promise.resolve(x + 1)));
    }
    return e;
  })();
  bench("Eff: composed flatMap + tryPromise(Promise.resolve)", async () =>
    run(composedTryPromise as any));

  // Plain Promise.then chain — what we're competing against
  bench("Promise: .then(Promise.resolve(...)) chain", async () => {
    let p: Promise<number> = Promise.resolve(0);
    for (let i = 0; i < N; i++) p = p.then((x) => Promise.resolve(x + 1));
    return p;
  });

  // Plain Promise.then with sync callback (closer to flatMap+succeed semantically)
  bench("Promise: .then(sync) chain (no inner Promise)", async () => {
    let p: Promise<number> = Promise.resolve(0);
    for (let i = 0; i < N; i++) p = p.then((x) => x + 1);
    return p;
  });
});

await mitataRun();
