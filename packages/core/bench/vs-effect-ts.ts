// Head-to-head: Perfect vs effect-ts vs raw Promise.
//
// Same shape as promin's pipeline.bench.ts (mitata: group() + bench()).
// Run: bun packages/core/bench/vs-effect-ts.ts

import { group, bench, run } from "mitata";
import { Effect } from "effect";
import {
  succeed as pSucceed,
  fail as pFail,
  sync as pSync,
  sleep as pSleep,
  all as pAll,
  race as pRace,
  retry as pRetry,
  timeout as pTimeout,
  run as pRun,
  runSync as pRunSync,
  type Eff,
} from "../src";

// ── Basic execution overhead ───────────────────────────────────────

group("basic: succeed → resolve", () => {
  bench("raw Promise", async () => Promise.resolve(42));

  bench("perfect succeed + run", async () => pRun(pSucceed(42)));

  bench("perfect succeed + runSync", () => pRunSync(pSucceed(42)));

  bench("effect succeed + runPromise", async () => Effect.runPromise(Effect.succeed(42)));

  bench("effect succeed + runSync", () => Effect.runSync(Effect.succeed(42)));
});

// ── flatMap chain depth ────────────────────────────────────────────

group("flatMap chain × 5", () => {
  bench("raw Promise.then × 5", async () =>
    Promise.resolve(1)
      .then((x) => x + 1)
      .then((x) => x + 1)
      .then((x) => x + 1)
      .then((x) => x + 1)
      .then((x) => x + 1));

  bench("perfect .flatMap × 5 + runSync", () =>
    pRunSync(
      pSucceed(1)
        .flatMap((x) => pSucceed(x + 1))
        .flatMap((x) => pSucceed(x + 1))
        .flatMap((x) => pSucceed(x + 1))
        .flatMap((x) => pSucceed(x + 1))
        .flatMap((x) => pSucceed(x + 1)),
    ));

  bench("effect flatMap × 5 + runSync", () =>
    Effect.runSync(
      Effect.succeed(1).pipe(
        Effect.flatMap((x) => Effect.succeed(x + 1)),
        Effect.flatMap((x) => Effect.succeed(x + 1)),
        Effect.flatMap((x) => Effect.succeed(x + 1)),
        Effect.flatMap((x) => Effect.succeed(x + 1)),
        Effect.flatMap((x) => Effect.succeed(x + 1)),
      ),
    ));
});

// ── Deep chain to surface interpreter overhead ─────────────────────

for (const depth of [100, 1_000, 10_000]) {
  group(`deep flatMap × ${depth}`, () => {
    const buildPerfect = (): Eff<number, never> => {
      let eff: Eff<number, never> = pSucceed(0);
      for (let i = 0; i < depth; i++) eff = eff.flatMap((x) => pSucceed(x + 1));
      return eff;
    };
    const buildEffect = (): Effect.Effect<number> => {
      let eff: Effect.Effect<number> = Effect.succeed(0);
      for (let i = 0; i < depth; i++) eff = eff.pipe(Effect.flatMap((x) => Effect.succeed(x + 1)));
      return eff;
    };
    const buildPromise = () => {
      let p: Promise<number> = Promise.resolve(0);
      for (let i = 0; i < depth; i++) p = p.then((x) => x + 1);
      return p;
    };

    const perfectEff = buildPerfect();
    const effectEff = buildEffect();

    bench("perfect runSync (built once)", () => pRunSync(perfectEff));
    bench("effect runSync (built once)", () => Effect.runSync(effectEff));
    bench("perfect run + build", async () => pRun(buildPerfect()));
    bench("effect runPromise + build", async () => Effect.runPromise(buildEffect()));
    bench("Promise.then × N", async () => buildPromise());
  });
}

// ── map chain ──────────────────────────────────────────────────────

group("map chain × 100", () => {
  const N = 100;
  const perfectEff = (() => {
    let e: Eff<number, never> = pSucceed(0);
    for (let i = 0; i < N; i++) e = e.map((x) => x + 1);
    return e;
  })();
  const effectEff = (() => {
    let e: Effect.Effect<number> = Effect.succeed(0);
    for (let i = 0; i < N; i++) e = e.pipe(Effect.map((x) => x + 1));
    return e;
  })();

  bench("perfect map × 100 + runSync", () => pRunSync(perfectEff));
  bench("effect map × 100 + runSync", () => Effect.runSync(effectEff));
});

// ── sync(() => …) chain — measures sync wrapper overhead ───────────

group("sync(() => x) chain × 100", () => {
  const N = 100;
  const perfectEff = (() => {
    let e: Eff<number, never> = pSync(() => 0);
    for (let i = 0; i < N; i++) e = e.flatMap((x) => pSync(() => x + 1));
    return e;
  })();
  const effectEff = (() => {
    let e: Effect.Effect<number> = Effect.sync(() => 0);
    for (let i = 0; i < N; i++) e = e.pipe(Effect.flatMap((x) => Effect.sync(() => x + 1)));
    return e;
  })();

  bench("perfect sync × 100 + runSync", () => pRunSync(perfectEff));
  bench("effect sync × 100 + runSync", () => Effect.runSync(effectEff));
});

// ── all() — parameterised by breadth ───────────────────────────────

for (const count of [10, 100, 1_000]) {
  group(`all × ${count}`, () => {
    const items = Array.from({ length: count }, (_, i) => i);

    bench("Promise.all", async () => Promise.all(items.map((i) => Promise.resolve(i * 2))));

    bench("perfect all + run", async () => pRun(pAll(items.map((i) => pSucceed(i * 2))) as any));

    bench("effect all + runPromise", async () =>
      Effect.runPromise(Effect.all(items.map((i) => Effect.succeed(i * 2)))));
  });
}

// ── race ───────────────────────────────────────────────────────────

group("race × 3 (all succeed fast)", () => {
  bench("Promise.race × 3", async () =>
    Promise.race([Promise.resolve(1), Promise.resolve(2), Promise.resolve(3)]));

  bench("perfect race × 3 + run", async () => pRun(pRace([pSucceed(1), pSucceed(2), pSucceed(3)])));

  bench("effect race × 3 + runPromise", async () =>
    Effect.runPromise(
      Effect.race(Effect.race(Effect.succeed(1), Effect.succeed(2)), Effect.succeed(3)),
    ));
});

// ── error handling ─────────────────────────────────────────────────

group("error: fail → handle → succeed", () => {
  bench("perfect fail + .catch + run", async () =>
    pRun((pFail("boom") as any).catch(() => pSucceed(0))));

  bench("effect fail + catchAll + runPromise", async () =>
    Effect.runPromise(Effect.fail("boom").pipe(Effect.catchAll(() => Effect.succeed(0)))));
});

// ── retry: succeeds first attempt (no actual retry) ────────────────

group("retry: succeeds first try", () => {
  bench("perfect retry × 3 + run", async () => pRun(pRetry(pSucceed(42), { times: 3 })));

  bench("effect retry × 3 + runPromise", async () =>
    Effect.runPromise(Effect.succeed(42).pipe(Effect.retry({ times: 3 }))));
});

// ── timeout: succeeds fast ─────────────────────────────────────────

group("timeout: succeeds fast", () => {
  bench("perfect timeout 5s + run", async () =>
    pRun(pTimeout(pSucceed(42), 5000, () => "timeout" as const)));

  bench("effect timeout 5s + runPromise", async () =>
    Effect.runPromise(Effect.succeed(42).pipe(Effect.timeout("5 seconds"))));
});

// ── async with sleep(0) — scheduler round-trip ─────────────────────

group("sleep(0) chain × 50", () => {
  const N = 50;
  const buildPerfect = (): Eff<number, never> => {
    let e: Eff<number, never> = pSucceed(0);
    for (let i = 0; i < N; i++) e = e.flatMap((x) => pSleep(0).flatMap(() => pSucceed(x + 1)));
    return e;
  };
  const buildEffect = (): Effect.Effect<number> => {
    let e: Effect.Effect<number> = Effect.succeed(0);
    for (let i = 0; i < N; i++) {
      e = e.pipe(
        Effect.flatMap((x) => Effect.sleep(0).pipe(Effect.flatMap(() => Effect.succeed(x + 1)))),
      );
    }
    return e;
  };

  bench("perfect run + sleep(0) × 50", async () => pRun(buildPerfect()));
  bench("effect runPromise + sleep(0) × 50", async () => Effect.runPromise(buildEffect()));
});

await run({
  colors: process.stdout.isTTY,
  format: process.env.MITATA_FORMAT === "markdown" ? "markdown" : "mitata",
});
