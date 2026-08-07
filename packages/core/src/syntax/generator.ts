// Runtime generator syntax for effects — the native alternative to the
// build-time `eff(($) => ...)` rewriter.
//
//   const program = eff(function* () {
//     const a = yield* effectA;
//     const b = yield* effectB(a);
//     return a + b;
//   });
//
// Runs at roughly the same cost as a composed `.flatMap` chain: the
// interpreter walks a FlatMap tree built lazily by a driver loop. No
// microtask per step, no build step required.
//
// `try/catch` inside the generator catches failures (both typed errors and
// defects) because the driver threads causes back via `gen.throw`.

import { type Eff, Suspend, Op } from "../eff";
import { succeed, fail } from "../constructors";
import { Cause } from "../cause";

// Make Suspend iterable so `yield* effect` works inside generator bodies.
declare module "../eff" {
  interface Suspend {
    [Symbol.iterator](): Iterator<Suspend, any, any>;
  }
}

(Suspend.prototype as any)[Symbol.iterator] = function* (
  this: Suspend,
): Generator<Suspend, any, any> {
  return yield this;
};

type EffGenFn<A, S> = () => Generator<Eff<any, S>, A, any>;

export function eff<A, S = never>(fn: EffGenFn<A, S>): Eff<A, S> {
  // Lazy: build the generator inside a Sync so the fn runs on each execution.
  return (new Suspend(Op.Sync, () => fn(), null) as any).flatMap((gen: any) =>
    drive(gen, undefined, false),
  );
}

function drive<A, S>(gen: Generator<Eff<any, S>, A, any>, input: any, isError: boolean): Eff<A, S> {
  let step: IteratorResult<Eff<any, S>, A>;
  try {
    step = isError ? gen.throw(input) : gen.next(input);
  } catch (e) {
    return fail(e) as any;
  }
  if (step.done) {
    const v = step.value;
    return (v != null && v instanceof Suspend ? v : succeed(v)) as any;
  }
  const yielded = step.value;
  // Reify success/failure into a tagged sum so downstream `drive` recursion
  // doesn't get re-caught by the outer catchAllCause.
  const reified = new Suspend(
    Op.CatchAll,
    new Suspend(Op.FlatMap, yielded, (a: any) => succeed({ ok: true, val: a })),
    (cause: Cause) => succeed({ ok: false, err: Cause.squash(cause) }),
  );
  return new Suspend(Op.FlatMap, reified, (r: any) =>
    r.ok ? drive(gen, r.val, false) : drive(gen, r.err, true),
  ) as any;
}
