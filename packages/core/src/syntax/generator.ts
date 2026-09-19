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

import { type Eff, type InferEffects, Suspend, Op } from "../eff.js";
import { die, succeed, fail, failCause } from "../constructors.js";
import { Cause } from "../cause.js";

// Make Suspend iterable so `yield* effect` works inside generator bodies.
// Yield the concrete effect so the generator retains each requirement, and
// return its value type so yield* preserves the type at the call site.
declare module "../eff.js" {
  interface Suspend {
    [Symbol.iterator](): Generator<
      this,
      this extends { readonly _A: infer A } ? A : never,
      unknown
    >;
  }
}

(Suspend.prototype as any)[Symbol.iterator] = function* (
  this: Suspend,
): Generator<Suspend, any, any> {
  return yield this;
};

type EffGenFn<A, S> = () => Generator<Eff<any, S>, A, any>;

export function eff<Y extends Eff<any, any>, A>(
  fn: () => Generator<Y, A, any>,
): Eff<A extends Eff<infer B, any> ? B : A, InferEffects<Y> | InferEffects<A>>;
export function eff<A, S = never>(fn: EffGenFn<A, S>): Eff<A, S>;
export function eff(fn: EffGenFn<any, any>): Eff<any, any> {
  // Lazy: build the generator inside a Sync so the fn runs on each execution.
  // The closing finalizer is in place before the first next() enters the body,
  // so no interrupt can land between entering a `try` and its `finally` being
  // guaranteed.
  return new Suspend(
    Op.Sync,
    () => {
      let gen: Generator<Eff<any, any>, any, any> | null = null;
      return new Suspend(
        Op.Ensuring,
        new Suspend(Op.Sync, () => drive((gen = fn()), undefined, null), null),
        () => (gen === null ? null : closeGenerator(gen)),
      );
    },
    null,
  ) as any;
}

// An interrupted fiber bypasses the handler in `reify` that would throw the
// interrupt into the generator, leaving it suspended at a `yield*`. Returning
// it runs its `finally` blocks (effects they yield run within this finalizer)
// and skips its `catch` blocks. A generator that already finished is left as is.
function closeGenerator(gen: Generator<Eff<any, any>, any, any>): Eff<any, any> | null {
  let step: IteratorResult<Eff<any, any>, any>;
  try {
    step = gen.return(undefined);
  } catch (e) {
    return die(e);
  }
  return step.done ? null : reify(gen, step.value);
}

function drive<A, S>(
  gen: Generator<Eff<any, S>, A, any>,
  input: any,
  errorCause: Cause | null,
): Eff<A, S> {
  let step: IteratorResult<Eff<any, S>, A>;
  const thrown = errorCause !== null ? Cause.squash(errorCause) : undefined;
  try {
    step = errorCause !== null ? gen.throw(thrown) : gen.next(input);
  } catch (e) {
    // The generator let our own throw propagate uncaught — restore the full
    // original Cause so defects stay defects and interrupts stay interrupts.
    // A different thrown value is a new typed failure from the body.
    if (errorCause !== null && e === thrown) return failCause(errorCause) as any;
    return fail(e) as any;
  }
  if (step.done) {
    const v = step.value;
    return (v != null && v instanceof Suspend ? v : succeed(v)) as any;
  }
  return reify(gen, step.value);
}

// Reify success/failure into a tagged sum so downstream `drive` recursion
// doesn't get re-caught by the outer catchAllCause. The Cause is carried
// whole; it's only squashed at the gen.throw boundary above.
function reify<A, S>(gen: Generator<Eff<any, S>, A, any>, yielded: Eff<any, S>): Eff<A, S> {
  const reified = new Suspend(
    Op.CatchAll,
    new Suspend(Op.FlatMap, yielded, (a: any) => succeed({ ok: true, val: a })),
    (cause: Cause) => succeed({ ok: false, cause }),
  );
  return new Suspend(Op.FlatMap, reified, (r: any) =>
    r.ok ? drive(gen, r.val, null) : drive(gen, undefined, r.cause),
  ) as any;
}
