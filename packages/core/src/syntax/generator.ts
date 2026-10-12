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

Suspend.prototype[Symbol.iterator] = function* (
  this: Suspend,
): Generator<Suspend, unknown, unknown> {
  return yield this;
} as Suspend[typeof Symbol.iterator];

type EffGenFn<A, S> = () => Generator<Eff<unknown, S>, A, unknown>;
// A running generator body, with its value and effect types erased.
type EffGen = Generator<Eff<unknown, unknown>, unknown, unknown>;

/** The `$` of `eff(($) => ...)`: takes an effect and gives back its value. */
export type Bind<S> = <A>(effect: Eff<A, S>) => A;

/**
 * The `$` form, `eff(($) => { const x = $(effect); return x; })`. It needs
 * the Bun or SWC plugin, which rewrites it into flatMap calls; this overload
 * only gives it types.
 *
 * TypeScript can't see which effects the `$(...)` calls inside the body bind,
 * so they are declared up front: `S` is what `$` accepts. It defaults to
 * `never`, so effects that can't fail need nothing; for ones that can, write
 * the types, e.g. `eff<User, Throws<NotFound>>(($) => ...)`.
 *
 * It is listed first so TypeScript types `$` before trying the generator
 * forms; a generator function can't match it, because its return type
 * would have to be a Generator.
 */
export function eff<A, S = never>(
  fn: ($: Bind<S>) => A extends Generator<unknown, unknown, never> ? never : A,
): Eff<A, S>;
export function eff<Y extends Eff<unknown, unknown>, A>(
  fn: () => Generator<Y, A, unknown>,
): Eff<A extends Eff<infer B, unknown> ? B : A, InferEffects<Y> | InferEffects<A>>;
export function eff<A, S = never>(fn: EffGenFn<A, S>): Eff<A, S>;
export function eff(
  fn: EffGenFn<unknown, unknown> | (($: Bind<unknown>) => unknown),
): Eff<unknown, unknown> {
  // The $ form only works after the Bun or SWC plugin has rewritten it; here
  // it would fail later with a confusing "$ is not a function".
  if (fn.length > 0) {
    throw new TypeError(
      "eff(($) => ...) needs the Bun or SWC plugin from @spilne/perfect-transform or " +
        "@spilne/perfect-swc-plugin; without one, use eff(function* () { ... })",
    );
  }
  const body = fn as EffGenFn<unknown, unknown>;
  // Lazy: build the generator inside a Sync so the fn runs on each execution.
  // The closing finalizer is in place before the first next() enters the body,
  // so no interrupt can land between entering a `try` and its `finally` being
  // guaranteed.
  return new Suspend(
    Op.Sync,
    () => {
      let gen: EffGen | null = null;
      return new Suspend(
        Op.Ensuring,
        new Suspend(Op.Sync, () => drive((gen = body()), undefined, null), null),
        () => (gen === null ? null : closeGenerator(gen)),
      );
    },
    null,
  ) as Eff<unknown, unknown>;
}

// An interrupted fiber bypasses the handler in `reify` that would throw the
// interrupt into the generator, leaving it suspended at a `yield*`. Returning
// it runs its `finally` blocks (effects they yield run within this finalizer)
// and skips its `catch` blocks. A generator that already finished is left as is.
function closeGenerator(gen: EffGen): Eff<unknown, unknown> | null {
  let step: IteratorResult<Eff<unknown, unknown>, unknown>;
  try {
    step = gen.return(undefined);
  } catch (e) {
    return die(e);
  }
  return step.done ? null : reify(gen, step.value);
}

function drive(gen: EffGen, input: unknown, errorCause: Cause | null): Eff<unknown, unknown> {
  let step: IteratorResult<Eff<unknown, unknown>, unknown>;
  const thrown = errorCause !== null ? Cause.squash(errorCause) : undefined;
  try {
    step = errorCause !== null ? gen.throw(thrown) : gen.next(input);
  } catch (e) {
    // The generator let our own throw propagate uncaught — restore the full
    // original Cause so defects stay defects and interrupts stay interrupts.
    // A different thrown value is a new typed failure from the body.
    if (errorCause !== null && e === thrown) return failCause(errorCause);
    return fail(e);
  }
  if (step.done) {
    const v = step.value;
    return v != null && v instanceof Suspend ? (v as Eff<unknown, unknown>) : succeed(v);
  }
  return reify(gen, step.value);
}

// Reify success/failure into a tagged sum so downstream `drive` recursion
// doesn't get re-caught by the outer catchAllCause. The Cause is carried
// whole; it's only squashed at the gen.throw boundary above.
type Reified = { ok: true; val: unknown } | { ok: false; cause: Cause };

function reify(gen: EffGen, yielded: Eff<unknown, unknown>): Eff<unknown, unknown> {
  const reified = new Suspend(
    Op.CatchAll,
    new Suspend(Op.FlatMap, yielded, (a: unknown) => succeed({ ok: true, val: a })),
    (cause: Cause) => succeed({ ok: false, cause }),
  );
  return new Suspend(Op.FlatMap, reified, (r: Reified) =>
    r.ok ? drive(gen, r.val, null) : drive(gen, undefined, r.cause),
  ) as Eff<unknown, unknown>;
}
