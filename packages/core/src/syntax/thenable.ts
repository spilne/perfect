// Make Eff<A, S> a thenable — `await eff` works in any async function.
//
// This is an ESCAPE HATCH, not a composition primitive. Every `await eff` that
// actually suspends pays for one microtask hop (spec-mandated by Promises/A+).
// For hot loops, use `.flatMap` / `eff($)` / `for { } yield` instead — those
// compose inside a single fiber at ~14 ns per step.
//
// Literal leaves (Succeed/Fail) and bare tryPromise resolve without spawning
// a fiber; anything richer goes through run(). The microtask hop is
// unavoidable either way — still ~10× slower than composed .flatMap.

import { Suspend, Op } from "../eff.js";
import { Cause } from "../cause.js";
import { run } from "../runtime.js";
import { PROMISE_SOURCES } from "../constructors.js";

// The value an effect produces, read from the instance's own type, so that
// `await eff` is typed as that value instead of unknown.
type ValueOf<T> = T extends { readonly _A: infer A } ? A : unknown;

declare module "../eff.js" {
  interface Suspend {
    /**
     * Make `await eff` work. For composition in hot paths prefer `.flatMap`
     * (~14 ns/step vs ~200 ns+ per await).
     */
    then<TResult1 = ValueOf<this>, TResult2 = never>(
      onFulfilled?: ((value: ValueOf<this>) => TResult1 | PromiseLike<TResult1>) | null,
      // `reason: any` mirrors lib.es5's PromiseLike/Promise `then` exactly —
      // required for structural thenable compatibility.
      onRejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null,
    ): Promise<TResult1 | TResult2>;
  }
}

Suspend.prototype.then = function (this: Suspend, onFulfilled?: any, onRejected?: any): any {
  // Shortcut for bare `await tryPromise(p)` — skip fiber spawn entirely.
  // Saves ~500 ns vs run() for promise bridging in the no-composition case.
  // Only fires when this Suspend IS the tryPromise leaf (no surrounding
  // FlatMap/Catch/Provide/etc. — those Suspends won't carry the markers).
  const source = PROMISE_SOURCES.get(this);
  if (source !== undefined) {
    const onReject = source.onReject;
    return (source.promise() as Promise<any>).then(
      (v) => (onFulfilled ? onFulfilled(v) : v),
      (e) => {
        const mapped = onReject ? onReject(e) : e;
        return onRejected ? onRejected(mapped) : Promise.reject(mapped);
      },
    );
  }

  // Literal-leaf fast path — see runtime.ts top-of-file comment for rationale.
  if (this.op === Op.Succeed) return Promise.resolve(this.a).then(onFulfilled, onRejected);
  if (this.op === Op.Fail)
    return Promise.reject(Cause.squash(this.a as Cause)).then(onFulfilled, onRejected);
  // Anything richer (Sync/FlatMap/Async/Fork/...) goes through the fiber runtime.
  return run(this as any).then(onFulfilled, onRejected);
};
