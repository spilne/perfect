// Make Eff<A, S> a thenable — `await eff` works in any async function.
//
// This is an ESCAPE HATCH, not a composition primitive. Every `await eff` that
// actually suspends pays for one microtask hop (spec-mandated by Promises/A+).
// For hot loops, use `.flatMap` / `eff($)` / `for { } yield` instead — those
// compose inside a single fiber at ~14 ns per step.
//
// We fast-path purely-synchronous chains through `evalSync` — no fiber, no
// scheduler, just a Promise resolution. That makes `await eff` on pure effects
// roughly 10× faster than spawning a fresh fiber per await (but still 10×
// slower than composed .flatMap because the microtask hop is unavoidable).

import { type Eff, Suspend } from "../eff"
import { Cause } from "../cause"
import { emptyContext } from "../service"
import { evalSync, run } from "../runtime"

declare module "../eff" {
  interface Suspend {
    /**
     * Make `await eff` work. For composition in hot paths prefer `.flatMap`
     * (~14 ns/step vs ~200 ns+ per await).
     */
    then<TResult1 = unknown, TResult2 = never>(
      onFulfilled?: ((value: unknown) => TResult1 | PromiseLike<TResult1>) | null,
      onRejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null,
    ): Promise<TResult1 | TResult2>
  }
}

Suspend.prototype.then = function (
  this: Suspend,
  onFulfilled?: any,
  onRejected?: any,
): any {
  // Fast path: evaluate purely-synchronous effects without the fiber runtime.
  const syncResult = evalSync(this as any, emptyContext)
  if (syncResult !== null) {
    return syncResult.ok
      ? Promise.resolve(syncResult.value).then(onFulfilled, onRejected)
      : Promise.reject(Cause.squash(syncResult.cause)).then(onFulfilled, onRejected)
  }
  // Needs fiber runtime (Async/Fork/Race/All/etc.)
  return run(this as any).then(onFulfilled, onRejected)
}
