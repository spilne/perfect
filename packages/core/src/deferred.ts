// Deferred<A, E> — write-once promise/handle.
// Eff-typed contract; in-process implementation by default.

import { type Eff, type Throws } from "./eff";
import { succeed, fail, sync, async } from "./constructors";

export type DeferredState<A, E> =
  | { readonly _tag: "Pending"; readonly waiters: Array<(result: DeferredResult<A, E>) => void> }
  | { readonly _tag: "Done"; readonly result: DeferredResult<A, E> };

type DeferredResult<A, E> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly error: E };

export interface Deferred<A, E = never> {
  /** Complete with success. Returns true if this call set the value, false if already done. */
  succeed(value: A): Eff<boolean, never>;
  /** Complete with failure. Returns true if this call set the error, false if already done. */
  fail(error: E): Eff<boolean, never>;
  /** Block until completed. */
  readonly await: Eff<A, Throws<E>>;
  /** True if already settled. */
  readonly isDone: Eff<boolean, never>;
}

/**
 * Concrete in-process implementation. Exposed for primitives that need to
 * construct a Deferred synchronously (e.g. Singleflight's atomic check+
 * register). Most users should call `Deferred.make()` instead.
 */
export class InProcessDeferred<A, E = never> implements Deferred<A, E> {
  private state: DeferredState<A, E> = { _tag: "Pending", waiters: [] };

  succeed(value: A): Eff<boolean, never> {
    return sync(() => {
      if (this.state._tag === "Done") return false;
      const waiters = this.state.waiters;
      this.state = { _tag: "Done", result: { ok: true, value } };
      for (const w of waiters) w({ ok: true, value });
      return true;
    });
  }

  fail(error: E): Eff<boolean, never> {
    return sync(() => {
      if (this.state._tag === "Done") return false;
      const waiters = this.state.waiters;
      this.state = { _tag: "Done", result: { ok: false, error } };
      for (const w of waiters) w({ ok: false, error });
      return true;
    });
  }

  get await(): Eff<A, Throws<E>> {
    return async<A, E>((resume) => {
      if (this.state._tag === "Done") {
        const r = this.state.result;
        resume(r.ok ? (succeed(r.value) as any) : (fail(r.error) as any));
        return;
      }
      this.state.waiters.push((result) => {
        resume(result.ok ? (succeed(result.value) as any) : (fail(result.error) as any));
      });
    }) as any;
  }

  get isDone(): Eff<boolean, never> {
    return sync(() => this.state._tag === "Done");
  }
}

export const Deferred = {
  make<A, E = never>(): Eff<Deferred<A, E>, never> {
    return sync(() => new InProcessDeferred<A, E>());
  },
} as const;
