import { type Eff, type Throws, Suspend, Op } from "./eff";
import { Cause } from "./cause";
import { succeed, fail, sync, async } from "./constructors";

export type DeferredState<A, E> =
  | { readonly _tag: "Pending"; readonly waiters: Array<(result: DeferredResult<A, E>) => void> }
  | { readonly _tag: "Done"; readonly result: DeferredResult<A, E> };

type DeferredResult<A, E> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly error: E };

export class Deferred<A, E = never> {
  private state: DeferredState<A, E> = { _tag: "Pending", waiters: [] };

  private constructor() {}

  static make<A, E = never>(): Eff<Deferred<A, E>, never> {
    return sync(() => new Deferred<A, E>());
  }

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
