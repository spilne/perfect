import type { Eff } from "./eff";
import { Suspend, Op } from "./eff";
import { succeed } from "./constructors";

export type Finalizer = () => Eff<void, unknown>;

export class Scope {
  private finalizers: Finalizer[] = [];
  private closed = false;

  addFinalizer(f: Finalizer): void {
    if (this.closed) return;
    this.finalizers.push(f);
  }

  close(): Eff<void, unknown> {
    if (this.closed) return succeed(undefined);
    this.closed = true;
    // run in reverse order (LIFO) — last acquired, first released
    const fns = this.finalizers.reverse();
    if (fns.length === 0) return succeed(undefined);

    // chain: run fns[0], then fns[1], ... via flatMap — every finalizer
    // (including the first) is invoked only when the returned Eff runs
    let chain: Eff<void, unknown> = new Suspend(
      Op.FlatMap,
      new Suspend(Op.Succeed, undefined, null),
      () => fns[0]!(),
    ) as any;
    for (let i = 1; i < fns.length; i++) {
      const fin = fns[i]!;
      chain = new Suspend(Op.FlatMap, chain, () => fin()) as any;
    }
    return chain;
  }

  get isClosed(): boolean {
    return this.closed;
  }
}

export interface Scoped {
  readonly _scoped: unique symbol;
}
