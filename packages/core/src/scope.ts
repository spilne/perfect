import type { Eff } from "./eff";
import { Suspend, Op } from "./eff";
import { succeed, sync } from "./constructors";

export type Finalizer = () => Eff<void, never>;

export class Scope {
  private finalizers: Finalizer[] = [];
  private closed = false;

  addFinalizer(f: Finalizer): void {
    if (this.closed) return;
    this.finalizers.push(f);
  }

  close(): Eff<void, never> {
    if (this.closed) return succeed(undefined);
    this.closed = true;
    // run in reverse order (LIFO) — last acquired, first released
    const fns = this.finalizers.reverse();
    if (fns.length === 0) return succeed(undefined);

    // chain: run fns[0], then fns[1], ... via flatMap
    let chain: Eff<void, never> = fns[0]!();
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
