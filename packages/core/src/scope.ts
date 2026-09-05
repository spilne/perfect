import type { Eff } from "./eff";
import { succeed, suspend, ensuring } from "./constructors";

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

    let chain = suspend(fns[0]!);
    for (let i = 1; i < fns.length; i++) {
      const fin = fns[i]!;
      chain = ensuring(chain, suspend(fin));
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
