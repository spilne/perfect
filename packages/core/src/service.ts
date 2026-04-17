import { type Eff, type Needs, Suspend, Op } from "./eff";

const SERVICE_TAG: unique symbol = Symbol.for("spilne/service");

export interface ServiceTag<T> {
  readonly [SERVICE_TAG]: true;
  readonly key: symbol;
  readonly name: string;
  readonly get: Eff<T, Needs<T>>;
}

export function service<T>(name: string): ServiceTag<T> {
  const key = Symbol.for(`spilne/svc/${name}`);
  const tag: ServiceTag<T> = {
    [SERVICE_TAG]: true,
    key,
    name,
    get: new Suspend(Op.GetCtx, key, null) as any,
  };
  return tag;
}

export type Context = Map<symbol, unknown>;

// "Empty" minus defaults — populated with a real Clock by runtime.ts.
// Kept mutable so bootstrap can seed it without a circular import.
export const emptyContext: Context = new Map();

export function makeContext<T>(tag: ServiceTag<T>, impl: T): Context {
  return new Map([[tag.key, impl]]);
}

export function mergeContexts(a: Context, b: Context): Context {
  const merged = new Map(a);
  for (const [k, v] of b) merged.set(k, v);
  return merged;
}

export function provide<A, S, T>(
  eff: Eff<A, S | Needs<T>>,
  tag: ServiceTag<T>,
  impl: T,
): Eff<A, Exclude<S, Needs<T>>> {
  return new Suspend(Op.Provide, eff, makeContext(tag, impl)) as any;
}
