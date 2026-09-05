import { type Eff, type Needs, Suspend, Op } from "./eff";

const SERVICE_TAG: unique symbol = Symbol.for("spilne/service");

export interface ServiceTag<T, Name extends string = string> {
  readonly [SERVICE_TAG]: true;
  readonly key: symbol;
  readonly name: Name;
  readonly get: Eff<T, Needs<T, Name>>;
}

export function service<T>(): <const Name extends string>(name: Name) => ServiceTag<T, Name> {
  return (name) => {
    const key = Symbol.for(`spilne/svc/${name}`);
    return {
      [SERVICE_TAG]: true,
      key,
      name,
      get: new Suspend(Op.GetCtx, key, null) as any,
    };
  };
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

type IsUnion<T, Whole = T> = T extends Whole ? ([Whole] extends [T] ? false : true) : never;

export type ProvidedService<S, T, Name extends string> = string extends Name
  ? S
  : true extends IsUnion<Name>
    ? S
    : Exclude<S, Needs<T, Name>>;

export function provide<A, S, T, Name extends string>(
  eff: Eff<A, S>,
  tag: ServiceTag<T, Name>,
  impl: NoInfer<T>,
): Eff<A, ProvidedService<S, T, Name>> {
  return new Suspend(Op.Provide, eff, makeContext(tag, impl)) as any;
}
