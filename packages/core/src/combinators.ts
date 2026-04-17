import { type Eff, type InferValue, type InferEffects, Suspend, Op } from "./eff";

// standalone combinators only — fluent methods live in syntax/

type AllValues<T extends readonly Eff<any, any>[]> = {
  [K in keyof T]: InferValue<T[K]>;
};
type AllEffects<T extends readonly Eff<any, any>[]> = InferEffects<T[number]>;

export function all<const T extends readonly Eff<any, any>[]>(
  effects: T,
): Eff<AllValues<T>, AllEffects<T>> {
  return new Suspend(Op.All, effects, null) as any;
}
