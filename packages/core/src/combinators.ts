import { type Eff, type InferValue, type InferEffects, Suspend, Op } from "./eff";

// standalone combinators only — fluent methods live in syntax/

type AllValues<T extends readonly Eff<any, any>[]> = {
  [K in keyof T]: InferValue<T[K]>;
};
type AllEffects<T extends readonly Eff<any, any>[]> = InferEffects<T[number]>;

type ObjValues<T extends Record<string, Eff<any, any>>> = {
  [K in keyof T]: T[K] extends Eff<infer A, any> ? A : never;
};
type ObjEffects<T extends Record<string, Eff<any, any>>> =
  T[keyof T] extends Eff<any, infer S> ? S : never;

/**
 * Run effects in parallel, collect results.
 *
 * Tuple form — `all([a, b, c])` → `Eff<[A, B, C]>`
 * Object form — `all({ a, b, c })` → `Eff<{ a: A, b: B, c: C }>`
 *
 * If any effect fails, the rest are interrupted.
 */
export function all<const T extends readonly Eff<any, any>[]>(
  effects: T,
): Eff<AllValues<T>, AllEffects<T>>;
export function all<T extends Record<string, Eff<any, any>>>(
  effects: T,
): Eff<ObjValues<T>, ObjEffects<T>>;
export function all(effects: any): any {
  if (Array.isArray(effects)) {
    return new Suspend(Op.All, effects, null);
  }
  // Object form: collect by keys, run in parallel, rebuild record.
  const keys = Object.keys(effects);
  const arr = keys.map((k) => effects[k]);
  return new Suspend(Op.FlatMap, new Suspend(Op.All, arr, null), (results: any[]) => {
    const out: any = {};
    for (let i = 0; i < keys.length; i++) out[keys[i]!] = results[i];
    return new Suspend(Op.Succeed, out, null);
  });
}
