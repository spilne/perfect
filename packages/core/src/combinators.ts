import { type Eff, type InferValue, type InferEffects, Suspend, Op } from "./eff";

// standalone combinators only — fluent methods live in syntax/

type AllValues<T extends readonly Eff<unknown, unknown>[]> = {
  [K in keyof T]: InferValue<T[K]>;
};
type AllEffects<T extends readonly Eff<unknown, unknown>[]> = InferEffects<T[number]>;

type ObjValues<T extends Record<string, Eff<unknown, unknown>>> = {
  [K in keyof T]: T[K] extends Eff<infer A, unknown> ? A : never;
};
type ObjEffects<T extends Record<string, Eff<unknown, unknown>>> =
  T[keyof T] extends Eff<unknown, infer S> ? S : never;

/**
 * Run effects in parallel, collect results.
 *
 * Tuple form — `all([a, b, c])` → `Eff<[A, B, C]>`
 * Object form — `all({ a, b, c })` → `Eff<{ a: A, b: B, c: C }>`
 *
 * If any effect fails, the rest are interrupted.
 */
export function all<const T extends readonly Eff<unknown, unknown>[]>(
  effects: T,
): Eff<AllValues<T>, AllEffects<T>>;
export function all<T extends Record<string, Eff<unknown, unknown>>>(
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

export interface ForEachParOptions {
  /**
   * Maximum number of effects in flight at once: a positive integer, or
   * `"unbounded"` (the default) to start every effect immediately like `all`.
   * `1` runs the items sequentially.
   */
  readonly concurrency?: number | "unbounded";
}

/**
 * Map each item to an effect and run them in parallel, at most
 * `concurrency` at a time, collecting results in input order.
 *
 * `f` is called as slots free up, so only `concurrency` fibers exist at once
 * regardless of input size. The first failure (typed error or defect)
 * interrupts the in-flight effects and starts no new ones; the combined effect
 * fails with that cause once the interrupted effects have finished their
 * finalizers. Interrupting the combined effect interrupts every in-flight one.
 *
 * @example
 *   forEachPar(userIds, (id) => fetchUser(id), { concurrency: 8 })
 *   // → Eff<User[], Throws<HttpError>>
 */
export function forEachPar<A, R extends Eff<unknown, unknown>>(
  items: Iterable<A>,
  f: (item: A, index: number) => R,
  options?: ForEachParOptions,
): Eff<InferValue<R>[], InferEffects<R>> {
  const concurrency = options?.concurrency ?? "unbounded";
  if (
    concurrency !== "unbounded" &&
    !(concurrency >= 1 && (Number.isInteger(concurrency) || concurrency === Infinity))
  ) {
    throw new RangeError(
      `forEachPar: concurrency must be a positive integer or "unbounded", got ${String(concurrency)}`,
    );
  }
  const limit = concurrency === "unbounded" ? Infinity : concurrency;
  const array = Array.isArray(items) ? items : Array.from(items);
  return new Suspend(Op.ForEachPar, array, { f, limit }) as any;
}
