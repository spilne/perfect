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
 * `items` is read when the effect runs, and the next item is pulled and passed
 * to `f` only when a slot frees up — memory for pending work stays bounded by
 * `concurrency`, and an infinite iterable is fine under a timeout or interrupt.
 * A one-shot iterable (a generator object) is used up by the first run; pass
 * an array or a re-iterable object to run the effect more than once.
 *
 * The first failure — a typed error, a defect, `f` throwing, or the iterator
 * throwing — interrupts the in-flight effects, closes the iterator, and starts
 * nothing new. The combined effect fails once every in-flight effect has
 * settled, with that first cause plus (via `Cause.both`) any non-interrupt
 * failures raised while the others were torn down. Interrupting the combined
 * effect also waits for the in-flight effects to settle, so their finalizers
 * finish before any finalizer around the traversal runs.
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
  return new Suspend(Op.ForEachPar, items, { f, limit }) as any;
}
