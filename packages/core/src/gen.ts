// Property-based testing primitives.
//
// A Gen<A> is a recipe for producing values of type A from the Random service.
// `forAll` runs a property N times against fresh random samples and fails on
// the first counterexample, reporting the value that broke it.
//
// No shrinking yet — counterexamples are reported as-generated. Adding
// shrinking is a separate, much larger piece of work (cf. fast-check).

import { type Eff, type Throws, Suspend, Op } from "./eff";
import { Random } from "./random";
import { fail, sync } from "./constructors";

// ── Gen<A> ─────────────────────────────────────────────────────────

export interface Gen<A> {
  readonly generate: Eff<A, never>;
}

const ALPHANUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

function mk<A>(generate: Eff<A, never>): Gen<A> {
  return { generate };
}

// ── Primitives ─────────────────────────────────────────────────────

function intGen(min: number, max: number): Gen<number> {
  if (max <= min) throw new Error(`Gen.int: max must be > min (got min=${min}, max=${max})`);
  return mk(
    new Suspend(Op.FlatMap, Random.get as any, (r: Random) => r.nextRange(min, max)) as any,
  );
}

const boolGen: Gen<boolean> = mk(
  new Suspend(Op.FlatMap, Random.get as any, (r: Random) => r.nextBool()) as any,
);

function charGen(alphabet: string = ALPHANUM): Gen<string> {
  if (alphabet.length === 0) throw new Error("Gen.char: alphabet must be non-empty");
  return mk(
    new Suspend(Op.FlatMap, Random.get as any, (r: Random) =>
      (r.nextInt(alphabet.length) as any).map((i: number) => alphabet[i]!),
    ) as any,
  );
}

function stringGen(
  opts: { length?: number; minLength?: number; maxLength?: number; alphabet?: string } = {},
): Gen<string> {
  const { length, minLength = 0, maxLength = 32, alphabet = ALPHANUM } = opts;
  return mk(
    new Suspend(Op.FlatMap, Random.get as any, (r: Random) => {
      const lenEff: Eff<number, never> =
        length !== undefined
          ? (sync(() => length) as any)
          : (r.nextRange(minLength, maxLength + 1) as any);
      return new Suspend(Op.FlatMap, lenEff, (n: number) => {
        // n picks of a random alphabet char
        const collect = (i: number, acc: string): Eff<string, never> => {
          if (i >= n) return sync(() => acc) as any;
          return new Suspend(Op.FlatMap, r.nextInt(alphabet.length) as any, (idx: number) =>
            collect(i + 1, acc + alphabet[idx]),
          ) as any;
        };
        return collect(0, "");
      });
    }) as any,
  );
}

function choiceGen<A>(...items: A[]): Gen<A> {
  if (items.length === 0) throw new Error("Gen.choice: at least one item required");
  return mk(new Suspend(Op.FlatMap, Random.get as any, (r: Random) => r.choice(items)) as any);
}

function arrayGen<A>(
  item: Gen<A>,
  opts: { length?: number; minLength?: number; maxLength?: number } = {},
): Gen<A[]> {
  const { length, minLength = 0, maxLength = 16 } = opts;
  return mk(
    new Suspend(Op.FlatMap, Random.get as any, (r: Random) => {
      const lenEff: Eff<number, never> =
        length !== undefined
          ? (sync(() => length) as any)
          : (r.nextRange(minLength, maxLength + 1) as any);
      return new Suspend(Op.FlatMap, lenEff, (n: number) => {
        const collect = (i: number, acc: A[]): Eff<A[], never> => {
          if (i >= n) return sync(() => acc) as any;
          return new Suspend(Op.FlatMap, item.generate, (a: A) => {
            acc.push(a);
            return collect(i + 1, acc);
          }) as any;
        };
        return collect(0, []);
      });
    }) as any,
  );
}

function objectGen<T extends Record<string, Gen<unknown>>>(
  shape: T,
): Gen<{ [K in keyof T]: T[K] extends Gen<infer A> ? A : never }> {
  const keys = Object.keys(shape);
  return mk(
    (() => {
      const collect = (i: number, acc: any): Eff<any, never> => {
        if (i >= keys.length) return sync(() => acc) as any;
        const k = keys[i]!;
        return new Suspend(Op.FlatMap, shape[k]!.generate, (v: any) => {
          acc[k] = v;
          return collect(i + 1, acc);
        }) as any;
      };
      return collect(0, {});
    })(),
  );
}

function oneOfGen<A>(...gens: Gen<A>[]): Gen<A> {
  if (gens.length === 0) throw new Error("Gen.oneOf: at least one generator required");
  return mk(
    new Suspend(
      Op.FlatMap,
      Random.get as any,
      (r: Random) =>
        new Suspend(Op.FlatMap, r.nextInt(gens.length) as any, (i: number) => gens[i]!.generate),
    ) as any,
  );
}

function constantGen<A>(value: A): Gen<A> {
  return mk(sync(() => value) as any);
}

function mapGen<A, B>(g: Gen<A>, f: (a: A) => B): Gen<B> {
  return mk(new Suspend(Op.FlatMap, g.generate, (a: A) => sync(() => f(a))) as any);
}

function flatMapGen<A, B>(g: Gen<A>, f: (a: A) => Gen<B>): Gen<B> {
  return mk(new Suspend(Op.FlatMap, g.generate, (a: A) => f(a).generate) as any);
}

function tupleGen<A extends readonly Gen<unknown>[]>(
  ...gens: A
): Gen<{ [K in keyof A]: A[K] extends Gen<infer V> ? V : never }> {
  return mk(
    (() => {
      const collect = (i: number, acc: any[]): Eff<any[], never> => {
        if (i >= gens.length) return sync(() => acc) as any;
        return new Suspend(Op.FlatMap, gens[i]!.generate, (v: any) => {
          acc.push(v);
          return collect(i + 1, acc);
        }) as any;
      };
      return collect(0, []);
    })(),
  );
}

export const Gen = {
  int: intGen,
  bool: boolGen,
  char: charGen,
  string: stringGen,
  choice: choiceGen,
  array: arrayGen,
  object: objectGen,
  tuple: tupleGen,
  oneOf: oneOfGen,
  constant: constantGen,
  map: mapGen,
  flatMap: flatMapGen,
} as const;

// ── forAll ─────────────────────────────────────────────────────────

export interface PropertyFailure<A> {
  readonly value: A;
  readonly attempt: number;
  readonly reason: string;
}

/**
 * Run `predicate` against `count` randomly-generated values.
 * Fails (typed) with a PropertyFailure on the first counterexample.
 *
 * `predicate` may return:
 *   - boolean (true = pass, false = fail)
 *   - an Eff<boolean, never> for stateful checks
 *
 * No shrinking — the failing value is reported as generated. For richer
 * property-based testing, integrate with fast-check or similar later.
 */
export function forAll<A>(
  gen: Gen<A>,
  count: number,
  predicate: (value: A) => boolean | Eff<boolean, never>,
): Eff<void, Throws<PropertyFailure<A>>> {
  const step = (attempt: number): Eff<void, Throws<PropertyFailure<A>>> => {
    if (attempt > count) return sync(() => undefined) as any;
    return new Suspend(Op.FlatMap, gen.generate, (value: A) => {
      const result = predicate(value);
      const boolEff: Eff<boolean, never> =
        typeof result === "boolean" ? (sync(() => result) as any) : (result as Eff<boolean, never>);
      return new Suspend(Op.FlatMap, boolEff, (ok: boolean) => {
        if (!ok) {
          return fail({
            value,
            attempt,
            reason: `property failed for ${attemptStringify(value)}`,
          } as PropertyFailure<A>);
        }
        return step(attempt + 1);
      });
    }) as any;
  };
  return step(1);
}

function attemptStringify(v: unknown): string {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
