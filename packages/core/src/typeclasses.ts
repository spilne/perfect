// Value typeclasses — Eq / Ord / Show / Monoid. Ported from promin.
// Small structural contracts for generic code (sortBy, fold, dedupe-by-eq);
// perfect keeps them in one file until there's cross-type machinery that
// justifies a package.

import type { Codec } from "./connect/codec";

// ---------------------------------------------------------------------------
// Eq<T> — Equality typeclass
// "I can compare two T values"
// ---------------------------------------------------------------------------

export interface Eq<T> {
  equals(a: T, b: T): boolean;
}

/** Default: JSON deep equality via stringify comparison. */
export const JsonEq: Eq<unknown> = {
  equals: (a, b) => JSON.stringify(a) === JSON.stringify(b),
};

/** Derive Eq from a Codec (serialize both, compare serialized forms). */
export function eqFromCodec<T>(codec: Codec<T>): Eq<T> {
  return {
    equals: (a, b) => JSON.stringify(codec.encode(a)) === JSON.stringify(codec.encode(b)),
  };
}

// ---------------------------------------------------------------------------
// Ord<T> — Orderable typeclass
// "I can compare two T values for ordering"
// ---------------------------------------------------------------------------

export interface Ord<T> extends Eq<T> {
  compare(a: T, b: T): -1 | 0 | 1;
}

/** Ord for numbers. */
export const numberOrd: Ord<number> = {
  equals: (a, b) => a === b,
  compare: (a, b) => (a < b ? -1 : a > b ? 1 : 0),
};

/** Ord for strings (lexicographic). */
export const stringOrd: Ord<string> = {
  equals: (a, b) => a === b,
  compare: (a, b) => (a < b ? -1 : a > b ? 1 : 0),
};

/** Derive Ord from a key extractor function. */
export function ordBy<T>(toNumber: (value: T) => number): Ord<T> {
  return {
    equals: (a, b) => toNumber(a) === toNumber(b),
    compare: (a, b) => {
      const na = toNumber(a);
      const nb = toNumber(b);
      return na < nb ? -1 : na > nb ? 1 : 0;
    },
  };
}

// ---------------------------------------------------------------------------
// Show<T> — Displayable typeclass
// "I can produce a human-readable representation of T"
// ---------------------------------------------------------------------------

export interface Show<T> {
  show(value: T): string;
}

/** Default: truncated JSON representation. */
export const JsonShow: Show<unknown> = {
  show: (v) => JSON.stringify(v)?.slice(0, 200) ?? "undefined",
};

// ---------------------------------------------------------------------------
// Monoid<T> — Combinable typeclass
// "I can combine two T values and I have an identity element"
// ---------------------------------------------------------------------------

export interface Monoid<T> {
  readonly empty: T;
  concat(a: T, b: T): T;
}

/** Monoid for arrays — empty is [], concat is concatenation. */
export function arrayMonoid<T>(): Monoid<T[]> {
  return { empty: [], concat: (a, b) => [...a, ...b] };
}

/** Monoid for numbers under addition. */
export const sumMonoid: Monoid<number> = {
  empty: 0,
  concat: (a, b) => a + b,
};

/** Monoid for strings under concatenation. */
export const stringMonoid: Monoid<string> = {
  empty: "",
  concat: (a, b) => a + b,
};
