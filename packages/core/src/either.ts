import type { Throws } from "./eff.js";

export type Either<E, A> =
  | { readonly _tag: "Left"; readonly left: E }
  | { readonly _tag: "Right"; readonly right: A };

export const Either = {
  left: <E>(left: E): Either<E, never> => ({ _tag: "Left", left }),

  right: <A>(right: A): Either<never, A> => ({ _tag: "Right", right }),

  isLeft: <E, A>(e: Either<E, A>): e is { readonly _tag: "Left"; readonly left: E } =>
    e._tag === "Left",

  isRight: <E, A>(e: Either<E, A>): e is { readonly _tag: "Right"; readonly right: A } =>
    e._tag === "Right",
} as const;

/** S plus a typed failure E, leaving S untouched when E is never. */
export type WithError<S, E> = [E] extends [never] ? S : S | Throws<E>;
