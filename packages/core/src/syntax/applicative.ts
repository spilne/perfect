import { type Eff, Suspend, Op } from "../eff.js";
import { succeed } from "../constructors.js";

declare module "../eff.js" {
  interface Suspend {
    zip<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<[A, B], S1 | S2>;

    zipWith<A, S1, B, S2, C>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
      f: (a: A, b: B) => C,
    ): Eff<C, S1 | S2>;

    zipLeft<A, S1, S2>(this: Eff<A, S1>, that: Eff<unknown, S2>): Eff<A, S1 | S2>;

    zipRight<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<B, S1 | S2>;
  }
}

Suspend.prototype.zip = function (this: Suspend, that: Suspend) {
  return new Suspend(
    Op.FlatMap,
    this,
    (a: unknown) => new Suspend(Op.FlatMap, that, (b: unknown) => succeed([a, b])),
  ) as Eff<unknown, unknown>;
} as Suspend["zip"];

Suspend.prototype.zipWith = function (
  this: Suspend,
  that: Suspend,
  f: (a: unknown, b: unknown) => unknown,
) {
  return new Suspend(
    Op.FlatMap,
    this,
    (a: unknown) => new Suspend(Op.FlatMap, that, (b: unknown) => succeed(f(a, b))),
  ) as Eff<unknown, unknown>;
} as Suspend["zipWith"];

Suspend.prototype.zipLeft = function (this: Suspend, that: Suspend) {
  return new Suspend(
    Op.FlatMap,
    this,
    (a: unknown) => new Suspend(Op.FlatMap, that, () => succeed(a)),
  ) as Eff<unknown, unknown>;
} as Suspend["zipLeft"];

Suspend.prototype.zipRight = function (this: Suspend, that: Suspend) {
  return new Suspend(Op.FlatMap, this, () => that) as Eff<unknown, unknown>;
} as Suspend["zipRight"];
