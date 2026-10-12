import { type Eff, Suspend, Op } from "../eff.js";
import { succeed } from "../constructors.js";

declare module "../eff.js" {
  interface Suspend {
    flatMap<A, S1, B, S2>(this: Eff<A, S1>, f: (a: A) => Eff<B, S2>): Eff<B, S1 | S2>;

    tap<A, S1, S2>(this: Eff<A, S1>, f: (a: A) => Eff<unknown, S2>): Eff<A, S1 | S2>;

    flatten<A, S1, S2>(this: Eff<Eff<A, S2>, S1>): Eff<A, S1 | S2>;
  }
}

Suspend.prototype.flatMap = function (this: Suspend, f: (a: unknown) => Suspend) {
  return new Suspend(Op.FlatMap, this, f) as Eff<unknown, unknown>;
} as Suspend["flatMap"];

Suspend.prototype.tap = function (this: Suspend, f: (a: unknown) => Suspend) {
  return new Suspend(
    Op.FlatMap,
    this,
    (a: unknown) => new Suspend(Op.FlatMap, f(a), () => succeed(a)),
  ) as Eff<unknown, unknown>;
} as Suspend["tap"];

Suspend.prototype.flatten = function (this: Suspend) {
  return new Suspend(Op.FlatMap, this, (inner: unknown) => inner) as Eff<unknown, unknown>;
} as Suspend["flatten"];
