import { type Eff, Suspend, Op } from "../eff";
import { succeed } from "../constructors";

declare module "../eff" {
  interface Suspend {
    flatMap<A, S1, B, S2>(this: Eff<A, S1>, f: (a: A) => Eff<B, S2>): Eff<B, S1 | S2>;

    tap<A, S1, S2>(this: Eff<A, S1>, f: (a: A) => Eff<unknown, S2>): Eff<A, S1 | S2>;

    flatten<A, S1, S2>(this: Eff<Eff<A, S2>, S1>): Eff<A, S1 | S2>;
  }
}

Suspend.prototype.flatMap = function (f: any) {
  return new Suspend(Op.FlatMap, this, f) as any;
};

Suspend.prototype.tap = function (f: any) {
  return new Suspend(
    Op.FlatMap,
    this,
    (a: any) => new Suspend(Op.FlatMap, f(a), () => succeed(a)),
  ) as any;
};

Suspend.prototype.flatten = function () {
  return new Suspend(Op.FlatMap, this, (inner: any) => inner) as any;
};
