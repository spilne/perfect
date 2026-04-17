import { type Eff, Suspend, Op } from "../eff";
import { succeed } from "../constructors";

declare module "../eff" {
  interface Suspend {
    map<A, B, S>(this: Eff<A, S>, f: (a: A) => B): Eff<B, S>;
    as<A, S, B>(this: Eff<A, S>, value: B): Eff<B, S>;
    asVoid<A, S>(this: Eff<A, S>): Eff<void, S>;
  }
}

Suspend.prototype.map = function (f: any) {
  return new Suspend(Op.FlatMap, this, (a: any) => succeed(f(a))) as any;
};

Suspend.prototype.as = function (value: any) {
  return new Suspend(Op.FlatMap, this, () => succeed(value)) as any;
};

Suspend.prototype.asVoid = function () {
  return new Suspend(Op.FlatMap, this, () => succeed(undefined)) as any;
};
