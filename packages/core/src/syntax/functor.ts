import { type Eff, Suspend, Op } from "../eff.js";
import { succeed } from "../constructors.js";

declare module "../eff.js" {
  interface Suspend {
    map<A, B, S>(this: Eff<A, S>, f: (a: A) => B): Eff<B, S>;
    as<A, S, B>(this: Eff<A, S>, value: B): Eff<B, S>;
    asVoid<A, S>(this: Eff<A, S>): Eff<void, S>;
  }
}

Suspend.prototype.map = function (this: Suspend, f: (a: unknown) => unknown) {
  return new Suspend(Op.FlatMap, this, (a: unknown) => succeed(f(a))) as Eff<unknown, unknown>;
} as Suspend["map"];

Suspend.prototype.as = function (this: Suspend, value: unknown) {
  return new Suspend(Op.FlatMap, this, () => succeed(value)) as Eff<unknown, unknown>;
} as Suspend["as"];

Suspend.prototype.asVoid = function (this: Suspend) {
  return new Suspend(Op.FlatMap, this, () => succeed(undefined)) as Eff<unknown, unknown>;
} as Suspend["asVoid"];
