import { type Eff, Suspend, Op } from "../eff"
import { succeed } from "../constructors"

declare module "../eff" {
  interface Suspend {
    zip<A, S1, B, S2>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
    ): Eff<[A, B], S1 | S2>

    zipWith<A, S1, B, S2, C>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
      f: (a: A, b: B) => C,
    ): Eff<C, S1 | S2>

    zipLeft<A, S1, S2>(
      this: Eff<A, S1>,
      that: Eff<any, S2>,
    ): Eff<A, S1 | S2>

    zipRight<A, S1, B, S2>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
    ): Eff<B, S1 | S2>
  }
}

Suspend.prototype.zip = function (that: any) {
  return new Suspend(Op.FlatMap, this, (a: any) =>
    new Suspend(Op.FlatMap, that, (b: any) =>
      succeed([a, b])
    )
  ) as any
}

Suspend.prototype.zipWith = function (that: any, f: any) {
  return new Suspend(Op.FlatMap, this, (a: any) =>
    new Suspend(Op.FlatMap, that, (b: any) =>
      succeed(f(a, b))
    )
  ) as any
}

Suspend.prototype.zipLeft = function (that: any) {
  return new Suspend(Op.FlatMap, this, (a: any) =>
    new Suspend(Op.FlatMap, that, () => succeed(a))
  ) as any
}

Suspend.prototype.zipRight = function (that: any) {
  return new Suspend(Op.FlatMap, this, () => that) as any
}
