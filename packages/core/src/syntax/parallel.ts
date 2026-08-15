import { type Eff, Suspend, Op } from "../eff";

declare module "../eff" {
  interface Suspend {
    parZip<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<[A, B], S1 | S2>;

    parZipWith<A, S1, B, S2, C>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
      f: (a: A, b: B) => C,
    ): Eff<C, S1 | S2>;

    parMap<A, S1, B, S2, C>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
      f: (a: A, b: B) => C,
    ): Eff<C, S1 | S2>;

    parFlatMap<A, S1, B, S2, C, S3>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
      f: (a: A, b: B) => Eff<C, S3>,
    ): Eff<C, S1 | S2 | S3>;
  }
}

// parZip: run both in parallel, collect results
Suspend.prototype.parZip = function (that: any) {
  return new Suspend(Op.All, [this, that], null).map(([a, b]: any) => [a, b]) as any;
};

// parZipWith: run both in parallel, combine results
Suspend.prototype.parZipWith = function (that: any, f: any) {
  return new Suspend(Op.All, [this, that], null).map(([a, b]: any) => f(a, b)) as any;
};

// parMap: alias for parZipWith (Scala naming)
Suspend.prototype.parMap = Suspend.prototype.parZipWith;

// parFlatMap: run both in parallel, then flatMap the combined result
Suspend.prototype.parFlatMap = function (that: any, f: any) {
  return new Suspend(Op.FlatMap, new Suspend(Op.All, [this, that], null), ([a, b]: any) =>
    f(a, b),
  ) as any;
};
